/**
 * Copyright (c) 2019-2023 Alibaba Damo DBLab. All rights reserved.
 *
 * @desc: implementation for enc rule mgmts.
 */
#include "encdb_rules.h"
#include <algorithm>
#include <mutex>
#include <sstream>
#include <vector>
#include "../../common/include/encdb_constants.h"
#include "db_operation.h"
#include "encdb_crypto_exception.h"
#include "encdb_crypto_provider.h"
#include "encdb_key_exception.h"
#include "encdb_rand.h"
#include "encdb_sens_rule.h"
#include "global_timer.h"
#include "helper.hpp"
#include "key_mgmt.h"
#include "logger.h"
#include "shared_lock.hpp"
#define RULE_CACHE_SIZE 10240
#define THD_RULE_CACHE_SIZE 30

// reference: https://yuque.alibaba-inc.com/aone871146/whuu0v/kuhhkspt29mz0wfo
#define RULE_VERSION_230830 20230830
#define RULE_VERSION_240131 20240131

#define SPREADED_RULE_NAME_PREFIX "encdb_spreaded_rule_"
#define SPREADED_RULE_NAME_SPLITTER \
  "."  // In mysql, database table and column names cannot contain "."

/*
 * This file provides main functions for Encdb Sensitive Rules management and
 * user authorization control. There are four main data structures:
 * user_accs_cache (for caching user's authorization)
 * user_expire_time_cache (for caching expire time of user's authorization)
 * rule_query_cache (for caching data's sensitive
 * category, used to accelerate rule matching) sensitive_data_rules (a in-memory
 * copy of sensitive rules speicified by user)
 *
 * For sensitive rules management, we make sure that sensitive_data_rules is
 * always consistant with table mysql.encdb_sensitive_rules. It serves as a
 * in-memory copy of the table. We also make sure rule_query_cache is consistent
 * with all rules in sensitive_data_rules.
 *
 * For user authorization control, we make sure user_accs_cache is a in-memory
 * copy of table mysql.encdb_auth_users.
 */

extern uint lower_case_table_names;
namespace encdb {

/** Default user Access Level. We don't define UserAccess here to avoid race
 * condition. Maps to UserAccess enum: 0=NO_ACCESS, 1=RESTRICTED_ACCESS,
 * 2=FULL_ACCESS. */
uint8_t default_user_access = 0;

// After a server reboot, is a reload operation ever done?
static bool rules_reloaded = false;
/*
 * Flush privilege hook is meant to trigger a reload rule operation on RO node
 * So we use this variable to avoid local hook being triggered
 */
static bool disable_local_flush_priv_hook = false;

struct EncSearchTupHash {
  template <typename T1, typename T2, typename T3>
  std::size_t operator()(const std::tuple<T1, T2, T3> &t) const {
    std::size_t h1 = std::hash<T1>()(std::get<0>(t));
    std::size_t h2 = std::hash<T2>()(std::get<1>(t));
    std::size_t h3 = std::hash<T3>()(std::get<2>(t));
    return h1 ^ (h2 << 1) ^ (h3 << 2);
  }
};

// A in-memory cache of sensitive rules
static uint8_t global_cache_version = 0;

static std::vector<EncdbSenstiveDataRule> sensitive_data_rules;
static SharedLock sensitive_data_rules_lock;

// Whenever user_accs_cache or rule_query_cache modified, we should update
// global_cache_version
static ConcurrentLimitedCache<std::string, UserAccess> user_accs_cache(
    RULE_CACHE_SIZE);
static ConcurrentLimitedCache<std::string, Timestamp> user_expire_time_cache(
    RULE_CACHE_SIZE);

static ConcurrentLimitedCache<EncdbSearchTup, bool, EncSearchTupHash>
    rule_query_cache(RULE_CACHE_SIZE);

// For read only
thread_local uint8_t local_cache_version = 0;
thread_local LimitedCache<std::string, UserAccess> thread_user_accs_cache(
    THD_RULE_CACHE_SIZE);
thread_local LimitedCache<EncdbSearchTup, bool, EncSearchTupHash>
    thread_rule_query_cache(THD_RULE_CACHE_SIZE);

inline void updateGlobalCache() {
  static std::mutex mut;
  const std::lock_guard<std::mutex> lock(mut);
  global_cache_version++;
}

inline void updateLocalCacheIfNecessary() {
  if (local_cache_version != global_cache_version) {
    thread_rule_query_cache.clear();
    rule_query_cache.clear();
    thread_user_accs_cache.clear();
    local_cache_version = global_cache_version;
  }
}

/*
 * Reload all rules and user authorizations from database storage.
 */
void reloadAllEncRules() {
  std::vector<std::vector<std::string>> ret_vec;

  hook::encdb_execute_sql_read(encdb_sql_load_enc_rules(), ret_vec);
  updateGlobalCache();

  {
    Writer w(sensitive_data_rules_lock);
    sensitive_data_rules.clear();
    for (const std::vector<std::string> &rule : ret_vec) {
      encdb::RuleType rt;
      try {
        rt = static_cast<encdb::RuleType>(stoi(rule[RULE_TYPE_IDX]));
      } catch (const std::invalid_argument &e) {
        // thrown by stoi
        throw InvalidValueReadFromDatabaseException(
            "read data type " + rule[RULE_TYPE_IDX] +
            " from database, it's clearly invalid");
      }
      sensitive_data_rules.push_back(encdb::EncdbSenstiveDataRule(
          rule[RULE_SENS_DBS_IDX], rule[RULE_SENS_TBLS_IDX],
          rule[RULE_SENS_COLS_IDX], rule[RULE_NAME_IDX],
          STR_TINYINT_TO_BOOL(rule[RULE_ENABLED_IDX]), rt,
          rule[RULE_DESCRIPTION_IDX],
          lower_case_table_names != 0));
    }
  }

  user_accs_cache.clear();
  user_expire_time_cache.clear();
  ret_vec.clear();
  hook::encdb_execute_sql_read(encdb_sql_load_users(), ret_vec);
  for (const std::vector<std::string> &info : ret_vec) {
    if (info[AUTH_TYPE_IDX].empty()) {
      continue;
    } else if (info[AUTH_TYPE_IDX].length() != 1 ||
               info[AUTH_TYPE_IDX][0] - '0' >
                   static_cast<int>(UserAccess::FULL_ACCESS)) {
      throw InvalidValueReadFromDatabaseException(
          "read " + info[AUTH_TYPE_IDX] + " as accs_type");
    }
    user_accs_cache.put(info[AUTH_USER_IDX],
                        static_cast<UserAccess>(info[AUTH_TYPE_IDX][0] - '0'));
    // DB DEFAULT ensures expire_time is always non-empty.
    user_expire_time_cache.put(info[AUTH_USER_IDX],
                               Timestamp(info[AUTH_EXPTIME_IDX], false));
  }

  updateGlobalCache();
}

inline void reloadRulesIfNecessary() {
  static std::mutex reload_mut;  // added to guarantee reload only happen once
  if (!rules_reloaded) {
    const std::lock_guard<std::mutex> lock_reload(reload_mut);
    if (rules_reloaded) {
      return;
    }
    bool need_reload = false;
    {
      Reader r(sensitive_data_rules_lock);
      need_reload = sensitive_data_rules.empty();
    }
    if (need_reload) {
      reloadAllEncRules();
    }
    rules_reloaded = true;
  }
}

inline void disableFlushPrivHook() { disable_local_flush_priv_hook = true; }

inline void enableFlushPrivHook() { disable_local_flush_priv_hook = false; }

void encdbFlushPrivileges() {
  static std::mutex mut;
  const std::lock_guard<std::mutex> lock(mut);
  disableFlushPrivHook();
  hook::encdb_execute_sql_write(encdb_sql_flush_privileges());
  enableFlushPrivHook();
}

/**
 * @brief Add or update rule, depending on whether there exists a rule with same
 * rule_name.
 * @param rule The rule to be added or update (if there exists a rule with same
 * name)
 * @param hook This rule changing behavior is triggered by a hook operation
 * @param tmp If tmp is false, this rule will be updated or added into
 * mysql.encdb_sensitive_rules
 */
void encdbAddOrUpdateEncRule(const EncdbSenstiveDataRule &rule,
                             bool hook = false, bool tmp = false) {
  std::string dbs, tbls, cols, algos, default_algo, endpoints, rule_name,
      description;
  bool enabled;
  int rule_type;
  updateGlobalCache();
  rule.dump(dbs, tbls, cols, algos, default_algo, endpoints, rule_name, enabled,
            rule_type, description);
  // When we import rules, always write to db before caching rule in memory,
  // since we rely on
  //   MySQL's user access control on database
  if (!tmp) {
    if (hook) {
      hook::encdb_execute_sql_write(encdb_sql_add_or_update_enc_rule(
          dbs, tbls, cols, algos, default_algo, endpoints, rule_name, enabled,
          rule_type, description));
    } else {
      udf::encdb_execute_sql_write(encdb_sql_add_or_update_enc_rule(
          dbs, tbls, cols, algos, default_algo, endpoints, rule_name, enabled,
          rule_type, description));
    }
  }
  encdbFlushPrivileges();
  Writer w(sensitive_data_rules_lock);
  for (auto &iter : sensitive_data_rules) {
    if (iter.getRuleName() == rule.getRuleName()) {
      iter = rule;  // repalce
      return;
    }
  }
  sensitive_data_rules.push_back(rule);
}

void encdbDeleteEncRule(const std::string &rule_name, bool hook = false,
                        bool tmp = false) {
  updateGlobalCache();
  if (!tmp) {
    if (hook) {
      hook::encdb_execute_sql_write(encdb_sql_delete_enc_rule(rule_name));
    } else {
      udf::encdb_execute_sql_write(encdb_sql_delete_enc_rule(rule_name));
    }
  }
  encdbFlushPrivileges();
  Writer w(sensitive_data_rules_lock);
  for (int i = 0, size = sensitive_data_rules.size(); i < size; i++) {
    if (sensitive_data_rules[i].getRuleName() == rule_name) {
      sensitive_data_rules.erase(sensitive_data_rules.begin() + i);
      break;
    }
  }
}

namespace udf {

const bool is_hook = false;

void setUserAccs(const std::string &name, UserAccess accs,
                 const std::string &t) {
  Timestamp timestamp(t, true);
  udf::encdb_execute_sql_write(
      encdb_sql_set_accs_type(name, static_cast<int>(accs), t));
  encdbFlushPrivileges();
  user_accs_cache.put(name, accs);
  user_expire_time_cache.put(name, timestamp);
  updateGlobalCache();
}

// Clear all rules from memory and database
void encdbClearAllEncRules() {
  encdb_execute_sql_write(encdb_sql_clear_enc_rules());
  encdbFlushPrivileges();
  {
    Writer w(sensitive_data_rules_lock);
    sensitive_data_rules.clear();
  }
  updateGlobalCache();
}

void encdbClearAuthUsers() {
  encdb_execute_sql_write(encdb_sql_clear_users());
  encdbFlushPrivileges();
  user_accs_cache.clear();
  user_expire_time_cache.clear();
  updateGlobalCache();
}

/**
 * @brief Dump all rules stored in memory, to help debugging.
 */
std::string encdbExportEncRules() {
  reloadRulesIfNecessary();
  json::JSON ret_obj = json::Array();
  {
    Writer r(sensitive_data_rules_lock);
    std::sort(sensitive_data_rules.begin(), sensitive_data_rules.end(),
              EncdbSenstiveDataRule::compareName);
    for (const auto &rule : sensitive_data_rules) {
      ret_obj.append(rule.toJson());
    }
  }
  return ret_obj.dump();
}

typedef std::pair<std::string, UserAccess> StrNAccsPair;
static bool StrNAccsPairComparartor(const StrNAccsPair &s1,
                                    const StrNAccsPair &s2) {
  return s1.first.compare(s2.first) < 0;
}

std::string encdbExportUserAuths() {
  reloadRulesIfNecessary();
  // user_accs_cache serves as a in-memory copy of table mysql.encdb_auth_user
  // however, this cache has a limited size. So if it was once fullfilled, the
  // in-memory copy can be only a subset of whole table. Thus we need to read
  // table directly.
  if (user_accs_cache.hasBeenFull()) {
    std::vector<std::vector<std::string>> ret_vec;
    json::JSON ret_obj = json::Array();
    hook::encdb_execute_sql_read(encdb_sql_load_users(), ret_vec);
    for (const std::vector<std::string> &info : ret_vec) {
      if (info[AUTH_TYPE_IDX].empty()) {
        continue;
      } else if (info[AUTH_TYPE_IDX].length() != 1 ||
                 info[AUTH_TYPE_IDX][0] - '0' >
                     static_cast<int>(UserAccess::FULL_ACCESS)) {
        throw InvalidValueReadFromDatabaseException(
            "read " + info[AUTH_TYPE_IDX] + " as accs_type");
      }
      // Filter out expired users — their records are kept in DB but
      // should not appear in the exported list.
      Timestamp expire_time(info[AUTH_EXPTIME_IDX], false);
      Timestamp now(Timer::Now(), false);
      if (now > expire_time) {
        continue;
      }
      json::JSON ua_obj;
      ua_obj[JSON_UNAME] = info[AUTH_USER_IDX];
      ua_obj[JSON_UACCS] = userAccessToStr(
          static_cast<UserAccess>(info[AUTH_TYPE_IDX][0] - '0'));
      ua_obj[JSON_EXPIRE] = info[AUTH_EXPTIME_IDX];
      ret_obj.append(ua_obj);
    }
    return ret_obj.dump();
  } else {
    std::vector<std::pair<std::string, UserAccess>> user_accs_v =
        user_accs_cache.dump();
    std::sort(user_accs_v.begin(), user_accs_v.end(), StrNAccsPairComparartor);
    json::JSON ret_obj = json::Array();
    for (auto &p : user_accs_v) {
      json::JSON ua_obj;
      ua_obj[JSON_UNAME] = p.first;
      ua_obj[JSON_UACCS] = userAccessToStr(p.second);
      try {
        Timestamp t = user_expire_time_cache.get(p.first);
        Timestamp now(Timer::Now(), false);
        if (now > t) {
          continue;  // Filter out expired users
        }
        ua_obj[JSON_EXPIRE] = t.to_string();
      } catch (const CacheMissException &e) {
        continue;  // expire_time must exist; skip if data incomplete
      }
      ret_obj.append(ua_obj);
    }
    return ret_obj.dump();
  }
}

void encdbAddOrUpdateEncRules(const std::string &rule_json_strs) {
  reloadRulesIfNecessary();
  json::JSON rule_objs = json::JSON::Load(rule_json_strs);
  if (rule_objs.length() == -1) {
    // single obj
    EncdbSenstiveDataRule rule(rule_objs, lower_case_table_names != 0);
    if (rule.getRuleName().find(SPREADED_RULE_NAME_PREFIX) == 0) {
      throw InvalidJsonDataException(
          "Encryption rules cannot start with name '" SPREADED_RULE_NAME_PREFIX
          "'");
    }
    encdbAddOrUpdateEncRule(rule, is_hook, false);
  } else {
    // array
    for (int i = 0, size = rule_objs.length(); i < size; i++) {
      EncdbSenstiveDataRule rule(rule_objs[i], lower_case_table_names != 0);
      if (rule.getRuleName().find(SPREADED_RULE_NAME_PREFIX) == 0) {
        throw InvalidJsonDataException(
            "Encryption rules cannot start with name "
            "'" SPREADED_RULE_NAME_PREFIX "'");
      }
      encdbAddOrUpdateEncRule(rule, is_hook, false);
    }
  }
}

void encdbDeleteEncRules(const std::string &enc_rule_names) {
  reloadRulesIfNecessary();
  json::JSON rule_name_objs = json::JSON::Load(enc_rule_names);
  if (rule_name_objs.length() == -1) {
    encdbDeleteEncRule(rule_name_objs.ToString());
  } else {
    for (int i = 0, size = rule_name_objs.length(); i < size; i++) {
      encdbDeleteEncRule(rule_name_objs[i].ToString());
    }
  }
}

// param:
// option1:
// {"noneAccess":["yyy"],"restrictedAccess":["xxx"],"fullAccess":["zzz"]}
// option2: [{"username": "xxx", "access": "RESTRICTED_ACCESS"}]
void encdbSetUserAuths(const std::string &param) {
  json::JSON user_accs_obj = json::JSON::Load(param);
  if (user_accs_obj.length() >= 0) {
    for (int i = 0, size = user_accs_obj.length(); i < size; i++) {
      json::JSON ua_obj = user_accs_obj[i];
      ua_obj.checkField(JSON_UACCS);
      ua_obj.checkField(JSON_UNAME);
      UserAccess ua = strToUserAccess(ua_obj[JSON_UACCS].ToString());
      std::string uname = ua_obj[JSON_UNAME].ToString();

      std::string expired = ua_obj.hasKey(JSON_EXPIRE)
                                ? ua_obj[JSON_EXPIRE].ToString()
                                : MYSQL_MAX_TIME.to_string();

      setUserAccs(uname, ua, expired);
    }
  } else {
    if (user_accs_obj.hasKey(JSON_USRS_FULL)) {
      json::JSON full_accs_users = user_accs_obj[JSON_USRS_FULL];
      std::string expire_time = user_accs_obj.hasKey(JSON_EXPIRE)
                                    ? user_accs_obj[JSON_EXPIRE].ToString()
                                    : MYSQL_MAX_TIME.to_string();
      for (int i = 0; i < full_accs_users.size(); i++) {
        setUserAccs(full_accs_users[i].ToString(), UserAccess::FULL_ACCESS,
                    expire_time);
      }
    }
    if (user_accs_obj.hasKey(JSON_USRS_RESTRICTED)) {
      json::JSON restricted_accs_users = user_accs_obj[JSON_USRS_RESTRICTED];
      std::string expire_time_r = user_accs_obj.hasKey(JSON_EXPIRE)
                                      ? user_accs_obj[JSON_EXPIRE].ToString()
                                      : MYSQL_MAX_TIME.to_string();
      for (int i = 0; i < restricted_accs_users.size(); i++) {
        setUserAccs(restricted_accs_users[i].ToString(),
                    UserAccess::RESTRICTED_ACCESS, expire_time_r);
      }
    }
    if (user_accs_obj.hasKey(JSON_USRS_NOACCESS)) {
      json::JSON none_accs_users = user_accs_obj[JSON_USRS_NOACCESS];
      std::string expire_time_n = user_accs_obj.hasKey(JSON_EXPIRE)
                                      ? user_accs_obj[JSON_EXPIRE].ToString()
                                      : MYSQL_MAX_TIME.to_string();
      for (int i = 0; i < none_accs_users.size(); i++) {
        setUserAccs(none_accs_users[i].ToString(), UserAccess::NO_ACCESS,
                    expire_time_n);
      }
    }
  }
}

void encdbSetDefaultUserAuths(const std::string &rights) {
  if (rights == "noneAccess") {
    default_user_access = UserAccess::NO_ACCESS;
  } else if (rights == "restrictedAccess") {
    default_user_access = UserAccess::RESTRICTED_ACCESS;
  } else if (rights == "fullAccess") {
    default_user_access = UserAccess::FULL_ACCESS;
  }
  updateGlobalCache();
}

}  // namespace udf

namespace hook {

const bool is_hook = true;
/**
 * @brief For rules that are generated by rule spreading mechanism, we must use
 * these functions to generate its rule name. For rules that are defined by
 * users, we must ensure that they cannot use same rule name format.
 */
inline std::string generateType2RuleName(const std::string &db,
                                         const std::string &tbl,
                                         const std::string &col) {
  std::string ret = SPREADED_RULE_NAME_PREFIX;
  ret += db + SPREADED_RULE_NAME_SPLITTER + tbl + SPREADED_RULE_NAME_SPLITTER +
         col;
  return ret;
}
inline std::string generateType1RuleName(const std::string &db,
                                         const std::string &tbl) {
  std::string ret = SPREADED_RULE_NAME_PREFIX;
  ret += db + SPREADED_RULE_NAME_SPLITTER + tbl;
  return ret;
}

bool needEncryption(const std::string &db, const std::string &tbl,
                    const std::string &col) {
  reloadRulesIfNecessary();
  updateLocalCacheIfNecessary();
  EncdbSearchTup search_tup = encdb_generate_search_tup(db, tbl, col);
  bool ret;
  try {
    ret = thread_rule_query_cache.get(search_tup);
    return ret;
  } catch (const CacheMissException &e) {
    try {
      ret = rule_query_cache.get(search_tup);
      thread_rule_query_cache.put(search_tup, ret);
      return ret;
    } catch (const CacheMissException &e) {
      {
        Reader r(sensitive_data_rules_lock);
        for (const auto &rule : sensitive_data_rules) {
          if (rule.isEnabled() && rule.isDataSensitive(db, tbl, col)) {
            rule_query_cache.put(search_tup, true);
            thread_rule_query_cache.put(search_tup, true);
            return true;
          }
        }
      }
      rule_query_cache.put(search_tup, false);
      thread_rule_query_cache.put(search_tup, false);
      return false;
    }
  }
}

UserAccess encdbGetUserAccess(const std::string &uname) {
#if ENCDB_DB_TYPE == POLARDB_MYSQL
  return UserAccess::RESTRICTED_ACCESS;
#elif ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB
  reloadRulesIfNecessary();
  updateLocalCacheIfNecessary();
  UserAccess ua;
  UserAccess default_ua = static_cast<UserAccess>(default_user_access);

  /** 1. Try to find specified user access. */
  try {
    ua = thread_user_accs_cache.get(uname);
  } catch (const CacheMissException &e) {
    try {
      ua = user_accs_cache.get(uname);
    } catch (const CacheMissException &e) {
      // try to read from database system
      std::vector<std::vector<std::string>> ret_vec;
      encdb_execute_sql_read(encdb_sql_query_user_info(uname), ret_vec);
      if (!ret_vec.empty() && !ret_vec[0].empty()) {
        std::string accs_type = ret_vec[0][AUTH_TYPE_IDX];
        if (accs_type.empty()) {
          ua = default_ua;
        } else if (accs_type.length() != 1 ||
                   accs_type[0] - '0' >
                       static_cast<int>(UserAccess::FULL_ACCESS)) {
          throw InvalidValueReadFromDatabaseException("read " + accs_type +
                                                      " as accs_type");
        } else {
          user_accs_cache.put(uname,
                              static_cast<UserAccess>(accs_type[0] - '0'));
          ua = static_cast<UserAccess>(accs_type[0] - '0');
          user_expire_time_cache.put(
              uname, Timestamp(ret_vec[0][AUTH_EXPTIME_IDX], false));
        }
      } else {
        ua = default_ua;
      }
    }
  }

  /** 2. Check the expire time. */
  try {
    Timestamp expire = user_expire_time_cache.get(uname);
    Timestamp now(Timer::Now(), false);
    if (now > expire) {
      ua = default_ua;
    }
  } catch (const CacheMissException &e) {
    /** No explicit expiry entry means user got access from default. Do
     * nothing. */
  }
  thread_user_accs_cache.put(uname, ua);
  return ua;
#endif
}

bool encdbNeedEncryption(const std::string &db, const std::string &tbl,
                         const std::string &col) {
  return needEncryption(db, tbl, col);
}

void encdbRuleSpread(const std::string &src_db, const std::string &src_tbl,
                     const std::string &src_col, const std::string &dst_db,
                     const std::string &dst_tbl, const std::string &dst_col,
                     bool rename, bool tmp) {
  reloadRulesIfNecessary();
  updateGlobalCache();
  std::vector<std::string> dbs, tbls, tmp_cols, cols, algos, endpoints;
  std::string default_alg;
  std::string src_rule_type_1 = generateType1RuleName(src_db, src_tbl);
  std::string src_rule_type_2 = generateType2RuleName(src_db, src_tbl, src_col);
  std::string dst_rule_type_1 = generateType1RuleName(dst_db, dst_tbl);
  std::string dst_rule_type_2 = generateType2RuleName(dst_db, dst_tbl, dst_col);

  RuleType dst_rule_type =
      dst_col.empty() ? RuleType::TABLE_ORIENTED : RuleType::COLUMN_ORIENTED;
  std::string dst_rule_name =
      dst_col.empty() ? dst_rule_type_1 : dst_rule_type_2;
  EncdbSenstiveDataRule new_rule(dst_rule_name, dst_rule_type);
  std::vector<std::string> rules_2_del;

  // Check for existing dst_rule_name
  {
    Reader r(sensitive_data_rules_lock);
    for (auto &rule : sensitive_data_rules) {
      if (rule.isEnabled() && rule.getRuleName() == dst_rule_name) {
        new_rule.mergeColsWith(rule.getCols());
      }
    }
  }

  // This part modifies existing rules which are generated by rule spreading.
  // Those rules speicified by user are never modified.
  if (rename) {
    // rename if exists
    if (dst_rule_type == RuleType::COLUMN_ORIENTED) {
      {
        Reader r(sensitive_data_rules_lock);
        for (auto &rule : sensitive_data_rules) {
          if (rule.isEnabled() && rule.getRuleName() == src_rule_type_2) {
            new_rule.mergeColsWith(rule.getCols());
          }
        }
      }
      rules_2_del.push_back(src_rule_type_2);
    } else {
      Reader r(sensitive_data_rules_lock);
      for (auto &rule : sensitive_data_rules) {
        if (rule.isEnabled() && rule.getRuleName().find(generateType1RuleName(
                                    src_db, src_tbl)) == 0) {
          new_rule.mergeColsWith(rule.getCols());
          rules_2_del.push_back(rule.getRuleName());
        }
      }
    }
  }
  for (const auto &rname : rules_2_del) {
    encdbDeleteEncRule(rname, true, tmp);
  }

  // A special case: if we are dealing with table rename, and <src_db, src_tbl>
  // appears in multiple rules, how should we deal with it? Here for each rule
  // that covers <src_db, src_tbl>, we extract the cols field, and generate a
  // union set for columns
  if (dst_rule_type == RuleType::TABLE_ORIENTED) {
    Reader r(sensitive_data_rules_lock);
    for (const auto &rule : sensitive_data_rules) {
      if (rule.isEnabled() && rule.isDataSensitive(src_db, src_tbl, src_col)) {
        tmp_cols = vector_union(tmp_cols, rule.getCols());
      }
    }
  }
  dbs.push_back(dst_db);
  tbls.push_back(dst_tbl);
  if (!dst_col.empty()) {
    tmp_cols.push_back(dst_col);
  }
  for (const std::string &col : tmp_cols) {
    if (!needEncryption(dst_db, dst_tbl, col)) {
      cols.push_back(col);
    }
  }
  new_rule.mergeColsWith(cols);
  new_rule.setDbs(dbs);
  new_rule.setTbls(tbls);
  encdbAddOrUpdateEncRule(new_rule, is_hook, tmp);
}

int encdbRuleSpreadInsertOrCreateFrom(
    const std::string &src_db, const std::string &src_tbl,
    const std::string &src_col, const std::string &dst_db,
    const std::string &dst_tbl, const std::string &dst_col, bool dst_tmp) {
  if (needEncryption(src_db, src_tbl, src_col) &&
      !needEncryption(dst_db, dst_tbl, dst_col)) {
    encdbRuleSpread(src_db, src_tbl, src_col, dst_db, dst_tbl, dst_col, false,
                    dst_tmp);
  }
  return 0;
}

int encdbRuleSpreadRenameTable(const std::string &src_db,
                               const std::string &src_tbl,
                               const std::string &dst_db,
                               const std::string &dst_tbl, bool tmp) {
  if (needEncryption(src_db, src_tbl, "")) {
    encdbRuleSpread(src_db, src_tbl, "", dst_db, dst_tbl, "", true, tmp);
  }
  return 0;
}

int encdbRuleSpreadRenameColumn(const std::string &src_db,
                                const std::string &src_tbl,
                                const std::string &src_col,
                                const std::string &dst_col, bool tmp) {
  if (needEncryption(src_db, src_tbl, src_col)) {
    encdbRuleSpread(src_db, src_tbl, src_col, src_db, src_tbl, dst_col, true,
                    tmp);
  }
  return 0;
}

// This function should be called outside this module by mysql flush privileges
// hook
void encdbReloadAllEncRules() {
  if (!disable_local_flush_priv_hook) {
    reloadAllEncRules();
  }
}

}  // namespace hook

}  // namespace encdb
