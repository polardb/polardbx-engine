/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#include "db_operation.h"
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include "encdb_common_exception.h"
#include "encdb_constants.h"
#include "encdb_error.h"
#include "helper.hpp"
#include "json.hpp"
#include "logger.h"
#include "storage.h"

namespace encdb {

namespace hook {

void encdb_execute_sql_read(const std::string &sql, std::string &ret) {
  std::vector<std::vector<std::string>> ret_vec;
  encdb_execute_sql_read(sql, ret_vec);
  if (ret_vec.size() == 0 || ret_vec[0].size() == 0) {
    ret = "";
  } else {
    ret = ret_vec[0][0];
  }
}
void encdb_execute_sql_read(const std::string &sql,
                            std::vector<std::vector<std::string>> &ret) {
  int err;
  if ((err = encdb_plugin_exec_sql(sql, ret)) != 0) {
    ENCDB_LOG(LL_ERROR, "failed to execute %s with error code %d", sql.c_str(),
              err);
    throw EncdbInternalSQLException("failed to execute \"" + sql +
                                    "\" with error code " +
                                    std::to_string(err));
  }
}
void encdb_execute_sql_write(const std::string &sql) {
  int err;
  if ((err = encdb_plugin_exec_sql(sql, false)) != 0) {
    ENCDB_LOG(LL_ERROR, "failed to execute %s with error code %d", sql.c_str(),
              err);
    throw EncdbInternalSQLException("failed to execute \"" + sql +
                                    "\" with error code " +
                                    std::to_string(err));
  }
}

}  // namespace hook

namespace udf {

void encdb_execute_sql_read(const std::string &sql, std::string &ret) {
  std::vector<std::vector<std::string>> ret_vec;
  encdb_execute_sql_read(sql, ret_vec);
  if (ret_vec.size() == 0 || ret_vec[0].size() == 0) {
    ret = "";
  } else {
    ret = ret_vec[0][0];
  }
}
void encdb_execute_sql_read(const std::string &sql,
                            std::vector<std::vector<std::string>> &ret) {
  int err;
  if ((err = encdb_plugin_exec_sql(sql, ret)) != 0) {
    ENCDB_LOG(LL_ERROR, "failed to execute %s with error code %d", sql.c_str(),
              err);
    throw EncdbInternalSQLException("failed to execute \"" + sql +
                                    "\" with error code " +
                                    std::to_string(err));
  }
}
void encdb_execute_sql_write(const std::string &sql) {
  int err;
  if ((err = encdb_plugin_exec_sql(sql, true)) != 0) {
    ENCDB_LOG(LL_ERROR, "failed to execute %s with error code %d", sql.c_str(),
              err);
    throw EncdbInternalSQLException("failed to execute \"" + sql +
                                    "\" with error code " +
                                    std::to_string(err));
  }
}

}  // namespace udf

std::string escape_string_mysql(std::string str) {
  std::string esc("'\\");
  for (std::string::iterator it = esc.begin(); it != esc.end(); ++it) {
    std::string::size_type idx = 0;
    while ((idx = str.find(*it, idx)) != std::string::npos) {
      str.insert(idx, 1, '\\');
      idx += 2;
    }
  }
  esc.assign("\n");
  for (std::string::iterator it = esc.begin(); it != esc.end(); ++it) {
    std::string::size_type idx = 0;
    while ((idx = str.find(*it, idx)) != std::string::npos) {
      str[idx] = 'n';
      str.insert(idx, 1, '\\');
      idx += 2;
    }
  }
  return str;
}

/*
 * AUTH_USER_TABLE definition:
 * CREATE TABLE mysql.encdb_auth_users (user TEXT(64) NOT NULL, salt_mac TEXT,
 * accs_type INT, expire_time DATETIME DEFAULT '9999-12-31 23:59:59', PRIMARY KEY(user(64)));
 * SENS_RULES_TABLE defnition:
 * CREATE TABLE mysql.encdb_sensitive_rules (sensitive_databases TEXT,
 * sensitive_tables TEXT, sensitive_columns TEXT, algos TEXT, default_algo TEXT,
 * endpoints TEXT, rule_name TEXT(512), enabled bool, rule_type INT,
 * description TEXT NOT NULL DEFAULT '', PRIMARY KEY(rule_name(512)));
 */
std::string encdb_sql_clear_enc_rules() {
  std::string sql = "DELETE FROM " SENS_RULES_TABLE;
  return sql;
}

inline const std::string escape_string(const std::string &str) {
  return escape_string_mysql(str);
}

std::string encdb_sql_add_or_update_enc_rule(
    const std::string &dbs, const std::string &tbls, const std::string &cols,
    const std::string &algos, const std::string &default_algo,
    const std::string &endpoints, const std::string &rule_name, bool enabled,
    int rule_type, const std::string &description,
    const std::string &old_rule_name) {
  const char *sql_template =
      "INSERT INTO " SENS_RULES_TABLE " (" SENS_RULE_FIELDS_FOR_WRITE
      ") VALUES ('%s','%s','%s','%s','%s','%s','%s',%s,%d,'%s') "
      "ON DUPLICATE KEY UPDATE " SENSITIVE_DBS "='%s', " SENSITIVE_TBLS
      "='%s', " SENSITIVE_COLS "='%s', " ALGOS "='%s', " DEFAULT_ALGO
      "='%s', " ENDPOINTS "='%s', " RULE_NAME "='%s', " ENABLED "=%s, "
      RULE_DESCRIPTION "='%s'";
  std::string sql = Utils::prepare_format_string(
      sql_template, escape_string(dbs).c_str(), escape_string(tbls).c_str(),
      escape_string(cols).c_str(), escape_string(algos).c_str(),
      escape_string(default_algo).c_str(), escape_string(endpoints).c_str(),
      escape_string(old_rule_name.empty() ? rule_name : old_rule_name).c_str(),
      BOOL_TO_STRING(enabled), rule_type, escape_string(description).c_str(),
      escape_string(dbs).c_str(), escape_string(tbls).c_str(),
      escape_string(cols).c_str(), escape_string(algos).c_str(),
      escape_string(default_algo).c_str(), escape_string(endpoints).c_str(),
      escape_string(rule_name).c_str(), BOOL_TO_STRING(enabled),
      escape_string(description).c_str());
  return sql;
}

std::string encdb_sql_delete_enc_rule(const std::string &rule_name) {
  const char *sql_template =
      "DELETE FROM " SENS_RULES_TABLE " WHERE " RULE_NAME "='%s'";
  std::string sql = Utils::prepare_format_string(
      sql_template, escape_string(rule_name).c_str());
  return sql;
}

std::string encdb_sql_load_enc_rules() {
  std::string sql =
      "SELECT " SENS_RULE_FIELDS_FOR_READ " FROM " SENS_RULES_TABLE;
  return sql;
}

std::string encdb_sql_get_saltmac(const std::string &user) {
  const char *sql_template =
      "SELECT salt_mac FROM " AUTH_USER_TABLE " WHERE user = '%s'";
  std::string sql =
      Utils::prepare_format_string(sql_template, escape_string(user).c_str());
  return sql;
}
std::string encdb_sql_set_saltmac(const std::string &user,
                                  const std::string &saltmac) {
  const char *sql_template =
      "INSERT INTO " AUTH_USER_TABLE " (" USER "," SALT_MAC
      ") VALUES ('%s', '%s')"
      " ON DUPLICATE KEY UPDATE salt_mac = '%s'";
  std::string sql = Utils::prepare_format_string(
      sql_template, escape_string(user).c_str(), escape_string(saltmac).c_str(),
      escape_string(saltmac).c_str());
  return sql;
}

std::string encdb_sql_read_sys_var(const std::string &var_name) {
  std::string sql = "SELECT CAST(@@" + var_name + " AS CHAR)";
  return sql;
}

std::string encdb_sql_flush_privileges() {
  std::string sql = "FLUSH PRIVILEGES";
  return sql;
}

std::string encdb_sql_set_accs_type(const std::string &user, int accs) {
  const char *sql_template =
      "INSERT INTO " AUTH_USER_TABLE " (" USER "," SALT_MAC "," ACCS_TYPE
      ") VALUES ('%s', '', %d)"
      " ON DUPLICATE KEY UPDATE " ACCS_TYPE " = %d";
  std::string sql = Utils::prepare_format_string(
      sql_template, escape_string(user).c_str(), accs, accs);
  return sql;
}

std::string encdb_sql_set_accs_type(const std::string &user, int accs,
                                    const std::string &time) {
  const char *sql_template =
      "INSERT INTO " AUTH_USER_TABLE
      " VALUES ('%s', '', %d, '%s') ON DUPLICATE KEY UPDATE " ACCS_TYPE
      " = %d, " EXPIRE_TIME " = '%s'";
  std::string sql =
      Utils::prepare_format_string(sql_template, escape_string(user).c_str(),
                                   accs, time.c_str(), accs, time.c_str());
  return sql;
}

std::string encdb_sql_load_users() {
  std::string sql =
      "SELECT " AUTH_USERS_FIELDS_FOR_READ " FROM " AUTH_USER_TABLE;
  return sql;
}

std::string encdb_sql_clear_users() {
  std::string sql = "DELETE FROM " AUTH_USER_TABLE;
  return sql;
}

std::string encdb_sql_query_user_info(const std::string &user) {
  const char *sql_template =
      "SELECT " AUTH_USERS_FIELDS_FOR_READ " FROM " AUTH_USER_TABLE
      " WHERE " USER "= '%s'";
  std::string sql =
      Utils::prepare_format_string(sql_template, escape_string(user).c_str());
  return sql;
}

std::string encdb_sql_read_sys_time() {
  std::string sql = "SELECT CAST(NOW() AS CHAR)";
  return sql;
}

}  // namespace encdb
