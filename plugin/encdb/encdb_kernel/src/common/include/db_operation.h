/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once

#include <cstdint>
#include <string>
#include "EncDBConfig.h"
#include "concurrent_limited_cache.hpp"
#include "encdb_error.h"  // encdb_status_t
#include "helper.hpp"     // Utils

#define MAKE_CHAR(x) "CAST(" x " AS CHAR)"
#define BOOL_TO_STRING(x) (x ? "true" : "false")
#define STR_TINYINT_TO_BOOL(x) (encdb::Utils::toLower(x) == "1")
#define SENS_RULE_FIELDS_FOR_WRITE                                       \
  SENSITIVE_DBS "," SENSITIVE_TBLS "," SENSITIVE_COLS "," ALGOS          \
                "," DEFAULT_ALGO "," ENDPOINTS "," RULE_NAME "," ENABLED \
                "," RULE_TYPE "," RULE_DESCRIPTION
#define SENS_RULE_FIELDS_FOR_READ                                        \
  SENSITIVE_DBS "," SENSITIVE_TBLS "," SENSITIVE_COLS "," ALGOS          \
                "," DEFAULT_ALGO "," ENDPOINTS "," RULE_NAME            \
                "," MAKE_CHAR(ENABLED) "," MAKE_CHAR(RULE_TYPE) ","     \
                RULE_DESCRIPTION
#define RULE_SENS_DBS_IDX 0
#define RULE_SENS_TBLS_IDX 1
#define RULE_SENS_COLS_IDX 2
#define RULE_ALGOS_IDX 3
#define RULE_DEF_ALGO_IDX 4
#define RULE_ENDPTS_IDX 5
#define RULE_NAME_IDX 6
#define RULE_ENABLED_IDX 7
#define RULE_TYPE_IDX 8
#define RULE_DESCRIPTION_IDX 9

#define AUTH_USERS_FIELDS_FOR_WRITE \
  USER "," SALT_MAC "," ACCS_TYPE "," EXPIRE_TIME
#define AUTH_USERS_FIELDS_FOR_READ \
  USER "," SALT_MAC "," MAKE_CHAR(ACCS_TYPE) "," MAKE_CHAR(EXPIRE_TIME)
#define AUTH_USER_IDX 0
#define AUTH_MAC_IDX 1
#define AUTH_TYPE_IDX 2
#define AUTH_EXPTIME_IDX 3

namespace encdb {

namespace hook {

void encdb_execute_sql_read(const std::string &sql, std::string &ret);
void encdb_execute_sql_read(const std::string &sql,
                            std::vector<std::vector<std::string>> &ret);
void encdb_execute_sql_write(const std::string &sql);

}  // namespace hook

namespace udf {

void encdb_execute_sql_read(const std::string &sql, std::string &ret);
void encdb_execute_sql_read(const std::string &sql,
                            std::vector<std::vector<std::string>> &ret);
void encdb_execute_sql_write(const std::string &sql);

}  // namespace udf

std::string encdb_sql_clear_enc_rules();
std::string encdb_sql_add_or_update_enc_rule(
    const std::string &dbs, const std::string &tbls, const std::string &cols,
    const std::string &algos, const std::string &default_algo,
    const std::string &endpoints, const std::string &rule_name, bool enabled,
    int rule_type, const std::string &description,
    const std::string &old_rule_name = "");
std::string encdb_sql_delete_enc_rule(const std::string &rule_name);
std::string encdb_sql_load_enc_rules();

std::string encdb_sql_get_saltmac(const std::string &user);
std::string encdb_sql_set_saltmac(const std::string &user,
                                  const std::string &saltmac);

std::string encdb_sql_read_sys_var(const std::string &var_name);
std::string encdb_sql_flush_privileges();

std::string encdb_sql_set_accs_type(const std::string &user, int accs);
std::string encdb_sql_set_accs_type(const std::string &user, int accs,
                                    const std::string &time);
std::string encdb_sql_load_users();
std::string encdb_sql_clear_users();
std::string encdb_sql_query_user_info(const std::string &user);
std::string encdb_sql_read_sys_time();

}  // namespace encdb
