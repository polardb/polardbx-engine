#pragma once
// EncDB internal table names and columns definition
// Tables
#define ENCDB_SCHEMA "mysql"
#define AUTH_USER_TABLE ENCDB_SCHEMA ".encdb_auth_users"
#define SENS_RULES_TABLE ENCDB_SCHEMA ".encdb_sensitive_rules"
// Columns for mysql.encdb_auth_users
#define SALT_MAC "salt_mac"        // e.g. 18SJ238sm== MEK salt & mac
#define USER "user"                // e.g. root username
#define ACCS_TYPE "accs_type"      // e.g. 0|1|2
#define EXPIRE_TIME "expire_time"  // e.g. A MySQL DATETIME
// Columns for mysql.encdb_sensitive_rules
#define SENSITIVE_DBS "sensitive_databases"  // e.g. ["db1","db2"]
#define SENSITIVE_TBLS "sensitive_tables"    // e.g. ["tbl1","tbl2"]
#define SENSITIVE_COLS "sensitive_columns"   // e.g. ["col1","col2"]
#define ALGOS \
  "algos"  // e.g. [{"name": "aes-128-gcm", "params": NULL},{"name": "sha256",
           // "params": NULL},{"name": "replace-regex", "params": {"regex":
           // ""}}]
#define DEFAULT_ALGO "default_algo"  // e.g. aes-128-gcm
#define ENDPOINTS "endpoints"        // e.g. ["*"]
#define RULE_NAME "rule_name"        // e.g. rule1
#define ENABLED "enabled"            // e.g. true
#define RULE_TYPE "rule_type"        // e.g. 0
#define RULE_DESCRIPTION "description"

// Rule format:
// {
//   "version": 1,
//   "rules": [
//     {
//       "name": "rule1",
//       "description": "rule description",
//       "enabled": true,
//       "algo":[
//         {"name": "aes-128-gcm", "params": null},
//         {"name": "sha256", "params": null},
//         {"name": "replace-regex", "params": {"regex": ""}}
//       ],
//       "defaultAlgo": "aes-128-gcm",
//       "meta": {
//         "databases": ["test"],
//         "tables": ["test"],
//         "columns": ["a","b"]
//       },
//       "users": {
//         "restrictedAccess": ["appuser","test_*"],
//         "fullAccess": ["dba"],
//         "filterFields": ["tenantid", "roleid"]
//       },
//       "endpoints": ["*"]
//     }
//   ]
// }
#define JSON_VERSION "version"
#define JSON_RULES "rules"
#define JSON_RULE_NAME "name"
#define JSON_RULE_DESCRIPTION "description"
#define JSON_ENABLED "enabled"
#define JSON_ALGO "algo"
#define JSON_DEFAULT_ALGO "defaultAlgo"
#define JSON_SENSITIVE_DATA "meta"
#define JSON_SENSITIVE_DBS "databases"
#define JSON_SENSITIVE_TBLS "tables"
#define JSON_SENSITIVE_COLS "columns"
#define JSON_USRS "users"
#define JSON_USRS_RESTRICTED "restrictedAccess"
#define JSON_USRS_FULL "fullAccess"
#define JSON_USRS_NOACCESS "noneAccess"
#define JSON_FILTER_FIELDS "filterFields"  // Not in use yet
#define JSON_ENDPTS "endpoints"
#define JSON_EXPIRE "expired"
#define JSON_UNAME "username"
#define JSON_UACCS "access"
