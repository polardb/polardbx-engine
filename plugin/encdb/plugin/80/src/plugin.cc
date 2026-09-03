/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */

#include "plugin.h"
#include <iostream>
#include <sstream>
#include "mysql/service_rpl_transaction_write_set.h"
#include "plugin_log.h"

#include "encdb.h"
#include "encdb_error.h"
#include "memory/encdb_memory.h"
#include "my_dbug.h"
#include "mysql/plugin_group_replication.h"
#include "sql/auto_thd.h"
#include "sql/column_encryption/encdb_service.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/replication.h"
#include "sql/rpl_table_access.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "sql/srv_session.h"
#include "sql/table.h"
#include "sql_service/sql_service_interface.h"
#include "encdb_cipher_types.h"
#include "storage.h"

#define MYSQL_RESULT_SIZE 255
#define SQL_CREATE_UDF_RETURNS_STRING(udf_name) \
  ("CREATE FUNCTION IF NOT EXISTS " udf_name    \
   " RETURNS STRING SONAME 'encdb_udf.so'")
#define SQL_DROP_UDF_IF_EXISTS(udf_name) ("DROP FUNCTION IF EXISTS " udf_name)
#define PLUGIN_GET_USERNAME() \
  to_string(current_thd->security_context()->priv_user())
#define SKIP_GRANTS_FOR_SRVI(srvi) \
  srvi.get_session()->get_thd()->security_context()->skip_grants()
using std::string;

MYSQL_PLUGIN encdb_plugin_ptr = NULL;

// charptr variables managed by MySQL plugin system variable framework
static char *default_encryption_algorithm_str = nullptr;
static char *default_user_access_str = nullptr;
static uint kms_meta_max_entries_val = 10;
static bool cc_miss_error_val = false;

/** Max rows to retain in encdb_kms_meta; older rows are pruned on insert. */
uint encdb_plugin_kms_meta_get_max_entries() {
  return kms_meta_max_entries_val;
}

/** Whether CC_MISS on encrypt path should throw (true) or fallback (false). */
bool encdb_plugin_cc_miss_error() { return cc_miss_error_val; }

extern bool srv_read_only_mode;
extern ulong srv_force_recovery;

// Internal function for plugin
// Should only be used for udf creation & deletion
inline int run_query_skip_grants(const char *sql) {
  encdb::Sql_service_interface srvi;
  int e = srvi.open_session();
  CHECK_ERROR_AND_RETURN(e);
  // skip grants for purpose of create udf
  SKIP_GRANTS_FOR_SRVI(srvi);
  e = srvi.execute_query("set sql_log_bin = 0");
  CHECK_ERROR_AND_RETURN(e);
  e = srvi.execute_query(sql);
  CHECK_ERROR_AND_RETURN(e);
  return e;
}

// This function returns 0 only when innodb_force_recovery is set to 0
//   and all SQLs are executed correctly
inline int check_innodb_force_recovery() {
  encdb::Sql_service_interface srvi;
  int e = srvi.open_session();
  CHECK_ERROR_AND_RETURN(e);
  // skip grants for purpose of create udf
  SKIP_GRANTS_FOR_SRVI(srvi);
  e = srvi.execute_query("set sql_log_bin = 0");
  encdb::Sql_resultset rset;
  e = srvi.execute_query("select @@innodb_force_recovery", &rset);
  CHECK_ERROR_AND_RETURN(e);
  return rset.getLong(0);
}

#ifndef NDEBUG
void debug_set_priv_empty(bool set) {
  static std::string priv;
  if (set) {
    priv = PLUGIN_GET_USERNAME();
    current_thd->security_context()->assign_priv_user(NULL, 0);
  } else {
    current_thd->security_context()->assign_priv_user(priv.c_str(),
                                                      priv.length());
  }
}
#else
void debug_set_priv_empty(bool) {}
#endif

/*
 * encdb_plugin_exec_sql and encdb_plugin_exec_sql are functions
 * used by encdb to execute SQLs.
 */

int encdb_plugin_exec_sql(const std::string &sql, bool use_session_user) {
  std::string uname = PLUGIN_GET_USERNAME();
  encdb::Sql_service_interface srvi;
  int e = srvi.open_session();
  CHECK_ERROR_AND_RETURN(e);
  if (use_session_user) {
    // use current session user
    e = srvi.set_session_user(uname.c_str());
    CHECK_ERROR_AND_RETURN(e);
  } else {
    // execute sql as root
    SKIP_GRANTS_FOR_SRVI(srvi);
  }
  e = srvi.execute_query(sql);
  CHECK_ERROR_AND_RETURN(e);
  return e;
}

int encdb_plugin_exec_sql(const std::string &sql,
                          std::vector<std::vector<std::string>> &retVec) {
  // execute sql as root
  encdb::Sql_service_interface srvi;
  int e = srvi.open_session();
  CHECK_ERROR_AND_RETURN(e);
  // read operations are always allowed
  SKIP_GRANTS_FOR_SRVI(srvi);
  encdb::Sql_resultset rset;
  e = srvi.execute_query(sql, &rset);
  int rows = rset.get_rows();
  int cols = rset.get_cols();
  if (rows == 0) {
    retVec.clear();
  } else {
    // put rows in rset into retVec
    for (int i = 0; i < rows; i++) {
      std::vector<std::string> row;
      for (int j = 0; j < cols; j++) {
        row.push_back(rset.getString(j));
      }
      retVec.push_back(row);
      rset.next();
    }
  }
  return e;
}

void *encdb_plugin_malloc(size_t size) {
  return encdb::encdb_rds_malloc<void *>(size);
}

void encdb_plugin_free(void *ptr) {
  if (ptr != NULL) {
    encdb::encdb_rds_free(ptr);
  }
}

int encdb_log_message(enum LogLevel level, const char *format, ...) {
  enum plugin_log_level mysql_level = MY_INFORMATION_LEVEL;
  if (level == LL_DEBUG) {
    mysql_level = MY_INFORMATION_LEVEL;
  } else if (level == LL_WARNING) {
    mysql_level = MY_WARNING_LEVEL;
  } else if (level == LL_ERROR) {
    mysql_level = MY_ERROR_LEVEL;
  }
  va_list args;
  char buff[1024];

  va_start(args, format);
  vsnprintf(buff, sizeof(buff), format, args);
  va_end(args);
  return my_plugin_log_message(&encdb_plugin_ptr, mysql_level, "%s", buff);
}

// Plugins
int encdb_expected_length_hook(uint32_t src_len, uint32_t *dst_len) {
  DBUG_EXECUTE_IF("simulate_encdb_len_hook_null", { dst_len = NULL; });
  if (dst_len == NULL) {
    my_error(ER_ENCDB_HOOK_ERROR, MYF(0), __FUNCTION__,
             "param dst_len is NULL");
    return 1;
  }
  DBUG_EXECUTE_IF("simulate_encdb_expected_length_empty_user",
                  { debug_set_priv_empty(true); });
  *dst_len =
      encdb::EncdbCipher::expectedCipherSize(PLUGIN_GET_USERNAME(), src_len);
  DBUG_EXECUTE_IF("simulate_encdb_expected_length_empty_user",
                  { debug_set_priv_empty(false); });
  return 0;
}

int encdb_encrypt_hook(uint8_t *dst, uint32_t *dst_len, const uint8_t *src,
                       uint32_t src_len, const char *, int type) {
  DBUG_EXECUTE_IF("simulate_encdb_encrypt_hook_null", {
    dst = NULL;
    dst_len = NULL;
  });
  if (dst == NULL || dst_len == NULL || src == NULL) {
    my_error(ER_ENCDB_HOOK_ERROR, MYF(0), __FUNCTION__,
             "param dst or dst_len or src is NULL");
    return 1;
  }
  DBUG_EXECUTE_IF("simulate_encdb_encrypt_hook_not_enough_space",
                  { *dst_len = src_len; });
  DBUG_EXECUTE_IF("simulate_encdb_encrypt_hook_empty_user",
                  { debug_set_priv_empty(true); });
  int ret = encdb::EncdbCipher::encrypt(std::string(PLUGIN_GET_USERNAME()), dst,
                                        dst_len, src, src_len, type);
  DBUG_EXECUTE_IF("simulate_encdb_encrypt_hook_empty_user",
                  { debug_set_priv_empty(false); });
  if (ret != 0) {
    if (static_cast<EncdbStatus>(ret) == ENCDB_EXCEPTION_CC_MISS ||
        static_cast<EncdbStatus>(ret) == ENCDB_EXCEPTION_DEFAULT_CC_MISS) {
      my_error(ER_ENCDB_HOOK_ERROR, MYF(0), __FUNCTION__,
               "CipherContext not found for current connection. "
               "Please ensure MEK_PROVISION is performed before encryption "
               "(upgrade EncDB JDBC driver if necessary).");
    } else {
      my_error(ER_ENCDB_HOOK_ERROR, MYF(0), __FUNCTION__,
               "encrypt failed for unknown reason, check log for detail.");
    }
    return 1;
  }
  return 0;
}

int encdb_rule_hook(const st_org_field *field, bool *need_enc) {
  // A very rare case, caused by a call to skip_grants
  if (field == NULL || field->db == NULL || field->field == NULL ||
      field->table == NULL ||
      DBUG_EVALUATE_IF("simulate_encdb_rule_hook_null_2", true, false)) {
    *need_enc = false;
    return 0;
  }
  DBUG_EXECUTE_IF("simulate_encdb_rule_hook_empty_user",
                  { debug_set_priv_empty(true); });
  *need_enc = encdb::KeyStore::whetherNeedEncryption(
      field->db, field->table, field->field, PLUGIN_GET_USERNAME());
  DBUG_EXECUTE_IF("simulate_encdb_rule_hook_empty_user",
                  { debug_set_priv_empty(false); });
  return 0;
}

#if 0  // NOT_IN_USE
int encdb_rule_spread_insert_or_create_select(
    const char *source_db, const char *source_table, const char *source_field,
    const char *target_db, const char *target_table, const char *target_field,
    bool target_tmp_table) {
  std::string src_db(source_db), src_tbl(source_table), src_col(source_field);
  std::string dst_db(target_db), dst_tbl(target_table), dst_col(target_field);
  return encdb::KeyStore::ruleSpreadInsertOrCreateFrom(
      src_db, src_tbl, src_col, dst_db, dst_tbl, dst_col, target_tmp_table);
}

int encdb_rule_spread_rename_table(const char *source_db,
                                   const char *source_table,
                                   const char *target_db,
                                   const char *target_table,
                                   bool target_tmp_table) {
  std::string src_db(source_db), src_tbl(source_table);
  std::string dst_db(target_db), dst_tbl(target_table);
  return encdb::KeyStore::ruleSpreadRenameTable(src_db, src_tbl, dst_db,
                                                dst_tbl, target_tmp_table);
}

int encdb_rule_spread_rename_column(const char *source_db,
                                    const char *source_table,
                                    const char *source_field,
                                    const char *target_field,
                                    bool target_tmp_table) {
  std::string src_db(source_db), src_tbl(source_table), src_col(source_field);
  std::string dst_tbl(source_table), dst_col(target_field);
  return encdb::KeyStore::ruleSpreadRenameColumn(src_db, src_tbl, src_col,
                                                 dst_col, target_tmp_table);
}
#endif

int encdb_reload_rules() {
  encdb::KeyStore::reloadAllRules();
  return 0;
}

Encdb_observer encdb_observer = {sizeof(Encdb_observer),
                                 encdb_rule_hook,
                                 NULL,
                                 NULL,
                                 NULL,
                                 encdb_reload_rules,
                                 encdb_encrypt_hook,
                                 encdb_expected_length_hook};

struct st_mysql_group_replication encdb_descriptor = {
    0x0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
};

int encdb_before_handle_connection(Server_state_param *) {
  if ((srv_force_recovery == 0) && !srv_read_only_mode) {
    /*
      Auto_THD provides a temporary THD with a valid thread_stack on the
      main server thread.  srv_session_open_internal() saves and restores
      current_thd, so the subsequent Srv_session::attach() inside
      execute_query() can derive a non-null stack address from old_thd->
      thread_stack, which avoids the assert(thread_stack) failure in
      THD::store_globals().
    */
    Auto_THD thd;
    // create UDFs
    run_query_skip_grants(
        SQL_CREATE_UDF_RETURNS_STRING("encdb_process_message"));
    run_query_skip_grants(SQL_CREATE_UDF_RETURNS_STRING("encdb_rule_op"));
    run_query_skip_grants(SQL_CREATE_UDF_RETURNS_STRING("encdb_rule_export"));
    run_query_skip_grants(
        SQL_CREATE_UDF_RETURNS_STRING("encdb_userauth_export"));
  }
  return 0;
}

int encdb_before_recovery(Server_state_param *) { return 0; }

int encdb_after_engine_recovery(Server_state_param *) { return 0; }

int encdb_after_recovery(Server_state_param *) { return 0; }

int encdb_before_server_shutdown(Server_state_param *) { return 0; }

int encdb_after_server_shutdown(Server_state_param *) { return 0; }

int encdb_after_dd_upgrade(Server_state_param *) { return 0; }

Server_state_observer rds_server_state_observer = {
    sizeof(Server_state_observer),

    encdb_before_handle_connection,  // before the client connects to the server
    encdb_before_recovery,           // before recovery
    encdb_after_engine_recovery,     // after engine recovery
    encdb_after_recovery,            // after recovery
    encdb_before_server_shutdown,    // before shutdown
    encdb_after_server_shutdown,     // after shutdown
    encdb_after_dd_upgrade,
};

int plugin_encdb_init(MYSQL_PLUGIN plugin_info) {
  encdb_plugin_ptr = plugin_info;
  if (encdb::encdb_rds_memory_init()) {
    return 1;
  }
  if (encdb::sql_service_interface_init()) {
    return 1;
  }
  if (register_encdb_observer(&encdb_observer, plugin_info)) {
    encdb_log_message(LL_ERROR, "Failure when registering the encdb observer");
    return 1;
  }
  if (register_server_state_observer(&rds_server_state_observer,
                                     (void *)encdb_plugin_ptr)) {
    /* purecov: begin inspected */
    encdb_log_message(LL_ERROR,
                      "Failure when registering the server state observers");
    return 1;
    /* purecov: end */
  }

  if (default_user_access_str != nullptr) {
    encdb::KeyStore::setDefaultUserAuths(default_user_access_str);
  } else {
    encdb::KeyStore::setDefaultUserAuths("restrictedAccess");
  }

  if (default_encryption_algorithm_str != nullptr) {
    encdb::KeyStore::setDefaultAlgorithm(default_encryption_algorithm_str);
  } else {
    encdb::KeyStore::setDefaultAlgorithm("SM4_128_GCM");
  }

  return 0;
}

static int plugin_encdb_check_uninstall(void *) {
  DBUG_TRACE;

  return 0;
}

int plugin_encdb_deinit(void *) {
  if (unregister_encdb_observer(&encdb_observer)) {
    encdb_log_message(LL_ERROR,
                      "Failure when unregistering the encdb observer");
    return 1;
  }
  encdb::sql_service_interface_deinit();
  encdb::encdb_rds_memory_deinit();
  if (unregister_server_state_observer(&rds_server_state_observer,
                                       (void *)encdb_plugin_ptr)) {
    encdb_log_message(LL_ERROR,
                      "Failure in unregistering the server state observers");
    return 1;
  }
  return 0;
}

// --- EncDB System Variables: check and update callbacks ---

static int check_default_encryption_algorithm(MYSQL_THD thd, SYS_VAR *,
                                              void *save,
                                              struct st_mysql_value *value) {
  char buff[256];
  const char *str;
  int length = sizeof(buff);
  if (!(str = value->val_str(value, buff, &length))) return 1;
  if (strcmp(str, "AES_128_GCM") != 0 && strcmp(str, "AES_128_ECB") != 0 &&
      strcmp(str, "AES_128_CTR") != 0 && strcmp(str, "AES_128_CBC") != 0 &&
      strcmp(str, "SM4_128_CBC") != 0 && strcmp(str, "SM4_128_ECB") != 0 &&
      strcmp(str, "SM4_128_CTR") != 0 && strcmp(str, "SM4_128_GCM") != 0) {
    return 1;
  }
  *(const char **)save = thd->strmake(str, length);
  return 0;
}

static void update_default_encryption_algorithm(MYSQL_THD, SYS_VAR *,
                                                void *var_ptr,
                                                const void *save) {
  const char *new_val = *static_cast<char *const *>(save);
  *static_cast<const char **>(var_ptr) = new_val;
  encdb::KeyStore::setDefaultAlgorithm(new_val);
}

static int check_default_user_access(MYSQL_THD thd, SYS_VAR *, void *save,
                                     struct st_mysql_value *value) {
  char buff[256];
  const char *str;
  int length = sizeof(buff);
  if (!(str = value->val_str(value, buff, &length))) return 1;
  if (strcmp(str, "noneAccess") != 0 && strcmp(str, "restrictedAccess") != 0 &&
      strcmp(str, "fullAccess") != 0) {
    return 1;
  }
  *(const char **)save = thd->strmake(str, length);
  return 0;
}

static void update_default_user_access(MYSQL_THD, SYS_VAR *, void *var_ptr,
                                       const void *save) {
  const char *new_val = *static_cast<char *const *>(save);
  *static_cast<const char **>(var_ptr) = new_val;
  encdb::KeyStore::setDefaultUserAuths(new_val);
}

static MYSQL_SYSVAR_STR(default_encryption_algorithm,
                        default_encryption_algorithm_str,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Default encryption algorithm for EncDB. Valid values: "
                        "AES_128_GCM, AES_128_ECB, AES_128_CTR, AES_128_CBC, "
                        "SM4_128_CBC, SM4_128_ECB, SM4_128_CTR, SM4_128_GCM.",
                        check_default_encryption_algorithm,
                        update_default_encryption_algorithm, "SM4_128_GCM");

static MYSQL_SYSVAR_STR(
    default_user_access, default_user_access_str,
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
    "Default user access level for EncDB. Valid values: noneAccess, "
    "restrictedAccess, fullAccess.",
    check_default_user_access, update_default_user_access, "restrictedAccess");

static MYSQL_SYSVAR_UINT(kms_meta_max_entries, kms_meta_max_entries_val,
                         PLUGIN_VAR_OPCMDARG,
                         "Maximum number of entries to keep in the KMS meta "
                         "table. Oldest entries are purged on each insert.",
                         nullptr, nullptr, 10, 1, 1000, 1);

static MYSQL_SYSVAR_BOOL(cc_miss_error, cc_miss_error_val,
                          PLUGIN_VAR_OPCMDARG,
                          "When OFF (default), encrypt with ephemeral key if "
                          "CipherContext is missing (no MEK_PROVISION). "
                          "When ON, return an error to the client.",
                          nullptr, nullptr, false);

static SYS_VAR *encdb_system_variables[] = {
    MYSQL_SYSVAR(default_encryption_algorithm),
    MYSQL_SYSVAR(default_user_access),
    MYSQL_SYSVAR(kms_meta_max_entries),
    MYSQL_SYSVAR(cc_miss_error), nullptr};

mysql_declare_plugin(encdb){
    MYSQL_ENCRYPT_DB_PLUGIN,
    &encdb_descriptor,
    "encdb",
    "AliCloud",
    "EncDB for RDS MySQL (1.0.0)", /* Plugin name with full version*/
    PLUGIN_LICENSE_GPL,
    plugin_encdb_init, /* Plugin Init */
    plugin_encdb_check_uninstall,
    plugin_encdb_deinit,    /* Plugin Deinit */
    0x0100,                 /* Plugin Version: major.minor */
    NULL,                   /* status variables */
    encdb_system_variables, /* system variables */
    NULL,                   /* config options */
    0,                      /* flags */
} mysql_declare_plugin_end;

// ===========================================================================
// KMS meta table handler-API hooks.
//
// These functions bypass SQL parsing and operate directly on the InnoDB
// handler, following the pattern established by Gtid_table_access_context.
//
// Target table schema (mysql.encdb_kms_meta):
//
//   field[0]  version    INT  PRIMARY KEY   -- monotonically increasing,
//                                           -- latest row = max(version)
//   field[1]  enc_mek    TEXT               -- KMS-encrypted master key
//   field[2]  region     VARCHAR(64)        -- KMS region
//   field[3]  key_id     VARCHAR(128)       -- KMS key ID
//   field[4]  salt_mac   TEXT               -- HMAC(MEK, salt) for integrity
//
// KMS_META_NUM_FIELDS must match the column count above.  open_table() rejects
// a table whose field count is smaller than this value, guarding against
// accidental ALTER TABLE.
// ===========================================================================

static constexpr uint KMS_META_NUM_FIELDS = 5;

// Minimal concrete subclass -- sets no special flags, deferring to MySQL's
// normal table-open checks (same behavior as encdb auth/user SQL writes).
class Encdb_kms_meta_table_access : public System_table_access {
 public:
  void before_open(THD *) override { m_flags = 0; }
};

static int run_in_internal_session(
    Srv_session::Attached_thd_callback callback, void *context) {
  THD *caller_thd = current_thd;
  if (!caller_thd) return 1;

  int error = 1;
  {
    encdb::Sql_service_interface session;
    if (session.open_session() == 0) {
      SKIP_GRANTS_FOR_SRVI(session);
      error = session.get_session()->execute_with_attached_thd(callback,
                                                               context);
    }
  }

  assert(current_thd == caller_thd);
  return error;
}

static bool prepare_thd_for_internal_command(THD *thd) {
  if (lex_start(thd)) return true;
  mysql_reset_thd_for_next_command(thd);
  thd->set_command(COM_QUERY);
  return false;
}

/** Read a Field value into std::string; binary-safe, no null-terminator assumed. */
static std::string field_to_string(Field *f) {
  char buf[4096];
  String str(buf, sizeof(buf), &my_charset_bin);
  f->val_str(&str);
  return std::string(str.c_ptr_safe(), str.length());
}

// Read the row with the highest version from mysql.encdb_kms_meta.
//
// Because `version` is the INTEGER PRIMARY KEY and rows are inserted with
// strictly increasing version numbers, the "latest" row is always the one
// with the largest key.  ha_index_last() positions the cursor at that row
// in O(log N) time without a full scan.
//
// Returns 0 and populates *rec on success.
// Returns 1 if there is no current THD, the table cannot be opened, the
// index cursor cannot be initialized, or the table is empty.
//
// The caller must treat a non-zero return as "KMS mode not active" --
// an empty table means the instance is running in non-KMS mode.
int encdb_plugin_kms_meta_read_latest(encdb::KmsMetaRecord *rec) {
  THD *thd = current_thd;
  if (!thd) return 1;

  Encdb_kms_meta_table_access ctx;
  TABLE *table = nullptr;
  Open_tables_backup backup;

  if (ctx.open_table(thd, "mysql", "encdb_kms_meta", KMS_META_NUM_FIELDS,
                     TL_READ, &table, &backup))
    return 1;

  // ret=1 by default; set to 0 only on success, so ha_index_end() and
  // close_table() are always reached regardless of outcome.
  int ret = 1;
  int err;

  // index 0 = PRIMARY KEY (version INT); sorted=true required for ha_index_last.
  // Table is already open on failure, so close_table() must still be called.
  if ((err = table->file->ha_index_init(0, true))) {
    ctx.close_table(thd, table, &backup, true, false);
    return 1;
  }

  // Seek to max(version) row -- PK is monotonically increasing, so the last
  // index entry is always the latest.  HA_ERR_END_OF_FILE means table is empty.
  if (!(err = table->file->ha_index_last(table->record[0]))) {
    // field[0..4] matches column order: version, enc_mek, region, key_id, salt_mac
    rec->version = static_cast<int>(table->field[0]->val_int());
    rec->enc_mek = field_to_string(table->field[1]);
    rec->region  = field_to_string(table->field[2]);
    rec->key_id  = field_to_string(table->field[3]);
    rec->kms_salt_mac = field_to_string(table->field[4]);
    ret = 0;
  }

  table->file->ha_index_end();  // must pair with ha_index_init
  ctx.close_table(thd, table, &backup, false, false);  // read-only: no trx commit needed
  return ret;
}

// Best-effort: keep only the newest max_entries rows, delete the rest.
// Operates on an already-open table with an active index on field[0] (version).
// Returns without error even if pruning fails -- callers only log a warning.
static void kms_meta_prune(TABLE *table, uint max_entries) {
  int err;

  if ((err = table->file->ha_index_init(0, true))) {
    encdb_log_message(LL_WARNING,
                      "[EncDB] kms_meta_prune: ha_index_init failed (%d)", err);
    return;
  }

  // Walk backward from newest, counting max_entries rows to find the boundary.
  // Using position-based counting so version gaps don't matter.
  uint kept = 0;
  err = table->file->ha_index_last(table->record[0]);
  while (!err && kept < max_entries) {
    kept++;
    if (kept < max_entries)
      err = table->file->ha_index_prev(table->record[0]);
  }

  if (!err && kept == max_entries) {
    // record[0] is the boundary row (the oldest row to keep).
    // Capture its version, then scan forward from the beginning deleting
    // everything older.  delete+next is the proven InnoDB pattern (cf. GTID).
    const int boundary = static_cast<int>(table->field[0]->val_int());
    err = table->file->ha_index_first(table->record[0]);
    while (!err && static_cast<int>(table->field[0]->val_int()) < boundary) {
      if (table->file->ha_delete_row(table->record[0])) {
        encdb_log_message(LL_WARNING,
                          "[EncDB] kms_meta_prune: delete failed, pruning stopped");
        break;
      }
      err = table->file->ha_index_next(table->record[0]);
    }
  }

  table->file->ha_index_end();
}

// Write one row into the already-open table, letting InnoDB assign the
// AUTO_INCREMENT version.  Returns 0 on success, 1 on failure.
static int kms_meta_write_row(TABLE *table, const std::string *enc_mek,
                              const std::string *region,
                              const std::string *key_id,
                              const std::string *kms_salt_mac) {
  empty_record(table);
  table->next_number_field = table->field[0];  // AUTO_INCREMENT
  table->field[1]->set_notnull();
  table->field[1]->store(enc_mek->data(), enc_mek->size(), &my_charset_bin);
  table->field[2]->set_notnull();
  table->field[2]->store(region->data(), region->size(), &my_charset_bin);
  table->field[3]->set_notnull();
  table->field[3]->store(key_id->data(), key_id->size(), &my_charset_bin);
  table->field[4]->set_notnull();
  table->field[4]->store(kms_salt_mac->data(), kms_salt_mac->size(),
                         &my_charset_bin);
  int ret = table->file->ha_write_row(table->record[0]) ? 1 : 0;
  table->next_number_field = nullptr;
  // Release AUTO_INCREMENT lock; required before close to satisfy
  // ha_external_lock(F_UNLCK) assertion `next_insert_id == 0`.
  table->file->ha_release_auto_increment();
  return ret;
}

struct Kms_meta_insert_context {
  const std::string *enc_mek;
  const std::string *region;
  const std::string *key_id;
  const std::string *kms_salt_mac;
};

static int do_kms_meta_insert(THD *thd, void *opaque) {
  auto *context = static_cast<Kms_meta_insert_context *>(opaque);
  if (prepare_thd_for_internal_command(thd)) return 1;

  Encdb_kms_meta_table_access table_access;
  TABLE *table = nullptr;
  Open_tables_backup backup;

  bool error = table_access.open_table(
      thd, "mysql", "encdb_kms_meta", KMS_META_NUM_FIELDS, TL_WRITE, &table,
      &backup);

  if (!error) {
    error = kms_meta_write_row(table, context->enc_mek, context->region,
                               context->key_id, context->kms_salt_mac) != 0;
  }
  if (!error) {
    kms_meta_prune(table, encdb_plugin_kms_meta_get_max_entries());
  }

  return table_access.close_table(thd, table, &backup, error, true) || error;
}

/** Insert a KMS metadata row and prune old rows to stay within max-entries. */
int encdb_plugin_kms_meta_insert(const std::string *enc_mek,
                                 const std::string *region,
                                 const std::string *key_id,
                                 const std::string *kms_salt_mac) {
  Kms_meta_insert_context context{enc_mek, region, key_id, kms_salt_mac};
  return run_in_internal_session(do_kms_meta_insert, &context);
}

static int do_kms_meta_delete_all(THD *thd, void *) {
  if (prepare_thd_for_internal_command(thd)) return 1;

  Encdb_kms_meta_table_access table_access;
  TABLE *table = nullptr;
  Open_tables_backup backup;

  bool error = table_access.open_table(
      thd, "mysql", "encdb_kms_meta", KMS_META_NUM_FIELDS, TL_WRITE, &table,
      &backup);

  if (!error) {
    int handler_error = table->file->ha_rnd_init(true);
    if (handler_error) {
      error = true;
    } else {
      while (!(handler_error =
                   table->file->ha_rnd_next(table->record[0]))) {
        if (table->file->ha_delete_row(table->record[0])) {
          error = true;
          break;
        }
      }
      table->file->ha_rnd_end();
      if (!error && handler_error != HA_ERR_END_OF_FILE) error = true;
    }
  }

  return table_access.close_table(thd, table, &backup, error, true) || error;
}

/** Delete all rows in encdb_kms_meta; called when exiting KMS mode. */
int encdb_plugin_kms_meta_delete_all() {
  return run_in_internal_session(do_kms_meta_delete_all, nullptr);
}

// ===========================================================================
// Session CC access — kernel calls these to read/write per-connection CC.
// ===========================================================================

/** THD cleanup callback: frees the heap-allocated CipherContext. */
static void plugin_cleanup_session_cc(void *p) {
  delete static_cast<encdb::CipherContext *>(p);
}

/** Return per-connection CC from THD, or nullptr if not yet provisioned. */
const encdb::CipherContext *encdb_plugin_get_session_cc() {
  THD *thd = current_thd;
  if (thd && thd->encdb_session_data) {
    return static_cast<const encdb::CipherContext *>(thd->encdb_session_data);
  }
  return nullptr;
}

/** Store a heap copy of *cc in THD; previous CC is freed via cleanup callback. */
void encdb_plugin_set_session_cc(const encdb::CipherContext *cc) {
  THD *thd = current_thd;
  if (!thd) return;
  // Construct the replacement first. If the deep copy throws (e.g. OOM), the
  // existing CC is left untouched, so there is no dangling pointer (UAF) nor
  // double free.
  auto *new_cc = new encdb::CipherContext(*cc);
  if (thd->encdb_session_data && thd->encdb_session_data_cleanup) {
    thd->encdb_session_data_cleanup(thd->encdb_session_data);
  }
  thd->encdb_session_data = new_cc;
  thd->encdb_session_data_cleanup = plugin_cleanup_session_cc;
}
