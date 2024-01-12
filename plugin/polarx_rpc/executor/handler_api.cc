
#include "../global_defines.h"
#ifndef MYSQL8
#define MYSQL_SERVER
#endif
#include "sql/binlog.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/key.h"
#include "sql/mysqld.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/transaction.h"
#ifndef MYSQL8
#include "sql/rds_audit_log.h"
#endif

#include "handler_api.h"
#include "log.h"

namespace rpc_executor {

namespace handler {
int check_table_init(ExecTable *exec_table) {
  int ret = HA_EXEC_SUCCESS;
  if (!exec_table) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("exec_table is null");
  } else if (!exec_table->is_init()) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("exec_table not init");
  }
  return ret;
}

int check_key_init(ExecKeyMeta *exec_key) {
  int ret = HA_EXEC_SUCCESS;
  if (!exec_key) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("exec_key is null");
  } else if (!exec_key->is_init()) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("exec_key not init");
  }
  return ret;
}

int check_meta_init(ExecTable *exec_table, ExecKeyMeta *exec_key) {
  int ret = HA_EXEC_SUCCESS;
  if ((ret = check_table_init(exec_table))) {
    // table init error, should check in handler_open_table
  } else if ((ret = check_key_init(exec_key))) {
    // key init error, should check in handler_open_table
  }
  return ret;
}

}  // namespace handler

THD *handler_create_thd(bool enable_binlog) {
  THD *thd;

  if (enable_binlog && !binlog_enabled()) {
    fprintf(stderr,
            "  InnoDB_Memcached: MySQL server"
            " binlog not enabled\n");
    return (NULL);
  }

  thd = new (std::nothrow) THD;

  if (!thd) {
    return (NULL);
  }

  thd->get_protocol_classic()->init_net((Vio *)0);
  thd->set_new_thread_id();
  thd->thread_stack = reinterpret_cast<char *>(&thd);
  thd->store_globals();

  if (enable_binlog) {
    thd->binlog_setup_trx_data();

    /* set binlog_format to "ROW" */
    thd->set_current_stmt_binlog_format_row();
  }

  return (thd);
}

void handler_close_thd(THD *thd) {
  /* destructor will not free it, because net.vio is 0. */
  thd->get_protocol_classic()->end_net();
  thd->release_resources();
  delete (thd);
}

void handler_thd_attach(THD *thd, THD **original_thd) {
  if (original_thd) {
    *original_thd = current_thd;
  }

  thd->store_globals();
}

void handler_set_thd_source(THD *thd, const char *host_or_ip, const char *host,
                            const char *ip, uint16_t port, const char *user) {
  thd->peer_port = port;
  thd->m_main_security_ctx.set_host_or_ip_ptr(host_or_ip, strlen(host_or_ip));
  thd->m_main_security_ctx.set_host_ptr(host, strlen(host));
  thd->m_main_security_ctx.set_ip_ptr(ip, strlen(ip));
  thd->m_main_security_ctx.set_user_ptr(user, strlen(user));
}

int handler_open_table(THD *thd, const char *db_name, const char *table_name,
                       int lock_type, ExecTable *&exec_table) {
  int ret = HA_EXEC_SUCCESS;
  std::unique_ptr<ExecTable> new_exec_table(new (std::nothrow) ExecTable());
  if (!new_exec_table) {
    ret = HA_ERR_OUT_OF_MEM;
    log_exec_error("create new ExecTable failed, ret: %d", ret);
  } else {
    THD_STAGE_INFO(thd, stage_opening_tables);
    auto &tables = new_exec_table->tables;
    Open_table_context table_ctx(thd, 0);
    thr_lock_type lock_mode = (lock_type <= HDL_READ) ? TL_READ : TL_WRITE;

#ifdef MYSQL8PLUS
    tables = Table_ref(db_name, strlen(db_name), table_name, strlen(table_name),
                       table_name, lock_mode);
#else
    tables.init_one_table(db_name, strlen(db_name), table_name,
                          strlen(table_name), table_name, lock_mode);
#endif

    /* For flush, we need to request exclusive mdl lock. */
    if (lock_type == HDL_FLUSH) {
      MDL_REQUEST_INIT(&tables.mdl_request, MDL_key::TABLE, db_name, table_name,
                       MDL_EXCLUSIVE, MDL_TRANSACTION);
    } else {
      MDL_REQUEST_INIT(
          &tables.mdl_request, MDL_key::TABLE, db_name, table_name,
          (lock_mode > TL_READ) ? MDL_SHARED_WRITE : MDL_SHARED_READ,
          MDL_TRANSACTION);
    }

    if ((ret = open_table(thd, &tables, &table_ctx))) {
      log_exec_error(
          "call open_table failed, "
          "ret: %d, name: %s.%s, lock_type: %d",
          ret, db_name, table_name, lock_type);
    } else if ((ret = new_exec_table->init(tables.table))) {
      log_exec_error(
          "Executor table init failed, "
          "ret: %d,name: %s.%s, lock_type: %d",
          ret, db_name, table_name, lock_type);
    } else {
#ifdef MYSQL8
      lizard::simulate_snapshot_clause(thd, &tables);
      /// set vision manually
      auto hint = tables.snapshot_hint;
      if (hint != nullptr) hint->evoke_vision(tables.table, thd);
#endif
      // In any other case this function fails,
      // new_exec_table will be released by unique_ptr
      exec_table = new_exec_table.release();
    }
  }

  return ret;
}

#ifdef MYSQL8
long long thd_test_options(const MYSQL_THD thd, long long test_options) {
  return thd->variables.option_bits & test_options;
}
#endif

int handler_close_table(THD *thd, ExecTable *&exec_table, int mode) {
  int ret = HA_EXEC_SUCCESS;

  TABLE *my_table = exec_table->table();
  thr_lock_type lock_mode;

  lock_mode = (mode & HDL_READ) ? TL_READ : TL_WRITE;

  if (lock_mode == TL_WRITE) {
    my_table->file->ha_release_auto_increment();
  }

  ret = trans_commit_stmt(thd);

  THD_STAGE_INFO(thd, stage_closing_tables);
  close_thread_tables(thd);

  if (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
    // auto commit and no begin
    ret = trans_commit(thd);
    thd->mdl_context.release_transactional_locks();
  }

  thd->lock = 0;
  delete exec_table;
  exec_table = nullptr;

  return ret;
}

int handler_set_key_read_only(ExecTable *exec_table) {
  assert(HA_EXEC_SUCCESS == handler::check_table_init(exec_table));
  return exec_table->table()->file->extra(HA_EXTRA_KEYREAD);
}

int handler_set_no_key_read_only(ExecTable *exec_table) {
  assert(HA_EXEC_SUCCESS == handler::check_table_init(exec_table));
  return exec_table->table()->file->extra(HA_EXTRA_NO_KEYREAD);
}

// --------------------------- CRUD API ---------------------------------------
int handler_index_read_impl(THD *thd, ExecTable *exec_table,
                            ExecKeyMeta *exec_key, uchar *search_buf,
                            key_part_map part_map, ha_rkey_function read_flags,
                            bool &found) {
  TABLE *my_table = exec_table->table();
  uint32_t idx_to_use = exec_key->index();

  int ret = HA_EXEC_SUCCESS;

  // handler get routine
  ::handler *handle = my_table->file;

  if ((ret = handle->ha_index_init(idx_to_use, true /* sorted */))) {
    log_exec_error("handler index init failed, ret: %d, idx_to_use: %d", ret,
                   idx_to_use);
  } else {
    if (0 == part_map) {
      // TODO: See what kind of get we should offer and if we can optimize so
      // many if else
      ret = handle->ha_index_first(my_table->record[0]);
    } else {
      ret = handle->ha_index_read_map(my_table->record[0], search_buf, part_map,
                                      read_flags);
    }
    if (ret == HA_ERR_KEY_NOT_FOUND || ret == HA_ERR_END_OF_FILE) {
      ret = HA_EXEC_SUCCESS;
    } else if (ret) {
      log_exec_error("handler index read failed, ret: %d, flags: %d", ret,
                     read_flags);
    } else {
      thd->inc_examined_row_count(1);
      found = true;
    }
  }

  return ret;
}

int handler_get(THD *thd, ExecTable *exec_table, ExecKeyMeta *exec_key,
                const SearchKey &search_key, bool &found) {
  // exec_table and exec_key must be generated from handler_open_table and
  // should have been checked there.
  assert(HA_EXEC_SUCCESS == handler::check_meta_init(exec_table, exec_key));

  int ret = HA_EXEC_SUCCESS;
  if (!thd) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("THD is NULL");
  } else {
    ret = handler_index_read_impl(
        thd, exec_table, exec_key, search_key.key_buffer(),
        search_key.used_part_map(), HA_READ_KEY_EXACT, found);
  }
  return ret;
}

int handler_index_first(THD *thd, ExecTable *exec_table, ExecKeyMeta *exec_key,
                        bool &found) {
  // exec_table and exec_key must be generated from handler_open_table and
  // should have been checked there.
  assert(HA_EXEC_SUCCESS == handler::check_meta_init(exec_table, exec_key));
  TABLE *my_table = exec_table->table();
  uint32_t idx_to_use = exec_key->index();

  int ret = HA_EXEC_SUCCESS;

  // handler get routine
  ::handler *handle = my_table->file;

  if ((ret = handle->ha_index_init(idx_to_use, true /* sorted */))) {
    log_exec_error("handler index init failed, ret: %d, idx_to_use: %d", ret,
                   idx_to_use);
  } else if ((ret = handle->ha_index_first(my_table->record[0]))) {
    if (ret == HA_ERR_KEY_NOT_FOUND || ret == HA_ERR_END_OF_FILE) {
      ret = HA_EXEC_SUCCESS;
      found = false;
    } else if (ret) {
      log_exec_error("handler index read failed, ret: %d", ret);
    }
  } else {
    thd->inc_examined_row_count(1);
    found = true;
  }
  return ret;
}

int handler_index_next(THD *thd, ExecTable *exec_table, bool &found) {
  assert(HA_EXEC_SUCCESS == handler::check_table_init(exec_table));

  int ret = HA_EXEC_SUCCESS;
  if (!thd) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("THD is NULL");
  } else {
    TABLE *my_table = exec_table->table();
    if ((ret = my_table->file->ha_index_next(my_table->record[0]))) {
      if (HA_ERR_END_OF_FILE == ret || HA_ERR_KEY_NOT_FOUND == ret) {
        ret = HA_EXEC_SUCCESS;
        found = false;
      }
      // else this is an error
    } else {
      thd->inc_examined_row_count(1);
      found = true;
    }
  }
  return ret;
}

int handler_next_same(THD *thd, ExecTable *exec_table,
                      const SearchKey &search_key, bool &found) {
  assert(HA_EXEC_SUCCESS == handler::check_table_init(exec_table));

  int ret = HA_EXEC_SUCCESS;
  if (!thd) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("THD is NULL");
  } else {
    TABLE *my_table = exec_table->table();
    if ((ret = my_table->file->ha_index_next_same(my_table->record[0],
                                                  search_key.key_buffer(),
                                                  search_key.used_length()))) {
      if (HA_ERR_END_OF_FILE == ret || HA_ERR_KEY_NOT_FOUND == ret) {
        ret = HA_EXEC_SUCCESS;
        found = false;
      }
      // else this is an error
    } else {
      thd->inc_examined_row_count(1);
      found = true;
    }
  }
  return ret;
}

int handler_index_end(THD *thd, ExecTable *exec_table) {
  int ret = HA_EXEC_SUCCESS;
  TABLE *my_table = exec_table->table();
  if (!thd) {
    ret = HA_ERR_INTERNAL_ERROR;
  } else {
    ::handler *handle = my_table->file;
    if ((ret = handle->ha_index_or_rnd_end())) {
      log_exec_error("handler end failed, ret: %d", ret);
    }
  }
  return HA_EXEC_SUCCESS;
}

int handler_seek(THD *thd, ExecTable *exec_table, ExecKeyMeta *exec_key,
                 const RangeSearchKey &range_key, bool &found) {
  // exec_table and exec_key must be generated from handler_open_table and
  // should have been checked there.
  assert(HA_EXEC_SUCCESS == handler::check_meta_init(exec_table, exec_key));

  int ret = HA_EXEC_SUCCESS;
  if (!thd) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("THD is NULL");
  } else {
    TABLE *my_table = exec_table->table();
    ::handler *handle = my_table->file;
    uint32_t idx_to_use = exec_key->index();
    if ((ret = handle->ha_index_init(idx_to_use, false))) {
      log_exec_error("handler index init failed, ret: %d, idx_to_use: %d", ret,
                     idx_to_use);
    } else {
#ifdef MYSQL8
      ret = handle->ha_read_range_first(range_key.begin_range(),
                                        range_key.end_range(), false, false);
#else
      ret = handle->read_range_first(range_key.begin_range(),
                                     range_key.end_range(), false, false);
#endif
      if (ret == HA_ERR_KEY_NOT_FOUND || ret == HA_ERR_END_OF_FILE) {
        ret = HA_EXEC_SUCCESS;
      } else if (ret) {
        log_exec_error("handler range first failed, ret: %d, idx_to_use: %d",
                       ret, idx_to_use);
      } else {
        thd->inc_examined_row_count(1);
        found = true;
      }
    }
  }
  return ret;
}

int handler_range_next(THD *thd, ExecTable *exec_table, bool &found) {
  assert(HA_EXEC_SUCCESS == handler::check_table_init(exec_table));

  int ret = HA_EXEC_SUCCESS;
  if (!thd) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("THD is NULL");
  } else {
    TABLE *my_table = exec_table->table();
    ::handler *handle = my_table->file;
#ifdef MYSQL8
    ret = handle->ha_read_range_next();
#else
    ret = handle->read_range_next();
#endif
    if (ret) {
      if (HA_ERR_END_OF_FILE == ret || HA_ERR_KEY_NOT_FOUND == ret) {
        ret = HA_EXEC_SUCCESS;
        found = false;
      }
      // else this is an error
    } else {
      thd->inc_examined_row_count(1);
      found = true;
    }
  }
  return ret;
}

/*
int handler_insert_rec(TABLE *my_table, field_arg_t *store_args) {
  uchar *insert_buf;
  KEY *key_info = &(my_table->key_info[0]);
  handler *handle = my_table->file;

  empty_record(my_table);

  assert(my_table->reginfo.lock_type > TL_READ &&
         my_table->reginfo.lock_type <= TL_WRITE_ONLY);

  insert_buf = my_table->record[0];
  memset(insert_buf, 0, my_table->s->null_bytes);

  assert(store_args->num_arg == key_info->user_defined_key_parts);

  for (unsigned int i = 0; i < key_info->user_defined_key_parts; i++) {
    Field *fld;

    fld = my_table->field[i];
    if (store_args->len[i]) {
      fld->store(store_args->value[i], store_args->len[i], &my_charset_bin);
      fld->set_notnull();
    } else {
      fld->set_null();
    }
  }

  return (handle->ha_write_row((uchar *)my_table->record[0]));
}

int handler_update_rec(TABLE *my_table, field_arg_t *store_args) {
  uchar *buf = my_table->record[0];
  handler *handle = my_table->file;
  KEY *key_info = &my_table->key_info[0];

  store_record(my_table, record[1]);

  for (unsigned int i = 0; i < key_info->user_defined_key_parts; i++) {
    Field *fld;

    fld = my_table->field[i];
    fld->store(store_args->value[i], store_args->len[i], &my_charset_bin);
    fld->set_notnull();
  }

  return (handle->ha_update_row(my_table->record[1], buf));
}

int handler_delete_rec(TABLE *my_table) {
  return (my_table->file->ha_delete_row(my_table->record[0]));
}
*/

MYSQL_LOCK *handler_lock_table(THD *thd, ExecTable *exec_table,
                               thr_lock_type lock_mode) {
  TABLE *my_table = exec_table->table();
  my_table->reginfo.lock_type = lock_mode;
  thd->lock = mysql_lock_tables(thd, &my_table, 1, 0);
  // mark table locked, will mark unlocked in close_thread_tables
  thd->lex->lock_tables_state = Query_tables_list::LTS_LOCKED;
  // Set this to make Item_field::set_field work
  my_table->pos_in_table_list = nullptr;
  return thd->lock;
}

}  // namespace rpc_executor
