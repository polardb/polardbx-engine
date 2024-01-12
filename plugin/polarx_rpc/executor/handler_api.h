
#pragma once

#include <stdint.h>
#include <unordered_map>
#include <vector>

#include "sql/lock.h"

#include "meta.h"

/** Defines for handler_unlock_table()'s mode field */
#define HDL_READ 0x1
#define HDL_WRITE 0x2
#define HDL_FLUSH 0x3

namespace rpc_executor {

// map to THD operation
THD *handler_create_thd(bool enable_binlog);
void handler_close_thd(THD *thd);
void handler_thd_attach(THD *thd, THD **original_thd);
void handler_set_thd_source(THD *thd, const char *host_or_ip, const char *host,
                            const char *ip, uint16_t port, const char *user);

// map to TABLE::file operation
int handler_open_table(THD *thd, const char *db_name, const char *table_name,
                       int lock_type, ExecTable *&exec_table);
int handler_close_table(THD *thd, ExecTable *&exec_table, int mode);

int handler_set_key_read_only(ExecTable *exec_table);
int handler_set_no_key_read_only(ExecTable *exec_table);

int handler_get(THD *thd, ExecTable *exec_table, ExecKeyMeta *exec_key,
                const SearchKey &search_key, bool &found);
int handler_index_first(THD *thd, ExecTable *exec_table, ExecKeyMeta *exec_key,
                        bool &found);
int handler_index_next(THD *thd, ExecTable *exec_table, bool &found);
int handler_next_same(THD *thd, ExecTable *exec_table,
                      const SearchKey &search_key, bool &found);
int handler_index_end(THD *thd, ExecTable *exec_table);

int handler_seek(THD *thd, ExecTable *exec_table, ExecKeyMeta *exec_key,
                 const RangeSearchKey &range_key, bool &found);
int handler_range_next(THD *thd, ExecTable *exec_table, bool &found);

MYSQL_LOCK *handler_lock_table(THD *thd, ExecTable *exec_table,
                               thr_lock_type lock_mode);

}  // namespace rpc_executor
