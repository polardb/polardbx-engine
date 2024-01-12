/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/common/reload.h"
#include "sql/error_handler.h"
#include "sql/sql_class.h"
#include "sql/system_variables.h"
#include "sql/table.h"

#include <sstream>

namespace im {

/* Global container for all reload entries. */
Reload_entries reload_entries;

/**
  Show status for reload operations.
*/
SHOW_VAR reload_entry_status[] = {
    {"outline",
     (char *)offsetof(System_status_var, reload_stat[(uint)RELOAD_OUTLINE]),
     SHOW_LONG_STATUS, SHOW_SCOPE_ALL},
    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_ALL}};

/**
  Ignore the error when reload.
*/
class Reload_error_handler : public Dummy_error_handler {
 public:
  Reload_error_handler(THD *thd) : m_thd(thd) {
    m_thd->push_internal_handler(this);
  }

  virtual ~Reload_error_handler() { m_thd->pop_internal_handler(); }

 private:
  THD *m_thd;
};

/**
  Register the reload entry

  @param[in]      key       DB + '\0' + table_name + '\0'
  @param[in]      len       key length = db_len + table_len + 2
  @param[in]      entry     Reload object
*/
void register_reload_entry(const char *key, std::size_t len,
                           const Reload *entry) {
  reload_entries.insert(
      std::pair<std::string, const Reload *>(std::string(key, len), entry));
}

/**
  Look up reload entry by key.

  @param[in]      key       DB + '\0' + table_name + '\0'
  @param[in]      len       key length = db_len + table_len + 2

  @retval         entry     Reload entry
*/
const Reload *lookup_reload_entry(const char *key, std::size_t len) {
  auto it = reload_entries.find(std::string(key, len));
  if (it == reload_entries.end())
    return nullptr;
  else
    return it->second;
}

/**
  Look up reload entry by db and table.

  @param[in]      db            db name
  @param[in]      db_len        db name length
  @param[in]      table         table name
  @param[in]      table_length  table name length

  @retval         entry     Reload entry
*/
const Reload *lookup_reload_entry(const char *db, std::size_t db_len,
                                  const char *table_name,
                                  std::size_t table_name_len) {
  std::stringstream ss;
  ss << db << '\0' << table_name << '\0';
  std::string key = ss.str();
  assert(key.length() == (db_len + table_name_len + 2));
  (void)db_len;
  (void)table_name_len;

  auto it = reload_entries.find(key);
  if (it == reload_entries.end())
    return nullptr;
  else
    return it->second;
}
/**
  Remove reload entry by key.

  @param[in]      key       DB + '\0' + table_name + '\0'
  @param[in]      len       key length = db_len + table_len + 2
*/
void remove_reload_entry(const char *key, std::size_t len) {
  auto it = reload_entries.find(std::string(key, len));
  if (it == reload_entries.end())
    return;
  else {
    delete it->second;
    reload_entries.erase(it);
  }
}

/**
  Execute the reload operation when transaction commit.
  @param[in]      thd           slave thread
  @param[in]      ctx           transaction info
*/
void execute_reload_on_slave(THD *thd, Transaction_ctx *ctx) {
  if (thd->slave_thread && !thd->is_error() && ctx) {
    Reload **entry = ctx->reload_entries;
    /* Ignore the error */
    Reload_error_handler error_handler(thd);
    for (uint i = RELOAD_OUTLINE; i < MAX_RELOAD_ENTRY_COUNT; i++, entry++) {
      if (*entry) {
        (*entry)->execute(thd);
        /* Aggregate status */
        thd->status_var.reload_stat[i]++;
        ctx->reload_entries[i] = nullptr;
      }
    }
  }
}

/**
  Mark the reload entry in thd->transaction_info object,
  so it can execute corresponding operation  when transaction
  commit on slave;

  @param[in]      thd           slave thread
  @param[in]      ctx           transaction info
  @param[in]      table_share   TABLE_SHARE object
*/
void mark_trx_reload(THD *thd, Transaction_ctx *ctx, TABLE_SHARE *table_share) {
  const Reload *entry = nullptr;
  /* Require slave thread */
  if (thd->slave_thread && table_share &&
      ((entry = table_share->reload_entry)) && ctx) {
    Reload_type type = entry->type();
    ctx->reload_entries[type] = const_cast<Reload *>(entry);
  }
}

} /* namespace im */
