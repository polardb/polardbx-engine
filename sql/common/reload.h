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

#ifndef SQL_COMMON_RELOAD_INCLUDED
#define SQL_COMMON_RELOAD_INCLUDED

#include "map_helpers.h"
#include "mysql/status_var.h"

class THD;
struct TABLE_SHARE;
class Transaction_ctx;

namespace im {

/**
  All the reload type, pls add new type if support more.
*/
enum Reload_type { RELOAD_OUTLINE = 0, MAX_RELOAD_ENTRY_COUNT };

/**
  Show status for reload operations.
*/
extern SHOW_VAR reload_entry_status[];

/**
  Base class for different reload type.
*/
class Reload {
 public:
  Reload() {}
  virtual ~Reload() {}

  virtual void execute(THD *thd) = 0;

  virtual Reload_type type() const = 0;
};

using Reload_entries = std::map<std::string, const Reload *>;

/**
  Register the reload entry

  @param[in]      key       DB + '\0' + table_name + '\0'
  @param[in]      len       key length = db_len + table_len + 2
  @param[in]      entry     Reload object
*/
extern void register_reload_entry(const char *key, std::size_t len,
                                  const Reload *entry);

/**
  Remove reload entry by key.

  @param[in]      key       DB + '\0' + table_name + '\0'
  @param[in]      len       key length = db_len + table_len + 2
*/
extern void remove_reload_entry(const char *key, std::size_t len);

/**
  Look up reload entry by key.

  @param[in]      key       DB + '\0' + table_name + '\0'
  @param[in]      len       key length = db_len + table_len + 2

  @retval         entry     Reload entry
*/
extern const Reload *lookup_reload_entry(const char *key, std::size_t len);

/**
  Look up reload entry by db and table.

  @param[in]      db            db name
  @param[in]      db_len        db name length
  @param[in]      table         table name
  @param[in]      table_length  table name length

  @retval         entry     Reload entry
*/
extern const Reload *lookup_reload_entry(const char *db, std::size_t db_len,
                                         const char *table_name,
                                         std::size_t table_name_len);

/**
  Execute the reload operation when transaction commit.
  @param[in]      thd           slave thread
  @param[in]      ctx           transaction info
*/
void execute_reload_on_slave(THD *thd, Transaction_ctx *ctx);

/**
  Mark the reload entry in thd->transaction_info object,
  so it can execute corresponding operation  when transaction
  commit on slave;

  @param[in]      thd           slave thread
  @param[in]      ctx           transaction info
  @param[in]      table_share   TABLE_SHARE object
*/
extern void mark_trx_reload(THD *thd, Transaction_ctx *ctx,
                            TABLE_SHARE *table_share);
} /* namespace im */

#endif
