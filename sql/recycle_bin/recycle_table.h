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

#ifndef SQL_RECYCLE_BIN_RECYCLE_TABLE_INCLUDED
#define SQL_RECYCLE_BIN_RECYCLE_TABLE_INCLUDED

#include <set>
#include <vector>
#include "lex_string.h"
#include "m_ctype.h"
#include "sql/error_handler.h"
#include "sql/mdl.h"
#include "sql/tztime.h"

class handlerton;
class THD;
class Foreign_key_parents_invalidator;

namespace im {
namespace recycle_bin {

extern LEX_CSTRING RECYCLE_BIN_SCHEMA;

/* Seconds before really purging the recycled table. */
extern ulonglong recycle_bin_retention;

/* Whether the db is recycle schema */
inline bool is_recycle_db(const char *str) {
  return !my_strcasecmp(system_charset_info, RECYCLE_BIN_SCHEMA.str, str);
}

/* Recycle table result */
enum class Recycle_result { OK, DROP_CONTINUE, ERROR };
/**
  Recycle error handling.
*/
class Recycle_error_handler : public Internal_error_handler {
 public:
  Recycle_error_handler(THD *thd, Table_ref *table_list);
  virtual bool handle_condition(THD *, uint sql_errno, const char *,
                                Sql_condition::enum_severity_level *,
                                const char *message) override;

  virtual ~Recycle_error_handler();

  bool is_error() { return m_error; }

 private:
  THD *m_thd;
  Table_ref *m_table_list;
  bool m_error;
};
/**
  Recycle context.
*/
class Recycle_context_wrapper {
 public:
  explicit Recycle_context_wrapper(THD *thd, Table_ref *table_list);

  virtual ~Recycle_context_wrapper();

 private:
  THD *m_thd;
  MDL_savepoint m_mdl_savepoint;
  Recycle_error_handler m_error_handler;
};

/**
  Fetch all tables should set timezone offset = 0.
*/
class Timestamp_timezone_guard {
 public:
  Timestamp_timezone_guard(THD *thd);

  ~Timestamp_timezone_guard();

 private:
  ::Time_zone *m_tz;
  THD *m_thd;
};

/**
  Rename the table into recycle_bin schema, and drop related object,
  Only keep the fundamental table elements, drop FK , triggers.

  Logic of dealing with table elements.

  1) View:
     Didn't update view metadata, treat as invalid.
  2) drop FK
  3) drop trigger
  4) left table stats & index stats
  5) histograms
     Rename column statistics

  @param[in]      thd             thread context
  @param[in]      post_ddl_htons  atomic hton container
  @param[in]      table_list      dropping table

  @retval         ok              Success
  @retval         drop_continue   Should continue to drop table
  @retval         error           Report client error
*/
Recycle_result recycle_base_table(
    THD *thd, std::set<handlerton *> *post_ddl_htons,
    Foreign_key_parents_invalidator *fk_invalidator, Table_ref *table_list);

/**
  dbms_recycle.show_tables result
*/
struct Recycle_show_result {
  LEX_STRING schema;
  LEX_STRING table;
  LEX_STRING origin_schema;
  LEX_STRING origin_table;
  ulonglong recycled_time;

 public:
  Recycle_show_result() {
    schema = {nullptr, 0};
    table = {nullptr, 0};
    origin_schema = {nullptr, 0};
    origin_table = {nullptr, 0};
    recycled_time = 0;
  }
};

/**
  Retrieve all recycle schema tables.

  @param[in]        thd         thread context
  @param[in]        mem_root    Memory pool
  @param[in/out]    container   show table result container

  @retval           true        Error
  @retval           false       success
*/
bool get_recycle_tables(THD *thd, MEM_ROOT *mem_root,
                        std::vector<Recycle_show_result *> *container);

/**
  Purge the table in recycle_bin

  @param[in]      thd       thread context
  @param[in]      table     Target table name

  @retval         false     success
  @retval         true      failure
*/
bool recycle_purge_table(THD *thd, const char *table);

/**
  drop the table in recycle_bin

  @param[in]      thd       thread context
  @param[in]      table     Target table name

  @retval         false     success
  @retval         true      failure
*/
bool drop_base_recycle_table(THD *thd, const char *table);
/**
  Build table_list object to check access conveniently.

  @param[in]      thd       thread context
  @param[in]      db        db string
  @param[in]      db_len    db string length
  @param[in]      table     table name string
  @param[in]      table_len table name string length

  @retval         table_list      single table list object
*/
Table_ref *build_table_list(THD *thd, const char *db, size_t db_len,
                            const char *table, size_t table_len);
} /* namespace recycle_bin */
} /* namespace im */

/* Only declare here, definition see sql/sql_table.cc */
bool rename_histograms(THD *thd, const char *old_schema_name,
                       const char *old_table_name, const char *new_schema_name,
                       const char *new_table_name);

#endif
