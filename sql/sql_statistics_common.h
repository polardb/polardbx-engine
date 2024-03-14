/* Copyright (c) 2000, 2018, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_STATISTICS_COMMON_INCLUDED
#define SQL_STATISTICS_COMMON_INCLUDED

#include "my_inttypes.h"
#include "sql/sql_const.h"  // MAX_KEY

struct ST_FIELD_INFO;
class THD;
class Table_ref;
class Item;

/*All the extern variables and functions */
extern bool opt_tablestat;
extern bool opt_indexstat;

extern ST_FIELD_INFO table_stats_fields_info[];
extern ST_FIELD_INFO index_stats_fields_info[];

extern int fill_schema_table_stats(THD *thd, Table_ref *tables,
                                   Item *__attribute__((unused)));

extern int fill_schema_index_stats(THD *thd, Table_ref *tables,
                                   Item *__attribute__((unused)));

extern void object_statistics_context_init();
extern void object_statistics_context_destroy();

/**
  These structure will be used by sql_statistics and handler, so defined it
  here, in order to decrease the compile dependency.
*/

typedef struct Stats_data {
 public:
  ulonglong rows_read;
  ulonglong rows_changed;
  ulonglong rows_deleted;
  ulonglong rows_inserted;
  ulonglong rows_updated;
  ulonglong rds_rows_read_del_mark;
  ulonglong index_rows_read[MAX_KEY];
  ulonglong index_scan_used[MAX_KEY];

  Stats_data() { reset(); }

  void reset_table() {
    rows_read = rows_changed = rows_deleted = rows_inserted = rows_updated = 0;
    rds_rows_read_del_mark = 0;
  }
  void reset_index() {
    for (size_t i = 0; i < MAX_KEY; i++) {
      index_rows_read[i] = 0;
      index_scan_used[i] = 0;
    }
  }
  void reset_index(size_t i) {
    if (i < MAX_KEY) index_rows_read[i] = 0;
    if (i < MAX_KEY) index_scan_used[i] = 0;
  }
  void reset() {
    reset_table();
    reset_index();
  }
} Stats_data;

#endif
