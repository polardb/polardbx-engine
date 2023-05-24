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

#include "sql/ppi/ppi_table_iostat.h"

#include "ppi/ppi_global.h"
#include "sql/field.h"  // store
#include "sql/sql_show.h"
#include "sql/table.h"
#include "sql/tztime.h"  // gmt_sec_to_TIME

/* Table IO_STATISTICS fields information */
ST_FIELD_INFO table_iostat_fields_info[] = {
    {"TIME", 0, MYSQL_TYPE_DATETIME, 0, 0, "Time", 0},
    {"DATA_READ", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Data_read", 0},
    {"DATA_READ_TIME", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Data_read_time", 0},
    {"DATA_READ_MAX_TIME", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, "Data_read_max_time", 0},
    {"DATA_READ_BYTES", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Data_read_bytes", 0},
    {"DATA_WRITE", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Data_write", 0},
    {"DATA_WRITE_TIME", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Data_write_time", 0},
    {"DATA_WRITE_MAX_TIME", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, "Data_write_max_time", 0},
    {"DATA_WRITE_BYTES", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Data_write_bytes", 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

/* Fill the io statistic rows */
int fill_schema_table_iostat(THD *thd, Table_ref *tables,
                             Item *__attribute__((unused))) {
  bool first_read = true;
  int index = 0;
  PPI_iostat_data stat;
  MYSQL_TIME time;
  TABLE *table = tables->table;

  PPI_GLOBAL_CALL(get_io_statistic)(&stat, first_read, &index);

  while (index != -1) {
    first_read = false;

    restore_record(table, s->default_values);
    my_tz_SYSTEM->gmt_sec_to_TIME(&time, (my_time_t)stat.time);

    table->field[0]->store_time(&time, true);
    table->field[1]->store((longlong)stat.data_read, true);
    table->field[2]->store((longlong)stat.data_read_time, true);
    table->field[3]->store((longlong)stat.data_read_max_time, true);
    table->field[4]->store((longlong)stat.data_read_bytes, true);
    table->field[5]->store((longlong)stat.data_write, true);
    table->field[6]->store((longlong)stat.data_write_time, true);
    table->field[7]->store((longlong)stat.data_write_max_time, true);
    table->field[8]->store((longlong)stat.data_write_bytes, true);

    if (schema_table_store_record(thd, table)) return 1;

    PPI_GLOBAL_CALL(get_io_statistic)(&stat, first_read, &index);
  }

  return 0;
}
