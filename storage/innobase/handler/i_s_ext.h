/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.

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

/** @file handler/i_s_ext.h
 Information of data file operation.

 Created 5/14/2019 Galaxy SQL
 *******************************************************/

#ifndef i_s_file_h
#define i_s_file_h

#include <sys/types.h>
#include <time.h>

#include "fil0fil.h"
#include "sql/table.h"

#include "univ.i"

class Field;
class THD;
class Table_ref;
class Item;

extern struct st_mysql_plugin i_s_innodb_data_file_purge;
extern struct st_mysql_plugin i_s_innodb_tablespace_master_key;
extern struct st_mysql_plugin i_s_innodb_table_status;
extern struct st_mysql_plugin i_s_innodb_gpp_stats;
extern struct st_mysql_plugin i_s_innodb_index_status;

/* Defined with in 'handler/i_s.cc' */
extern int field_store_string(Field *field, const char *str);

/* Defined with in 'handler/i_s.cc' */
extern int field_store_time_t(Field *field, time_t time);

/* Defined with in 'handler/i_s.cc' */
extern const char *i_s_index_page_type_to_str(page_type_t page_type);

#endif
