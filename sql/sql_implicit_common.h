/* Copyright (c) 2000, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_SQL_IMPLICIT_COMMON_H_INCLUDED
#define SQL_SQL_IMPLICIT_COMMON_H_INCLUDED

#include "m_string.h"

#include "sql/key_spec.h"
#include "sql/mem_root_array.h"

struct TABLE_SHARE;
class Field;
class handler;
class Alter_info;
class THD;
class KEY;

extern const LEX_CSTRING IMPLICIT_COLUMN_NAME;

extern const LEX_CSTRING IMPLICIT_COLUMN_COMMENT;

extern const uint IMPLICIT_NOT_SUPPORT_PARTITION_ALTER ;

extern bool opt_enable_implicit_row_id;

#define SESSION_SHOW_IPK(thd) ((thd) && ((thd)->variables.opt_enable_show_ipk_info))

#define NAME_IS_IMPLICIT(name)                                                 \
  ((name) &&                                                                   \
   !my_strcasecmp(system_charset_info, (name), IMPLICIT_COLUMN_NAME.str))

/* Need to be hidden if table has implicit row and didn't show it */
#define TABLE_HAS_IPK_AND_HIDDEN(thd, table)                                   \
  ((!(SESSION_SHOW_IPK(thd))) && (table)->s->has_implicit_row_id)

/* For the I_S views */
#define ITEM_IS_IMPLICIT_AND_HIDDEN(thd, name) \
  (!(SESSION_SHOW_IPK(thd)) && NAME_IS_IMPLICIT(name))

/**
  Judge the field or key is implicit object and current session
  show_ipk_info setting */
template <typename T>
extern bool item_is_implicit_and_hide(THD *thd, T *item) {
  if (item == nullptr) return false;
  return (!(SESSION_SHOW_IPK(thd)) && item->is_implicit());
}

/**
  Judge whether the storage engine support auto_increment

  @retval   true      Support
  @retval   false     Not support
*/
extern bool is_handler_support_implicit_autoinc(handler *file);

/**
  Judge whether deny the implicit ipk for ALTER.

  @retval   true      deny
  @retval   false     allow
*/
bool deny_table_add_implicit_ipk(THD *thd, handler *file,
                                 Alter_info *alter_info);

/**
  Judge whether CREATE table need and support to
  add implicit object.

  @retval   true      Need
  @retval   false     Not need or not support
*/
extern bool is_support_and_need_implicit_ipk(THD *thd, handler *file,
                                             HA_CREATE_INFO *create_info,
                                             Alter_info *alter_info);

/**
  Add implicit column and corresponding index

  @retval     true    Success
  @retval     false   Failure
*/
extern bool add_implicit_ipk(THD *thd, Alter_info *alter_info,
                             List<Create_field> *create_list,
                             Mem_root_array<Key_spec *> *key_list);

/** Drop the implicit object

  @retval     true      Not found
  @retval     false     Success
*/
extern bool drop_implicit_ipk(Alter_info *alter_info,
                              HA_CREATE_INFO *create_info,
                              List<Create_field> *create_list,
                              Mem_root_array<Key_spec *> *key_list);

/**
  Fill NULL value into implicit field when insert.

  @retval     true    Failure
  @retval     false   Success
*/
bool fill_implicit_field_with_null(THD *thd, Field *field);

#ifndef DBUG_OFF
bool debug_field_is_implicit_and_hide(THD *, TABLE *table, Field *field);
bool debug_key_is_implicit_and_hide(THD *, TABLE *table, KEY *key);
#endif

#endif

