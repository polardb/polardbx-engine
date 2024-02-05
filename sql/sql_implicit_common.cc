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

#include "sql/create_field.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/mysqld.h"
#include "sql/sql_alter.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"

#include "sql/sql_implicit_common.h"

/* Implicit column name in TABLE object */
const LEX_CSTRING IMPLICIT_COLUMN_NAME = {
    C_STRING_WITH_LEN("__#alibaba_rds_row_id#__")};

/* Implicit column comment in TABLE object */
const LEX_CSTRING IMPLICIT_COLUMN_COMMENT = {
    C_STRING_WITH_LEN("Implicit Primary Key by RDS")};

LEX_CSTRING IMPLICIT_COLUMN_COMMENT_NO_CONST = {
    const_cast<char*>(IMPLICIT_COLUMN_COMMENT.str), IMPLICIT_COLUMN_COMMENT.length};

LEX_CSTRING IMPLICIT_NULL_STRING_CONST = {NULL, 0};

/**
  PARTITION ALTER isn't the whole table structure change,
  so disable IPK logic when alter partition.
*/
const uint IMPLICIT_NOT_SUPPORT_PARTITION_ALTER =
    (Alter_info::ALTER_ADD_PARTITION | Alter_info::ALTER_DROP_PARTITION |
     Alter_info::ALTER_COALESCE_PARTITION |
     Alter_info::ALTER_REORGANIZE_PARTITION | Alter_info::ALTER_PARTITION |
     Alter_info::ALTER_ADMIN_PARTITION | Alter_info::ALTER_REBUILD_PARTITION |
     Alter_info::ALTER_TRUNCATE_PARTITION | Alter_info::ALTER_ALL_PARTITION |
     Alter_info::ALTER_REMOVE_PARTITIONING);

/**
  Judge whether the storage engine support auto_increment

  @retval   true      Support
  @retval   false     Not support
*/
bool is_handler_support_implicit_autoinc(handler *file) {
  return !(file->ha_table_flags() & HA_NO_AUTO_INCREMENT);
}

/**
  Judge whether deny the implicit object for ALTER.

  @retval   true      deny
  @retval   false     allow
*/
bool deny_table_add_implicit_ipk(THD *thd, handler *file,
                                 Alter_info *alter_info) {
  // Skip the install db;
  if (!opt_enable_implicit_row_id || opt_initialize) return true;

  // Skip the dd restart that will create table metadata.
  if (thd->is_dd_system_thread() || thd->is_initialize_system_thread())
    return true;

  // Check whether SE support auto_increment.
  if (!is_handler_support_implicit_autoinc(file))
    return true;

  // deny if SE didn't support keys.
  if (file->max_keys() == 0) return true;

  // Not support partition operation.
  if (alter_info->flags & IMPLICIT_NOT_SUPPORT_PARTITION_ALTER) return true;

  return false;
}

/**
  Judge whether CREATE table need and support to
  add implicit object.

  @retval   true      Need
  @retval   false     Not need or not support
*/
bool is_support_and_need_implicit_ipk(THD *thd, handler *file,
                                      HA_CREATE_INFO *create_info,
                                      Alter_info *alter_info) {
  if (deny_table_add_implicit_ipk(thd, file, alter_info)) return false;

  // Not support temporary table
  if (create_info->options & HA_LEX_CREATE_TMP_TABLE) return false;

  // If table contains PK/UK or the same IPK name, then needn't IPK.
  for (size_t key_counter = 0; key_counter < alter_info->key_list.size();
       key_counter++) {
    const Key_spec *key = alter_info->key_list[key_counter];
    if (key->type == KEYTYPE_PRIMARY || key->type == KEYTYPE_UNIQUE ||
        NAME_IS_IMPLICIT(key->name.str))
      return false;
  }

  /** If table contain auto_increment or field
      that has the same IPK name, it needn't */
  for (const Create_field &it : alter_info->create_list) {
    if ((it.auto_flags & Field::NEXT_NUMBER) || NAME_IS_IMPLICIT(it.field_name))
      return false;
  }
  return true;
}

/**
  Add implicit column and corresponding index

  @retval     true    Success
  @retval     false   Failure
*/
bool add_implicit_ipk(THD *thd, Alter_info *alter_info,
                      List<Create_field> *create_list,
                      Mem_root_array<Key_spec *> *key_list) {
  Create_field *new_field = nullptr;
  List<Create_field> *tmp_create_list = nullptr;
  Mem_root_array<Key_spec *> *tmp_key_list = nullptr;
  List<Key_part_spec> key_col_list;
  Key_spec *key = nullptr;
  List<String> *interval_list = nullptr;
  uint type_modifier = 0;

  tmp_create_list = create_list ? create_list : &(alter_info->create_list);
  tmp_key_list = key_list ? key_list : &(alter_info->key_list);

  type_modifier |= (MULTIPLE_KEY_FLAG | AUTO_INCREMENT_FLAG | NOT_NULL_FLAG);

  if (!(new_field = new (thd->mem_root) Create_field()) ||
      new_field->init(thd, IMPLICIT_COLUMN_NAME.str, MYSQL_TYPE_LONGLONG,
                      nullptr, nullptr, type_modifier, nullptr, nullptr,
                      &IMPLICIT_COLUMN_COMMENT_NO_CONST, nullptr,
                      interval_list, &my_charset_bin, false, 0,
                      nullptr, nullptr, std::optional<gis::srid_t>(),
                      dd::Column::enum_hidden_type::HT_VISIBLE, false)) {
    return true;
  }

  // Add implicit column.
  tmp_create_list->push_back(new_field);
  alter_info->flags |= alter_info->ALTER_ADD_COLUMN;

  // add implicit key.
  key_col_list.push_back(new (thd->mem_root)
                             Key_part_spec(IMPLICIT_COLUMN_NAME, 0, ORDER_ASC));

  if (!(key = new (thd->mem_root) Key_spec(
            thd->mem_root, KEYTYPE_MULTIPLE, IMPLICIT_NULL_STRING_CONST,
            &default_key_create_info, false, false, key_col_list)))
    return true;

  tmp_key_list->insert(tmp_key_list->cbegin(), key);
  alter_info->flags |= alter_info->ALTER_ADD_INDEX;
  return false;
}

/** Drop the implicit object

  @retval     true      Not found
  @retval     false     Success
*/
bool drop_implicit_ipk(Alter_info *alter_info, HA_CREATE_INFO *create_info,
                       List<Create_field> *create_list,
                       Mem_root_array<Key_spec *> *key_list) {
  List_iterator<Create_field> def_it(*create_list);
  bool found_ipk = false;

  for (size_t key_counter = 0; key_counter < key_list->size(); key_counter++) {
    const Key_spec *key = key_list->at(key_counter);
    if (key->columns.size() == 1) {
      Key_part_spec *key_part = key->columns[0];
      if (NAME_IS_IMPLICIT(key_part->get_field_name())) {
        found_ipk = true;
        key_list->erase(key_counter);
        break;
      }
    }
  }

  if (found_ipk) {
    Create_field *def;
    def_it.rewind();
    while ((def = def_it++)) {
      if (NAME_IS_IMPLICIT(def->field_name)) {
        def_it.remove();
        break;
      }
    }
    /* Reset auto_increment value because it was dropped */
    create_info->auto_increment_value = 0;
    create_info->used_fields |= HA_CREATE_USED_AUTO;

    alter_info->flags |= alter_info->ALTER_DROP_COLUMN;
  } else {
    return true;
  }

  return false;
}

/**
  Fill NULL value into implicit field when insert.

  @retval     true    Failure
  @retval     false   Success
*/
bool fill_implicit_field_with_null(THD *thd, Field *field) {
  Item *item = new (thd->mem_root) Item_null();
  return (item->save_in_field(field, false) ==
              TYPE_ERR_NULL_CONSTRAINT_VIOLATION ||
          thd->is_error());
}

#ifndef DBUG_OFF
bool debug_field_is_implicit_and_hide(THD *, TABLE *table, Field *field) {
  bool is_implicit_name = NAME_IS_IMPLICIT(field->field_name);
  if (is_implicit_name) {
    MY_UNUSED(table);
    assert(table->s->has_implicit_row_id);
    assert(field->is_implicit());
  } else {
    assert(!(field->is_implicit()));
  }

  return true;
}

bool debug_key_is_implicit_and_hide(THD *, TABLE *table, KEY *key) {
  bool is_implicit_name = NAME_IS_IMPLICIT(key->name);
  if (is_implicit_name) {
    MY_UNUSED(table);
    assert(table->s->has_implicit_row_id);
    assert(key->is_implicit());
  } else {
    assert(!(key->is_implicit()));
  }

  return true;
}
#endif
