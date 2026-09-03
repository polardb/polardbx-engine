/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/** @file sql/column_encryption/item_encryption.h

  Item Encryption
  Created 2025-05-19 by Zefeng.Liu
 *******************************************************/

#ifndef SQL_COLUMN_ENCRYPTION_ITEM_ENCRYPTION_H
#define SQL_COLUMN_ENCRYPTION_ITEM_ENCRYPTION_H
#include "sql/item_strfunc.h"

namespace komodo {

/** Mask internal user */
class Item_func_mask_internal_user final : public Item_str_func {
  String tmp_value;
  static constexpr char mask_string[] = "******";

 public:
  Item_func_mask_internal_user(const POS &pos, Item *a)
      : Item_str_func(pos, a) {}
  String *val_str(String *) override;
  bool resolve_type(THD *thd) override;
  const char *func_name() const override { return "mask_internal_user"; }
  const char *original_func_name() const { return args[0]->full_name(); }
};

/** Base class for columns that should always be displayed as a fixed mask. */
class Item_func_mask_fixed_string : public Item_str_func {
  String tmp_value;
  static constexpr char mask_string[] = "******";

 public:
  Item_func_mask_fixed_string(const POS &pos, Item *a)
      : Item_str_func(pos, a) {}
  String *val_str(String *) override;
  bool resolve_type(THD *thd) override;
  const char *original_func_name() const { return args[0]->full_name(); }
};

/** Mask users password */
class Item_func_mask_users_password final : public Item_func_mask_fixed_string {
 public:
  Item_func_mask_users_password(const POS &pos, Item *a)
      : Item_func_mask_fixed_string(pos, a) {}
  const char *func_name() const override { return "mask_users_password"; }
};

/** Mask mysql.ai_model_config.api_key */
class Item_func_mask_ai_api_key final : public Item_func_mask_fixed_string {
 public:
  Item_func_mask_ai_api_key(const POS &pos, Item *a)
      : Item_func_mask_fixed_string(pos, a) {}
  const char *func_name() const override { return "mask_ai_api_key"; }
};

/** Encrypt column */
class Item_func_encdb_column final : public Item_str_func {
  String tmp_value;
  enum_field_types original_type;  // the original data type of referenced item

 public:
  Item_func_encdb_column(const POS &pos, Item *a)
      : Item_str_func(pos, a), original_type(a->data_type()) {}
  String *val_str(String *) override;
  bool resolve_type(THD *thd) override;
  const char *func_name() const override { return "encdb_column"; }
  const char *original_func_name() const { return args[0]->full_name(); }
};
}  // namespace komodo

#endif
