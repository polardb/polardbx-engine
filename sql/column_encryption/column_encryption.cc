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
/** @file sql/column_encryption/column_encryption.cc

  Column Encryption Implementation
  Created 2025-05-19 by Zefeng.Liu
 *******************************************************/

#include "sql/column_encryption/column_encryption.h"
#include "sql/column_encryption/encdb_service.h"
#include "sql/column_encryption/item_encryption.h"
#include "sql/sql_class.h"
namespace komodo {

bool opt_enable_column_encryption = false;
bool opt_mask_ai_model_config_api_key = true;

/**
 * @brief Get the encryption type for field object
 *
 * @param field[in] The field object.
 * @return The encryption type.
 */
Encryption_type get_encryption_type_for_field(Field *field) {
  /**
   * The encrytion type could comes from three sources:
   * 1. If the field is a column of mysql.user table, some encryption types have
   *    been set when calling fill_column_from_dd().
   * 2. If the field is encrypted by encdb, we can get the encryption type from
   *    the encdb.
   * 3. If the field is mysql.ai_model_config.api_key, its mask type has been
   *    set when calling fill_column_from_dd() and is controlled by
   *    mask_ai_model_config_api_key.
   * Ideally, all fields should get the encryption type when calling
   * fill_column_from_dd(). However, when some encrypted rules are specified by
   * encdb, it' so hard to keep the encryption type in dd_share consistent with
   * the rules in encdb. So we delay the work here.
   */

  Encryption_type ret = field->encryption_type();

  if (!current_thd->enable_mask_internal_user) {
    ret &= (~(COLUMN_ENCRYPTION_TYPE_MASK_INTERNAL_USERS |
              COLUMN_ENCRYPTION_TYPE_MASK_USERS_PASSWORD));
  }

  if (!current_thd->enable_mask_ai_model_config_api_key) {
    ret &= (~COLUMN_ENCRYPTION_TYPE_MASK_AI_API_KEY);
  }

  if (current_thd->enable_column_encryption) {
    st_org_field org_field(field->table->s->db.str,
                           field->table->s->table_name.str, field->field_name,
                           current_thd->security_context()->user().str);
    bool is_encrypted = false;
    RUN_HOOK(encdb, encdb_rule_func, (&org_field, &is_encrypted));

    if (is_encrypted) {
      ret |= COLUMN_ENCRYPTION_TYPE_ENCDB_DEFAULT;
    }
  }

  return ret;
}

/**
 * @brief Create a column encryption item object based on the encryption type.
 * @details:
 * The encryption type is stored in the item->encryption_type(). One item might
 * have multiple encryption types. For example, the item is concat function, and
 * it has two columns with different encryption types. We will create a new item
 * with the most restrictive encryption type.
 *
 * @param thd[in] The current thread.
 * @param item[in] The item to be encrypted.
 * @return The encrypted item.
 */
Item *create_column_encryption_item(THD *thd, Item *item) {
  assert(!item->hidden && item->has_encryption_type());

  Item *encryption_func = nullptr;

  switch (get_real_encryption_type(item->encryption_type())) {
    case COLUMN_ENCRYPTION_TYPE_ENCDB_DEFAULT:
      encryption_func = new (thd->mem_root) Item_func_encdb_column(POS(), item);
      break;
    case COLUMN_ENCRYPTION_TYPE_MASK_AI_API_KEY:
      encryption_func =
          new (thd->mem_root) Item_func_mask_ai_api_key(POS(), item);
      break;
    case COLUMN_ENCRYPTION_TYPE_MASK_USERS_PASSWORD:
      encryption_func =
          new (thd->mem_root) Item_func_mask_users_password(POS(), item);
      break;
    case COLUMN_ENCRYPTION_TYPE_MASK_INTERNAL_USERS:
      encryption_func =
          new (thd->mem_root) Item_func_mask_internal_user(POS(), item);
      break;
    case COLUMN_ENCRYPTION_TYPE_NONE:
      break;
    default:
      assert(false);
  }
  return encryption_func;
}

/**
 * @brief Encrypt the result set if need.
 * @details:
 * Here, we check all visible columns in the result set to clients to
 * determine if we need to encrypt. If the result set is not for clients, e.g.
 * insert ... select, we just return.
 *
 * Otherwise, for each visible column, if it is specified with Mask
 * Algorithm(i.e mask_internal_user or mask_users_password), we will encrypt it
 * by creating a new Item_func_xxx and replace the original Item.
 *
 * If the SQL is a prepared statement, the fields are prepared in advance and
 * reused every time we execute the statement. So, we will copy the fields at
 * first and do the replacement.
 *
 * @param thd[in] The current thread.
 * @param query_result[in] The query result.
 * @param fields[in/out] The fields of the result set.
 */
void encrypt_result_set_if_need(THD *thd, Query_result *query_result,
                                mem_root_deque<Item *> *&fields) {
  /** Only for result to users. PolarX RPC authenticates by reading
   * mysql.user.authentication_string with its internal system user, so that
   * session must see the real password hash. */
  if (dynamic_cast<Query_result_send *>(query_result) == nullptr ||
      thd->is_polarx_sys_user) {
    return;
  }

  bool need_encrypt = false;
  for (auto it = fields->begin(); it != fields->end(); ++it) {
    Item *item = *it;
    if (!item->hidden && item->has_encryption_type()) {
      need_encrypt = true;
      break;
    }
  }

  if (!need_encrypt) return;

  mem_root_deque<Item *> *fields_list =
      new (thd->mem_root) mem_root_deque<Item *>(thd->mem_root);
  for (auto it = fields->begin(); it != fields->end(); ++it) {
    Item *item = *it;
    Item **item_pos = &*it;
    if (!item->hidden && item->has_encryption_type()) {
      Item *encryption_func = create_column_encryption_item(thd, item);
      /** Keep name and original name same */
      encryption_func->item_name.set(item->item_name.ptr());
      encryption_func->orig_name.set(item->orig_name.ptr());
      encryption_func->fix_fields(thd, item_pos);
      fields_list->push_back(encryption_func);
      /** To avoid memory leak, we need to add it to the query arena */
      thd->add_item(encryption_func);
    } else {
      fields_list->push_back(item);
    }
  }
  fields = fields_list;
}
}  // namespace komodo
