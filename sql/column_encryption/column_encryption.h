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
/** @file sql/column_encryption/column_encryption.h

  Column Encryption
  Created 2025-05-19 by Zefeng.Liu
 *******************************************************/

#ifndef SQL_COLUMN_ENCRYPTION_COLUMN_ENCRYPTION_H
#define SQL_COLUMN_ENCRYPTION_COLUMN_ENCRYPTION_H
#include "column_encryption_type.h"
#include "mem_root_deque.h"

class Item;
class THD;
class Field;
class Query_result;

namespace komodo {

extern bool opt_enable_column_encryption;
extern bool opt_mask_ai_model_config_api_key;

/**
 * @brief Get the encryption type for field object
 *
 * @param field[in] The field object.
 * @return The encryption type.
 */
Encryption_type get_encryption_type_for_field(Field *field);

/**
 * @brief Create a column encryption item object based on the encryption type
 * @details:
 * The encryption type is stored in the item->encryption_type(). One item might
 * have multiple encryption types. For example, the item is concat function, and
 * it has two columns with different encryption types. We will create a new item
 * with the most restrictive encryption type.
 * @param thd[in] The current thread.
 * @param item[in] The item to be encrypted.
 * @return The encrypted item.
 */
Item *create_column_encryption_item(THD *thd, Item *item);

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
                                mem_root_deque<Item *> *&fields);
}  // namespace komodo

#endif  // SQL_COLUMN_ENCRYPTION_COLUMN_ENCRYPTION_H
