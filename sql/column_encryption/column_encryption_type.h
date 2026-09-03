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
/** @file sql/column_encryption/column_encryption_type.h

  Column Encryption Type
  Created 2025-05-14 by Zefeng.Liu
 *******************************************************/

#ifndef SQL_COLUMN_ENCRYPTION_COLUMN_ENCRYPTION_TYPE_H
#define SQL_COLUMN_ENCRYPTION_COLUMN_ENCRYPTION_TYPE_H
#include "my_inttypes.h"

namespace komodo {

typedef uint16 Encryption_type;

/** Here, we define the column encryption type. The reason why we define it as
 * bitmap is that one item func might have multiple column encryption types.
 * For example, one item func might has two columns, one is encrypted with
 * mask_internal_user and the other is encrypted with mask_users_password.
 * In this case, the item func should have both the column encryption type of
 * mask_internal_user and mask_users_password. When we really need to encrypt
 * the item func, we should adopt the most restrictive encryption type rather
 * than choose one randomly, which is not secure.
 *
 * The restrictive order is as follows:
 * 1. COLUMN_ENCRYPTION_TYPE_MASK_AI_API_KEY
 * 2. COLUMN_ENCRYPTION_TYPE_ENCDB_DEFAULT
 * 3. COLUMN_ENCRYPTION_TYPE_MASK_USERS_PASSWORD
 * 4. COLUMN_ENCRYPTION_TYPE_MASK_INTERNAL_USERS
 * 5. COLUMN_ENCRYPTION_TYPE_NONE
 */
constexpr Encryption_type COLUMN_ENCRYPTION_TYPE_NONE = 0x0000;
constexpr Encryption_type COLUMN_ENCRYPTION_TYPE_MASK_USERS_PASSWORD = 0x0001;
constexpr Encryption_type COLUMN_ENCRYPTION_TYPE_MASK_INTERNAL_USERS = 0x0002;
constexpr Encryption_type COLUMN_ENCRYPTION_TYPE_ENCDB_DEFAULT = 0x0004;
constexpr Encryption_type COLUMN_ENCRYPTION_TYPE_MASK_AI_API_KEY = 0x0008;

inline bool has_encryption_type(const Encryption_type type) {
  return type != COLUMN_ENCRYPTION_TYPE_NONE;
}

inline bool has_encdb_default_type(const Encryption_type type) {
  return type & COLUMN_ENCRYPTION_TYPE_ENCDB_DEFAULT;
}

inline bool has_mask_ai_api_key_type(const Encryption_type type) {
  return type & COLUMN_ENCRYPTION_TYPE_MASK_AI_API_KEY;
}

inline bool has_mask_users_password_type(const Encryption_type type) {
  return type & COLUMN_ENCRYPTION_TYPE_MASK_USERS_PASSWORD;
}

inline bool has_mask_internal_users_type(const Encryption_type type) {
  return type & COLUMN_ENCRYPTION_TYPE_MASK_INTERNAL_USERS;
}

/**
 * @brief Decide the real encryption type (i.e most restrictive type)
 *
 * @param type[in] The encryption type.
 * @return The real encryption type.
 */
inline Encryption_type get_real_encryption_type(const Encryption_type type) {
  if (has_mask_ai_api_key_type(type)) {
    return COLUMN_ENCRYPTION_TYPE_MASK_AI_API_KEY;
  } else if (has_encdb_default_type(type)) {
    return COLUMN_ENCRYPTION_TYPE_ENCDB_DEFAULT;
  } else if (has_mask_users_password_type(type)) {
    return COLUMN_ENCRYPTION_TYPE_MASK_USERS_PASSWORD;
  } else if (has_mask_internal_users_type(type)) {
    return COLUMN_ENCRYPTION_TYPE_MASK_INTERNAL_USERS;
  }
  return COLUMN_ENCRYPTION_TYPE_NONE;
}

}  // namespace komodo
#endif