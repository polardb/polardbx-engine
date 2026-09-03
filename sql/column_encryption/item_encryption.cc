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
/** @file sql/column_encryption/item_encryption.cc

  Item Encryption
  Created 2025-05-19 by Zefeng.Liu
 *******************************************************/
#include "sql/column_encryption/item_encryption.h"
#include "sql/auth/sql_internal_account.h"
#include "sql/column_encryption/encdb_service.h"
#include "sql/current_thd.h"
#include "sql/sql_class.h"

namespace komodo {

String *Item_func_mask_internal_user::val_str(String *str) {
  assert(fixed);
  THD *thd = current_thd;
  null_value = false;
  tmp_value.length(0);

  String *res = eval_string_arg(collation.collation, args[0], str);
  if (res == nullptr) {
    assert(thd->is_error() || (args[0]->null_value && is_nullable()));
    return error_str();
  }

  char user_name[USERNAME_LENGTH + 1];
  size_t copy_len = std::min<size_t>(res->length(), USERNAME_LENGTH);
  memcpy(user_name, res->ptr(), copy_len);
  user_name[copy_len] = '\0';

  if (im::internal_account_need_protected(user_name)) {
    /** mask the username with mask_string. */
    if (tmp_value.append(mask_string)) return error_str();
  } else {
    if (tmp_value.append(user_name)) return error_str();
  }

  if (tmp_value.length() > thd->variables.max_allowed_packet) {
    return push_packet_overflow_warning(thd, original_func_name());
  }

  tmp_value.set_charset(collation.collation);
  return &tmp_value;
}

bool Item_func_mask_internal_user::resolve_type(THD *thd) {
  assert(arg_count == 1);

  if (param_type_is_default(thd, 0, -1)) return true;

  if (agg_arg_charsets_for_string_result(collation, args, arg_count))
    return true;

  set_data_type_string(args[0]->max_char_length(collation.collation));
  set_nullable(is_nullable() || max_length > thd->variables.max_allowed_packet);
  return false;
}

String *Item_func_mask_fixed_string::val_str(String *) {
  assert(fixed);
  THD *thd = current_thd;
  null_value = false;
  tmp_value.length(0);
  if (tmp_value.append(mask_string)) return error_str();

  if (tmp_value.length() > thd->variables.max_allowed_packet) {
    return push_packet_overflow_warning(thd, original_func_name());
  }
  tmp_value.set_charset(collation.collation);
  return &tmp_value;
}

bool Item_func_mask_fixed_string::resolve_type(THD *thd) {
  assert(arg_count == 1);
  if (param_type_is_default(thd, 0, -1)) return true;

  if (agg_arg_charsets_for_string_result(collation, args, arg_count))
    return true;

  set_data_type_string(args[0]->max_char_length(collation.collation));
  set_nullable(is_nullable() || max_length > thd->variables.max_allowed_packet);
  return false;
}

String *Item_func_encdb_column::val_str(String *str) {
  assert(fixed);
  THD *thd = current_thd;
  null_value = false;
  tmp_value.length(0);

  String *res = eval_string_arg(collation.collation, args[0], str);
  if (res == nullptr) {
    assert(thd->is_error() || (args[0]->null_value && is_nullable()));
    return error_str();
  }

  /** Prepare the buffer to store the encrypted data. */
  uint32_t cipher_text_length = 0;
  if (RUN_HOOK(encdb, encdb_expected_length,
               (res->length(), &cipher_text_length))) {
    assert(thd->is_error());
    return error_str();
  }

  if (tmp_value.alloc(cipher_text_length)) {
    return error_str();
  }

  /** Encrypt the data. */
  if (RUN_HOOK(encdb, encdb_encrypt,
               ((uint8_t *)tmp_value.c_ptr(), &cipher_text_length,
                (const uint8_t *)res->c_ptr(), res->length(),
                thd->security_context()->user().str, original_type))) {
    assert(thd->is_error());
    return error_str();
  }
  tmp_value.length(cipher_text_length);

  /** Return the old name to hide this function. */
  if (tmp_value.length() > thd->variables.max_allowed_packet) {
    return push_packet_overflow_warning(thd, original_func_name());
  }

  tmp_value.set_charset(collation.collation);
  return &tmp_value;
}

bool Item_func_encdb_column::resolve_type(THD *thd) {
  assert(arg_count == 1);
  if (param_type_is_default(thd, 0, -1)) return true;

  collation.set(&my_charset_bin);

  uint32_t cipher_text_length = 0;
  uint32_t src_length = args[0]->max_char_length(collation.collation);

  if (RUN_HOOK(encdb, encdb_expected_length,
               (src_length, &cipher_text_length))) {
    assert(thd->is_error());
    return true;
  }

  set_data_type_string(cipher_text_length);
  set_nullable(is_nullable() || max_length > thd->variables.max_allowed_packet);
  return false;
}
}  // namespace komodo