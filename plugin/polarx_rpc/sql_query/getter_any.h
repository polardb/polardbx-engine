/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#pragma once

#include <sstream>
#include <string>
#include <vector>

#include "m_ctype.h"

#include "../coders/protocol_fwd.h"
#include "../helper/str_converter.h"
#include "../utility/error.h"

#include "identifier.h"
#include "raw_binary.h"
#include "raw_string.h"

namespace polarx_rpc {

static inline std::string covert_if_needed(
    const ::PolarXRPC::Datatypes::Scalar_String &string,
    const CHARSET_INFO &charset) {
  CHARSET_INFO *src_charset;
  if (string.has_collation() && string.collation() != charset.number &&
      (src_charset = get_charset(string.collation(), MYF(0))) != nullptr) {
    /// need convert
    const auto &org_str = string.value();
    CconvertIfNecessary converted(&charset, org_str.data(), org_str.length(),
                                  src_charset);
    return {converted.get_ptr(), converted.get_length()};
  } else
    return string.value();
}

class Getter_any {
 public:
  template <typename Functor>
  static void put_scalar_value_to_functor(
      const ::PolarXRPC::Datatypes::Any &any, Functor &functor,
      const CHARSET_INFO &charset) {
    if (!any.has_type())
      throw err_t(ER_POLARX_RPC_ERROR_MSG, "Invalid data, expecting type");

    if (::PolarXRPC::Datatypes::Any::SCALAR != any.type())
      throw err_t(ER_POLARX_RPC_ERROR_MSG, "Invalid data, expecting scalar");

    using ::PolarXRPC::Datatypes::Scalar;
    const Scalar &scalar = any.scalar();

    switch (scalar.type()) {
      case Scalar::V_SINT:
        throw_invalid_type_if_false(scalar, scalar.has_v_signed_int());
        functor(scalar.v_signed_int());
        break;

      case Scalar::V_UINT:
        throw_invalid_type_if_false(scalar, scalar.has_v_unsigned_int());
        functor(scalar.v_unsigned_int());
        break;

      case Scalar::V_NULL:
        functor();
        break;

      case Scalar::V_OCTETS: {
        throw_invalid_type_if_false(
            scalar, scalar.has_v_octets() && scalar.v_octets().has_value());
        RawBinary raw_binary(scalar.v_octets().value());
        functor(raw_binary);
      } break;

      case Scalar::V_DOUBLE:
        throw_invalid_type_if_false(scalar, scalar.has_v_double());
        functor(scalar.v_double());
        break;

      case Scalar::V_FLOAT:
        throw_invalid_type_if_false(scalar, scalar.has_v_float());
        functor(scalar.v_float());
        break;

      case Scalar::V_BOOL:
        throw_invalid_type_if_false(scalar, scalar.has_v_bool());
        functor(scalar.v_bool());
        break;

      case Scalar::V_IDENTIFIER: {
        throw_invalid_type_if_false(scalar, scalar.has_v_identifier());
        Identifier identifier(covert_if_needed(scalar.v_identifier(), charset));
        functor(identifier);
      } break;

      case Scalar::V_RAW_STRING: {
        throw_invalid_type_if_false(
            scalar, scalar.has_v_string() && scalar.v_string().has_value());
        RawString raw_string(covert_if_needed(scalar.v_string(), charset));
        functor(raw_string);
      } break;

      case Scalar::V_STRING: {
        throw_invalid_type_if_false(
            scalar, scalar.has_v_string() && scalar.v_string().has_value());
        functor(covert_if_needed(scalar.v_string(), charset));
      } break;

      default:
        throw err_t::Error(ER_POLARX_RPC_ERROR_MSG, "Invalid ScalarType: %d",
                           scalar.type());
    }
  }

 private:
  static void throw_invalid_type_if_false(
      const ::PolarXRPC::Datatypes::Scalar &scalar, const bool is_valid) {
    if (!is_valid)
      throw err_t::Error(ER_POLARX_RPC_ERROR_MSG,
                         "Missing field required for ScalarType: %d",
                         scalar.type());
  }
};

}  // namespace polarx_rpc
