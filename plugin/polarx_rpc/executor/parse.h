/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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


#pragma once

#include <unordered_map>

#include "../coders/protocol_fwd.h"
#include "error.h"

namespace rpc_executor {

using MysqlxScalar = ::PolarXRPC::Datatypes::Scalar;
using ScalarList = ::google::protobuf::RepeatedPtrField<MysqlxScalar>;
using ParamsList = ScalarList;
using Placeholder = ::google::protobuf::uint32;
using FieldIndex = ::google::protobuf::uint32;

extern thread_local const ParamsList *tls_params;

inline const MysqlxScalar &real(const MysqlxScalar &scalar) {
  if (scalar.type() == MysqlxScalar::V_PLACEHOLDER) {
    return tls_params->Get(scalar.v_position());
  } else {
    return scalar;
  }
}

// scalar type should be ensured by caller
inline const std::string &pb2str(const MysqlxScalar &scalar) {
  return real(scalar).v_string().value();
}

// scalar type should be ensured by caller
inline const char *pb2ptr(const MysqlxScalar &scalar) {
  return real(scalar).v_string().value().data();
}

template <typename M>
inline const char *parse_index_name(M &msg) {
  return msg.has_index_info() ? pb2ptr(msg.index_info().name()) : "PRIMARY";
}

// Server representation of Mysqlx.Datatypes.Scalar
// should be able to convert to and from Mysqlx.Datatypes.Scalar.Type
class DataRep {
 public:
  enum DataType {
    SIGNED_INT = 1,
    UNSIGNED_INT = 2,
    NULL_TYPE = 3,
    DOUBLE = 5,
    FLOAT = 6,
    BOOL = 7,
    STRING = 8,
    PLACEHOLDER = 9,
  };
  DataType type_;
  union {
    int64_t v_int_;
    uint64_t v_uint_;
    double v_double_;
    float v_float_;
    bool v_bool_;
    const std::string *v_string_;
  };
  int32_t size_;

  int init(const MysqlxScalar &scalar) {
    int ret = HA_EXEC_SUCCESS;
    type_ = static_cast<DataRep::DataType>(scalar.type());
    switch (type_) {
      case DataRep::SIGNED_INT:
        v_int_ = scalar.v_signed_int();
        size_ = sizeof(scalar.v_signed_int());
        break;
      case DataRep::UNSIGNED_INT:
        v_uint_ = scalar.v_unsigned_int();
        size_ = sizeof(scalar.v_unsigned_int());
        break;
      case DataRep::NULL_TYPE:
        size_ = 0;
        break;
      case DataRep::DOUBLE:
        v_double_ = scalar.v_double();
        size_ = sizeof(scalar.v_double());
        break;
      case DataRep::FLOAT:
        v_float_ = scalar.v_float();
        size_ = sizeof(scalar.v_float());
        break;
      case DataRep::BOOL:
        v_bool_ = scalar.v_bool();
        size_ = sizeof(scalar.v_bool());
        break;
      case DataRep::STRING:
        v_string_ = &(scalar.v_string().value());
        size_ = scalar.v_string().value().size();
        break;
      case DataRep::PLACEHOLDER:
        ret = init(real(scalar));
        break;
      default:
        ret = HA_ERR_UNSUPPORTED;
    }
    return ret;
  }
};

// Server representation of GetExpr which is an array of (field_name, value)
// Convert the array to a map
class ExecKeyMap {
 public:
  int init(const PolarXRPC::ExecPlan::GetExpr &mysqlx_key) {
    int ret = HA_EXEC_SUCCESS;
    int32_t key_parts_offered = mysqlx_key.keys().size();
    key_field_map_.reserve(key_parts_offered);
    DataRep temp_data;
    auto &field_expr_list = mysqlx_key.keys();
    for (int32_t i = 0; i < key_parts_offered; ++i) {
      const std::string &field_name = pb2str(field_expr_list.Get(i).field());
      if ((ret = temp_data.init(field_expr_list.Get(i).value()))) {
        break;
      }
      key_field_map_.emplace(field_name, temp_data);
    }
    return ret;
  }

  int get_field(const std::string &field_name, DataRep &data) const {
    int ret = HA_EXEC_SUCCESS;
    auto iter = key_field_map_.find(field_name);
    if (key_field_map_.end() == iter) {
      ret = HA_ERR_KEY_NOT_FOUND;
    } else {
      data = iter->second;
    }
    return ret;
  }

  int64_t parts() const { return key_field_map_.size(); }

 private:
  std::unordered_map<std::string, DataRep> key_field_map_;
};

}  // namespace rpc_executor
