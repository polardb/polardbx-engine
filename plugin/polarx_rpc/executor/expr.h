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

#include "sql/item.h"

#include "../coders/protocol_fwd.h"

#include "error.h"
#include "meta.h"
#include "parse.h"

namespace rpc_executor {

class ExprParser {
 public:
  static ExprParser &instance();
  int parse(const ::PolarXRPC::Expr::Expr &arg, InternalDataSet &dataset,
            ExprItem *&item) const;
  int parse_field(const ::PolarXRPC::Datatypes::Scalar &literal,
                  InternalDataSet &dataset, ExprItem *&item,
                  const char *&field_name) const;

 private:
  enum OP_TYPE { OP_PLUS, OP_GT, OP_GE, OP_EQ };

  int parse(const ::PolarXRPC::Expr::Identifier &arg, InternalDataSet &dataset,
            ExprItem *&item, const bool is_function = false) const;
  int parse(const ::PolarXRPC::Expr::ColumnIdentifier &arg,
            InternalDataSet &dataset, ExprItem *&item) const;
  int parse(const ::PolarXRPC::Expr::FunctionCall &arg,
            InternalDataSet &dataset, ExprItem *&item) const;
  int parse(const ::PolarXRPC::Expr::Operator &op, InternalDataSet &dataset,
            ExprItem *&item) const;
  int parse(const ::PolarXRPC::Datatypes::Any &arg, InternalDataSet &dataset,
            ExprItem *&item) const;
  int parse(const ::PolarXRPC::Datatypes::Scalar &literal,
            InternalDataSet &dataset, ExprItem *&item) const;
  int parse(const ::PolarXRPC::Datatypes::Scalar::Octets &arg,
            InternalDataSet &dataset, ExprItem *&item) const;
  int parse_placeholder(const Placeholder &arg, InternalDataSet &dataset,
                        ExprItem *&item) const;
  int parse(const ::PolarXRPC::Expr::Object &arg, InternalDataSet &dataset,
            ExprItem *&item) const;
  int parse(const ::PolarXRPC::Expr::Object::ObjectField &arg,
            InternalDataSet &dataset, ExprItem *&item) const;
  int parse(const ::PolarXRPC::Expr::Array &arg, InternalDataSet &dataset,
            ExprItem *&item) const;
  int parse_fieldref(const FieldIndex &arg, InternalDataSet &dataset,
                     ExprItem *&item) const;
};

}  // namespace rpc_executor
