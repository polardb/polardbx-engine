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
