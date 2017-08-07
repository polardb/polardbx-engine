/* Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; version 2 of the License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include "mysqlx_pb_wrapper.h"

#include <utility>

#include "ngs_common/to_string.h"

namespace xpl {

namespace test {

Identifier::Identifier(const std::string &name,
                       const std::string &schema_name) {
  if (name.empty() == false) set_name(name);

  if (schema_name.empty() == false) set_schema_name(schema_name);
}

Document_path::Document_path(const Path &path) {
  for (Path::const_iterator i = path.begin(); i != path.end(); ++i) {
    Mysqlx::Expr::DocumentPathItem *item = Add();

    item->set_type(i->first);
    if (i->first == Mysqlx::Expr::DocumentPathItem::ARRAY_INDEX)
      item->set_index(ngs::stoi(i->second));
    else
      item->set_value(i->second);
  }
}

Document_path::Path::Path(const std::string &value) { add_member(value); }

Document_path::Path &Document_path::Path::add_member(const std::string &value) {
  push_back(std::make_pair(Mysqlx::Expr::DocumentPathItem::MEMBER, value));
  return *this;
}

Document_path::Path &Document_path::Path::add_index(int index) {
  push_back(std::make_pair(Mysqlx::Expr::DocumentPathItem::ARRAY_INDEX,
                           ngs::to_string(index)));
  return *this;
}

Document_path::Path &Document_path::Path::add_asterisk() {
  push_back(
      std::make_pair(Mysqlx::Expr::DocumentPathItem::MEMBER_ASTERISK, ""));
  return *this;
}

Document_path::Path &Document_path::Path::add_double_asterisk() {
  push_back(
      std::make_pair(Mysqlx::Expr::DocumentPathItem::DOUBLE_ASTERISK, ""));
  return *this;
}

ColumnIdentifier::ColumnIdentifier(const std::string &name,
                                   const std::string &table_name,
                                   const std::string &schema_name,
                                   const Document_path::Path *path) {
  if (name.empty() == false) set_name(name);

  if (table_name.empty() == false) set_table_name(table_name);

  if (schema_name.empty() == false) set_schema_name(schema_name);

  if (path) *mutable_document_path() = Document_path(*path);
}

ColumnIdentifier::ColumnIdentifier(const Document_path &path,
                                   const std::string &name,
                                   const std::string &table_name,
                                   const std::string &schema_name) {
  if (!name.empty()) set_name(name);

  if (!table_name.empty()) set_table_name(table_name);

  if (!schema_name.empty()) set_schema_name(schema_name);

  mutable_document_path()->CopyFrom(path);
}

Scalar::Scalar(int value) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_SINT);
  set_v_signed_int(value);
}

Scalar::Scalar(unsigned int value) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_UINT);
  set_v_unsigned_int(value);
}

Scalar::Scalar(bool value) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_BOOL);
  set_v_bool(value);
}

Scalar::Scalar(float value) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_FLOAT);
  set_v_float(value);
}

Scalar::Scalar(double value) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_DOUBLE);
  set_v_double(value);
}

Scalar::Scalar(const char *value, unsigned type) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_OCTETS);
  set_allocated_v_octets(new Scalar::Octets(value, type));
}

Scalar::Scalar(Scalar::Octets *value) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_OCTETS);
  set_allocated_v_octets(value);
}

Scalar::Scalar(const Scalar::Octets &value) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_OCTETS);
  mutable_v_octets()->CopyFrom(value);
}

Scalar::Scalar(Scalar::String *value) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_STRING);
  set_allocated_v_string(value);
}

Scalar::Scalar(const Scalar::String &value) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_STRING);
  mutable_v_string()->CopyFrom(value);
}

Scalar::Scalar(Scalar::Null) {
  set_type(Mysqlx::Datatypes::Scalar_Type_V_NULL);
}

Scalar::String::String(const std::string &value) { set_value(value); }

Scalar::Octets::Octets(const std::string &value, unsigned type) {
  set_value(value);
  set_content_type(type);
}

Any::Any(const Scalar &scalar) {
  set_type(Mysqlx::Datatypes::Any_Type_SCALAR);
  mutable_scalar()->CopyFrom(scalar);
}

Any::Any(const Object &obj) {
  set_type(Mysqlx::Datatypes::Any_Type_OBJECT);
  mutable_obj()->CopyFrom(obj);
}

Any::Any(const Array &array) {
  set_type(Mysqlx::Datatypes::Any_Type_ARRAY);
  mutable_array()->CopyFrom(array);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, const Scalar &value) {
  expr->set_type(Mysqlx::Expr::Expr_Type_LITERAL);
  expr->mutable_literal()->CopyFrom(value);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, Operator *oper) {
  expr->set_type(Mysqlx::Expr::Expr_Type_OPERATOR);
  expr->set_allocated_operator_(oper);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, const Operator &oper) {
  expr->set_type(Mysqlx::Expr::Expr_Type_OPERATOR);
  expr->mutable_operator_()->CopyFrom(oper);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, const Identifier &ident) {
  expr->set_type(Mysqlx::Expr::Expr_Type_IDENT);
  expr->mutable_operator_()->CopyFrom(ident);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, FunctionCall *func) {
  expr->set_type(Mysqlx::Expr::Expr_Type_FUNC_CALL);
  expr->set_allocated_function_call(func);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, const FunctionCall &func) {
  expr->set_type(Mysqlx::Expr::Expr_Type_FUNC_CALL);
  expr->mutable_function_call()->CopyFrom(func);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, ColumnIdentifier *id) {
  expr->set_type(Mysqlx::Expr::Expr_Type_IDENT);
  expr->set_allocated_identifier(id);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, const ColumnIdentifier &id) {
  expr->set_type(Mysqlx::Expr::Expr_Type_IDENT);
  expr->mutable_identifier()->CopyFrom(id);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, Object *obj) {
  expr->set_type(Mysqlx::Expr::Expr_Type_OBJECT);
  expr->set_allocated_object(obj);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, const Object &obj) {
  expr->set_type(Mysqlx::Expr::Expr_Type_OBJECT);
  expr->mutable_object()->CopyFrom(obj);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, Array *arr) {
  expr->set_type(Mysqlx::Expr::Expr_Type_ARRAY);
  expr->set_allocated_array(arr);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, const Array &arr) {
  expr->set_type(Mysqlx::Expr::Expr_Type_ARRAY);
  expr->mutable_array()->CopyFrom(arr);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, const Placeholder &ph) {
  expr->set_type(Mysqlx::Expr::Expr_Type_PLACEHOLDER);
  expr->set_position(ph.value);
}

void Expr::initialize(Mysqlx::Expr::Expr *expr, const Variable &var) {
  expr->set_type(Mysqlx::Expr::Expr_Type_VARIABLE);
  expr->set_variable(var.value);
}

Any::Object::Object(std::initializer_list<Any::Object::Fld> list) {
  for (const Fld &f : list) {
    Mysqlx::Datatypes::Object_ObjectField *item = add_fld();
    item->set_key(f.key);
    item->mutable_value()->CopyFrom(f.value);
  }
}

Object::Object(const std::string &key, Expr *value) {
  Mysqlx::Expr::Object_ObjectField *item = add_fld();
  item->set_key(key);
  item->set_allocated_value(value);
}

Object::Object(std::initializer_list<Fld> list) {
  for (const Fld &f : list) {
    Mysqlx::Expr::Object_ObjectField *item = add_fld();
    item->set_key(f.key);
    item->mutable_value()->CopyFrom(f.value);
  }
}

Column::Column(const std::string &name, const std::string &alias) {
  if (!name.empty()) set_name(name);
  if (!alias.empty()) set_alias(alias);
}

Column::Column(const Document_path &path, const std::string &name,
               const std::string &alias) {
  mutable_document_path()->CopyFrom(path);
  if (!name.empty()) set_name(name);
  if (!alias.empty()) set_alias(alias);
}

Collection::Collection(const std::string &name, const std::string &schema) {
  if (!name.empty()) set_name(name);
  if (!schema.empty()) set_schema(schema);
}

Projection::Projection(const Expr &source, const std::string &alias) {
  mutable_source()->CopyFrom(source);
  if (!alias.empty()) set_alias(alias);
}

Order::Order(const Expr &expr, const ::Mysqlx::Crud::Order_Direction dir) {
  mutable_expr()->CopyFrom(expr);
  set_direction(dir);
}

Limit::Limit(const uint64_t row_count, const uint64_t offset) {
  if (row_count > 0) set_row_count(row_count);
  if (offset > 0) set_offset(offset);
}

}  // namespace test
}  // namespace xpl
