
#include <string>
#include <unordered_map>

#include "../global_defines.h"

#ifdef MYSQL8
#include "sql/current_thd.h"
#else
#define MYSQL_SERVER
#endif
#include "sql/sql_class.h"

#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/my_decimal.h"

#include "bloomfilter.h"
#include "expr.h"
#include "log.h"
#include "parse.h"

namespace rpc_executor {

template <class ITEM>
class Op1Factory {
 public:
  static ITEM *create(ExprItem *param) {
    ITEM *item_op = new ITEM(param);
#ifdef MYSQL8
    item_op->resolve_type(current_thd);
#else
    item_op->fix_length_and_dec();
    item_op->update_used_tables();
#endif
    item_op->fixed = 1;
    return item_op;
  }
};

template <class ITEM>
class Op2Factory {
 public:
  static ITEM *create(ExprItem *lhs, ExprItem *rhs) {
    ITEM *item_op = new ITEM(lhs, rhs);
#ifdef MYSQL8
    item_op->resolve_type(current_thd);
#else
    item_op->fix_length_and_dec();
    item_op->update_used_tables();
#endif
    item_op->fixed = 1;
    return item_op;
  }
};

template <>
class Op2Factory<Item_func_div> {
 public:
  static Item_func_div *create(ExprItem *lhs, ExprItem *rhs) {
    POS pos;
    Item_func_div *item_op = new Item_func_div(pos, lhs, rhs);
#ifdef MYSQL8
    item_op->resolve_type(current_thd);
#else
    item_op->fix_length_and_dec();
    item_op->update_used_tables();
#endif
    item_op->fixed = 1;
    return item_op;
  }
};

template <>
class Op2Factory<Item_func_isnull> {
 public:
  static Item_func_isnull *create(ExprItem *lhs, ExprItem *rhs) {
    if (rhs->type() != ExprItem::NULL_ITEM) {
      log_exec_error("operator is_null takes a not null value as 2nd param");
      return nullptr;
    }
    Item_func_isnull *item_op = new Item_func_isnull(lhs);
#ifdef MYSQL8
    item_op->resolve_type(current_thd);
#else
    item_op->fix_length_and_dec();
    item_op->update_used_tables();
#endif
    item_op->fixed = 1;
    return item_op;
  }
};

template <>
class Op2Factory<Item_func_isnotnull> {
 public:
  static Item_func_isnotnull *create(ExprItem *lhs, ExprItem *rhs) {
    if (rhs->type() != ExprItem::NULL_ITEM) {
      log_exec_error("operator not_null takes a not null value as 2nd param");
      return nullptr;
    }
    Item_func_isnotnull *item_op = new Item_func_isnotnull(lhs);
#ifdef MYSQL8
    item_op->resolve_type(current_thd);
#else
    item_op->fix_length_and_dec();
    item_op->update_used_tables();
#endif
    item_op->fixed = 1;
    return item_op;
  }
};

ExprParser &ExprParser::instance() {
  static ExprParser parser;
  return parser;
}

int ExprParser::parse(const ::PolarXRPC::Expr::Expr &arg,
                      InternalDataSet &dataset, ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  THD *thd = current_thd;

  switch (arg.type()) {
    case ::PolarXRPC::Expr::Expr::IDENT:
      ret = parse(arg.identifier(), dataset, item);
      break;

    case ::PolarXRPC::Expr::Expr::LITERAL:
      ret = parse(arg.literal(), dataset, item);
      break;

    case ::PolarXRPC::Expr::Expr::VARIABLE:
      ret = HA_ERR_UNSUPPORTED;
      log_exec_error("::PolarXRPC::Expr::Expr::VARIABLE is not supported yet");
      break;

    case ::PolarXRPC::Expr::Expr::FUNC_CALL:
      ret = parse(arg.function_call(), dataset, item);
      break;

    case ::PolarXRPC::Expr::Expr::OPERATOR:
      ret = parse(arg.operator_(), dataset, item);
      break;

    case ::PolarXRPC::Expr::Expr::PLACEHOLDER:
      ret = parse_placeholder(arg.position(), dataset, item);
      break;

    case ::PolarXRPC::Expr::Expr::OBJECT:
      ret = parse(arg.object(), dataset, item);
      break;

    case ::PolarXRPC::Expr::Expr::ARRAY:
      ret = parse(arg.array(), dataset, item);
      break;

    case ::PolarXRPC::Expr::Expr::REF:
      ret = parse_fieldref(arg.ref_id(), dataset, item);
      break;

    default:
      ret = HA_ERR_UNSUPPORTED;
      log_exec_error("Invalid value for type: %d", arg.type());
      break;
  }

  if (thd && thd->is_error()) {
    ret = thd->get_stmt_da()->mysql_errno();
  }

  return ret;
}

int ExprParser::parse(const ::PolarXRPC::Expr::Operator &op,
                      InternalDataSet &dataset, ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  static std::unordered_map<std::string,
                            std::function<ExprItem *(ExprItem *, ExprItem *)>>
      op2_map = {{"+", Op2Factory<Item_func_plus>::create},
                 {"-", Op2Factory<Item_func_minus>::create},
                 {"*", Op2Factory<Item_func_mul>::create},
                 {"/", Op2Factory<Item_func_div>::create},
#ifdef MYSQL8PLUS
                 {"div", Op2Factory<Item_func_div_int>::create},
#else
                 {"div", Op2Factory<Item_func_int_div>::create},
#endif
                 {"%", Op2Factory<Item_func_mod>::create},
                 {">", Op2Factory<Item_func_gt>::create},
                 {">=", Op2Factory<Item_func_ge>::create},
                 {"<", Op2Factory<Item_func_lt>::create},
                 {"<=", Op2Factory<Item_func_le>::create},
                 {"==", Op2Factory<Item_func_eq>::create},
                 {"<=>", Op2Factory<Item_func_equal>::create},
                 {"!=", Op2Factory<Item_func_ne>::create},
                 {"&&", Op2Factory<Item_cond_and>::create},
                 {"||", Op2Factory<Item_cond_or>::create},
                 {"is", Op2Factory<Item_func_isnull>::create},
                 {"is_not", Op2Factory<Item_func_isnotnull>::create}};

  static std::unordered_map<std::string, std::function<ExprItem *(ExprItem *)>>
      op1_map = {{"!", Op1Factory<Item_func_not>::create}};

  if (op.param_size() == 2) {
    const auto &iter = op2_map.find(op.name());
    if (op2_map.end() == iter) {
      ret = HA_ERR_UNSUPPORTED;
      log_exec_error("Expr parse, operator type not support %s",
                     op.name().data());
    } else {
      ExprItem *lhs = nullptr;
      ExprItem *rhs = nullptr;
      if ((ret = parse(op.param(0), dataset, lhs))) {
        log_exec_error(
            "Expr parse left op of 2op failed, "
            "ret: %d",
            ret);
      } else if ((ret = parse(op.param(1), dataset, rhs))) {
        log_exec_error(
            "Expr parse right op of 2op failed, "
            "ret: %d",
            ret);
      } else {
        auto *item_op = (iter->second)(lhs, rhs);
        if (!item_op) {
          ret = HA_ERR_OUT_OF_MEM;
        } else {
          dataset.defer_delete_item(item_op);
          item = item_op;
        }
      }
    }
  } else if (op.param_size() == 1) {
    const auto &iter = op1_map.find(op.name());
    if (op1_map.end() == iter) {
      ret = HA_ERR_UNSUPPORTED;
      log_exec_error("Expr parse, operator type not support %s",
                     op.name().data());
    } else {
      ExprItem *param = nullptr;
      if ((ret = parse(op.param(0), dataset, param))) {
        log_exec_error(
            "Expr parse op of 1op failed, "
            "ret: %d",
            ret);
      } else {
        auto *item_op = (iter->second)(param);

        if (!item_op) {
          ret = HA_ERR_OUT_OF_MEM;
        } else {
          dataset.defer_delete_item(item_op);
          item = item_op;
        }
      }
    }
  } else if (op.param_size() > 2) {
    if (op.name() == "&&" || op.name() == "||") {
      const auto &iter = op2_map.find(op.name());
      assert(op2_map.end() != iter);
      ExprItem *last_item = nullptr;
      int last_param_idx = op.param_size() - 1;
      if ((ret = parse(op.param(last_param_idx), dataset, last_item))) {
        log_exec_error(
            "Expr parse last(%d) param failed, "
            "ret: %d",
            last_param_idx, ret);
      } else {
        for (int param_idx = last_param_idx - 1; param_idx >= 0; --param_idx) {
          ExprItem *another_item = nullptr;
          if ((ret = parse(op.param(param_idx), dataset, another_item))) {
            log_exec_error(
                "Expr parse %d param failed, "
                "ret: %d",
                param_idx, ret);
          } else {
            last_item = (iter->second)(another_item, last_item);
            if (!last_item) {
              log_exec_error(
                  "Expr parse generate op item failed, "
                  "param_idx: %d, ret: %d",
                  param_idx, ret);
              ret = HA_ERR_OUT_OF_MEM;
              break;
            } else {
              dataset.defer_delete_item(last_item);
            }
          }
        }
        if (HA_EXEC_SUCCESS == ret) {
          item = last_item;
        }
      }
    } else {
      ret = HA_ERR_UNSUPPORTED;
      log_exec_error("Expr parse, operator count not support %s, %d",
                     op.name().data(), op.param_size());
    }
  }
  return ret;
}

int ExprParser::parse(const ::PolarXRPC::Datatypes::Scalar &literal,
                      InternalDataSet &dataset, ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  ::PolarXRPC::Datatypes::Scalar_Type type = literal.type();
  my_decimal decimal_value;
  switch (type) {
    case MysqlxScalar::V_SINT:
      item = new ::Item_int(static_cast<longlong>(literal.v_signed_int()));
      dataset.defer_delete_item(item);
      break;
    case MysqlxScalar::V_UINT:
      item = new ::Item_int(static_cast<ulonglong>(literal.v_unsigned_int()));
      dataset.defer_delete_item(item);
      break;
    case MysqlxScalar::V_NULL:
      item = new ::Item_null();
      dataset.defer_delete_item(item);
      break;
    case MysqlxScalar::V_OCTETS:
      ret = parse(literal.v_octets(), dataset, item);
      break;
    case MysqlxScalar::V_DOUBLE:
      double2my_decimal(E_DEC_FATAL_ERROR, literal.v_double(), &decimal_value);
      item = new ::Item_float(literal.v_double(), decimal_value.frac);
      dataset.defer_delete_item(item);
      break;
    case MysqlxScalar::V_FLOAT:
      double2my_decimal(E_DEC_FATAL_ERROR, literal.v_float(), &decimal_value);
      item = new ::Item_float(literal.v_float(), decimal_value.frac);
      dataset.defer_delete_item(item);
      break;
    case MysqlxScalar::V_BOOL:
      item = new ::Item_int(literal.v_bool() ? 1 : 0);
      dataset.defer_delete_item(item);
      break;
    case MysqlxScalar::V_STRING:
      item = new ::Item_string(literal.v_string().value().data(),
                               literal.v_string().value().size(),
                               &my_charset_utf8mb4_bin);
      dataset.defer_delete_item(item);
      break;
    case MysqlxScalar::V_PLACEHOLDER:
      ret = parse(real(literal), dataset, item);
      break;
    default:
      ret = HA_ERR_UNSUPPORTED;
  }
  if (!ret) {
    if (!item) {
      ret = HA_ERR_OUT_OF_MEM;
    }
  }
  return ret;
}

int ExprParser::parse(const ::PolarXRPC::Expr::Identifier &arg,
                      InternalDataSet &dataset, ExprItem *&item,
                      const bool is_function) const {
  int ret = HA_EXEC_SUCCESS;
  abort();
  return ret;
}
int ExprParser::parse(const ::PolarXRPC::Expr::ColumnIdentifier &arg,
                      InternalDataSet &dataset, ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  abort();
  return ret;
}
int ExprParser::parse(const ::PolarXRPC::Expr::FunctionCall &func,
                      InternalDataSet &dataset, ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  const std::string &func_name = func.name().name();
  if (func_name != "bloomfilter") {
    log_exec_error("unknown function name: %s", func_name.data());
    ret = HA_ERR_UNSUPPORTED;
    return ret;
  }
  std::vector<ExprItem *> params(func.param_size(), nullptr);
  for (int32_t i = 0; i < func.param_size(); ++i) {
    if ((ret = parse(func.param(i), dataset, params[i]))) {
      log_exec_error(
          "ExprParser parse FunctionCall.param failed, "
          "ret: %d, idx: %d",
          ret, i);
      break;
    }
  }
  if (!ret) {
    BloomFilterItem *bloomfilter_item = new BloomFilterItem();
    if (!bloomfilter_item) {
      ret = HA_ERR_OUT_OF_MEM;
      log_exec_error("BloomFilterItem new error, out of memory");
    } else if ((ret = bloomfilter_item->init(params))) {
      delete bloomfilter_item;
      bloomfilter_item = nullptr;
      log_exec_error("BloomFilterItem init error, ret: %d", ret);
    } else {
      dataset.defer_delete_item(bloomfilter_item);
#ifdef MYSQL8
      bloomfilter_item->resolve_type(current_thd);
#endif
      bloomfilter_item->item_name.copy("bloom");
      bloomfilter_item->fixed = 1;
      item = bloomfilter_item;
    }
  }

  return ret;
}
int ExprParser::parse(const ::PolarXRPC::Datatypes::Any &arg,
                      InternalDataSet &dataset, ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  abort();
  return ret;
}
int ExprParser::parse(const ::PolarXRPC::Datatypes::Scalar::Octets &octets,
                      InternalDataSet &dataset, ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  item = new ::Item_string(octets.value().data(), octets.value().size(),
                           &my_charset_bin);
  dataset.defer_delete_item(item);
  return ret;
}
int ExprParser::parse_placeholder(const Placeholder &arg_pos,
                                  InternalDataSet &dataset,
                                  ExprItem *&item) const {
  return parse(tls_params->Get(arg_pos), dataset, item);
}
int ExprParser::parse(const ::PolarXRPC::Expr::Object &arg,
                      InternalDataSet &dataset, ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  abort();
  return ret;
}
int ExprParser::parse(const ::PolarXRPC::Expr::Object::ObjectField &arg,
                      InternalDataSet &dataset, ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  abort();
  return ret;
}
int ExprParser::parse(const ::PolarXRPC::Expr::Array &arg,
                      InternalDataSet &dataset, ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  abort();
  return ret;
}

int ExprParser::parse_fieldref(const FieldIndex &index,
                               InternalDataSet &dataset,
                               ExprItem *&item) const {
  int ret = HA_EXEC_SUCCESS;
  if (dataset.has_project()) {
    ProjectInfo &items = dataset.get_project_exprs();
    if (index >= items.size()) {
      ret = HA_ERR_TOO_MANY_FIELDS;
      log_exec_error(
          "Expr parse REF, "
          "field index > project expr size %u >= %lu",
          index, items.size());
    } else {
      item = items[index].second;
    }
  } else {
    Field **fields = nullptr;
    uint32_t field_count = 0;
    dataset.get_all_fields(fields, field_count);
    if (index >= field_count) {
      ret = HA_ERR_TOO_MANY_FIELDS;
      log_exec_error("Expr parse REF, field index > total field %u >= %u",
                     index, field_count);
    } else if (!(item = new Item_field(fields[index]))) {
      ret = HA_ERR_OUT_OF_MEM;
      log_exec_error("Expr new Item_field failed");
    } else {
      dataset.defer_delete_item(item);
      dataset.project_field(fields[index]);
    }
  }
  return ret;
}

int ExprParser::parse_field(const ::PolarXRPC::Datatypes::Scalar &field_mark,
                            InternalDataSet &dataset, ExprItem *&item,
                            const char *&field_name) const {
  int ret = HA_EXEC_SUCCESS;
  const ::PolarXRPC::Datatypes::Scalar &real_field = real(field_mark);
  Field *field = nullptr;
  if (real_field.type() == ::PolarXRPC::Datatypes::Scalar::V_UINT) {
    uint64_t field_index = real_field.v_unsigned_int();
    if ((ret = dataset.get_field(field_index, field))) {
      log_exec_error(
          "ExprParse can not parse field, "
          "field_index: %lu, ret: %d",
          field_index, ret);
    }
  } else {
    const std::string &target_field_name = real_field.v_string().value();
    if ((ret = dataset.get_field(&target_field_name, field))) {
      log_exec_error(
          "ExprParse can not parse field, "
          "field_name: %s, ret: %d",
          target_field_name.data(), ret);
    }
  }

  if (ret) {
  } else if (!(item = new Item_field(field))) {
    ret = HA_ERR_OUT_OF_MEM;
    log_exec_error("Expr new Item_field failed, no memory");
  } else {
    field_name = field->field_name;
    dataset.defer_delete_item(item);
    dataset.project_field(field);
  }
  return ret;
}

}  // namespace rpc_executor
