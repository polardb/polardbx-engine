/* Copyright (c) 2000, 2019, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/table_ext.h"
#include "sql/item.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/item_timefunc.h"

namespace im {

/* Cast text or decimal to integer */
class Item_typecast_scn : public Item_int_func {
 public:
  Item_typecast_scn(Item *a) : Item_int_func(a) {}

  const char *func_name() const override { return "cast_as_scn"; }
  enum Functype functype() const override { return TYPECAST_FUNC; }
  uint decimal_precision() const override {
    return args[0]->decimal_precision();
  }

  bool resolve_type(THD *) override {
    fix_char_length(std::min<uint32>(args[0]->max_char_length(),
                                     MY_INT64_NUM_DECIMAL_DIGITS));
    return reject_geometry_args(arg_count, args, this);
  }

  void print(const THD *thd, String *str,
             enum_query_type query_type) const override {
    str->append(STRING_WITH_LEN("cast("));
    args[0]->print(thd, str, query_type);
    str->append(STRING_WITH_LEN(" as scn)"));
  }

  longlong val_int() override {
    longlong value;
    int error;
    size_t length;
    const char *end;
    const CHARSET_INFO *cs;
    char buff[MAX_FIELD_WIDTH], *start;
    String tmp(buff, sizeof(buff), &my_charset_bin), *res;
    Item_result result_type = args[0]->cast_to_int_type();

    if (result_type == DECIMAL_RESULT) {
      value = args[0]->val_int();
      null_value = args[0]->null_value;
      return value;
    }

    assert(result_type == STRING_RESULT);

    /* null value */
    if (!(res = args[0]->val_str(&tmp))) {
      null_value = 1;
      goto fail;
    }

    null_value = 0;
    start = res->ptr();
    length = res->length();
    cs = res->charset();
    end = start + length;

    value = cs->cset->strtoll10(cs, start, &end, &error);

    if (error > 0 || end != start + length) goto fail;

    return value;

  fail:
    my_error(ER_AS_OF_BAD_SCN_TYPE, MYF(0));
    return 0;
  }
};

/* Itemize the snapshot items */
bool Table_snapshot::itemize(Parse_context *pc, Table_ref *owner) {
  assert(valid());
  assert(pc && pc->thd && pc->thd->lex);
  assert(owner && !owner->snapshot_expr.is_set());

  /* As of clause */
  if (ts || scn) {
    Query_block *query_block = owner->query_block;
    assert(query_block);

    if (query_block->select_number == 1 && pc->thd->lex->is_update_stmt) {
      my_error(ER_AS_OF_NOT_SELECT, MYF(0));
      return true;
    }

    if (ts) {
      if (ts->itemize(pc, &ts)) return true;
      owner->snapshot_expr.ts = ts;
    } else {
      if (scn->itemize(pc, &scn)) return true;
      owner->snapshot_expr.scn = scn;
    }

    /* Disable query cache if snapshot expression is set. */
    pc->thd->lex->safe_to_cache_query = false;
  }

  return false;
}

static bool is_string_item(Item *item) {
  switch (item->data_type()) {
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
      return true;
    default:
      return false;
  }
}

static bool is_decimal_zero(Item *item) {
  switch (item->data_type()) {
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      return (item->decimals == 0);
    default:
      return false;
  }
}

static bool try_cast_to_datetime(THD *thd, Item **item) {
  /* We only try to cast string item to datetime.
  If it cannot cast, errors will be raised when evaluating */
  if (!is_string_item(*item)) return true;

  Item *cast = new Item_typecast_datetime(*item, true);
  if (!cast) return true;

  cast->fix_fields(thd, item);
  thd->change_item_tree(item, cast);
  return false;
}

static bool try_cast_to_scn(THD *thd, Item **item) {
  /* We only try to cast string or decimal item to scn.
  If it cannot cast, errors will be raised when evaluating */
  if (!is_string_item(*item) && !is_decimal_zero(*item)) return true;

  Item *cast = new Item_typecast_scn(*item);
  if (!cast) return true;

  cast->fix_fields(thd, item);
  thd->change_item_tree(item, cast);
  return false;
}

static bool fix_and_check_const(THD *thd, Item **item) {
  if (!(*item)->fixed && (*item)->fix_fields(thd, item)) return true;

  if (!(*item)->const_for_execution()) {
    my_error(ER_AS_OF_EXPR_NOT_CONSTANT, MYF(0));
    return true;
  }

  return false;
}

/* Fix fields, and make sure expr is constant */
bool Table_snapshot::fix_fields(THD *thd) {
  assert(valid());

  if (ts) {
    if (fix_and_check_const(thd, &ts)) return true;

    if (!ts->is_temporal_with_date_and_time()) {
      if (try_cast_to_datetime(thd, &ts)) {
        my_error(ER_AS_OF_BAD_TIMESTAMP_TYPE, MYF(0));
        return true;
      }
    }
  } else if (scn) {
    if (fix_and_check_const(thd, &scn)) return true;

    if (!is_integer_type(scn->data_type())) {
      if (try_cast_to_scn(thd, &scn)) {
        my_error(ER_AS_OF_BAD_SCN_TYPE, MYF(0));
        return true;
      }
    }
  }

  return false;
}

/**
  Evaluate timestamp value.

  @param ts    Timestamp item
  @param out   UTC value caculated
  @return true Some error occurs.
*/
bool Table_snapshot::evaluate_timestamp(Item *ts, uint64_t *out) {
  assert(ts && out);
  assert(current_thd);

  int unused;
  my_timeval tm;

  if (ts->get_timeval(&tm, &unused)) {
    my_error(ER_AS_OF_BAD_TIMESTAMP_TYPE, MYF(0));
    return true;
  }

  if (current_thd->is_error()) return true;

  *out = tm.m_tv_sec;
  return false;
}

/**
  Evaluate timestamp value.

  @param scn   SCN item
  @param out   SCN value caculated
  @return true Some error occurs.
*/
bool Table_snapshot::evaluate_scn(Item *scn, uint64_t *out) {
  assert(scn && out);
  assert(current_thd);

  uint64_t val = scn->val_uint();

  if (current_thd->is_error()) return true;

  *out = val;
  return false;
}

/**
  Evaluate snapshot value.

  @param snapshot Snap value caculated
  @return true Some error occurs.
*/
bool Table_snapshot::evaluate(Snapshot_info_t *snapshot) {
  if (!is_set()) return false;

  uint64_t val;
  assert(valid());

  if (ts) {
    if (evaluate_timestamp(ts, &val)) return true;
    snapshot->set_timestamp(val);
  } else if (scn) {
    if (evaluate_scn(scn, &val)) return true;
    snapshot->set_scn(val);
  }

  return false;
}

/*
  Reset snapshot, increase the snapshot table count.
*/
void init_table_snapshot(TABLE *table, THD *thd) {
  assert(thd && thd->lex);

  table->snapshot.reset();

  if (table->pos_in_table_list->snapshot_expr.is_set())
    thd->lex->table_snap_expr_count_to_evaluate++;
}

/*
  Evaluate table snapshot expressions.

  @return true   If some error occurs.
*/
bool evaluate_snapshot(THD *thd, const LEX *lex) {
  assert(thd->lex->table_snap_expr_count_to_evaluate >= 0);

  /* Cases that need not do evaluating */
  if ((thd->lex->table_snap_expr_count_to_evaluate == 0) || /* No snapshot */
      (lex->is_explain() && !lex->is_explain_analyze))      /* Not analyze */
    return false;

  assert(thd->open_tables);

  for (TABLE *table = thd->open_tables; table; table = table->next) {
    assert(table->pos_in_table_list);
    assert(!table->snapshot.valid());

    if (table->pos_in_table_list->snapshot_expr.evaluate(&table->snapshot))
      return true;
  }

  return false;
}

} // namespace im

