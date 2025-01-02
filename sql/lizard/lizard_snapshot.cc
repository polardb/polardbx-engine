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

/* #define TURN_MVCC_SEARCH_TO_AS_OF */

#include "sql/lizard/lizard_service.h"
#include "sql/lizard/lizard_snapshot.h"

#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/item_timefunc.h"
#include "sql/mysqld.h"
#include "sql/parse_tree_node_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"

#include "sql/mysqld.h"

namespace lizard {

/* Cast text or decimal to integer */
class Item_typecast_cnum : public Item_int_func {
 public:
  Item_typecast_cnum(Item *a) : Item_int_func(a) {}

  enum Functype functype() const override { return TYPECAST_FUNC; }
  uint decimal_precision() const override {
    return args[0]->decimal_precision();
  }

  bool resolve_type(THD *) override {
    fix_char_length(std::min<uint32>(args[0]->max_char_length(),
                                     MY_INT64_NUM_DECIMAL_DIGITS));
    return reject_geometry_args(arg_count, args, this);
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

class Item_typecast_scn : public Item_typecast_cnum {
 public:
  Item_typecast_scn(Item *a) : Item_typecast_cnum(a) {}

  const char *func_name() const override { return "cast_as_scn"; }

  void print(const THD *thd, String *str,
             enum_query_type query_type) const override {
    str->append(STRING_WITH_LEN("cast("));
    args[0]->print(thd, str, query_type);
    str->append(STRING_WITH_LEN(" as scn)"));
  }
};

class Item_typecast_gcn : public Item_typecast_cnum {
 public:
  Item_typecast_gcn(Item *a) : Item_typecast_cnum(a) {}

  const char *func_name() const override { return "cast_as_gcn"; }

  void print(const THD *thd, String *str,
             enum_query_type query_type) const override {
    str->append(STRING_WITH_LEN("cast("));
    args[0]->print(thd, str, query_type);
    str->append(STRING_WITH_LEN(" as gcn)"));
  }
};

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

static bool fix_and_check_const(THD *thd, Item **item) {
  if (!(*item)->fixed && (*item)->fix_fields(thd, item)) return true;

  if (!(*item)->const_for_execution()) {
    my_error(ER_AS_OF_EXPR_NOT_CONSTANT, MYF(0));
    return true;
  }

  return false;
}

static bool try_cast_to_datetime(THD *thd, Item **item) {
  /* We only try to cast string item to datetime.
  If it cannot cast, errors will be raised when evaluating */
  if (!is_string_item(*item)) return true;

  Item *cast = new Item_typecast_datetime(*item, true);
  if (!cast) return true;

  cast->fix_fields(thd, item);
  // thd->change_item_tree(item, cast);
  return false;
}

static bool try_cast_to_scn(THD *thd, Item **item) {
  /* We only try to cast string or decimal item to scn.
  If it cannot cast, errors will be raised when evaluating */
  if (!is_string_item(*item) && !is_decimal_zero(*item)) return true;

  Item *cast = new Item_typecast_scn(*item);
  if (!cast) return true;

  cast->fix_fields(thd, item);
  // thd->change_item_tree(item, cast);
  return false;
}

static bool try_cast_to_gcn(THD *thd, Item **item) {
  /* We only try to cast string or decimal item to scn.
  If it cannot cast, errors will be raised when evaluating */
  if (!is_string_item(*item) && !is_decimal_zero(*item)) return true;

  Item *cast = new Item_typecast_gcn(*item);
  if (!cast) return true;

  cast->fix_fields(thd, item);
  // thd->change_item_tree(item, cast);
  return false;
}

/**
  Itemize the snapshot item and hook onto TABLE_LIST.

  My_error if failure.

  @retval	true	Failure
  @retval	false	Success
*/
bool Snapshot_hint::itemize(Parse_context *pc, Table_ref *owner) {
  assert(pc && pc->thd && pc->thd->lex);
  assert(owner);

  Query_block *query_block = owner->query_block;
  assert(query_block);

  if (query_block->select_number == 1 && pc->thd->lex->is_update_stmt) {
    my_error(ER_AS_OF_NOT_SELECT, MYF(0));
    return true;
  }

  if (m_item->itemize(pc, &m_item)) return true;

  owner->snapshot_hint = this;

  pc->thd->lex->safe_to_cache_query = false;

  return false;
}

/**
  Fix fields

  My_error if failure.

  @retval	true	Failure
  @retval	false	Success
 */
bool Snapshot_time_hint::fix_fields(THD *thd) {
  assert(m_item);

  if (fix_and_check_const(thd, &m_item)) return true;

  if (!m_item->is_temporal_with_date_and_time()) {
    if (try_cast_to_datetime(thd, &m_item)) {
      my_error(ER_AS_OF_BAD_TIMESTAMP_TYPE, MYF(0));
      return true;
    }
  }
  return false;
}

/** Calculate second from hint item. */
bool Snapshot_time_hint::val_int(uint64_t *value) {
  int unused;
  my_timeval tm;

  if (m_item->get_timeval(&tm, &unused)) {
    my_error(ER_AS_OF_BAD_TIMESTAMP_TYPE, MYF(0));
    return true;
  }

  if (current_thd->is_error()) return true;

  *value = tm.m_tv_sec;
  return false;
}

/**
  Invoke the table vision.
  My_error if failure.

  @retval	true	Failure
  @retval	false	Success
 */
int Snapshot_hint::invoke_vision(TABLE *table, THD *thd) {
  uint64_t value;
  bool error;
  if ((error = val_int(&value))) {
    return error;
  }

  Snapshot_vision *vision = table->table_snapshot.choose_once(type());
  vision->store_int(value);
  vision->set_flashback_area(m_flashback_area);

  return table->table_snapshot.activate(vision, thd);
}

/**
  Fix fields

  My_error if failure.

  @retval	true	Failure
  @retval	false	Success
 */
bool Snapshot_scn_hint::fix_fields(THD *thd) {
  if (fix_and_check_const(thd, &m_item)) return true;

  if (!is_integer_type(m_item->data_type())) {
    if (try_cast_to_scn(thd, &m_item)) {
      my_error(ER_AS_OF_BAD_SCN_TYPE, MYF(0));
      return true;
    }
  }

  return false;
}

bool Snapshot_scn_hint::val_int(uint64_t *value) {
  uint64_t val = m_item->val_uint();

  if (current_thd->is_error()) return true;

  *value = val;
  return false;
}

/**
  Fix fields

  My_error if failure.

  @retval	true	Failure
  @retval	false	Success
 */

bool Snapshot_gcn_hint::fix_fields(THD *thd) {
  if (fix_and_check_const(thd, &m_item)) return true;

  if (!is_integer_type(m_item->data_type())) {
    if (try_cast_to_gcn(thd, &m_item)) {
      my_error(ER_AS_OF_BAD_SCN_TYPE, MYF(0));
      return true;
    }
  }

  return false;
}

bool Snapshot_gcn_hint::val_int(uint64_t *value) {
  uint64_t val = m_item->val_uint();

  if (current_thd->is_error()) return true;

  *value = val;
  return false;
}

/**
  Invoke table snapshot vision.
  My_error if failure.

  @retval HA_ERR_SNAPSHOT_OUT_OF_RANGE, HA_ERR_AS_OF_INTERNAL on error.
  @retval 0 Success
 */
int Snapshot_simulate_gcn_hint::invoke_vision(TABLE *table, THD *thd) {
  Snapshot_gcn_vision *vision = dynamic_cast<Snapshot_gcn_vision *>(
      table->table_snapshot.choose_once(type()));
  vision->init(&m_owned_vision);
  vision->set_flashback_area(m_flashback_area);

  return table->table_snapshot.activate(vision, thd);
}

/** Whether is it too old.
 *
 *  @retval	true	too old
 *  @retval	false	normal
 */
bool Snapshot_scn_vision::too_old() const {
  assert(m_scn != SCN_NULL);

  if (innodb_hton->ext.snapshot_scn_too_old(m_scn, m_flashback_area)) {
    return true;
  }
  return false;
}

void Snapshot_scn_vision::after_activate(THD *) {
  handlerton *ttse = innodb_hton;
  trx_id_t tid = ttse->ext.search_up_limit_tid_for_scn(this);
  m_up_limit_tid = tid;
  return;
}

void Snapshot_automatic_gcn_vision::init(const MyVisionGCN *owned_gcn) {
  assert(m_gcn == GCN_NULL);
  m_gcn = owned_gcn->gcn;
  m_current_scn = owned_gcn->current_scn;
  m_up_limit_tid = 0;
}

void Snapshot_automatic_gcn_vision::after_activate(THD *thd) {
  handlerton *ttse = innodb_hton;
  MyVisionGCN *owned_gcn;

  owned_gcn = &thd->owned_vision_gcn;

  if (m_gcn == GCN_NULL) {
    m_gcn = ttse->ext.load_gcn();
    m_current_scn = ttse->ext.load_scn();

    *owned_gcn = {(csr_t)CSR_AUTOMATIC, (gcn_t)m_gcn, (scn_t)m_current_scn};
  } else {
    // already inherit
  }

  m_up_limit_tid = ttse->ext.search_up_limit_tid_for_gcn(this);

  return;
}

void Snapshot_assigned_gcn_vision::init(const MyVisionGCN *owned_gcn) {
  assert(m_gcn == GCN_NULL);
  assert(owned_gcn->gcn != GCN_NULL && owned_gcn->csr == CSR_ASSIGNED);
  m_gcn = owned_gcn->gcn;
  m_up_limit_tid = 0;
}

void Snapshot_assigned_gcn_vision::after_activate(THD *) {
  handlerton *ttse = innodb_hton;

  assert(ttse && m_gcn != GCN_NULL);

  m_up_limit_tid = ttse->ext.search_up_limit_tid_for_gcn(this);
  return;
}

/** Whether is it too old.
 *
 *  @retval	true	too old
 *  @retval	false	normal
 */
bool Snapshot_automatic_gcn_vision::too_old() const {
  assert(m_current_scn != SCN_NULL && m_gcn != GCN_NULL);
  return innodb_hton->ext.snapshot_scn_too_old(m_current_scn,
                                               m_flashback_area) ||
         innodb_hton->ext.snapshot_automatic_gcn_too_old(m_gcn,
                                                         m_flashback_area);
}

/** Whether is it too old.
 *
 *  @retval	true	too old
 *  @retval	false	normal
 */
bool Snapshot_assigned_gcn_vision::too_old() const {
  assert(m_gcn != GCN_NULL);
  return innodb_hton->ext.snapshot_assigned_gcn_too_old(m_gcn,
                                                        m_flashback_area);
}

/*
  Reset snapshot, increase the snapshot table count.
*/
void init_table_snapshot(TABLE *table, THD *thd) {
  assert(thd && thd->lex);

  table->table_snapshot.release_vision();

  if (table->pos_in_table_list->snapshot_hint)
    thd->lex->table_snap_expr_count_to_evaluate++;
}

/*
  Evaluate table snapshot expressions.

  @return true   If some error occurs.
*/
bool evaluate_snapshot(THD *thd, const LEX *lex) {
  int error;
  assert(thd->lex->table_snap_expr_count_to_evaluate >= 0);

  /* Cases that need not do evaluating */
  if ((thd->lex->table_snap_expr_count_to_evaluate == 0) || /* No snapshot */
      (lex->is_explain() && !lex->is_explain_analyze))      /* Not analyze */
    return false;

  assert(thd->open_tables);
  assert(innodb_hton && innodb_hton->ext.load_scn());

  for (TABLE *table = thd->open_tables; table; table = table->next) {
    assert(table->pos_in_table_list);
    assert(!table->table_snapshot.is_activated() || thd->sp_runtime_ctx);

    Snapshot_hint *hint = table->pos_in_table_list->snapshot_hint;
    if (hint && !table->table_snapshot.is_activated()) {
      error = hint->invoke_vision(table, thd);
      if (error) {
        table->file->print_error(error, 0);
        return true;
      }
    }
  }

  return false;
}

/**
Simulate asof syntax by adding Item onto Table_snapshot.
@param[in]        thd         current context
@param[in/out]    snapshot    ASOF attributes
*/
void simulate_snapshot_clause(THD *thd, Table_ref *all_tables) {
  bool hint = false;
  MyVisionGCN owned_gcn;

  if (thd->sp_runtime_ctx) {
    return;
  }

  /** If setting innodb_current_snapshot_gcn, then a view is generated
  internally */
  if (thd->owned_vision_gcn.is_null() &&
      thd->variables.innodb_current_snapshot_gcn) {
    owned_gcn = {CSR_AUTOMATIC, GCN_NULL, SCN_NULL};
    hint = true;

  } else if (!thd->owned_vision_gcn.is_null()) {
    owned_gcn = thd->owned_vision_gcn;
    hint = true;

    if (owned_gcn.csr == CSR_ASSIGNED) {
      handlerton *ttse = innodb_hton;
      ttse->ext.set_gcn_if_bigger(owned_gcn.gcn);
    }
  }

  if (hint) {
    Table_ref *table;
    Snapshot_hint *hint_ptr;
    for (table = all_tables; table; table = table->next_global) {
      if (table->snapshot_hint == nullptr) {
        hint_ptr = new (thd->mem_root) Snapshot_simulate_gcn_hint(owned_gcn);
        table->snapshot_hint = hint_ptr;
      }
    }
  }
#if defined TURN_MVCC_SEARCH_TO_AS_OF
  else {
    Table_ref *table;
    for (table = all_tables; table; table = table->next_global) {
      if (table->snapshot_hint == nullptr) {
        item = new (thd->mem_root)
            Item_int((ulonglong)innodb_hton->ext.load_scn());
        Snapshot_hint *hint_ptr = new (thd->mem_root) Snapshot_scn_hint(item);
        table->snapshot_hint = hint_ptr;
      }
    }
  }
#endif
}

int Table_snapshot::exchange_timestamp_vision_to_scn_vision(
    Snapshot_vision **vision, THD *thd) {
  int error = 0;
  handlerton *ttse = innodb_hton;

  bool flashback_area = (*vision)->get_flashback_area();
  utc_t utc_second = (*vision)->val_int();

  scn_t scn;
  error = ttse->ext.convert_timestamp_to_scn(thd, utc_second, &scn);
  if (!error) {
    /** Change the vision. */
    *vision = get(AS_OF_SCN);

    (*vision)->store_int(scn);
    (*vision)->set_flashback_area(flashback_area);
  }
  return error;
}

}  // namespace lizard

bool Table_ref::process_snapshot_hint(THD *thd, TABLE *tbl) {
  if (snapshot_hint) {
    choose_flashback_area(thd, tbl);

    return snapshot_hint->fix_fields(thd);
  }
  return false;
}

/**
  Set flashback_area flag in the snapshot_hint and force the use of the
  clustered index for as-of queries when the session variable
  'opt_query_via_flashback_area' is enabled.

  @param thd The current session.
  @param tbl the TABLE to operate on.
*/
void Table_ref::choose_flashback_area(THD *thd, TABLE *tbl) {
  if (!thd->variables.opt_query_via_flashback_area || !tbl->flashback_area) {
    return;
  }

  assert(snapshot_hint);
  assert(thd->variables.opt_query_via_flashback_area);
  assert(tbl->flashback_area);

  /** set flag in the snapshot_hint. */
  snapshot_hint->set_flashback_area(true);

  /* initialize the result variables */
  tbl->keys_in_use_for_query = tbl->keys_in_use_for_group_by =
      tbl->keys_in_use_for_order_by = tbl->s->usable_indexes(thd);

  /** force using primary key. */
  uint pos = 0;

  Key_map index_join;
  Key_map index_order;
  Key_map index_group;

  index_join.set_bit(pos);
  index_order.set_bit(pos);
  index_group.set_bit(pos);

  tbl->force_index = true;
  tbl->keys_in_use_for_query.intersect(index_join);

  tbl->force_index_order = true;
  tbl->keys_in_use_for_order_by.intersect(index_order);

  tbl->force_index_group = true;
  tbl->keys_in_use_for_group_by.intersect(index_group);

  /* make sure covering_keys don't include indexes disabled with a hint */
  tbl->covering_keys.intersect(tbl->keys_in_use_for_query);

  return;
}
