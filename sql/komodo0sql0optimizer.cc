/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/komodo0sql0optimizer.cc
  Classes used for query optimizations.

 Created 2025-08-11 by Jiyang.Zhang
 *******************************************************/

#include "sql/mysqld.h"
#include "sql/sql_class.h"
#include "sql/sql_optimizer.h"
#include "sql/join_optimizer/access_path.h"

bool opt_polarx_optimizer_rbo_mode = true;

/**
  Access primary index and lookup record if the statement tries to read
  columns with a where condition that specifies the value of primary key.
  This function aims to avoid the cost of optimizer.
  @retval -1 Can't access primary index directly
  @retval 0  success
  @retval >0 the error code if exception happens.
*/
int JOIN::rbo_optimize() {
  if (!opt_polarx_optimizer_rbo_mode) {
    return (-1);
  }

  /* Check if fast path can be used for lookup */
  if (thd->lex->sql_command != SQLCOM_SELECT /* SELECT command */
      || thd->lex->table_count != 1         /* Only one table involved */
      || thd->lex->query_tables->next_global != NULL ||
      group_list.order /* No having, group, limit clause */
      || implicit_grouping || having_cond ||
      (query_expression()->select_limit_cnt == 0) ||
      thd->lex->query_tables->is_view_or_derived() ||
      (query_block->active_options() & OPTION_FOUND_ROWS) ||
      !where_cond /* Check if the where_cond is field=val */
      || where_cond->type() != Item::FUNC_ITEM ||
      ((Item_func *)where_cond)->functype() != Item_func::EQ_FUNC ||
      ((Item_func *)where_cond)->argument_count() != 2)
    return (-1);
  /* Check if index hint forbids using primary key*/
  if (thd->lex->query_tables->index_hints) {
    if (thd->lex->query_tables->index_hints->elements != 1) return (-1);
    bool is_pk = false;
    if (thd->lex->query_tables->index_hints->head()->key_name.str != NULL)
      is_pk = !strcmp(thd->lex->query_tables->index_hints->head()->key_name.str,
                      "PRIMARY");
    bool is_ignore =
        thd->lex->query_tables->index_hints->head()->type == INDEX_HINT_IGNORE;
    if (is_ignore == is_pk) return (-1);
  }
  /* Extract the field and value from the where condition. */
  Item *left = ((Item_func *)where_cond)->arguments()[0];
  Item *right = ((Item_func *)where_cond)->arguments()[1];
  Field *field;
  Item_int *val;
  if (left->type() == Item::FIELD_ITEM && right->type() == Item::INT_ITEM) {
    field = ((Item_field *)left)->field;
    val = (Item_int *)right;
  } else if (left->type() == Item::INT_ITEM &&
             right->type() == Item::FIELD_ITEM) {
    field = ((Item_field *)right)->field;
    val = (Item_int *)left;
  } else {
    /* We now only support primary key = const int */
    return (-1);
  }

  TABLE_SHARE *s = thd->lex->query_tables->table->s;
  /*
     Skip non innodb table and temp table.
  */
  if (s->db_type() != innodb_hton || s->tmp_table != NO_TMP_TABLE) {
    return (-1);
  }

  /*
    Check if the specified field in where condition is the one
    and only column of the primary key. and currently we only support
    Int field.
  */
  if (!(field->is_flag_set(PRI_KEY_FLAG)) || (s->primary_key == MAX_KEY) ||
      (s->key_info[s->primary_key].user_defined_key_parts != 1) ||
      (field->type() != MYSQL_TYPE_LONGLONG &&
       field->type() != MYSQL_TYPE_LONG))
    return (-1);
  /*
    If secondary_engine is used, do not use fast_path method.
  */
  if (s->has_secondary_engine() || s->is_secondary_engine()) return (-1);
  /* If this is EXPLAIN, we simply set the tag and return. */
  is_fast_path_using_primary = true;
  if (thd->lex->is_explain()) {
    return (-1);
  }
  THD_STAGE_INFO(thd, stage_statistics);
  /* Prepare the key buffer for searching btree in engine. */
  uint key_length = field->pack_length();
  uchar *key_buff = (uchar *)thd->mem_calloc(ALIGN_SIZE(key_length));
  longlong nr = val->val_int();
  if (key_length == 4)
    longstore(key_buff, nr);
  else {
    assert(key_length == 8);
    longlongstore(key_buff, nr);
  }
  TABLE *table = query_block->get_table_list()->table;
  /* init for JOIN::exec */
  fields = query_block->get_fields_list();
  const_tables = primary_tables;
  order.clean();
  int error = 0;
  int error1 = 0;
  table->set_found_row();
  error = table->file->ha_index_init(s->primary_key, false);
  if (!error) {
    error = table->file->ha_index_read_map(table->record[0], (uchar *)key_buff,
                                           make_prev_keypart_map(1),
                                           HA_READ_KEY_EXACT);

    error1 = table->file->ha_index_or_rnd_end();

    error =  error ? error : error1;
  }

  if (error) {
    table->set_no_row();
    table->set_null_row();
    empty_record(table);
    if (error != HA_ERR_KEY_NOT_FOUND && error != HA_ERR_END_OF_FILE)
      return (report_handler_error(table, error));

    /* No matched record. */
    assert(m_root_access_path == nullptr);
    m_root_access_path =
        NewZeroRowsAccessPath(thd, "no matching row in const table (RBO)");
  } else {
    /* Store the value. */
    store_record(table, record[1]);

    assert(m_root_access_path == nullptr);
    m_root_access_path =
        NewFakeSingleRowAccessPath(thd, /*count_examined_rows=*/true);
  }


  return (0);
}

