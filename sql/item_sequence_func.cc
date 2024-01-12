/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyStore hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyStore.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file

  Implementation of SEQUENCE NEXTVAL() AND CURRVAL() function.

  Usage Like:
    'SELECT NEXTVAL(s1)'
    'SELECT CURRVAL(s1)'
*/

#include "sql/item_sequence_func.h"
#include "sql/parse_tree_items.h"
#include "sql/protocol.h"
#include "sql/sql_sequence.h"

/**
  @addtogroup Sequence Engine

  Sequence Engine native function implementation.

  @{
*/
bool Item_func_currval::parse_parameter() {
  assert(!m_table_list);

  if (!m_para_list && m_table) {
    return false;
  }

  assert(!m_table && !m_db);

  /**
    Grammar entry：IDENT_sys '(' opt_udf_expr_list ')',
    and here udf_expr expected: ident or ident.ident.
  */
  if (!m_para_list || m_para_list->elements() != 1) {
    my_error(ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, MYF(0), func_name());
    return true;
  }
  return parse_table_ident();
}

bool Item_func_nextval::parse_parameter() {
  assert(!m_table_list);

  if (!m_para_list && m_table) {
    return false;
  }

  assert(!m_table && !m_db);

  if (check_param_count()) return true;
  /**
    Grammar entry：IDENT_sys '(' opt_udf_expr_list ')',
    and here udf_expr expected: ident or ident.ident.
  */
  if (parse_table_ident()) return true;

  PTI_udf_expr *para = nullptr;
  Item_int *item = nullptr;
  if (m_para_list->elements() == 2) {
    if ((para = dynamic_cast<PTI_udf_expr *>(m_para_list->value[1])) &&
        (item = dynamic_cast<Item_int *>(para->expr))) {
      unsigned long long value = item->val_int();
      if (check_value()) {
        return true;
      } else {
        set_value(value);
      }
    } else {
      my_error(ER_WRONG_PARAMETERS_TO_NATIVE_FCT, MYF(0), func_name());
      return true;
    }
  }

  return false;
}

/**
  All sequence function first parameter is table_ident.
*/
bool Item_seq_func::parse_table_ident() {
  assert(m_para_list->elements() >= 1);

  assert(!m_table && !m_db);

  /**
    Grammar entry：IDENT_sys '(' opt_udf_expr_list ')',
    and here udf_expr expected: ident or ident.ident.
  */
  PTI_udf_expr *para = dynamic_cast<PTI_udf_expr *>(m_para_list->value[0]);
  if (!para) {
    my_error(ER_WRONG_PARAMETERS_TO_NATIVE_FCT, MYF(0), func_name());
    return true;
  }

  assert(para->expr);

  PTI_simple_ident_ident *tbl =
      dynamic_cast<PTI_simple_ident_ident *>(para->expr);
  if (tbl) {
    /* Only table provided (ident) */
    m_table = tbl->ident.str;
    return false;
  }

  PTI_simple_ident_q_3d *schema_table =
      dynamic_cast<PTI_simple_ident_q_3d *>(para->expr);
  if (schema_table) {
    /**
      Both schema and table are provided (ident.ident).
      schema_table->field corresponds to table,
      schema_table->table corresponds to db,
      schema_table->db not used.
      Don't be confused, please see simple_ident in sql_yacc.yy for details.
    */
    m_table = schema_table->field;
    if (!m_thd->get_protocol()->has_client_capability(CLIENT_NO_SCHEMA))
      m_db = schema_table->table;
    return false;
  }

  my_error(ER_WRONG_PARAMETERS_TO_NATIVE_FCT, MYF(0), func_name());
  return true;
}

void Item_func_nextval::set_sequence_scan() {
  TABLE *table = m_table_list->table;
  table->sequence_scan.set_batch(m_value);
}

void Item_func_nextval_skip::set_sequence_scan() {
  TABLE *table = m_table_list->table;
  table->sequence_scan.set_skip(m_value);
}

void Item_func_nextval_show::set_sequence_scan() {
  TABLE *table = m_table_list->table;
  table->sequence_scan.set_operation_show_cache();
}

bool Item_func_nextval::check_param_count() {
  uint elements = 0;

  if (!m_para_list) {
    my_error(ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, MYF(0), func_name());
    return true;
  }

  elements = m_para_list->elements();

  if (elements != 1 && elements != 2) {
    my_error(ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, MYF(0), func_name());
    return true;
  }
  return false;
}

bool Item_func_nextval_skip::check_param_count() {
  uint elements = 0;

  if (!m_para_list) {
    my_error(ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, MYF(0), func_name());
    return true;
  }

  elements = m_para_list->elements();
  if (elements != 2) {
    my_error(ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, MYF(0), func_name());
    return true;
  }
  return false;
}

bool Item_func_nextval_show::check_param_count() {
  uint elements = 0;

  if (!m_para_list) {
    my_error(ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, MYF(0), func_name());
    return true;
  }

  elements = m_para_list->elements();
  if (elements != 1) {
    my_error(ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, MYF(0), func_name());
    return true;
  }
  return false;
}

bool Item_func_nextval::check_value() {
  if (m_value <= 0 || m_value > SEQUENCE_MAX_BATCH_SIZE) {
    my_error(ER_SEQUENCE_BATCH_SIZE_INVALID, MYF(0), func_name());
    return true;
  }
  return false;
}

bool Item_func_nextval_skip::check_value() { return false; }

bool Item_func_nextval_show::check_value() { return false; }

/**
  NEXTVAL() function implementation.
*/
longlong Item_func_nextval::val_int() {
  ulonglong value;
  int error;
  TABLE *table = m_table_list->table;
  DBUG_ENTER("Item_func_nextval::val_int");
  assert(table->file);

  bitmap_set_bit(table->read_set, Sequence_field::FIELD_NUM_NEXTVAL);

  set_sequence_scan();

  if (table->file->ha_rnd_init(1))
    goto err;
  else {
    if ((error = table->file->ha_rnd_next(table->record[0]))) {
      table->file->print_error(error, MYF(0));
      table->file->ha_rnd_end();
      goto err;
    }
    table->file->ha_rnd_end();

    value = table->field[Sequence_field::FIELD_NUM_NEXTVAL]->val_int();
    null_value = 0;
    DBUG_RETURN(value);
  }
err:
  null_value = 1;
  DBUG_RETURN(0);
}

bool Item_func_nextval::add_table_to_lex_list(Parse_context *pc) {
  Table_ident *table_ident = nullptr;
  if (m_db == nullptr)
    table_ident = new (pc->mem_root) Table_ident(to_lex_cstring(m_table));
  else
    table_ident = new (pc->mem_root)
        Table_ident(to_lex_cstring(m_db), to_lex_cstring(m_table));

  if (!(m_table_list = pc->select->add_table_to_list(
            m_thd, table_ident, nullptr, TL_OPTION_SEQUENCE,
            TL_WRITE_CONCURRENT_DEFAULT, MDL_SHARED_WRITE, nullptr, nullptr,
            nullptr, nullptr, Sequence_scan_mode::ITERATION_SCAN)))
    return true;

  return false;
}

/**
  CURRVAL() function implementation.
*/
longlong Item_func_currval::val_int() {
  ulonglong value;
  int error;
  TABLE *table = m_table_list->table;
  DBUG_ENTER("Item_func_currval::val_int");
  assert(table->file);

  bitmap_set_bit(table->read_set, Sequence_field::FIELD_NUM_CURRVAL);

  if (table->file->ha_rnd_init(1))
    goto err;
  else {
    if ((error = table->file->ha_rnd_next(table->record[0]))) {
      table->file->print_error(error, MYF(0));
      table->file->ha_rnd_end();
      goto err;
    }
    table->file->ha_rnd_end();

    value = table->field[Sequence_field::FIELD_NUM_CURRVAL]->val_int();
    null_value = 0;
    DBUG_RETURN(value);
  }
err:
  null_value = 1;
  DBUG_RETURN(0);
}

bool Item_func_currval::add_table_to_lex_list(Parse_context *pc) {
  Table_ident *table_ident = nullptr;
  if (m_db == nullptr)
    table_ident = new (pc->mem_root) Table_ident(to_lex_cstring(m_table));
  else
    table_ident = new (pc->mem_root)
        Table_ident(to_lex_cstring(m_db), to_lex_cstring(m_table));

  if (!(m_table_list = pc->select->add_table_to_list(
            m_thd, table_ident, nullptr, TL_OPTION_SEQUENCE, TL_READ,
            MDL_SHARED_READ, nullptr, nullptr, nullptr, nullptr,
            Sequence_scan_mode::ITERATION_SCAN)))
    return true;

  return false;
}

/// @} (end of group Sequence Engine)
