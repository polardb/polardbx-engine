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

#ifndef SQL_ITEM_SEQUENCE_FUNC_INCLUDED
#define SQL_ITEM_SEQUENCE_FUNC_INCLUDED

#include "sql/item_func.h"

/**
  Base item of sequence functions.
*/
class Item_seq_func : public Item_int_func {
  typedef Item_int_func super;

 protected:
  THD *m_thd;
  const char *m_db;
  const char *m_table;
  const PT_item_list *m_para_list;
  Table_ref *m_table_list;

 protected:
  Item_seq_func(const POS &pos, THD *thd, const char *db, const char *table)
      : Item_int_func(pos),
        m_thd(thd),
        m_db(db),
        m_table(table),
        m_para_list(nullptr),
        m_table_list(nullptr) {}

  Item_seq_func(const POS &pos, THD *thd, const PT_item_list *para_list)
      : Item_int_func(pos),
        m_thd(thd),
        m_db(nullptr),
        m_table(nullptr),
        m_para_list(para_list),
        m_table_list(nullptr) {}

  virtual bool parse_parameter() = 0;
  bool parse_table_ident();

  virtual bool add_table_to_lex_list(Parse_context *pc) = 0;

 public:
  bool itemize(Parse_context *pc, Item **res) override {
    if (skip_itemize(res)) return false;
    if (super::itemize(pc, res)) return true;

    if (parse_parameter()) return true;

    return add_table_to_lex_list(pc);
  }

  void fix_length_and_dec() {
    unsigned_flag = 1;
    max_length = MAX_BIGINT_WIDTH;
    set_nullable(true);
  }

  bool const_item() const { return 0; }
};

/**
  Implementation of sequence function: NEXTVAL()
*/
class Item_func_nextval : public Item_seq_func {
 public:
  Item_func_nextval(const POS &pos, THD *thd, const char *db, const char *table)
      : Item_seq_func(pos, thd, db, table),
        m_value(SEQUENCE_DEFAULT_BATCH_SIZE) {}
  Item_func_nextval(const POS &pos, THD *thd, const PT_item_list *para_list)
      : Item_seq_func(pos, thd, para_list),
        m_value(SEQUENCE_DEFAULT_BATCH_SIZE) {}

  longlong val_int() override;

  const char *func_name() const override { return "nextval"; }

  virtual void set_sequence_scan();
  virtual bool check_value();
  virtual bool check_param_count();

 protected:
  bool add_table_to_lex_list(Parse_context *pc) override;
  bool parse_parameter() override;
  void set_value(unsigned long long value) { m_value = value; }

  unsigned long long m_value;
};

class Item_func_nextval_skip : public Item_func_nextval {
 public:
  Item_func_nextval_skip(const POS &pos, THD *thd, const char *db,
                         const char *table)
      : Item_func_nextval(pos, thd, db, table) {}

  Item_func_nextval_skip(const POS &pos, THD *thd,
                         const PT_item_list *para_list)
      : Item_func_nextval(pos, thd, para_list) {}

  virtual void set_sequence_scan() override;
  virtual bool check_value() override;
  virtual bool check_param_count() override;

  const char *func_name() const override { return "nextval_skip"; }
};

class Item_func_nextval_show : public Item_func_nextval {
 public:
  Item_func_nextval_show(const POS &pos, THD *thd, const char *db,
                         const char *table)
      : Item_func_nextval(pos, thd, db, table) {}

  Item_func_nextval_show(const POS &pos, THD *thd,
                         const PT_item_list *para_list)
      : Item_func_nextval(pos, thd, para_list) {}

  void set_sequence_scan() override;
  bool check_value() override;
  bool check_param_count() override;

  const char *func_name() const override { return "nextval_show"; }
};

/**
  Implementation of sequence function: CURRVAL()
*/
class Item_func_currval : public Item_seq_func {
 public:
  Item_func_currval(const POS &pos, THD *thd, const char *db, const char *table)
      : Item_seq_func(pos, thd, db, table) {}
  Item_func_currval(const POS &pos, THD *thd, const PT_item_list *para_list)
      : Item_seq_func(pos, thd, para_list) {}

  longlong val_int() override;

  const char *func_name() const override { return "currval"; }

 protected:
  bool add_table_to_lex_list(Parse_context *pc) override;
  bool parse_parameter() override;
};

#endif
