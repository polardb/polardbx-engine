/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxySQL hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxySQL.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_TRANS_PROC_COMMON_INCLUDED
#define SQL_TRANS_PROC_COMMON_INCLUDED

#include "my_inttypes.h"
#include "sql/package/proc.h"
#include "sql/sql_class.h"
#include "sql/sql_digest_stream.h"
#include "sql/sql_lex.h"

class THD;

namespace im {

/* Native procedure schema: dbms_trans */
extern LEX_CSTRING TRANS_PROC_SCHEMA;

/*  Base class of all of the native procedure interfaces */
class Trans_proc_base : public Proc {
 public:
  explicit Trans_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << TRANS_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/* Backup current execution context */
class Sub_statement_context {
 public:
  explicit Sub_statement_context(THD *thd);

  ~Sub_statement_context();

  void start_statement();

  void end_statement();

 private:
  THD *m_thd;
  query_id_t m_old_query_id;
  LEX *m_old_lex;
  LEX m_lex;
  Query_arena m_arena;
  Query_arena arena_backup;
  Query_arena *save_stmt_arena;
  sql_digest_state m_digest_state;
  sql_digest_state *m_old_digest_state;
#ifdef HAVE_PSI_STATEMENT_INTERFACE
  PSI_statement_locker_state m_psi_state;
  PSI_statement_locker *m_old_locker;
#endif
  LEX_CSTRING m_old_query;

  bool m_inited;
};

} /* namespace im */
#endif
