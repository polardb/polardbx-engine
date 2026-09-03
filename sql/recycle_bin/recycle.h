/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_RECYCLE_BIN_RECYCLE_INCLUDED
#define SQL_RECYCLE_BIN_RECYCLE_INCLUDED

#include "mysql/components/services/bits/psi_memory_bits.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"

class THD;

namespace im {

namespace recycle_bin {

/* Init recycle bin context */
void recycle_init();

/* Deinit recycle bin context */
void recycle_deinit();

/* Start recycle scheduler thread */
bool recycle_scheduler_start(bool bootstrap);

/* Recycle bin memory instrument */
extern PSI_memory_key key_memory_recycle;

/* Thread local state */
class Recycle_state {
 public:
  explicit Recycle_state()
      : m_set(false), m_priv_relax(false), m_name_len(0) {
    m_name[0] = '\0';
  }

  /* THD owns it by pointer and guards embed it by value, so copying a
     thread-local recycle state is meaningless. */
  Recycle_state(const Recycle_state &) = delete;
  Recycle_state &operator=(const Recycle_state &) = delete;

  void set() { m_set = true; }

  bool is_set() { return m_set; }

  bool is_priv_relax() { return m_priv_relax; }

  void set_priv_relax() { m_priv_relax = true; }

  void set_name(const char *name, size_t length) {
    assert(name != nullptr);
    assert(length > 0 && length <= NAME_LEN);
    memcpy(m_name, name, length);
    m_name[length] = '\0';
    m_name_len = length;
  }

  LEX_CSTRING name() const {
    return m_name_len == 0 ? LEX_CSTRING{nullptr, 0}
                           : LEX_CSTRING{m_name, m_name_len};
  }

  bool has_name() const { return m_name_len > 0; }

 private:
  /* label thread state to treat specially */
  bool m_set;
  /* Whether check the recycle schema access rule */
  bool m_priv_relax;
  /* The custom name selected by the current recycle DDL. */
  char m_name[NAME_LEN + 1];
  size_t m_name_len;
};

/* Lex local state */
class Recycle_lex {
 public:
  explicit Recycle_lex(THD *thd) : m_thd(thd), m_backed_up_lex(thd->lex) {
    thd->lex = &m_lex;
    lex_start(thd);
  }
  ~Recycle_lex() {
    lex_end(&m_lex);
    m_thd->lex = m_backed_up_lex;
  }

 private:
  THD *m_thd;
  LEX *m_backed_up_lex;
  LEX m_lex;
};

/* Save the thread recycle state within once command execution. */
class Recycle_process_context {
 public:
  explicit Recycle_process_context(THD *thd);

  virtual ~Recycle_process_context();

 private:
  THD *m_thd;
  Recycle_state *m_saved_recycle_state;
  Recycle_state m_recycle_state;
};

} /* namespace recycle_bin */

} /* namespace im */
#endif
