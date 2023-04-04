/* Copyright (c) 2008, 2023, Alibaba and/or its affiliates. All rights reserved.

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

#include "storage/innobase/include/lizard0xa0iface.h"
#include "sql/sql_class.h"
#include "sql/binlog.h"
#include "sql/binlog_ext.h"

namespace lizard {
namespace xa {

class Xa_active_pretender {
 public:
  Xa_active_pretender(THD *thd) {
    m_xid_state = thd->get_transaction()->xid_state();

    assert(m_xid_state->has_state(XID_STATE::XA_IDLE));

    m_xid_state->set_state(XID_STATE::XA_ACTIVE);
  }

  ~Xa_active_pretender() {
    assert(m_xid_state->has_state(XID_STATE::XA_ACTIVE));
    m_xid_state->set_state(XID_STATE::XA_IDLE);
  }

 private:
  XID_STATE *m_xid_state;
};

bool transaction_slot_assign(THD *thd, const XID *xid, TSA *tsa) {
  XID_STATE *xid_state = thd->get_transaction()->xid_state();
  assert(xid_state->has_state(XID_STATE::XA_IDLE));
  assert(xid_state->get_xid()->eq(xid));

  /**
    Lizard: In the past, the binlog can only be registered in the XA ACTIVE
    state. But now the registration is completed in the IDLE state.

    So the pretender is introduced to pretend the XA is still in ACTIVE
    status.
  */
  Xa_active_pretender pretender(thd);

  /** 1. start trx in innodb, and register innodb as a participants. */
  if (start_and_register_rw_trx_for_xa(thd)) {
    my_error(ER_XA_PROC_REPLAY_TRX_SLOT_ALLOC_ERROR, MYF(0));
    return true;
  }

  /** 2. alloc transaction slot in innodb, will also write xid into TXN. */
  if (trx_slot_assign_for_xa(thd, tsa)) {
    my_error(ER_XA_PROC_BLANK_XA_TRX, MYF(0));
    return true;
  }

  /** 3. register binlog as another participants if need */
  if (mysql_bin_log.is_open() &&
      (thd->variables.option_bits & OPTION_BIN_LOG) &&
      binlog_start_trans(thd)) {
    my_error(ER_XA_PROC_REPLAY_REGISTER_BINLOG_ERROR, MYF(0));
    return true;
  }

  return false;
}

}  // namespace xa
}  // namespace lizard
