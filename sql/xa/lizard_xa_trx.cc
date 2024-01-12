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

#include "sql/xa/lizard_xa_trx.h"

#include "sql/binlog.h"
#include "sql/handler.h"
#include "sql/lizard0handler.h"
#include "sql/lizard_binlog.h"
#include "sql/mysqld.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"

#include "mysql/components/services/log_builtins.h"

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

/**
  Apply for a readwrite transaction specially for external xa through allocating
  transaction slot from transaction slot storage engine.

    1. start trx in transaction slot storage engine.[ttse]
    2. register ttse as a participants
    3. alloc transaction slot in ttse
    4. register binlog as another participants if need

  @param[in]	Thread handler
  @param[in]	XID
  @param[out]	transaction slot address
*/
bool apply_trx_for_xa(THD *thd, const XID *xid, my_slot_ptr_t *slot_ptr) {
  /** Take innodb as transaction slot storage engine. */
  handlerton *ttse = innodb_hton;

#ifndef NDEBUG
  XID_STATE *xid_state = thd->get_transaction()->xid_state();
  assert(xid_state->has_state(XID_STATE::XA_IDLE));
  assert(xid_state->get_xid()->eq(xid));
  assert(ttse);
#endif
  (void)xid;

  /**
    Lizard: In the past, the binlog can only be registered in the XA ACTIVE
    state. But now the registration is completed in the IDLE state.

    So the pretender is introduced to pretend the XA is still in ACTIVE
    status.
  */
  Xa_active_pretender pretender(thd);

  /** 1. start trx within transaction slot storage engine, and register it as a
   * participants. */
  if (ttse->ext.start_trx_for_xa(ttse, thd, true)) {
    my_error(ER_XA_PROC_REPLAY_TRX_SLOT_ALLOC_ERROR, MYF(0));
    return true;
  }

  /** 2. alloc transaction slot in ttse, will also write xid into TXN. */
  if (ttse->ext.assign_slot_for_xa(thd, slot_ptr)) {
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

/*************************************************
 *                Heartbeat Freezer              *
 *************************************************/
bool opt_no_heartbeat_freeze;
bool no_heartbeat_freeze;

uint64_t opt_no_heartbeat_freeze_timeout;

bool Lazy_printer::print(const char *msg) {
  if (unlikely(m_first)) {
    LogErr(INFORMATION_LEVEL, ER_LIZARD, msg);
    m_timer.update();
    m_first = false;
    return true;
  } else if (m_timer.since_last_update() > m_internal_secs) {
    LogErr(INFORMATION_LEVEL, ER_LIZARD, msg);
    m_timer.update();
    return true;
  }

  return false;
}

void Lazy_printer::reset() { m_first = true; }

Heartbeat_freezer hb_freezer;

void hb_freezer_heartbeat() { hb_freezer.heartbeat(); }

bool hb_freezer_determine_freeze() { return hb_freezer.determine_freeze(); }

bool hb_freezer_is_freeze() {
  return opt_no_heartbeat_freeze && hb_freezer.is_freeze();
}

bool cn_heartbeat_timeout_freeze_updating(LEX *const lex) {
  DBUG_EXECUTE_IF("hb_timeout_do_not_freeze_operation", { return false; });
  switch (lex->sql_command) {
    case SQLCOM_ADMIN_PROC:
      break;

    default:
      if ((sql_command_flags[lex->sql_command] & CF_CHANGES_DATA) &&
          lizard::xa::hb_freezer_is_freeze() && likely(mysqld_server_started)) {
        my_error(ER_XA_PROC_HEARTBEAT_FREEZE, MYF(0));
        return true;
      }
  }

  return false;
}

bool cn_heartbeat_timeout_freeze_applying_event(THD *thd) {
  static Lazy_printer printer(60);

  if (lizard::xa::hb_freezer_is_freeze()) {
    THD_STAGE_INFO(thd, stage_wait_for_cn_heartbeat);

    printer.print(
        "Applying event is blocked because no heartbeat has been received "
        "for a long time. If you want to advance it, please call "
        "dbms_xa.send_heartbeat() (or set global innodb_cn_no_heartbeat_freeze "
        "= 0).");

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    return true;
  } else {
    printer.reset();
    return false;
  }
}

}  // namespace xa
}  // namespace lizard
