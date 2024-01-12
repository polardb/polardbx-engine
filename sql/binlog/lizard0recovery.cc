/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file sql/lizard0recovery.cc

  Transaction coordinator log recovery

  Created 2023-06-14 by Jianwei.zhao
 *******************************************************/

#include "my_dbug.h"

#include "sql/binlog/lizard0recovery.h"
#include "sql/binlog/recovery.h"
#include "sql/xa_specification.h"

#include "mysql/components/services/log_builtins.h"
#include "sql/log_event.h"

namespace binlog {
/** Add binlog xa spec into spec list */
void XA_spec_recovery::add(const my_xid xid,
                           const Binlog_xa_specification &spec) {
  XA_specification *xa_spec = new (&m_mem_root) Binlog_xa_specification(spec);
  m_spec_list.add(xid, xa_spec);
}

void XA_spec_recovery::add(const XID &xid,
                           const Binlog_xa_specification &spec) {
  XA_specification *xa_spec = new (&m_mem_root) Binlog_xa_specification(spec);
  m_spec_list.add(xid, xa_spec);
}

void log_gtid_set(Gtid_set *gtid_set, bool need_lock, const char *title) {
  char *str;
  gtid_set->to_string(&str, need_lock, nullptr);
  LogErr(SYSTEM_LEVEL, ER_LIZARD_GTID, title, str ? str : "out of memory");
  my_free(str);
}

}  // namespace binlog

void binlog::Binlog_recovery::process_gtid_event(Gtid_log_event &ev) {
  this->m_is_malformed = this->m_in_transaction;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "Gtid_log_event inside the boundary of a sequnece of events "
        "representing an active transaction");

    /** Compatiable with test case binlog_gtid_binlog_recovery_errors.test */
    DBUG_EXECUTE_IF("eval_force_bin_log_recovery", {
      this->m_is_malformed = false;
      this->m_failure_message.assign("");
    };);
    return;
  }
  m_xa_spec.sid()->copy_from(*(ev.get_sid()));
  m_xa_spec.gtid()->set(ev.get_sidno(true), ev.get_gno());
}

void binlog::Binlog_recovery::process_format_event(
    const Format_description_event &ev) {
  m_server_version = do_server_version_int(ev.server_version);
}

void binlog::Binlog_recovery::process_gcn_event(const Gcn_log_event &ev) {
  this->m_is_malformed = this->m_in_transaction;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "Gcn_log_event inside the boundary of a sequnece of events "
        "representing an active transaction");
    return;
  }

  m_xa_spec.set_gcn(ev.get_commit_gcn());
}

/** Gather internal commit xid and spec.*/
void binlog::Binlog_recovery::gather_internal_xa_spec(
    const my_xid xid, const Binlog_xa_specification &spec) {
  XA_spec_list *spec_list = m_xa_spec_recovery->xa_spec_list();

  /** Lookup first, XID didn't allowed to appear twice. */
  auto found = spec_list->commit_map()->find(xid);
  if (found != spec_list->commit_map()->end()) {
    /** Should be blocked by xa_commit_list. */
    assert(0);
    return;
  }
  assert(spec.is_legal_source());
  m_xa_spec_recovery->add(xid, spec);
}

/** Gather internal commit xid and spec.*/
void binlog::Binlog_recovery::gather_external_xa_spec(
    const XID &xid, const Binlog_xa_specification &spec) {
  XA_spec_list *spec_list = m_xa_spec_recovery->xa_spec_list();

  auto found = spec_list->state_map()->find(xid);
  if (found != spec_list->state_map()->end()) {
    // TODO confirm it's xa prepare already.
  }
  assert(spec.is_legal_source());
  m_xa_spec_recovery->add(xid, spec);
}
