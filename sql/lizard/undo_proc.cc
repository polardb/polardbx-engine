/* Copyright (c) 2018, 2023, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/auth/auth_acls.h"
#include "sql/auth/sql_security_ctx.h"

#include "sql/package/package_parse.h"
#include "sql/package/proc.h"
#include "sql/lizard/undo_proc.h"

#include "sql/mysqld.h"
#include "sql/protocol.h"

#include "sql/lizard/lizard_service.h"

namespace im {

const LEX_CSTRING PROC_UNDO_SCHEMA = {C_STRING_WITH_LEN("dbms_undo")};

Proc *Proc_trunc_status::instance() {
  static Proc_trunc_status *proc = new Proc_trunc_status(key_memory_package);
  return proc;
}

Sql_cmd *Proc_trunc_status::invoke_cmd(THD *thd,
                                       mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_trunc_status(thd, list, this);
}

bool Sql_cmd_trunc_status::pc_execute(THD *) {
  bool error = false;
  return error;
}

void Sql_cmd_trunc_status::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();

  if (error) {
    assert(thd->is_error());
    return;
  }

  std::vector<lizard::trunc_status_t> array;
  innodb_hton->ext.trunc_status(array);

  if (m_proc->send_result_metadata(thd)) return;

  for (auto &undo : array) {
    protocol->start_row();
    protocol->store_string(undo.undo_name.c_str(), undo.undo_name.length(),
                           system_charset_info);
    protocol->store_longlong(undo.file_pages, true);

    protocol->store_longlong(undo.rseg_stat.rseg_pages, true);
    protocol->store_longlong(undo.rseg_stat.history_length, true);
    protocol->store_longlong(undo.rseg_stat.history_pages, true);
    protocol->store_longlong(undo.rseg_stat.secondary_length, true);
    protocol->store_longlong(undo.rseg_stat.secondary_pages, true);

    ulonglong start_utime = my_micro_time();

    protocol->store_longlong(
        undo.oldest_history_utc > 0
            ? (start_utime - undo.oldest_history_utc) / 1000000
            : 0,
        true);
    protocol->store_longlong(
        undo.oldest_secondary_utc > 0
            ? (start_utime - undo.oldest_secondary_utc) / 1000000
            : 0,
        true);

    protocol->store_longlong(
        undo.oldest_history_scn == SCN_NULL ? 0 : undo.oldest_history_scn,
        true);
    protocol->store_longlong(
        undo.oldest_secondary_scn == SCN_NULL ? 0 : undo.oldest_secondary_scn,
        true);

    protocol->store_longlong(
        undo.oldest_history_gcn == GCN_NULL ? 0 : undo.oldest_history_gcn,
        true);
    protocol->store_longlong(
        undo.oldest_secondary_gcn == GCN_NULL ? 0 : undo.oldest_secondary_gcn,
        true);

    if (protocol->end_row()) return;
  }

  my_eof(thd);
}

Proc *Proc_purge_status::instance() {
  static Proc_purge_status *proc = new Proc_purge_status(key_memory_package);
  return proc;
}

Sql_cmd *Proc_purge_status::invoke_cmd(THD *thd,
                                       mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_purge_status(thd, list, this);
}

bool Sql_cmd_purge_status::pc_execute(THD *) {
  bool error = false;
  return error;
}

void Sql_cmd_purge_status::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();

  if (error) {
    assert(thd->is_error());
    return;
  }

  lizard::purge_status_t status;
  innodb_hton->ext.purge_status(status);

  if (m_proc->send_result_metadata(thd)) return;

    protocol->start_row();
    protocol->store_longlong(status.history_length, true);
    protocol->store_longlong(status.current_scn, true);
    protocol->store_longlong(status.current_gcn, true);
    protocol->store_longlong(status.purged_scn, true);
    protocol->store_longlong(status.purged_gcn, true);
    protocol->store_longlong(status.erased_scn, true);
    protocol->store_longlong(status.erased_gcn, true);

    if (protocol->end_row()) return;

    my_eof(thd);
}

} /* namespace im */
