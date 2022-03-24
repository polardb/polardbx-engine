/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyEngine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyEngine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/binlog_ext.h"
#include "sql/sql_class.h"
#include "sql/log_event_ext.h"

Binlog_ext mysql_bin_log_ext;

Binlog_ext::Binlog_ext() {}

bool Binlog_ext::assign_gcn_to_flush_group(THD *first_seen) {
  uint64_t gcn = MYSQL_GCN_NULL;
  bool error = false;

  for (THD *head = first_seen; head; head = head->next_to_commit) {
    if (head->variables.innodb_commit_gcn != MYSQL_GCN_NULL) {
      gcn = head->variables.innodb_commit_gcn;
    } else {
      error = ha_acquire_gcn(&gcn);
    }

#ifndef DBUG_OFF
    if (!error)
      DBUG_ASSERT(gcn != MYSQL_GCN_NULL);
#endif

    head->m_extra_desc.m_commit_gcn = gcn;
  }

  return error;
}

/**
  Write the Gcn_log_event to the binary log (prior to writing the
  statement or transaction cache).

  @param thd Thread that is committing.
  @param cache_data The cache that is flushing.
  @param writer The event will be written to this Binlog_event_writer object.

  @retval false Success.
  @retval true Error.
*/
bool Binlog_ext::write_gcn(THD *thd,  Binlog_event_writer *writer) {
  DBUG_TRACE;

  if (!opt_gcn_write_event) return false;

  Gcn_log_event gcn_evt(thd);
  bool ret = gcn_evt.write(writer);
  return ret;
}