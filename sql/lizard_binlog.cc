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

/** @file sql/lizard_binlog.h

  Binlog behaviours related to lizard system.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/
#include "sql/binlog.h"

#include "lizard_iface.h"
#include "sql/gcn_log_event.h"
#include "sql/lizard_binlog.h"

bool Gcn_manager::assign_gcn_to_flush_group(THD *first_seen) {
  // my_gcn_t gcn = MYSQL_GCN_NULL;
  bool err = false;

  for (THD *head = first_seen; head; head = head->next_to_commit) {
    /*
    if (head->variables.innodb_commit_gcn != MYSQL_GCN_NULL) {
      gcn = head->variables.innodb_commit_gcn;
    } else if (get_xa_opt(head) != XA_ONE_PHASE) {
      err = ha_acquire_gcn(&gcn);
      assert(err || gcn != MYSQL_GCN_NULL);
    } else {
      gcn = MYSQL_GCN_NULL;
      assert(head->get_transaction()->m_flags.real_commit);
      assert(!head->get_transaction()->m_flags.commit_low);
    }
    head->m_extra_desc.m_commit_gcn = gcn;

    */
  }
  return err;
}

