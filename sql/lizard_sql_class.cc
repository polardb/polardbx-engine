/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file sql/lizard_sql_class.cc

  lizard sql class.

  Created 2024-07-24 by Jiyang.zhang
 *******************************************************/

#include "sql/lizard_sql_class.h"
#include "sql/sql_class.h"

namespace lizard {
/** Constructor */
Transaction_policy_state::Transaction_policy_state()
    : m_innodb_snapshot_gcn(GCN_NULL),
      m_innodb_commit_gcn(GCN_NULL),
      m_innodb_current_snapshot_gcn(false),
      m_opt_query_via_flashback_area(false),
      m_owned_vision_gcn(),
      m_cpolicy_ctx() {}

/** Backup transaction policy
 *
 * @param[in]		which to backup.*/
void Transaction_policy_state::backup(const THD *thd) {
  m_innodb_snapshot_gcn = thd->variables.innodb_snapshot_gcn;
  m_innodb_commit_gcn = thd->variables.innodb_commit_gcn;
  m_innodb_current_snapshot_gcn = thd->variables.innodb_current_snapshot_gcn;
  m_opt_query_via_flashback_area = thd->variables.opt_query_via_flashback_area;
  m_owned_vision_gcn = thd->owned_vision_gcn;

  m_cpolicy_ctx.copy_from(thd->cpolicy_ctx);
}

  /** Restore transtaction policy to thd.
   *
   * @param[out]	which to restore. */
void Transaction_policy_state::restore(THD *thd) const {
  thd->variables.innodb_snapshot_gcn = m_innodb_snapshot_gcn;
  thd->variables.innodb_commit_gcn = m_innodb_commit_gcn;
  thd->variables.innodb_current_snapshot_gcn = m_innodb_current_snapshot_gcn;
  thd->variables.opt_query_via_flashback_area = m_opt_query_via_flashback_area;
  thd->owned_vision_gcn = m_owned_vision_gcn;

  thd->cpolicy_ctx.copy_from(m_cpolicy_ctx);
}

Implicit_trans_policy_guard::Implicit_trans_policy_guard(THD *thd)
    : m_thd(thd), m_state() {
  m_state.backup(m_thd);
  /** Reset query and commit policy, and take single shard. */
  m_thd->reset_trans_policy();
  m_thd->cpolicy_ctx.activate_single_shard();
}

}  // namespace lizard
