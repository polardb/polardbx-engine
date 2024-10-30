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

/** @file sql/lizard_sql_class.h

  lizard sql class.

  Created 2024-07-24 by Jiyang.zhang
 *******************************************************/

#ifndef SQL_LIZARD_SQL_CLASS_INCLUDED
#define SQL_LIZARD_SQL_CLASS_INCLUDED

#include "my_inttypes.h"

#include "sql/lizard/lizard_service.h"
#include "sql/xa/lizard_cmmt_policy.h"

class THD;

namespace lizard {

/** Transaction policy state which included query policy and commit policy */
class Transaction_policy_state {
 public:
  explicit Transaction_policy_state();

  ~Transaction_policy_state() {}

  /** Backup transaction policy
   *
   * @param[in]		which to backup.*/
  void backup(const THD *thd);

  /** Restore transtaction policy to thd.
   *
   * @param[out]	which to restore. */
  void restore(THD *thd) const;

 private:
  /** Query policy related context. */
  ulonglong m_innodb_snapshot_gcn;
  ulonglong m_innodb_commit_gcn;
  ulonglong m_innodb_current_snapshot_gcn;
  bool m_opt_query_via_flashback_area;
  MyVisionGCN m_owned_vision_gcn;

  /** Commit policy related context. */
  Commit_policy_ctx m_cpolicy_ctx;
};

/** Backup current transaction policy and begin a single shard commit when
 * statement cause implicit commit.*/
class Implicit_trans_policy_guard {
 public:
  explicit Implicit_trans_policy_guard(THD *thd);

  ~Implicit_trans_policy_guard() { m_state.restore(m_thd); }

 private:
  /** Backup target.*/
  THD *m_thd;
  /** backuped policy state. */
  Transaction_policy_state m_state;
};

}  // namespace lizard

#endif
