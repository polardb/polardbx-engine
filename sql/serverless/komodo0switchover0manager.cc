/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

#include "komodo0switchover0manager.h"
#include "sql/komodo0state0change0manager.h"
#include "sql/komodo0consensus0xlog.h"
#include "sql/consensus_recovery_manager.h"
#include "sql/events.h"
#include "sql/log.h"
#include "sql/log_event.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/thread_pool_priv.h"
#include "sql/rpl_gtid.h"
#include "sql/rpl_msr.h"
#include "sql/rpl_replica.h"
#include "sql/consensus_admin.h"
#include "sql/rpl_info_factory.h"
#include "sql/rpl_mi.h"
#include "sql/rpl_rli_pdb.h"
#include "sql/sql_parse.h"
#include "sql/sys_vars_consensus.h"
#include "my_inttypes.h"

#include "sql/xa/komodo0xa.h"

#define LOG_PREFIX "ML"

namespace komodo {

uint64_t Consensus_switchover_manager::get_wait_milliseconds_for_old_trx_finish() {
  return opt_consensus_wait_milliseconds_before_change_leader + opt_consensus_wait_unfinished_trx_timeout;
}

uint64_t Consensus_switchover_manager::wait_old_trx_finish()
{
  const ulonglong min_wait_time_ms = opt_consensus_wait_milliseconds_before_change_leader;
  const ulonglong max_wait_time_ms = opt_consensus_wait_unfinished_trx_timeout;
  ulonglong wait_time_ms = 0;

  if (min_wait_time_ms)
    my_sleep(min_wait_time_ms * 1000);

  while (wait_time_ms++ < max_wait_time_ms
         && innodb_hton->ext.has_started_mysql_trx()) {
    my_sleep(1000);//1ms
  }
  xp::system(ER_XP_COMMIT) << "leaderTransfer wait_old_trx_finish"
                         << ", wait_time_ms " << min_wait_time_ms + wait_time_ms
                         << ", has_started_mysql_trx " << innodb_hton->ext.has_started_mysql_trx();
  return min_wait_time_ms + wait_time_ms;
}

uint64_t Consensus_switchover_manager::wait_old_xa_finish() {
  const int max_wait_time_ms = opt_consensus_wait_unfinished_xa_timeout;
  int wait_time_ms = 0;
  while (wait_time_ms++ < max_wait_time_ms && m_xa_running_count > 0) {
    my_sleep(1000);//1ms
  }
  xp::system(ER_XP_COMMIT) << "leaderTransfer wait_old_xa_finish"
                           << ", wait_time_ms " << wait_time_ms
                           << ", xa_running_count "
                           << m_xa_running_count;
  return wait_time_ms;
}

std::pair<uint64_t, uint64_t> Consensus_switchover_manager::wait_old_bgc_finish(
    uint64_t final_sync_index, alisql::Paxos *paxos) {
  const int max_wait_time_ms = opt_consensus_wait_unfinished_bgc_timeout;
  int wait_time_ms = 0;
  while (wait_time_ms++ < max_wait_time_ms &&
         paxos->getCommitIndex() < final_sync_index) {
    my_sleep(1000);  // 1ms
  }
  xp::system(ER_XP_COMMIT) << "leaderTransfer wait_old_bgc_finish"
                           << ", wait_time_ms " << wait_time_ms
                           << ", commitIndex " << paxos->getCommitIndex()
                           << ", final_sync_index " << final_sync_index;

  return {final_sync_index, wait_time_ms};
}

int Consensus_switchover_manager::check_xa_limited(THD *thd, const bool is_commit_one_phase)
{
  if (!thd->slave_thread
      && opt_consensus_disable_commit_before_change_leader
      && ((!is_commit_one_phase && is_in_limit_xa_finish())
          || (is_commit_one_phase && is_in_limit_all()))) {
    xp::warn(ER_XP_COMMIT)
        << "Cannot do xa finish because leadership changing"
        << ", switchover_state: "
        << get_switchover_state()
        << ", xa_running_count: " << m_xa_running_count << ", xid "
        << *thd->get_transaction()->xid_state()->get_xid()
        << ", one_phase " << is_commit_one_phase << ", status "
        << thd->get_transaction()->xid_state()->state_name();
    my_error(ER_CONSENSUS_LEADERSHIP_IS_CHANGING, MYF(0));
    return 1;
  }
  return 0;
}

}