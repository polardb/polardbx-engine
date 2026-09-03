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

#include "komodo0state0change0manager.h"
#include "komodo0consensus0xlog.h"
#include "consensus_recovery_manager.h"
#include "events.h"
#include "log.h"
#include "log_event.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/thread_pool_priv.h"
#include "rpl_gtid.h"
#include "rpl_msr.h"
#include "rpl_replica.h"
#include "sql/consensus_admin.h"
#include "sql/rpl_info_factory.h"
#include "sql/rpl_mi.h"
#include "sql/rpl_rli_pdb.h"
#include "sql_parse.h"
#include "sys_vars_consensus.h"
#include "my_inttypes.h"

#include "sql/xa/komodo0xa.h"
#include "komodo0xlog.h"

#define LOG_PREFIX "ML"

namespace komodo {

int State_change_manager::init() {
  key_LOCK_consensus_state_change =
      key_CONSENSUSLOG_LOCK_Consensus_state_change;
  key_COND_consensus_state_change = key_COND_Consensus_state_change;
  mysql_mutex_init(key_LOCK_consensus_state_change,
                   &LOCK_consensus_state_change, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_consensus_state_change,
                  &COND_consensus_state_change);


  consensus_state_change_is_running = true;
  if (mysql_thread_create(key_thread_consensus_state_change,
                          &consensus_state_change_thread_handle, NULL,
                          run_consensus_state_change,
                          (void *)this)) {
    xp::error(ER_XP_0) << "Fail to create thread run_consensus_state_change.";
    abort();
  }
  return 0;
}

int State_change_manager::cleanup() {
  const bool old_consensus_state_change_is_running = consensus_state_change_is_running;
  mysql_mutex_lock(&LOCK_consensus_state_change);
  consensus_state_change_is_running = false;
  mysql_mutex_unlock(&LOCK_consensus_state_change);
  mysql_cond_broadcast(&COND_consensus_state_change);
  mysql_cond_broadcast(&COND_server_started);
  if (old_consensus_state_change_is_running)
    my_thread_join(&consensus_state_change_thread_handle, NULL);

  mysql_mutex_destroy(&LOCK_consensus_state_change);
  mysql_cond_destroy(&COND_consensus_state_change);
  return 0;
}

void State_change_manager::add_state_change_request(
    alisql::Paxos::StateType state, uint64_t term, uint64_t commitIndex) {
  ConsensusStateChange state_change = {state, term, commitIndex};

  mysql_mutex_lock(&LOCK_consensus_state_change);
  consensus_state_change_queue.push_back(state_change);
  mysql_mutex_unlock(&LOCK_consensus_state_change);
  mysql_cond_broadcast(&COND_consensus_state_change);
}

void State_change_manager::lock_consensus_state_change() {
  mysql_mutex_lock(&LOCK_consensus_state_change);
}

void State_change_manager::unlock_consensus_state_change() {
  mysql_mutex_unlock(&LOCK_consensus_state_change);
}

void State_change_manager::wait_state_change_cond() {
  mysql_cond_wait(&COND_consensus_state_change, &LOCK_consensus_state_change);
}

ConsensusStateChange State_change_manager::get_state_change_from_queue() {
  ConsensusStateChange state_change;

  if (consensus_state_change_queue.size() > 0) {
    state_change = consensus_state_change_queue.front();
    consensus_state_change_queue.pop_front();
  }
  return state_change;
}

void *run_consensus_state_change(void *arg) {
  if (my_thread_init()) return NULL;

  int error = 0;
  State_change_manager *state_change_manager = (State_change_manager *)arg;

  /*
    Waiting until mysqld_server_started == true to ensure that all server
    components have been successfully initialized.
  */
  mysql_mutex_lock(&LOCK_server_started);
  while (!mysqld_server_started && state_change_manager->consensus_state_change_is_running)
    mysql_cond_wait(&COND_server_started, &LOCK_server_started);

  mysql_mutex_unlock(&LOCK_server_started);
  while (state_change_manager->consensus_state_change_is_running) {
    auto &log_manager = xlog_engine_get_log_manager();
    state_change_manager->lock_consensus_state_change();
    if (state_change_manager->is_state_change_queue_empty() && state_change_manager->consensus_state_change_is_running)
      state_change_manager->wait_state_change_cond();
    if (state_change_manager->is_state_change_queue_empty()) {
      state_change_manager->unlock_consensus_state_change();
      continue;
    }
    ConsensusStateChange state_change =
        state_change_manager->get_state_change_from_queue();
    if (state_change.state != alisql::Paxos::LEADER) {
      if (xlog_engine_binlog_working()) {
        error = log_manager.wait_leader_degraded(state_change.term,
                                                           state_change.index);
      } else if (state_change.state != alisql::Paxos::CANDIDATE) {
        error =
            log_manager.wait_follower_change_term(state_change.term);
      }
    } else {
      // must be candidate ->leader
      error = log_manager.wait_follower_upgraded(state_change.term,
                                                           state_change.index);
    }

    state_change_manager->unlock_consensus_state_change();

    if (error) {
      xp::error(ER_XP_0) << "Consensus state change failed";
      abort();
    }
  }

  my_thread_end();
  return NULL;
}

}