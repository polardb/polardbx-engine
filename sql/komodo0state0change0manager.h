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

#ifndef KOMODO_STATE_CHANGE_MANAGER_INCLUDE
#define KOMODO_STATE_CHANGE_MANAGER_INCLUDE

#include <include/scope_guard.h>
#include <atomic>
#include <iterator>
#include <map>
#include <queue>
#include <vector>
#include "basic_ostream.h"
#include "binlog.h"
#include "my_macros.h"
#include "paxos.h"

namespace komodo {

struct ConsensusStateChange {
  alisql::Paxos::StateType state{alisql::Paxos::NOROLE};
  uint64_t term{0};
  uint64_t index{0};
};

class State_change_manager {
 public:
  State_change_manager() = default;
  ~State_change_manager() = default;

  int init();
  int cleanup();

  void lock_consensus_state_change();
  void unlock_consensus_state_change();
  void wait_state_change_cond();
  ConsensusStateChange get_state_change_from_queue();

  bool is_state_change_queue_empty() {
    return consensus_state_change_queue.empty();
  }
  void add_state_change_request(alisql::Paxos::StateType state, 
                                uint64_t term, uint64_t commitIndex);

 public:
  PSI_mutex_key key_LOCK_consensus_state_change;
  PSI_cond_key key_COND_consensus_state_change;
  mysql_mutex_t LOCK_consensus_state_change;
  mysql_cond_t COND_consensus_state_change;
  my_thread_handle consensus_state_change_thread_handle;
  std::deque<ConsensusStateChange> consensus_state_change_queue;
  bool consensus_state_change_is_running;
};


void *run_consensus_state_change(void *arg);

}  // namespace komodo

#endif  // KOMODO_STATE_CHANGE_MANAGER_INCLUDE