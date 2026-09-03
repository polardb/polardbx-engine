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

#ifndef CONSENSUS_SERVERLESS_MANAGER_INCLUDE
#define CONSENSUS_SERVERLESS_MANAGER_INCLUDE

#include <atomic>
#include <iterator>
#include <map>
#include <queue>
#include <vector>
#include "my_macros.h"
#include <include/scope_guard.h>

class THD;

namespace alisql {
class Paxos;
}

namespace komodo {
enum Consensus_switchover_state {
  CSS_LIMIT_NONE = 0,
  CSS_LIMIT_NEW_TRX = 1,
  CSS_LIMIT_XA_FINISH = 2,
  CSS_LIMIT_ALL = 3,
};

class Base_switchover_manager {
 public:
  Base_switchover_manager() = default;
  virtual ~Base_switchover_manager() = default;
  virtual int check_xa_limited(THD *, const bool ) = 0;
  virtual void inc_xa_running_count() = 0;
  virtual void dec_xa_running_count() = 0;
  virtual bool is_in_switchover() const = 0;

};

class Autonomy_switchover_manager : public Base_switchover_manager {
 public:
  Autonomy_switchover_manager() = default;
  virtual ~Autonomy_switchover_manager() = default;
  int check_xa_limited(THD *, const bool) override { return 0; }
  void inc_xa_running_count() override {}
  void dec_xa_running_count() override {}
  bool is_in_switchover() const override { return false; }
};

class Consensus_switchover_manager : public Base_switchover_manager {
 public:
  Consensus_switchover_manager() = default;
  virtual ~Consensus_switchover_manager() = default;

  void set_limit_none() { m_switchover_state.store(CSS_LIMIT_NONE); }
  void set_limit_new_trx() { m_switchover_state.store(CSS_LIMIT_NEW_TRX); }
  void set_limit_xa_finish() { m_switchover_state.store(CSS_LIMIT_XA_FINISH); }
  void set_limit_all() { m_switchover_state.store(CSS_LIMIT_ALL); }
  bool is_in_switchover() const override { return m_switchover_state.load() != CSS_LIMIT_NONE; }
  bool is_in_limit_all() const { return m_switchover_state.load() == CSS_LIMIT_ALL; }
  bool is_in_limit_xa_finish() const { return m_switchover_state.load() >= CSS_LIMIT_XA_FINISH; }
  uint64_t get_switchover_state() const { return m_switchover_state.load(); }
  uint64_t get_limit_new_trx_state() const { return CSS_LIMIT_NEW_TRX; }
  uint64_t get_limit_none_state() const { return CSS_LIMIT_NONE; }
  /** Increase thread running for xa command */
  void inc_xa_running_count() override { m_xa_running_count++; }
  /** Decrease thread running for xa command */
  void dec_xa_running_count() override { m_xa_running_count--; }
  uint64_t get_wait_milliseconds_for_old_trx_finish();
  /* 
    First wait: Wait old trx finish
    use opt_consensus_wait_milliseconds_before_change_leader as min_wait_time_ms,
    use opt_consensus_wait_unfinished_trx_timeout as extra max_wait_time_ms
    check condition: innodb has no started trx
  */
  uint64_t wait_old_trx_finish();
  /* 
    Second wait: Wait old xa finish
    use opt_consensus_wait_unfinished_xa_timeout as max_wait_time_ms
    check condition: no xa_running
  */
  uint64_t wait_old_xa_finish();
  /* 
    Third wait: Wait old binlog group commit finish
    use opt_consensus_wait_unfinished_bgc_timeout as max_wait_time_ms
    check condition: final_sync_index = commit_index
  */
  std::pair<uint64_t, uint64_t> wait_old_bgc_finish(uint64_t final_sync_index, alisql::Paxos *paxos);
  int check_xa_limited(THD *thd, const bool is_commit_one_phase) override ;

 private:
   std::atomic<uint32_t> m_switchover_state{CSS_LIMIT_NONE};
   std::atomic<uint64_t> m_xa_running_count{0};

};

void *run_consensus_state_change(void *arg);

}  // namespace komodo

#endif /* CONSENSUS_SERVERLESS_MANAGER_INCLUDE */