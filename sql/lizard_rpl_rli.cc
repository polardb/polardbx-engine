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

/** @file sql/lizard_rpl_rli.cc

  Lizard relay log.

  Created 2023-07-12 by Jiyang.zhang
 *******************************************************/

#include "sql/lizard_rpl_rli.h"
#include "sql/gcn_log_event.h"
#include "sql/log_event.h"
#include "sql/rpl_rli.h"
#include "sql/rpl_rli_pdb.h"

void start_sjg_and_enque_gaq(Relay_log_info *rli, Log_event *ev) {
  Slave_job_group group = Slave_job_group();
  Slave_committed_queue *gaq = rli->gaq;
  rli->mts_groups_assigned++;

  rli->curr_group_isolated = false;
  group.reset(ev->common_header->log_pos, rli->mts_groups_assigned,
              ev->consensus_index);
  // the last occupied GAQ's array index
  gaq->assigned_group_index = gaq->en_queue(&group);
  DBUG_PRINT("info", ("gaq_idx= %ld  gaq->size=%zu", gaq->assigned_group_index,
                      gaq->capacity));
  assert(gaq->assigned_group_index != MTS_WORKER_UNDEF);
  assert(gaq->assigned_group_index < gaq->capacity);
  assert(gaq->get_job_group(rli->gaq->assigned_group_index)
             ->group_relay_log_name == nullptr);
  assert(rli->last_assigned_worker == nullptr || !is_mts_db_partitioned(rli));
}

namespace lizard {
bool Begin_events_before_gtid_manager::is_b_events_before_gtid(Log_event *ev) {
  /** TODO: Cons_log_index <12-07-23, zanye.zjy> */
  return is_gcn_event(ev);
}

bool Begin_events_before_gtid_manager::seen_b_events_before_gtid() const {
  return rli->curr_group_seen_gcn;
}

void Begin_events_before_gtid_manager::mark_as_curr_group(Log_event *ev) {
  // 1. start a group and enque gaq.
  if (!seen_b_events_before_gtid()) {
    start_sjg_and_enque_gaq(rli, ev);
  }

  // 2. Save temporarily to curr_group_da because we don't know
  //    the partition information yet.
  // gcn event must before gtid event.
  assert(!rli->curr_group_seen_gtid && !rli->curr_group_seen_begin);
  Slave_job_item job_item = {ev, rli->get_event_relay_log_number(),
                             rli->get_event_start_pos()};
  // B-event is appended to the Deferred Array associated with GCAP
  rli->curr_group_da.push_back(job_item);

  /** TODO: Cons_log_index. <12-07-23, zanye.zjy> */
  assert(rli->curr_group_da.size() == 1);

  // 3. seen b_events before gtid.
  switch (ev->get_type_code()) {
    case binary_log::GCN_LOG_EVENT:
      rli->curr_group_seen_gcn = true;
      break;
    /** TODO: Cons_log_index. <12-07-23, zanye.zjy> */
    default:
      assert(0);
      break;
  }
}

void Begin_events_before_gtid_manager::reset_seen_status() {
  rli->curr_group_seen_gcn = false;
}

size_t Begin_events_before_gtid_manager::get_size() const {
  size_t n_events = rli->curr_group_seen_gcn ? 1 : 0;
  return n_events;
}

}  // namespace lizard
