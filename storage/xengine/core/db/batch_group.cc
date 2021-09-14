/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "db/batch_group.h"
#include <thread>
#include "db/column_family.h"
#include "port/port.h"
#include "util/random.h"
#include "util/sync_point.h"

using namespace xengine;
using namespace util;
using namespace common;

namespace xengine {
namespace db {

void GroupSlot::switch_leader_on_timeout(WriteRequest* current_leader) {
  if (leader_writer_.load() != current_leader) return;
  slot_mutex_.lock();
  if (leader_writer_.load() == current_leader)
    this->leader_writer_.store(nullptr);
  slot_mutex_.unlock();
}

bool GroupSlot::try_to_push(WriteRequest* writer,
                            JoinGroupStatus& join_status) {
  bool is_leader = false;
  join_status.async_commit_ = writer->async_commit_;
  if (!slot_mutex_.try_lock()) {
    return false;
  }
  // lock succeed;
  if (nullptr ==
      leader_writer_.load()) {  // no one is leader, set ourself leader
    leader_writer_.store(writer);
    is_leader = true;
  }

  WriteRequest* leader = leader_writer_.load();
  leader->add_follower(writer);

  // if writer_count_ > max_group_size
  // we should wake up the leader
  if ((join_status.fast_group_ == 0 && leader->follower_vector_.size() >= this->max_group_size_)
     ||join_status.fast_group_ == 1) {
    // wake up the leader writer, set_state first then switch
    WriteRequest* old_leader = leader_writer_.load();
    BatchGroupManager::set_state(old_leader, W_STATE_GROUP_LEADER);
    // we hold the slot mutex so it safe to set
    this->leader_writer_.store(nullptr);
  }
  slot_mutex_.unlock();

  if (is_leader)
    join_status.join_state_ = W_STATE_GROUP_LEADER;
  else
    join_status.join_state_ = W_STATE_GROUP_FOLLOWER;

  return true;
}

int BatchGroupManager::init() {
  int ret = 0;
  if (inited_) return ret;
  bool fail = false;
  for (size_t index = 0; index < this->slot_array_size_; index++) {
//    GroupSlot* slot = new GroupSlot(this->max_group_size_);
    GroupSlot* slot = MOD_NEW_OBJECT(memory::ModId::kWriteRequest, GroupSlot, this->max_group_size_);
    if (nullptr == slot) {
      fail = true;
      break;
    }
    slot_array_.push_back(slot);
  }
  if (fail) {
    for (GroupSlot* slot : slot_array_) {
//      delete slot;
      MOD_DELETE_OBJECT(GroupSlot, slot);
    }
    slot_array_.clear();
    ret = Status::MemoryLimit().code();
  }
  this->inited_ = true;
  return ret;
}

void BatchGroupManager::destroy() {
  if (!inited_) return;
  for (GroupSlot* slot : slot_array_) {
//    delete slot;
    MOD_DELETE_OBJECT(GroupSlot, slot);
  }
  slot_array_.clear();
  this->inited_ = false;
}

void BatchGroupManager::join_batch_group(WriteRequest* writer,
                                         JoinGroupStatus& join_status) {
  GroupSlot* slot = nullptr;
  size_t index = writer->start_time_us_;
  bool push_succeed = false;
  while (!push_succeed) {  // we try to find slot in endless loop,
    slot = slot_array_[index % this->slot_array_size_];
    push_succeed = slot->try_to_push(writer, join_status);
    index++;
  }

  assert(nullptr != slot);
  assert(join_status.join_state_ == W_STATE_GROUP_LEADER ||
         join_status.join_state_ == W_STATE_GROUP_FOLLOWER);

  if (W_STATE_GROUP_FOLLOWER == join_status.join_state_) {
    return;  // as follower we can returen directly
  }

  // as leader wait wait ourself be setted as leader or timeout
  bool wait_state =
      await_state(writer, W_STATE_GROUP_LEADER, this->max_leader_wait_time_us_);
  if (!wait_state) {  // timeout, we should set ourself leader
    slot->switch_leader_on_timeout(writer);
    set_state(writer, W_STATE_GROUP_LEADER);
  }

  assert(writer->state_ == W_STATE_GROUP_LEADER);
}

void BatchGroupManager::set_state(WriteRequest* w, uint16_t new_state) {
  auto state = w->state_.load(std::memory_order_acquire);
  if (state == W_STATE_LOCKED_WAITING ||
      !w->state_.compare_exchange_strong(state, new_state)) {
    assert(w->state_ == W_STATE_LOCKED_WAITING);
    std::lock_guard<std::mutex> guard(w->state_mutex());
    assert(w->state_.load(std::memory_order_relaxed) != new_state);
    w->state_.store(new_state, std::memory_order_relaxed);
    w->state_cv().notify_one();
  }
}

// param[WriteRequest] writer to wait_state
// param[goal_mask] [in]
// param[wait_time] [in] block wait time
bool BatchGroupManager::await_state(WriteRequest* w, uint16_t goal_mask,
                                    uint64_t wait_timeout_us) {
  bool state_satisfied = false;
  bool wait_satisfied = true;
  auto state = w->state_.load(std::memory_order_acquire);
  w->create_mutex();
  assert(state != W_STATE_LOCKED_WAITING);
  if ((state & goal_mask) == 0 &&
      w->state_.compare_exchange_strong(state, W_STATE_LOCKED_WAITING)) {
    // we have permission (and an obligation) to use StateMutex
    std::chrono::microseconds wait_time(wait_timeout_us);
    std::unique_lock<std::mutex> guard(w->state_mutex());
    if (wait_timeout_us > 0) {
      wait_satisfied = w->state_cv().wait_for(guard, wait_time, [w] {
        return w->state_.load(std::memory_order_relaxed) !=
               W_STATE_LOCKED_WAITING;
      });
    } else {
      w->state_cv().wait(guard, [w] {
        return w->state_.load(std::memory_order_relaxed) !=
               W_STATE_LOCKED_WAITING;
      });
    }
  }
  state = w->state_.load(std::memory_order_relaxed);
  if ((state & goal_mask) != 0 && wait_satisfied) {
    state_satisfied = true;
  }
  return state_satisfied;
}
}  // namespace db
}  // namespace xengine
