/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */
#include "appliedindex_checker.h"
#include <sstream>
#include "sql/replica_read_manager.h"

AppliedIndexChecker appliedindex_checker;

void AppliedIndexChecker::prepare(intervalType &inv) {
  std::lock_guard<std::mutex> lg(lock_);
  assert(group_queue_.size() == 0 ||
         group_queue_.back().group_max < inv.group_min);
  group_queue_.push_back(inv);
}

int AppliedIndexChecker::commit(uint64 index) {
  std::lock_guard<std::mutex> lg(lock_);
  for (auto &inv : group_queue_) {
    if (unlikely(inv.group_min > index)) return -1;
    if (inv.group_min <= index && index <= inv.group_max) {
      inv.size--;
      if (inv.size == 0) {
        // update appliedindex
        uint64 group_max = 0;
        while (group_queue_.front().size == 0) {
          group_max = group_queue_.front().group_max;
          group_queue_.pop_front();
          if (group_queue_.size() == 0) break;
        }
        if (group_max != 0) {
          group_max = opt_appliedindex_force_delay >= group_max
                          ? 0
                          : group_max - opt_appliedindex_force_delay;
          consensus_ptr->updateAppliedIndex(group_max);
          replica_read_manager.update_lsn(group_max);
        }
      }
      break;
    }
  }
  return 0;
}

void AppliedIndexChecker::reset() {
  std::lock_guard<std::mutex> lg(lock_);
  group_queue_.clear();
}

const char *AppliedIndexChecker::get_group_queue_status() {
  std::lock_guard<std::mutex> lg(lock_);
  status_buf_.clear();
  std::ostringstream status_stream;
  int cnt = 0;
  for (auto &inv : group_queue_) {
    cnt++;
    if (cnt > 10) {
      status_stream << "...";
      break;
    }
    status_stream << "[" << inv.size << "," << inv.group_min << "-"
                  << inv.group_max << "]";
  }
  status_buf_ = status_stream.str();
  return status_buf_.c_str();
}
