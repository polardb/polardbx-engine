/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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


//
// Created by zzy on 2023/1/5.
//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../coders/protocol_fwd.h"
#include "../common_define.h"
#include "../utility/lru.h"

namespace polarx_rpc {

class CrequestCache final {
  NO_COPY_MOVE(CrequestCache)

 public:
  struct plan_store_t final {
    std::string audit_str;
    std::shared_ptr<PolarXRPC::ExecPlan::AnyPlan> plan;

    plan_store_t() : audit_str(), plan() {}
  };

 private:
  const size_t hash_slots_;
  std::vector<
      std::unique_ptr<CcopyableLru<std::string, std::shared_ptr<std::string>>>>
      sql_cache_;  /// digest -> sql
  std::vector<std::unique_ptr<CcopyableLru<std::string, plan_store_t>>>
      plan_cache_;  /// digest -> <audit_str, plan>
  std::hash<std::string> hasher_;

 public:
  CrequestCache(size_t cache_size, size_t hash_slots)
      : hash_slots_(hash_slots) {
    sql_cache_.reserve(hash_slots_);
    plan_cache_.reserve(hash_slots_);
    auto cache_per_slot = cache_size / hash_slots_;
    if (cache_per_slot < 1) cache_per_slot = 1;
    for (size_t i = 0; i < hash_slots; ++i) {
      sql_cache_.emplace_back(
          new CcopyableLru<std::string, std::shared_ptr<std::string>>(
              cache_per_slot));
      plan_cache_.emplace_back(
          new CcopyableLru<std::string, plan_store_t>(cache_per_slot));
    }
  }

  std::shared_ptr<std::string> get_sql(const std::string &key) {
    auto ptr = sql_cache_[hasher_(key) % hash_slots_]->get(key);
    if (LIKELY(ptr))
      plugin_info.sql_hit.fetch_add(1, std::memory_order_release);
    else
      plugin_info.sql_miss.fetch_add(1, std::memory_order_release);
    return ptr;
  }

  void set_sql(std::string &&key, std::shared_ptr<std::string> &&val) {
    auto evicted = sql_cache_[hasher_(key) % hash_slots_]->put(
        std::forward<std::string>(key),
        std::forward<std::shared_ptr<std::string>>(val));
    plugin_info.sql_evict.fetch_add(evicted, std::memory_order_release);
  }

  plan_store_t get_plan(const std::string &key) {
    auto store = plan_cache_[hasher_(key) % hash_slots_]->get(key);
    if (LIKELY(store.plan))
      plugin_info.plan_hit.fetch_add(1, std::memory_order_release);
    else
      plugin_info.plan_miss.fetch_add(1, std::memory_order_release);
    return store;
  }

  void set_plan(std::string &&key, plan_store_t &&store) {
    auto evicted = plan_cache_[hasher_(key) % hash_slots_]->put(
        std::forward<std::string>(key), std::forward<plan_store_t>(store));
    plugin_info.plan_evict.fetch_add(evicted, std::memory_order_release);
  }

  void clear() {
    for (size_t i = 0; i < hash_slots_; ++i) {
      sql_cache_[i]->clear();
      plan_cache_[i]->clear();
    }
  }
};

}  // namespace polarx_rpc
