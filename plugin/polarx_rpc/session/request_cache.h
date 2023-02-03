//
// Created by zzy on 2023/1/5.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "../coders/protocol_fwd.h"
#include "../common_define.h"
#include "../utility/lru.h"

namespace polarx_rpc {

class CrequestCache final {
  NO_COPY_MOVE(CrequestCache);

private:
  const size_t hash_slots_;
  std::vector<
      std::unique_ptr<CcopyableLru<std::string, std::shared_ptr<std::string>>>>
      sql_cache_;
  std::vector<std::unique_ptr<
      CcopyableLru<std::string, std::shared_ptr<Polarx::ExecPlan::AnyPlan>>>>
      plan_cache_;
  std::hash<std::string> hasher_;

public:
  CrequestCache(size_t cache_size, size_t hash_slots)
      : hash_slots_(hash_slots) {
    sql_cache_.reserve(hash_slots_);
    plan_cache_.reserve(hash_slots_);
    auto cache_per_slot = cache_size / hash_slots_;
    if (cache_per_slot < 1)
      cache_per_slot = 1;
    for (size_t i = 0; i < hash_slots; ++i) {
      sql_cache_.emplace_back(
          new CcopyableLru<std::string, std::shared_ptr<std::string>>(
              cache_per_slot));
      plan_cache_.emplace_back(
          new CcopyableLru<std::string,
                           std::shared_ptr<Polarx::ExecPlan::AnyPlan>>(
              cache_per_slot));
    }
  }

  std::shared_ptr<std::string> get_sql(const std::string &key) {
    auto ptr = sql_cache_[hasher_(key) % hash_slots_]->get(key);
    if (LIKELY(ptr))
      plugin_info.sql_hit.fetch_add(1, std::memory_order_release);
    else
      plugin_info.sql_miss.fetch_add(1, std::memory_order_release);
    return std::move(ptr);
  }

  void set_sql(std::string &&key, std::shared_ptr<std::string> &&val) {
    auto evicted = sql_cache_[hasher_(key) % hash_slots_]->put(
        std::forward<std::string>(key),
        std::forward<std::shared_ptr<std::string>>(val));
    plugin_info.sql_evict.fetch_add(evicted, std::memory_order_release);
  }

  std::shared_ptr<Polarx::ExecPlan::AnyPlan> get_plan(const std::string &key) {
    auto ptr = plan_cache_[hasher_(key) % hash_slots_]->get(key);
    if (LIKELY(ptr))
      plugin_info.plan_hit.fetch_add(1, std::memory_order_release);
    else
      plugin_info.plan_miss.fetch_add(1, std::memory_order_release);
    return std::move(ptr);
  }

  void set_plan(std::string &&key,
                std::shared_ptr<Polarx::ExecPlan::AnyPlan> &&val) {
    auto evicted = plan_cache_[hasher_(key) % hash_slots_]->put(
        std::forward<std::string>(key),
        std::forward<std::shared_ptr<Polarx::ExecPlan::AnyPlan>>(val));
    plugin_info.plan_evict.fetch_add(evicted, std::memory_order_release);
  }
};

} // namespace polarx_rpc
