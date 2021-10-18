// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <string>

#include "monitoring/statistics.h"
#include "xengine/persistent_cache.h"

namespace xengine {
namespace common {

// PersistentCacheOptions
//
// This describe the caching behavior for page cache
// This is used to pass the context for caching and the cache handle
struct PersistentCacheOptions {
  PersistentCacheOptions() {}
  explicit PersistentCacheOptions(
      const std::shared_ptr<cache::PersistentCache>& _persistent_cache,
      const std::string _key_prefix, monitor::Statistics* const _statistics)
      : persistent_cache(_persistent_cache),
        key_prefix(_key_prefix),
        statistics(_statistics) {}

  virtual ~PersistentCacheOptions() {}

  std::shared_ptr<cache::PersistentCache> persistent_cache;
  std::string key_prefix;
  monitor::Statistics* statistics = nullptr;
};

}  // namespace common
}  // namespace xengine
