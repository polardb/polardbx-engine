/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <stdint.h>
#include <memory>
#include <string>

#include "memory/base_malloc.h"
#include "xengine/env.h"
#include "xengine/slice.h"
#include "xengine/statistics.h"
#include "xengine/status.h"

namespace xengine {
namespace cache {

// PersistentCache
//
// Persistent cache interface for caching IO pages on a persistent medium. The
// cache interface is specifically designed for persistent read cache.
class PersistentCache {
 public:
  typedef std::vector<std::map<std::string, double>> StatsType;

  virtual ~PersistentCache() {}

  // Insert to page cache
  //
  // page_key   Identifier to identify a page uniquely across restarts
  // data       Page data
  // size       Size of the page
  virtual common::Status Insert(const common::Slice& key, const char* data,
                                const size_t size) = 0;

  // Lookup page cache by page identifier
  //
  // page_key   Page identifier
  // buf        Buffer where the data should be copied
  // size       Size of the page
  virtual common::Status Lookup(
      const common::Slice& key,
      std::unique_ptr<char[], memory::ptr_delete<char>>* data, size_t* size) = 0;

  // Is cache storing uncompressed data ?
  //
  // True if the cache is configured to store uncompressed data else false
  virtual bool IsCompressed() = 0;

  // Return stats as map of {string, double} per-tier
  //
  // Persistent cache can be initialized as a tier of caches. The stats are per
  // tire top-down
  virtual StatsType Stats() = 0;

  virtual std::string GetPrintableOptions() const = 0;
};

// Factor method to create a new persistent cache
common::Status NewPersistentCache(util::Env* const env, const std::string& path,
                                  const uint64_t size,
                                  const bool optimized_for_nvm,
                                  std::shared_ptr<PersistentCache>* cache);
}  // namespace cache
}  // namespace xengine
