// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <string>

#include "monitoring/statistics.h"
#include "table/format.h"
#include "table/persistent_cache_options.h"

namespace xengine {
namespace table {

struct BlockContents;

// PersistentCacheHelper
//
// Encapsulates  some of the helper logic for read and writing from the cache
class PersistentCacheHelper {
 public:
  // insert block into raw page cache
  static void InsertRawPage(const common::PersistentCacheOptions& cache_options,
                            const BlockHandle& handle, const char* data,
                            const size_t size);

  // insert block into uncompressed cache
  static void InsertUncompressedPage(
      const common::PersistentCacheOptions& cache_options,
      const BlockHandle& handle, const BlockContents& contents);

  // lookup block from raw page cacge
  static common::Status LookupRawPage(
      const common::PersistentCacheOptions& cache_options,
      const BlockHandle& handle,
      std::unique_ptr<char[], memory::ptr_delete<char>>* raw_data,
      const size_t raw_data_size);

  // lookup block from uncompressed cache
  static common::Status LookupUncompressedPage(
      const common::PersistentCacheOptions& cache_options,
      const BlockHandle& handle, BlockContents* contents);
};

}  // namespace table
}  // namespace xengine
