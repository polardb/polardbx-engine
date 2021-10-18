// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/bloom_block.h"

#include <string>
#include "util/dynamic_bloom.h"
#include "xengine/slice.h"

using namespace xengine;
using namespace common;
using namespace util;

namespace xengine {
namespace table {

void BloomBlockBuilder::AddKeysHashes(
    const std::vector<uint32_t>& keys_hashes) {
  for (auto hash : keys_hashes) {
    bloom_.AddHash(hash);
  }
}

Slice BloomBlockBuilder::Finish() { return bloom_.GetRawData(); }

const std::string BloomBlockBuilder::kBloomBlock = "kBloomBlock";
}  // namespace table
}  // namespace xengine
