/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>
#include <unordered_map>

#ifndef ROCKSDB_LITE
namespace xengine {
namespace common {
// This enum defines the options sanity level.
enum OptionsSanityCheckLevel : unsigned char {
  // Performs no sanity check at all.
  kSanityLevelNone = 0x00,
  // Performs minimum check to ensure the XEngine instance can be
  // opened without corrupting / mis-interpreting the data.
  kSanityLevelLooselyCompatible = 0x01,
  // Perform exact match sanity check.
  kSanityLevelExactMatch = 0xFF,
};

// The sanity check level for DB options
static const std::unordered_map<std::string, OptionsSanityCheckLevel>
    sanity_level_db_options{};

// The sanity check level for column-family options
static const std::unordered_map<std::string, OptionsSanityCheckLevel>
    sanity_level_cf_options = {
        {"comparator", kSanityLevelLooselyCompatible},
        {"prefix_extractor", kSanityLevelLooselyCompatible},
        {"table_factory", kSanityLevelLooselyCompatible},
        {"merge_operator", kSanityLevelLooselyCompatible}};

// The sanity check level for block-based table options
static const std::unordered_map<std::string, OptionsSanityCheckLevel>
    sanity_level_bbt_options{};

OptionsSanityCheckLevel DBOptionSanityCheckLevel(
    const std::string& options_name);
OptionsSanityCheckLevel CFOptionSanityCheckLevel(
    const std::string& options_name);
OptionsSanityCheckLevel BBTOptionSanityCheckLevel(
    const std::string& options_name);

}  // namespace common
}  // namespace xengine

#endif  // !ROCKSDB_LITE
