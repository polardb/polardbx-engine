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

#include "meta_data.h"
#include "options/cf_options.h"
#include "xengine/write_buffer_manager.h"

using namespace xengine;
using namespace common;
using namespace db;
using namespace util;

namespace xengine {
namespace storage {

ExtentMetaValue::ExtentMetaValue() : start_key_(), extent_list_() {}

ExtentMetaValue::~ExtentMetaValue() {}

const Slice &ExtentMetaValue::get_start_key() const { return start_key_; }

void ExtentMetaValue::set_start_key(const Slice &start_key) {
  start_key_ = start_key;
}

const BlockPosition &ExtentMetaValue::get_extent_list() const {
  return extent_list_;
}

void ExtentMetaValue::set_extent_list(const BlockPosition &extents) {
  extent_list_ = extents;
}

void ExtentMetaValue::reset() {
  start_key_.clear();
  extent_list_.first = 0;
  extent_list_.second = 0;
}

DEFINE_SERIALIZATION(ExtentMetaValue, start_key_, extent_list_.first,
                     extent_list_.second);

ExtentMetaTable::ExtentMetaTable()
    : internal_comparator_(BytewiseComparator()), last_sequence_(1) {}
ExtentMetaTable::~ExtentMetaTable() { destroy(); }

Status ExtentMetaTable::init(const ImmutableCFOptions &ioptions,
                             const MutableCFOptions &mutable_cf_options,
                             WriteBufferManager *write_buffer_manager) {
  table_.reset(new MemTable(internal_comparator_, ioptions, mutable_cf_options,
                            write_buffer_manager, 0));
  return Status::OK();
}

void ExtentMetaTable::destroy() {}
}
}
