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

#include "range_iterator.h"
#include "compact/compaction.h"
#include "xengine/xengine_constants.h"

using namespace xengine;
using namespace common;
using namespace db;
using namespace util;
using namespace memory;

namespace xengine {
namespace storage {

Range Range::deep_copy(Allocator &allocator) const {
  Range ret;
  ret.start_key_ = start_key_.deep_copy(allocator);
  ret.end_key_ = end_key_.deep_copy(allocator);
  return ret;
}

Range Range::deep_copy(memory::SimpleAllocator &allocator) const {
  Range ret;
  ret.start_key_ = start_key_.deep_copy(allocator);
  ret.end_key_ = end_key_.deep_copy(allocator);
  return ret;
}

int64_t Range::to_string(char *buf, const int64_t buf_len) const {
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "{start_key:");
  pos += db::internal_key_to_string(start_key_, buf + pos, buf_len - pos);

  databuff_printf(buf, buf_len, pos, " } {end_key:");
  pos += db::internal_key_to_string(end_key_, buf + pos, buf_len + pos);
  databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

MetaType::MetaType()
    : store_type_(SSTable),
      data_type_(Extent),
      key_type_(InternalKey),
      level_(0),
      way_(0),
      sequence_(0) {}

MetaType::MetaType(int8_t st, int8_t dt, int8_t kt, int8_t lv, int16_t w,
                   int64_t seq)
    : store_type_(st),
      data_type_(dt),
      key_type_(kt),
      level_(lv),
      way_(w),
      sequence_(seq) {}

DEFINE_TO_STRING(MetaType, KV_(store_type), KV_(data_type), KV_(key_type),
                 KV_(level), KV_(way), KV_(sequence));

MetaDescriptor::MetaDescriptor()
    : type_(),
      range_(),
      block_position_(),
      layer_position_(),
      extent_id_(),
      key_(),
      value_() ,
//      schema_(nullptr),
      delete_percent_(0)
{
}

MetaDescriptor MetaDescriptor::deep_copy(Allocator &allocator) const {
  MetaDescriptor ret;
  ret.type_ = type_;
  ret.range_ = range_.deep_copy(allocator);
  ret.block_position_ = block_position_;
  ret.layer_position_ = layer_position_;
  ret.extent_id_ = extent_id_;
  ret.key_ = key_.deep_copy(allocator);
  ret.value_ = value_.deep_copy(allocator);
//  ret.schema_ = schema_;  // need be careful, schema is one pointer
  ret.delete_percent_ = delete_percent_;
  return ret;
}

MetaDescriptor MetaDescriptor::deep_copy(memory::SimpleAllocator &allocator) const {
  MetaDescriptor ret;
  ret.type_ = type_;
  ret.range_ = range_.deep_copy(allocator);
  ret.block_position_ = block_position_;
  ret.layer_position_ = layer_position_;
  ret.extent_id_ = extent_id_;
  ret.key_ = key_.deep_copy(allocator);
  ret.value_ = value_.deep_copy(allocator);
//  ret.schema_ = schema_;  // need be careful, schema is one pointer
  ret.delete_percent_ = delete_percent_;
  return ret;
}

DEFINE_TO_STRING(MetaDescriptor, KV_(type), KV_(range), "bp1",
                 block_position_.first, "bp2", block_position_.second,
                 KV_(layer_position), KV_(extent_id), KV_(delete_percent));
/*
  SEIterator => the single way iterator which provide reuse blocks/extents func.

             |--MemSEIterator  data source from memtables
  SEIterator-|
             |--ExtSEIterator  data source from extents

  MultipleSEIterator => the multiple ways iterator which provide reuse blocks/extents func
  for each way, achieved by multiple SEIterators.
*/

SEIterator::SEIterator()
    : compaction_(nullptr),
//      schema_(nullptr),
      iter_level_(kDataEnd),
      startkey_(),
      endkey_() {}

SEIterator::~SEIterator() {}

Slice SEIterator::get_start_ukey() const {
  assert(kDataEnd != iter_level_);
  return ExtractUserKey(startkey_);
}

Slice SEIterator::get_end_ukey() const {
  assert(kDataEnd != iter_level_);
  return ExtractUserKey(endkey_);
}

MemSEIterator::MemSEIterator()
    : SEIterator(),
      mem_iter_(nullptr) {}

MemSEIterator::~MemSEIterator() { reset(); }

void MemSEIterator::reset() {
  compaction_ = nullptr;
//  schema_ = nullptr;
  iter_level_ = kDataEnd;
  mem_iter_ = nullptr;
}

void MemSEIterator::seek_to_first() {
  assert(mem_iter_);
  mem_iter_->SeekToFirst();
  if (mem_iter_->Valid()) {
    iter_level_ = kKVLevel;
    startkey_ = mem_iter_->key();
    endkey_ = startkey_;
    ParsedInternalKey ikey;
  }
}

int MemSEIterator::next() {
  int ret = 0;
  if (IS_NULL(mem_iter_)) {
    ret = Status::kNotInit;
    COMPACTION_LOG(WARN, "mem_iter is null", K(ret));
  } else {
    mem_iter_->Next();
    if (mem_iter_->Valid()) {
      iter_level_ = kKVLevel;
      startkey_ = mem_iter_->key();
      endkey_ = startkey_;
      ParsedInternalKey ikey;
    } else {
      iter_level_ = kDataEnd;
    }
  }
  return ret;
}

int MemSEIterator::next_kv(IterLevel &output_level) {
  int ret = 0;
  if (IS_NULL(mem_iter_)) {
    ret = Status::kNotInit;
    COMPACTION_LOG(WARN, "mem_iter is null", K(ret));
  } else if (mem_iter_->Valid()) {
    output_level = kKVLevel;
  } else {
    output_level = kDataEnd;
  }
  return ret;
}

int MemSEIterator::get_current_row(const common::Slice &other_key,
                                   const common::Slice &last_key,
                                   const bool has_last_key,
                                   IterLevel &output_level) {
  return next_kv(output_level);
}

int MemSEIterator::get_special_curent_row(IterLevel &out_level) {
  return next_kv(out_level);
}

int MemSEIterator::get_single_row(const common::Slice &last_key,
                                  const bool has_last_key,
                                  IterLevel &out_level) {
  return next_kv(out_level);
}

ExtSEIterator::ExtSEIterator(const Comparator *cmp,
                             const util::Comparator *internal_cmp)
    : SEIterator(),
      current_iterator_(nullptr),
      current_block_iter_(nullptr),
      cmp_(cmp),
      internal_cmp_(internal_cmp),
      iterator_index_(0),
      extent_index_(0),
      reuse_(false),
      at_next_(false),
      delete_percent_(0),
      reuse_meta_(),
      meta_descriptor_arena_(memory::CharArena::DEFAULT_PAGE_SIZE, memory::ModId::kMetaDescriptor)
      {}

ExtSEIterator::~ExtSEIterator() { reset(); }

void ExtSEIterator::reset() {
  if (nullptr != current_iterator_) {  // destroy old iterator
    compaction_->destroy_extent_index_iterator(iterator_index_);
  }
  if (nullptr != current_block_iter_) {
    compaction_->destroy_data_block_iterator(current_block_iter_);
  }
  compaction_ = nullptr;
  current_iterator_ = nullptr;
  current_block_iter_ = nullptr;
  iter_level_ = kDataEnd;
  iterator_index_ = 0;
  extent_index_ = 0;
  reuse_ = false;
  at_next_ = false;
  extent_list_.clear();
  startkey_.clear();
  endkey_.clear();
  meta_descriptor_arena_.clear();
//  schema_ = nullptr;
}

int ExtSEIterator::create_current_iterator() {
  int ret = 0;
  if (nullptr == compaction_) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "compaction is null", K(ret));
  } else if (nullptr != current_iterator_) {  // destroy old iterator
    if (FAILED(compaction_->destroy_extent_index_iterator(iterator_index_))) {
      COMPACTION_LOG(WARN, "destroy extent index iterator failed.", K(ret),
                     K(iterator_index_));
    } else {
      current_iterator_ = nullptr;
    }
  }
  if (FAILED(ret)) {
  } else if (extent_index_ < extent_list_.size()) {
    if (FAILED(compaction_->create_extent_index_iterator(
            extent_list_[extent_index_], iterator_index_, current_iterator_,
            cur_rep_))) {
      COMPACTION_LOG(WARN, "create extent index iterator failed", K(ret), K(extent_index_));
    } else if (FAILED(compaction_->delete_extent_meta(extent_list_[extent_index_]))) {
      COMPACTION_LOG(WARN, "delete extent meta failed", K(ret), K(extent_list_[extent_index_]));
    } else {
//      schema_ = extent_list_[extent_index_].get_schema();
      current_iterator_->seek_to_first();
      prefetch_next_extent();
      // update startkey, endkey, level
      iter_level_ = kBlockLevel;
      startkey_ = current_iterator_->get_meta_descriptor().range_.start_key_;
      endkey_ = current_iterator_->get_meta_descriptor().range_.end_key_;
    }
  } else {
    iter_level_ = kDataEnd;
  }
  return ret;
}

void ExtSEIterator::prefetch_next_extent() {
  if (extent_index_ + 1 < extent_list_.size()) {
    int64_t next_extent_id =
        extent_list_.at(extent_index_ + 1).block_position_.first;
    compaction_->prefetch_extent(next_extent_id);
  }
}

int ExtSEIterator::create_block_iter(const MetaDescriptor &meta) {
  int ret = 0;

  if (nullptr == compaction_ || nullptr == current_iterator_) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "compactio or current_iterator is null.", K(ret));
  } else {
    if (nullptr != current_block_iter_) {
      compaction_->destroy_data_block_iterator(current_block_iter_);
    }
    ret = compaction_->create_data_block_iterator(
        meta.block_position_,
        cur_rep_.table_reader_, current_block_iter_);
    if (SUCCED(ret)) {
      current_block_iter_->SeekToFirst();
      if (current_block_iter_->Valid()) {  // update startkey,endkey
        startkey_ = current_block_iter_->key();
        endkey_ = startkey_;
        iter_level_ = kKVLevel;
      }
    } else {
      COMPACTION_LOG(WARN, "create data block iterator failed", K(ret), K(meta));
    }
  }
  return ret;
}

void ExtSEIterator::seek_to_first() {
  if (extent_index_ < extent_list_.size()) {
    reuse_ = false;
    iter_level_ = kExtentLevel;
    startkey_ = extent_list_[extent_index_].range_.start_key_;
    endkey_ = extent_list_[extent_index_].range_.end_key_;
  }
}

int ExtSEIterator::next_extent() {
  int ret = 0;
  if ((++extent_index_) >= extent_list_.size()) {
    iter_level_ = kDataEnd;
  } else {
    startkey_ = extent_list_[extent_index_].range_.start_key_;
    endkey_ = extent_list_[extent_index_].range_.end_key_;
    iter_level_ = kExtentLevel;
  }
  return ret;
}

int ExtSEIterator::next_block() {
  int ret = 0;
  if (nullptr == current_iterator_) {
    ret = Status::kCorruption;
    COMPACTION_LOG(WARN, "current iterator is null", K(ret));
  } else {
    if (at_next_) {
      at_next_ = false;
    } else {
      current_iterator_->next();
    }
    if (!current_iterator_->valid()) {
      ret = next_extent();
    } else {
      startkey_ = current_iterator_->get_meta_descriptor().range_.start_key_;
      endkey_ = current_iterator_->get_meta_descriptor().range_.end_key_;
      iter_level_ = kBlockLevel;
    }
  }
  return ret;
}

int ExtSEIterator::next_kv() {
  int ret = 0;
  if (nullptr == current_block_iter_) {
    ret = Status::kCorruption;
    COMPACTION_LOG(WARN, "current block iter is null", K(ret));
  } else {
    current_block_iter_->Next();
    if (!current_block_iter_->Valid()) {
      ret = next_block();
    } else {
      startkey_ = current_block_iter_->key();
      endkey_ = startkey_;
      iter_level_ = kKVLevel;
    }
  }
  return ret;
}

int ExtSEIterator::next() {
  int ret = 0;
  if (kExtentLevel == iter_level_) {
    if (!reuse_) {  // open extent
      ret = create_current_iterator();
    } else {
      ret = next_extent();
    }
  } else if (kBlockLevel == iter_level_) {
    if (!reuse_) {  // open block
      if (nullptr == current_iterator_) {
        ret = Status::kCorruption;
        COMPACTION_LOG(WARN, "current iterator is null", K(ret));
      } else {
        ret = create_block_iter(current_iterator_->get_meta_descriptor());
      }
    } else {
      ret = next_block();
    }
  } else if (kKVLevel == iter_level_) {
    ret = next_kv();
  } else {
    ret = Status::kAborted;
    COMPACTION_LOG(WARN, "iter level is DataEnd.", K(ret));
  }
  reuse_ = false;
  return ret;
}

int ExtSEIterator::check_reuse_meta(const Slice &last_key,
                                 const bool has_last_key,
                                 IterLevel &output_level)
{
  int ret = 0;
  const MetaDescriptor &meta = get_meta_descriptor();
  if (IS_NULL(cmp_) || IS_NULL(compaction_)) {
    ret = Status::kCorruption;
    COMPACTION_LOG(WARN, "cmp is null", K(ret), KP(cmp_));
  } else if (!compaction_->check_do_reuse(meta)) {
    // has many delete records, don't do reuse
  } else if (has_last_key
      && 0 == cmp_->Compare(last_key, meta.get_start_user_key())) {
    // check last block/extent's endkey
    // cann't reuse, will go next(open)
  } else  { // check next meta's startkey
    // reuse -> output
    //             [ at_next_ -> create_block_iter(cur_meta),output
    // not reuse ->[ not at_next_ -> no output, need open(next())
    //
    /**release the memory of last meta descriptor, the owner may been last meta descripor
     * or reuse_meta_, it's safe to relase here.*/
    meta_descriptor_arena_.clear();
    MetaDescriptor cur_meta = meta.deep_copy(meta_descriptor_arena_);
    Slice next_startkey; // default size is 0
    bool has_next_startkey = false;
    if (kExtentLevel == iter_level_) {
      if (extent_index_ + 1 < extent_list_.size()) {
        next_startkey = extent_list_[extent_index_ + 1].get_start_user_key();
        has_next_startkey = true;
      }
    } else if (nullptr == current_iterator_) {
      ret = Status::kCorruption;
      COMPACTION_LOG(WARN, "curent iterator is null", K(ret));
    } else {
      at_next_ = true;
      current_iterator_->next();
      if (current_iterator_->valid()) {
        next_startkey =
            current_iterator_->get_meta_descriptor().get_start_user_key();
        has_next_startkey = true;
      } else if (extent_index_ + 1 < extent_list_.size()) {
        next_startkey = extent_list_[extent_index_ + 1].get_start_user_key();
        has_next_startkey = true;
      }
    }
    if (FAILED(ret)) {
    } else if (has_next_startkey
        && 0 == cmp_->Compare(cur_meta.get_end_user_key(), next_startkey)) {
      if (at_next_) { //  no reuse and at_next
        if (FAILED(create_block_iter(cur_meta))) {
          COMPACTION_LOG(WARN, "cretae block iterator failed", K(ret));
        } else {
          output_level = iter_level_;
        }
      }
    } else { // can reuse
      reuse_ = true;
      output_level = iter_level_;// update, will not go to next
      if (kBlockLevel == iter_level_) {
        reuse_meta_ = cur_meta;
      } else if (kExtentLevel == iter_level_) {
        reuse_meta_ = meta;
      }
    }
  }
  return ret;
}

int ExtSEIterator::get_current_row(const Slice &other_start_key,
                                const common::Slice &last_key,
                                const bool has_last_key,
                                IterLevel &output_level) {
  int ret = 0;
  output_level = kDataEnd;
  if (nullptr == compaction_) {
    ret = Status::kCorruption;
    COMPACTION_LOG(WARN, "compaction is null", K(ret));
  } else if (nullptr == cmp_) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "cmp is null", K(ret));
  }
  while (valid() && SUCCED(ret) && kDataEnd == output_level) {
    int cmp = cmp_->Compare(get_end_ukey(), other_start_key);
    if (cmp < 0) {  // check reuse or get (key,value)
      if (kKVLevel == iter_level_) {
        output_level = kKVLevel;
        // get key, value
      } else if (FAILED(check_reuse_meta(last_key, has_last_key, output_level))) {
        COMPACTION_LOG(WARN, "check reuse meta failed", K(ret));
      } else if (kDataEnd == output_level) {
        // no reuse and not at_next, open normalloy
        ret = next();
      }
    } else {
      ret = next();
    }
  }
  if (SUCCED(ret) && kDataEnd == output_level) {
    ret = Status::kCorruption;
    COMPACTION_LOG(WARN, "invalid output level", K(ret));
  }
  return ret;
}

int ExtSEIterator::get_special_curent_row(IterLevel &output_level) {
  int ret = 0;
  output_level = kDataEnd;
  if (reuse_) {
    reuse_ = false;
  }
  while (SUCCED(ret) && valid() && kDataEnd == output_level) {
    if (kKVLevel == iter_level_) {
      output_level = iter_level_;
    } else if (FAILED(next())) {
      COMPACTION_LOG(WARN, "call next failed", K(ret));
    }
  }
  if (SUCCED(ret) && kDataEnd == output_level) {
    ret = Status::kCorruption;
    COMPACTION_LOG(WARN, "invalid output level", K(ret));
  }
  return ret;
}

int ExtSEIterator::get_single_row(const common::Slice &last_key,
                                  const bool has_last_key,
                                  IterLevel &output_level) {
  int ret = 0;
  output_level = kDataEnd;
  if (nullptr == compaction_) {
    ret = Status::kCorruption;
    COMPACTION_LOG(WARN, "compaction is null", K(ret));
  }
  while (kDataEnd == output_level && valid() && SUCCED(ret)) {
    if (kKVLevel == iter_level_) {
      output_level = iter_level_;
    } else if (FAILED(check_reuse_meta(last_key, has_last_key, output_level))) {
      COMPACTION_LOG(WARN, "check reuse meta failed", K(ret));
    } else if (kDataEnd == output_level) {
      if (FAILED(next())) {
        COMPACTION_LOG(WARN, "next failed", K(ret), K((int64_t)iter_level_));
      }
    }
  }
  output_level = iter_level_; // kv or data or extent
  return ret;
}

MultipleSEIterator::MultipleSEIterator(
    const util::Comparator* user_cmp,
    const util::Comparator* internal_cmp)
    : user_cmp_(user_cmp),
      internal_cmp_(internal_cmp),
      se_heap_(internal_cmp),
      current_iterator_(nullptr),
      last_user_key_(),
      output_level_(SEIterator::kDataEnd),
      has_one_way_(false) {
}

int MultipleSEIterator::add_se_iterator(SEIterator *se_iterator) {
  int ret = 0;
  if (IS_NULL(se_iterator)) {
    ret = Status::kErrorUnexpected;
    COMPACTION_LOG(WARN, "failed to add se_iterator", K(ret));
  } else {
    se_iterator->seek_to_first();
    if (se_iterator->valid()) {
      se_heap_.push(se_iterator);
    }
  }
  return ret;
}

int MultipleSEIterator::seek_to_first() {
  output_level_ = SEIterator::kDataEnd;
  current_iterator_ = nullptr;
  return next();
}

int MultipleSEIterator::get_single_row(const bool has_last_kv) {
  int ret = 0;
  output_level_ = SEIterator::kDataEnd;
  if (current_iterator_->valid()
      && FAILED(current_iterator_->get_single_row(last_user_key_, has_last_kv, output_level_))) {
    COMPACTION_LOG(WARN, "iter1 get next row failed", K(ret), K(last_user_key_));
  } else {
  }
  return ret;
}

int MultipleSEIterator::next() {
  int ret = 0;
  bool has_last_kv = false;
  if (SEIterator::kKVLevel == output_level_) {
    has_last_kv = true;
    assert(last_user_key_.size());
  }
  if (has_one_way_) {
    if (IS_NULL(current_iterator_)) {
      ret = Status::kAborted;
      COMPACTION_LOG(ERROR, "current_iterator is null", K(ret));
    } else if (FAILED(current_iterator_->next())) {
      COMPACTION_LOG(ERROR, "failed to get next row", K(ret));
    } else if (FAILED(get_single_row(has_last_kv))) {
      COMPACTION_LOG(ERROR, "failed to get single row", K(ret), K(has_last_kv));
    }
    return ret;
  }
  if (nullptr != current_iterator_) {
    if (FAILED(current_iterator_->next())) {
      COMPACTION_LOG(ERROR, "failed to get next row", K(ret));
      return ret;
    } else if (current_iterator_->valid()) {
      se_heap_.push(current_iterator_);
    } else {
      // current data stream stopped, no need to push again
    }
  }
  // 1. get the heap_top iterator and pop it from heap
  // 2. get next heap_top iterator
  // 3. SEIterator:compare the next_itertor->start_ukey and iterator->end_ukey
  // 4. SEIterator:check block/extent if can reuse
  // 5. set current_iterator
  assert(!se_heap_.empty());
  SEIterator *se_iterator = se_heap_.top();
  se_heap_.pop();
  if (se_heap_.empty()) { // only one way
    current_iterator_ = se_iterator;
    if (FAILED(get_single_row(has_last_kv))) {
      COMPACTION_LOG(ERROR, "failed to get single row", K(ret), K(has_last_kv));
    } else {
      has_one_way_ = true;
    }
  } else {
    SEIterator *next_iterator = nullptr;
    next_iterator = se_heap_.top();
    if (IS_NULL(se_iterator) || IS_NULL(next_iterator)) {
      ret = Status::kInvalidArgument;
      COMPACTION_LOG(WARN, "se_iterator or next_iterator is null", K(ret), KP(se_iterator));
    } else if (next_iterator->get_start_ukey() == se_iterator->get_start_ukey()) {
      se_iterator->get_special_curent_row(output_level_);
      current_iterator_ = se_iterator;
    } else if (FAILED(se_iterator->get_current_row(
        next_iterator->get_start_ukey(), last_user_key_, has_last_kv, output_level_))) {
      COMPACTION_LOG(ERROR, "failed to get current row", K(ret), K(last_user_key_),
          K(next_iterator->get_start_ukey()), K(has_last_kv), K((int)output_level_));
    } else {
      current_iterator_ = se_iterator;
    }
  }
  return ret;
}
}
}
