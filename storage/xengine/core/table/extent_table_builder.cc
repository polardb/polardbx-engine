// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <assert.h>
#include <inttypes.h>
#include <stdio.h>

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "cache/row_cache.h"
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "db/version_set.h"

#include "xengine/comparator.h"
#include "xengine/env.h"
#include "xengine/filter_policy.h"
#include "xengine/flush_block_policy.h"
#include "xengine/merge_operator.h"
#include "xengine/table.h"
#include "xengine/xengine_constants.h"

#include "table/block.h"
#include "table/block_based_filter_block.h"
#include "table/block_builder.h"
#include "table/extent_table_builder.h"
#include "table/extent_table_factory.h"
#include "table/extent_table_reader.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/full_filter_block.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"

#include "memory/base_malloc.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/xxhash.h"

#include "storage/extent_space_manager.h"
#include "storage/storage_manager.h"
#include "table/index_builder.h"
#include "table/partitioned_filter_block.h"

using namespace xengine;
using namespace util;
using namespace common;
using namespace db;
using namespace monitor;
using namespace cache;
using namespace memory;

namespace xengine {
namespace table {

extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;

typedef BlockBasedTableOptions::IndexType IndexType;

namespace {

bool good_compression_ratio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
//  delete block;
  MOD_DELETE_OBJECT(Block, block);
}

// format_version is the block format as defined in include/xengine/table.h
int compress_block(const Slice& raw,
                   const CompressionOptions& compression_options,
                   CompressionType& type, uint32_t format_version,
                   const Slice& compression_dict,
                   WritableBuffer* compressed_output, Slice& slice_out) {
  int ret = Status::kOk;
  size_t orig_size = compressed_output->size();
  assert(orig_size == 0);
  // Will return compressed block contents if (1) the compression method is
  // supported in this platform and (2) the compression rate is "good enough".
  bool compressed = false;
  switch (type) {
    case kSnappyCompression:
      if (Snappy_Compress(compression_options, raw.data(), raw.size(),
                          compressed_output) &&
          good_compression_ratio(compressed_output->size(), raw.size())) {
        compressed = true;
      }
      break;  // fall back to no compression.
    case kZlibCompression:
      if (Zlib_Compress(
              compression_options,
              GetCompressFormatForVersion(kZlibCompression, format_version),
              raw.data(), raw.size(), compressed_output, compression_dict) &&
          good_compression_ratio(compressed_output->size(), raw.size())) {
        compressed = true;
      }
      break;  // fall back to no compression.
    case kBZip2Compression:
      if (BZip2_Compress(
              compression_options,
              GetCompressFormatForVersion(kBZip2Compression, format_version),
              raw.data(), raw.size(), compressed_output) &&
          good_compression_ratio(compressed_output->size(), raw.size())) {
        compressed = true;
      }
      break;  // fall back to no compression.
    case kLZ4Compression:
      if (LZ4_Compress(
              compression_options,
              GetCompressFormatForVersion(kLZ4Compression, format_version),
              raw.data(), raw.size(), compressed_output, compression_dict) &&
          good_compression_ratio(compressed_output->size(), raw.size())) {
        compressed = true;
      }
      break;  // fall back to no compression.
    case kLZ4HCCompression:
      if (LZ4HC_Compress(
              compression_options,
              GetCompressFormatForVersion(kLZ4HCCompression, format_version),
              raw.data(), raw.size(), compressed_output, compression_dict) &&
          good_compression_ratio(compressed_output->size(), raw.size())) {
        compressed = true;
      }
      break;  // fall back to no compression.
    case kXpressCompression:
      if (XPRESS_Compress(raw.data(), raw.size(), compressed_output) &&
          good_compression_ratio(compressed_output->size(), raw.size())) {
        compressed = true;
      }
      break;
    case kZSTD:
    case kZSTDNotFinalCompression:
      if (ZSTD_Compress(compression_options, raw.data(), raw.size(),
                        compressed_output, compression_dict) &&
          good_compression_ratio(compressed_output->size(), raw.size())) {
        compressed = true;
      }
      break;     // fall back to no compression.
    default: {}  // Do not recognize this compression type
  }
  if (compressed == true) {
    slice_out = Slice(compressed_output->data(), compressed_output->size());
    return Status::kOk;
  }

  compressed_output->resize(orig_size);
  // Compression method is not supported, or not good compression ratio, so
  // just fall back to uncompressed form.
  type = kNoCompression;
  FAIL_RETURN(compressed_output->append(raw));

  slice_out = raw;
  return Status::kOk;
}

void AppendVarint32(IterKey& key, uint64_t v) {
  char buf[10];
  auto ptr = EncodeVarint32(buf, v);
  key.TrimAppend(key.Size(), buf, ptr - buf);
}
}

const uint64_t kExtentBasedTableMagicNumber = 0x568619d05ecd006eull;

ExtentBasedTableBuilder::Rep::Rep(const ImmutableCFOptions& ioptions,
    const BlockBasedTableOptions& table_options,
    const InternalKeyComparator& icomparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
    int_tbl_prop_collector_factories,
    uint32_t column_family_id,
    const InternalKeySliceTransform* internal_prefix_transform,
    WritableBuffer* block_buf, WritableBuffer* index_buf)
    : data_block(table_options.block_restart_interval,
                 table_options.use_delta_encoding, block_buf),
      index_builder(IndexBuilder::CreateIndexBuilder(
                    table_options.index_type, &icomparator, internal_prefix_transform,
                    table_options, index_buf)),
      flush_block_policy(
          table_options.flush_block_policy_factory->NewFlushBlockPolicy(table_options, data_block))
{
  for (auto& collector_factories : *int_tbl_prop_collector_factories) {
    table_properties_collectors.emplace_back(
        collector_factories->CreateIntTblPropCollector(column_family_id));
  }
  for (auto& properties_collector : table_properties_collectors) {
    if (!properties_collector->SupportAddBlock()) {
      collectors_support_add_block = false;
      break;
    }
  }
}

int ExtentBasedTableBuilder::init_one_sst() {
  int ret = Status::kOk;
  sst_buf_.reset();
  block_buf_.reset();
  index_buf_.reset();

#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("ExtentTableBuilder::ignore_flush_data", this);
#endif
  // 1) no compression, use aligned buf to collect the data blocks, so that it
  //    can be used for direct i/o without extra memcpy.
  // 2) using compression, data blocks are compressed anyway, it's unnecessary
  //    to put them into aligned buf. Instead, the compressed data will be put
  //    there.
  WritableBuffer* block_buf = &block_buf_;
  if (compression_type_ == kNoCompression) {
    block_buf = &sst_buf_;
  }
  assert(rep_ == nullptr);
//  rep_ = new Rep(ioptions_, table_options_, internal_comparator_,
//                 int_tbl_prop_collector_factories_, column_family_id_,
//                 &internal_prefix_transform_, block_buf, &index_buf_);
  rep_ = MOD_NEW_OBJECT(ModId::kRep, Rep, ioptions_, table_options_,
      internal_comparator_, int_tbl_prop_collector_factories_, column_family_id_,
      &internal_prefix_transform_, block_buf, &index_buf_);
  if (rep_ == nullptr) {
    __XENGINE_LOG(ERROR, "Failed to init SST rep");
    return Status::kNoSpace;
  }

  ret = mtables_->space_manager->allocate(mtables_->table_space_id_,
      storage::HOT_EXTENT_SPACE, rep_->extent_);
#ifndef NDEBUG
  if (TEST_is_ignore_flush_data()) {
    return Status::kOk;
  }
#endif

  Status s(ret);
  if (!s.ok()) {
    __XENGINE_LOG(ERROR, "Could not allocate extent");
    return s.code();
  } else {
    not_flushed_normal_extent_id_ = rep_->extent_.get_extent_id();
  }
  rep_->meta.fd = FileDescriptor(rep_->extent_.get_extent_id().id(), 0, 0);

  if (table_options_.block_cache) {
    ExtentBasedTable::GenerateCachePrefix(
        table_options_.block_cache.get(), &rep_->extent_,
        &rep_->cache_key_prefix[0], &rep_->cache_key_prefix_size);
  }
  if (table_options_.block_cache_compressed) {
    ExtentBasedTable::GenerateCachePrefix(
        table_options_.block_cache_compressed.get(), &rep_->extent_,
        &rep_->compressed_cache_key_prefix[0],
        &rep_->compressed_cache_key_prefix_size);
  }

  return Status::kOk;
}

ExtentBasedTableBuilder::ExtentBasedTableBuilder(
    const ImmutableCFOptions &ioptions,
    const BlockBasedTableOptions &table_options,
    const InternalKeyComparator &internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
        *int_tbl_prop_collector_factories,
    uint32_t column_family_id, MiniTables *mtables,
    const CompressionType compression_type,
    const CompressionOptions &compression_opts,
    const std::string *compression_dict, const bool skip_filters,
    const std::string &column_family_name,
    const storage::LayerPosition &layer_position, bool is_flush)
    : is_inited_(false),
      rep_(nullptr),
      status_(Status::kOk),
      ioptions_(ioptions),
      table_options_(table_options),
      internal_comparator_(internal_comparator),
      int_tbl_prop_collector_factories_(int_tbl_prop_collector_factories),
      column_family_id_(column_family_id),
      mtables_(mtables),
      compression_type_(compression_type),
      compression_opts_(compression_opts),
      compression_dict_(compression_dict),
      column_family_name_(column_family_name),
      output_position_(layer_position),
      internal_prefix_transform_(ioptions_.prefix_extractor),
      num_entries_(0),
      offset_(0),
      // give buffer a little bit more memory in case some compression
      // algorithms need more memory larger than the plain text size
      sst_buf_(PAGE_SIZE, DEF_BUF_SIZE, false),
      block_buf_(PAGE_SIZE, DEF_BUF_SIZE, true),
      index_buf_(PAGE_SIZE, DEF_BUF_SIZE, true),
      not_flushed_normal_extent_id_(),
      not_flushed_lob_extent_id_(),
      flushed_lob_extent_ids_(),
      is_flush_(is_flush) {
  assert(table_options_.format_version == 3);
  assert(table_options_.index_type !=
         BlockBasedTableOptions::kTwoLevelIndexSearch);
#ifndef NDEBUG
  test_ignore_flush_data_ = false;
#endif
  meta_size_ = sst_meta_size();
}

ExtentBasedTableBuilder::~ExtentBasedTableBuilder() {
//  delete rep_;
  MOD_DELETE_OBJECT(Rep, rep_);
}

int ExtentBasedTableBuilder::init()
{
  int ret = Status::kOk;

  if (is_inited_) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "ExtentBasedTableBuilder has been inited", K(ret));
  } else if (FAILED(init_one_sst())) {
    XENGINE_LOG(ERROR, "fail to init sst", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ExtentBasedTableBuilder::update_block_stats(const Slice& key,
                                                const Slice& value,
                                                const ParsedInternalKey &ikey) {
  block_stats_.data_size_ += key.size() + value.size();
  block_stats_.key_size_ += key.size();
  block_stats_.value_size_ += value.size();
  block_stats_.rows_ += 1;
  block_stats_.smallest_seqno_ = std::min(block_stats_.smallest_seqno_, ikey.sequence);
  block_stats_.largest_seqno_ = std::max(block_stats_.largest_seqno_, ikey.sequence);

  // record kTypeValueLarge and other type to block_stats.entry_others_
  switch (ikey.type) {
    case kTypeValue:
      block_stats_.entry_put_ += 1;
      break;
    case kTypeValueLarge:
      block_stats_.entry_others_ += 1;
      break;
    case kTypeDeletion:
      block_stats_.entry_deletes_ += 1;
      break;
    case kTypeSingleDeletion:
      // We treat both kTypeDeletion and kTypeSingleDeletion as entry_deletes_.
      block_stats_.entry_deletes_ += 1;
      block_stats_.entry_single_deletes_ += 1;
      break;
    case kTypeMerge:
      block_stats_.entry_merges_ += 1;
      break;
    default:
      block_stats_.entry_others_ += 1;
  }
  return Status::kOk;
}

// every time a record is added, the attrs are collected in block_stats,
// these attrs are sync-ed up to sst props at the time of flushing block
void ExtentBasedTableBuilder::sync_up_block_stats(
    const BlockStats& block_stats) {
  rep_->props.raw_key_size += block_stats.key_size_;
  rep_->props.raw_value_size += block_stats.value_size_;
}

int ExtentBasedTableBuilder::AddBlock(const Slice& block_contents,
                                      const Slice& block_stats_contents,
                                      const Slice& last_key, 
                                      const bool has_trailer) {
  if (!is_inited_) {
    XENGINE_LOG(WARN, "ExtentBasedTableBuilder should been inited first");
    return Status::kNotInit;
  }

  if (!SupportAddBlock()) {
    __XENGINE_LOG(WARN, "ExtentBasedTableBuilder does not support AddBlock. Mybay HashIndex is used");
    return Status::kNotSupported;
  }
  TEST_SYNC_POINT_CALLBACK("ExtentBasedTableBuilder::AddBlock:0", nullptr);
  int ret = Status::kOk;

  BlockStats add_block_stats;
  FAIL_RETURN_MSG(add_block_stats.decode(block_stats_contents),
                  "cannot decode stats(%d)", ret);
  // If the data_block is not empty, it should be flushed first.
  if (!rep_->data_block.empty()) {
    FAIL_RETURN_MSG(flush_data_block(),
                    "failed to flush data block(%d)", ret);
    block_stats_.actual_disk_size_ += rep_->offset - rep_->last_offset;
    Slice first_key{add_block_stats.first_key_};
    rep_->index_builder->AddIndexEntry(&rep_->last_key, &first_key,
                                       rep_->pending_handle, block_stats_);
    sync_up_block_stats(block_stats_);
    ++rep_->props.num_data_blocks;
    block_stats_.reset();
  }
  if (rep_->props.num_entries > 0 &&
      internal_comparator_.Compare(Slice(add_block_stats.first_key_),
                                   Slice(rep_->last_key)) <= 0) {
    XENGINE_LOG(ERROR, "Order wrong in AddBlock",
        "first_key", Slice(add_block_stats.first_key_), "last_key", Slice(rep_->last_key));
    // Abort in debug
    assert(false);
    return Status::kCorruption;
  }

  int sst_size_after =
      sst_size_after_block(block_contents, last_key, add_block_stats);
  bool should_flush_sst =
      (sst_size_after > EXTENT_SIZE && rep_->props.num_entries > 0);
  if (should_flush_sst) {
    FAIL_RETURN_MSG(finish_one_sst(), "failed on finish_one_sst(%d)", ret);
//    delete rep_;
//    rep_ = nullptr;
    MOD_DELETE_OBJECT(Rep, rep_);
    FAIL_RETURN_MSG(init_one_sst(), "failed on init_one_sst(%d)", ret);
  }

  if (rep_->props.num_entries == 0) {
    // if this only block is too large for an extent, reserve enough memory to
    // avoid memory realloc and copy
    sst_size_after =
        sst_size_after_block(block_contents, last_key, add_block_stats);
    if (sst_size_after > EXTENT_SIZE) {
      int sz = (sst_size_after + EXTENT_SIZE - 1) / EXTENT_SIZE * DEF_BUF_SIZE;
      FAIL_RETURN_MSG(sst_buf_.reserve(sz),
                      "failed to reverve memory(%d)", ret);
      FAIL_RETURN_MSG(index_buf_.reserve(sz),
                      "failed to reverve memory(%d)", ret);
    }
  }

  if (has_trailer) {
    // block_contents must include the check trailer, and
    // the size of block handle must not include trailer size.
    // This is because one block has 2 parts, contents(compressed if necessary)
    // and checksum. In order to add a block directly, read the block and the
    // checksum up to the block_contents together. But the handle will be used to
    // locate the block and should know the size of the block contents not
    // including checksum part. So the size of this handle is size of
    // block_contents - size of checksum.
    BlockHandle block_handle{rep_->offset,
      block_contents.size() - kBlockTrailerSize};
    // Flush current block
    FAIL_RETURN(flush_data_block(block_contents));
    rep_->index_builder->AddIndexEntry(&last_key, block_handle, add_block_stats);
  } else {
    // write block will skip data when compression disabled,
    // so we append block contents here
    if (compression_type_ == kNoCompression) {
      FAIL_RETURN(sst_buf_.append(block_contents));
    }
    // write_block will call write_raw_block, and it will advance rep_->offset
    // and rep_->last_offset
    FAIL_RETURN(write_block(block_contents, &rep_->pending_handle,
          true /* is_data_block */, 
          false /* is_index_block */));
    rep_->index_builder->AddIndexEntry(&last_key, rep_->pending_handle,
        add_block_stats);
  }

  // Add block_stats for the directly added block
  sync_up_block_stats(add_block_stats);
  ++rep_->props.num_data_blocks;

  rep_->meta.UpdateBoundaries(add_block_stats.first_key_,
                              add_block_stats.smallest_seqno_);
  rep_->meta.UpdateBoundaries(last_key, add_block_stats.largest_seqno_);

  rep_->last_key.assign(last_key.data(), last_key.size());
  rep_->props.num_entries += add_block_stats.rows_;
  rep_->props.num_deletes += add_block_stats.entry_deletes_;

  TEST_SYNC_POINT_CALLBACK("ExtentBasedTableBuilder::AddBlock:1", nullptr);
  return Status::kOk;
}

bool ExtentBasedTableBuilder::SupportAddBlock() const {
  return rep_->collectors_support_add_block &&
         rep_->index_builder->SupportAddBlock();
}

int ExtentBasedTableBuilder::sst_meta_size() const {
  int sz = 0;

  int properties_size = EST_PROP_BLOCK_SIZE;
  sz += properties_size;

  int dict_size = compression_dict_ ? compression_dict_->size() : 0;
  sz += dict_size;

  int metaindex_size = kHashIndexPrefixesBlock.size() +
                       kHashIndexPrefixesMetadataBlock.size() +
                       kPropertiesBlock.size() + kCompressionDictBlock.size() +
                       BlockHandle::kMaxEncodedLength * 6;
  sz += metaindex_size;

  int footer_size = Footer::size(table_options_.format_version);
  sz += footer_size;

  return sz;
}

int ExtentBasedTableBuilder::sst_size_after_block(
    const Slice& block_contents, const Slice& key,
    const BlockStats& block_stats) {
  int sz = meta_size_;
  sz += rep_->offset;  // data blocks already in sst
  sz += block_contents.size();
  sz += rep_->index_builder->EstimatedSizeAfter(key, block_stats);
  return sz;
}

int ExtentBasedTableBuilder::sst_size_after_row(const Slice& key,
                                                const Slice& value,
                                                const BlockStats& block_stats) {
  int sz = meta_size_;
  sz += rep_->offset;  // data blocks already in sst
  sz += rep_->data_block.EstimateSizeAfterKV(key.size(), value.size());
  sz += rep_->index_builder->EstimatedSizeAfter(key, block_stats);
  return sz;
}

int ExtentBasedTableBuilder::update_row_cache(const uint32_t cf_id,
                                              const common::Slice& key,
                                              const common::Slice& value,
                                              const common::ImmutableCFOptions &ioption) {
  int ret = 0;
  if (nullptr != ioption.row_cache) {
    IterKey row_cache_key;
    // 1. construct row cache key
    AppendVarint32(row_cache_key, cf_id);
    const Slice user_key = ExtractUserKey(key);
    row_cache_key.TrimAppend(row_cache_key.Size(), user_key.data(), user_key.size());
    // 2. Erase old row from row cache
    if (ioption.row_cache->check_in_cache(row_cache_key.GetUserKey())) {
      ioption.row_cache->Erase(row_cache_key.GetUserKey());
      QUERY_COUNT(CountPoint::ROW_CACHE_EVICT);
    }
  }
  return ret;
}

int ExtentBasedTableBuilder::set_in_cache_flag()
{
  int ret = 0;
  if (nullptr != rep_) {
    rep_->data_block.set_add_cache(true);
  }
  return ret;
}

int ExtentBasedTableBuilder::Add(const Slice& key, const Slice& value) {
#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("ExtentBasedTableBuilder::ignore_flush_data", this);
  if (TEST_is_ignore_flush_data()) {
    return Status::kOk;
  }
#endif
  if (!is_inited_) {
    XENGINE_LOG(WARN, "ExtentBasedTableBuilder should been inited first");
    return Status::kNotInit;
  }

  if (LIKELY(value.size() < LARGE_OBJECT_SIZE)) {
    return plain_add(key, value);
  }

  int ret = Status::kOk;
  char *oob = nullptr;
  std::unique_ptr<char[], void(&)(void *)> oob_uptr(nullptr, base_memalign_free);
  bool need_copy = true;
  uint64_t size_on_disk = value.size();

  // use L2 compression type for large value, the type is fixed as the value is
  // not rebuilt on compaction
  CompressionType zip_type = kNoCompression;
  if (ioptions_.compression_per_level.size() >= 3) {
    zip_type = ioptions_.compression_per_level[2];
  }
  WritableBuffer zip_buf(PAGE_SIZE, EXTENT_SIZE, false);
  if (zip_type != kNoCompression) {
    Slice dict;
    Slice slice_out;
    ret = compress_block(value, compression_opts_, zip_type,
              LargeValue::COMPRESSION_FORMAT_VERSION, dict, &zip_buf, slice_out);
    if ((ret == Status::kOk) && (zip_type != kNoCompression) &&
        (Roundup(value.size(), EXTENT_SIZE) > Roundup(slice_out.size(), EXTENT_SIZE))) {
      assert(slice_out.data() == zip_buf.data());
      assert(slice_out.size() == zip_buf.size());
      oob = const_cast<char *>(slice_out.data());  // aligned
      need_copy = false;
      size_on_disk = slice_out.size();
    } else {
      zip_type = kNoCompression;
    }
  }

  if (oob == nullptr) { // no zip
    if (reinterpret_cast<std::uintptr_t>(value.data()) % PAGE_SIZE == 0) {
      oob = const_cast<char *>(value.data());
      need_copy = false;
    } else {
      oob_uptr.reset((char *)base_memalign(PAGE_SIZE, EXTENT_SIZE, ModId::kLargeObject));
      oob = oob_uptr.get();
      if (oob == nullptr) {
        FAIL_RETURN_MSG(Status::kMemoryLimit,
                        "Fail to allocate memory(%d)", ret);
      }
      need_copy = true;
    }
  }

  char *oob_base = oob;
  storage::WritableExtent oob_extent;
  LargeObject lobj;
  // replace lob key
//  std::unique_ptr<char[]> new_key(new char[key.size()]);
  char *new_key = static_cast<char *>(base_malloc(key.size(), ModId::kDefaultMod));
  memcpy(new_key, key.data(), key.size());
  new_key[key.size() - 8] = kTypeValueLarge;
  Slice lob_key(key.data(), key.size());
  lobj.key_.assign(new_key, key.size());

  for (size_t off = 0; SUCCED(ret) && off < size_on_disk; off += EXTENT_SIZE) {
    oob_extent.reset();
    FAIL_RETURN_MSG(mtables_->space_manager->allocate(mtables_->table_space_id_,
                                                      storage::HOT_EXTENT_SPACE,
                                                      oob_extent),
                    "Could not allocate extent(%d)", ret);
    not_flushed_lob_extent_id_ = oob_extent.get_extent_id();
    size_t count = EXTENT_SIZE;
    if (count > size_on_disk - off) {
      count = size_on_disk - off;
    }
    if (need_copy) {
      memcpy(oob, value.data() + off, count);
    } else {
      oob = oob_base + off;
    }
    size_t io_count = util::Roundup(count, PAGE_SIZE);
    FAIL_RETURN_MSG(oob_extent.Append(Slice(oob, io_count)).code(),
                    "Could not append extent(%d)", ret);
    FAIL_RETURN_MSG(oob_extent.Sync().code(),
                    "Could not sync extent(%d)", ret);
    storage::ExtentMeta extent_meta;
    if (FAILED(build_large_object_extent_meta(lob_key, oob_extent.get_extent_id(), count, extent_meta))) {
      XENGINE_LOG(WARN, "fail to build large object extent meta", K(ret), "extent_id", oob_extent.get_extent_id());
    } else if (FAILED(write_extent_meta(extent_meta, true /*is_large_object_extent*/))) {
      XENGINE_LOG(WARN, "fail to write extent meta", K(ret), K(extent_meta));
    } else {
      lobj.value_.oob_extents_.push_back(oob_extent.get_extent_id());
      flushed_lob_extent_ids_.push_back(oob_extent.get_extent_id());
      not_flushed_lob_extent_id_.reset();
    }
  }

  if (SUCCED(ret)) {
    // construct lob value
    lobj.value_.version_ = LargeValue::LATEST_VERSION;
    lobj.value_.compression_type_ = zip_type;
    lobj.value_.size_ = size_on_disk;


    int64_t new_value_size = lobj.value_.get_serialize_size();
    if (new_value_size + key.size() + 128 /*a small buffer*/
        > storage::MAX_LOB_KV_SIZE) {
      Abandon();
      XENGINE_LOG(ERROR, "object too large", K(new_value_size), K(key.size()));
      ret =  Status::kNotSupported;
    } else {
//      std::unique_ptr<char[]> new_value(new char[new_value_size]);
      // todo use tmp_arena, reset each add kv
      char *new_value = static_cast<char *>(base_malloc(new_value_size, ModId::kDefaultMod));
      int64_t pos = 0;
      if (FAILED(lobj.value_.serialize(new_value, new_value_size, pos))) {
        XENGINE_LOG(ERROR, "failed to serialize value", K(new_value_size));
      } else {
        ret = plain_add(Slice(new_key, key.size()), Slice(new_value, new_value_size));
      }
      base_free(new_value);
    }
  }
  base_free(new_key);
  return ret;
}

int ExtentBasedTableBuilder::plain_add(const Slice& key, const Slice& value) {

  int& ret = status_;
  ret = Status::kOk;

  ValueType value_type = ExtractValueType(key);
  if (!IsValueType(value_type)) {  // range del unsupported
    FAIL_RETURN_MSG(Status::kNotSupported,
                    "doesn't support value type(%d)", value_type);
  }

  if (rep_->props.num_entries > 0) {
    if (internal_comparator_.Compare(key, Slice(rep_->last_key)) <= 0) {
      XENGINE_LOG(ERROR, "Order wrong in Add", "last_key", Slice(rep_->last_key), K(key));
      assert(false);
      return Status::kCorruption;
    }
  }

  if (output_position_.level_ == 0) {
    FAIL_RETURN_MSG(update_row_cache(column_family_id_, key, value, ioptions_),
                    "update row cache failed", value_type);
  }

  bool should_flush_block = rep_->flush_block_policy->Update(key, value);
  if (should_flush_block) {
    FAIL_RETURN_MSG(flush_data_block(), "failed on flush_data_block(%d)", ret);
    block_stats_.actual_disk_size_ += rep_->offset - rep_->last_offset;
    rep_->index_builder->AddIndexEntry(&rep_->last_key, &key,
                                       rep_->pending_handle, block_stats_);
    sync_up_block_stats(block_stats_);
    ++rep_->props.num_data_blocks;
    block_stats_.reset();
  }

  if (rep_->data_block.empty()) {
    block_stats_.first_key_.assign(key.data(), key.size());
  }

  int sst_size_after = sst_size_after_row(key, value, block_stats_);
  bool should_flush_sst =
      (sst_size_after > EXTENT_SIZE && rep_->props.num_entries > 0);
  if (should_flush_sst) {
    FAIL_RETURN_MSG(finish_one_sst(), "failed on finish_one_sst(%d)", ret);
//    delete rep_;
//    rep_ = nullptr;
    MOD_DELETE_OBJECT(Rep, rep_);
    FAIL_RETURN_MSG(init_one_sst(), "failed on init_one_sst(%d)", ret);

    block_stats_.first_key_.assign(key.data(), key.size());
  }

  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    FAIL_RETURN_MSG(Status::kCorruption, "cannot parse internal key(%d)", ret);
  }
  rep_->meta.UpdateBoundaries(key, ikey.sequence);
  update_block_stats(key, value, ikey);

  // if the 1st record in sst is too large, reserve enough memory for all buf
  if (UNLIKELY(rep_->props.num_entries == 0 &&
               key.size() + value.size() > EXTENT_SIZE / 4)) {
    sst_size_after = sst_size_after_row(key, value, block_stats_);
    if (sst_size_after > EXTENT_SIZE) {
      int sz = (sst_size_after + EXTENT_SIZE - 1) / EXTENT_SIZE * DEF_BUF_SIZE;
      FAIL_RETURN_MSG(sst_buf_.reserve(sz),
                      "failed to reverve memory(%d)", ret);
      FAIL_RETURN_MSG(index_buf_.reserve(sz),
                      "failed to reverve memory(%d)", ret);
      // if block buffer doesn't share memory with sst buffer
      if (compression_type_ != kNoCompression) {
        FAIL_RETURN_MSG(block_buf_.reserve(sz),
                        "failed to reverve memory(%d)", ret);
      }
    }
  }

  rep_->last_key.assign(key.data(), key.size());
  rep_->data_block.Add(key, value);
  // increase num_entries every row add instead of block add,
  // it's checked here and there
  rep_->props.num_entries += 1;
  // kSingleDeletion is not supported. 
  if (value_type == kTypeDeletion || value_type == kTypeSingleDeletion) {
    rep_->props.num_deletes += 1;
  }

  rep_->index_builder->OnKeyAdded(key);

  return Status::kOk;
}

// just copy block content to dest buf, shouldn't add anything to sst stats
int ExtentBasedTableBuilder::flush_data_block() {
  if (status_ != Status::kOk) return status_;

  int ret = Status::kOk;

  if (rep_->data_block.empty()) return ret;

  FAIL_RETURN_MSG(write_block(rep_->data_block.Finish(), &rep_->pending_handle,
                              true /* is_data_block */, 
                              false /* is_index_block */),
                  "failed on write_block(%d)", ret);

  rep_->data_block.Reset();
  return ret;
}

// for AddBlock case
int ExtentBasedTableBuilder::flush_data_block(const Slice& block_contents) {
  int ret = Status::kOk;

  FAIL_RETURN_MSG(sst_buf_.append(block_contents),
                  "failed to append buffer(%d)", ret);
  sst_buf_.clear();

  rep_->last_offset = rep_->offset;
  rep_->offset += block_contents.size();
  if (rep_->offset != sst_buf_.all_size()) {
    FAIL_RETURN_MSG(Status::kCorruption,
                    "failed to build data block(%d)", ret);
  }
  return ret;
}

int ExtentBasedTableBuilder::write_block(const Slice& raw_block_contents,
                                         BlockHandle* handle,
                                         bool is_data_block,
                                         bool is_index_block) {
  if (status_ != Status::kOk) return status_;

  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  int ret = Status::kOk;

  CompressionType type = compression_type_;
  Slice block_contents;
  bool abort_compression = false;
  bool skip_data = false;
  if (is_data_block && (type == kNoCompression)) {
    // block content has already been put into sst_buf_,
    skip_data = true;
  }

  size_t orig_size = sst_buf_.size();

  if (skip_data) {
    block_contents = raw_block_contents;
  } else if (raw_block_contents.size() < kCompressionSizeLimit) {
    Slice compression_dict;
    if (is_data_block && compression_dict_ && compression_dict_->size()) {
      compression_dict = *compression_dict_;
    }

    skip_data = true;
    FAIL_RETURN_MSG(compress_block(raw_block_contents, compression_opts_, type,
                                   table_options_.format_version,
                                   compression_dict, &sst_buf_, block_contents),
                    "failed on compress_block(%d)", ret);

    // Some of the compression algorithms are known to be unreliable. If
    // the verify_compression flag is set then try to de-compress the
    // compressed data and compare to the input.
    if (type != kNoCompression && table_options_.verify_compression) {
      // Retrieve the uncompressed contents into a new buffer
      BlockContents contents;
      Status stat = UncompressBlockContentsForCompressionType(
          block_contents.data(), block_contents.size(), &contents,
          table_options_.format_version, compression_dict, type);

      if (stat.ok()) {
        bool compressed_ok = contents.data.compare(raw_block_contents) == 0;
        if (!compressed_ok) {
          // The result of the compression was invalid. abort.
          abort_compression = true;
          __XENGINE_LOG(WARN, "Decompressed block did not match raw block");
        }
      } else {
        // Decompression reported an error. abort.
        __XENGINE_LOG(WARN, "Could not decompress");
        abort_compression = true;
      }
    }
  } else {
    // Block is too big to be compressed.
    abort_compression = true;
  }

  // Abort compression if the block is too big, or did not pass
  // verification.
  if (abort_compression) {
    QUERY_COUNT(CountPoint::NUMBER_BLOCK_NOT_COMPRESSED);
    type = kNoCompression;
    block_contents = raw_block_contents;
    skip_data = false;
    sst_buf_.resize(orig_size);  // ignore the appended block
  } else if (type != kNoCompression &&
             ShouldReportDetailedTime(ioptions_.env, ioptions_.statistics)) {
    QUERY_COUNT_ADD(CountPoint::BYTES_COMPRESSED, raw_block_contents.size());
    QUERY_COUNT(CountPoint::NUMBER_BLOCK_COMPRESSED);
  }

  return write_raw_block(block_contents, type, handle,
                         is_data_block, is_index_block, skip_data);
}

int ExtentBasedTableBuilder::write_raw_block(const Slice& block_contents,
                                             CompressionType type,
                                             BlockHandle* handle,
                                             bool is_data_block,
                                             bool is_index_block,
                                             bool skip_data) {
  if (status_ != Status::kOk) return status_;

  int ret = Status::kOk;
  handle->set_offset(rep_->offset);
  handle->set_size(block_contents.size());
  if (!skip_data) {
    FAIL_RETURN_MSG(sst_buf_.append(block_contents),
                    "failed to append buffer(%d)", ret);
  }

  char trailer[kBlockTrailerSize];
  trailer[0] = type;
  char* trailer_without_type = trailer + 1;
  switch (table_options_.checksum) {
    case kNoChecksum:
      // we don't support no checksum yet
      assert(false);
    // intentional fallthrough
    case kCRC32c: {
      auto crc = crc32c::Value(block_contents.data(), block_contents.size());
      crc = crc32c::Extend(crc, trailer, 1);  // Extend to cover block type
      EncodeFixed32(trailer_without_type, crc32c::Mask(crc));
      break;
    }
    case kxxHash: {
      void* xxh = XXH32_init(0);
      XXH32_update(xxh, block_contents.data(),
                   static_cast<uint32_t>(block_contents.size()));
      XXH32_update(xxh, trailer, 1);  // Extend  to cover block type
      EncodeFixed32(trailer_without_type, XXH32_digest(xxh));
      break;
    }
  }

  FAIL_RETURN_MSG(sst_buf_.append(Slice(trailer, kBlockTrailerSize)),
                  "failed to append buffer(%d)", ret);

  if (((is_flush_ && table_options_.flush_fill_block_cache)
      || rep_->data_block.need_add_cache())
      && (is_data_block || is_index_block)) {
    // ignore error as this is a better-to-have
    insert_block_in_cache(block_contents, type, handle,
                          is_data_block, is_index_block);
    rep_->data_block.set_add_cache(false);
  }

  rep_->last_offset = rep_->offset;
  rep_->offset += block_contents.size() + kBlockTrailerSize;
  // move to next block
  sst_buf_.clear();
  return ret;
}

int ExtentBasedTableBuilder::BuildPropertyBlock(PropertyBlockBuilder& builder) {
  if (status_ != Status::kOk) return status_;

  TableProperties& props = rep_->props;

  props.column_family_id = column_family_id_;
  props.column_family_name = column_family_name_;
  props.filter_policy_name = table_options_.filter_policy != nullptr
                                 ? table_options_.filter_policy->Name()
                                 : "";
  props.index_size = rep_->index_builder->EstimatedSize() + kBlockTrailerSize;
  props.comparator_name = ioptions_.user_comparator != nullptr
                              ? ioptions_.user_comparator->Name()
                              : "nullptr";
  props.merge_operator_name = ioptions_.merge_operator != nullptr
                                  ? ioptions_.merge_operator->Name()
                                  : "nullptr";
  props.compression_name = CompressionTypeToString(compression_type_);
  props.prefix_extractor_name = ioptions_.prefix_extractor != nullptr
                                    ? ioptions_.prefix_extractor->Name()
                                    : "nullptr";

  std::string property_collectors_names = "[";
  property_collectors_names = "[";
  for (size_t i = 0; i < ioptions_.table_properties_collector_factories.size();
       ++i) {
    if (i != 0) {
      property_collectors_names += ",";
    }
    property_collectors_names +=
        ioptions_.table_properties_collector_factories[i]->Name();
  }
  property_collectors_names += "]";
  props.property_collectors_names = property_collectors_names;

  // Add basic properties
  builder.AddTableProperty(props);

  // Add use collected properties
  NotifyCollectTableCollectorsOnFinish(rep_->table_properties_collectors,
                                       &builder);
  return Status::kOk;
}

int ExtentBasedTableBuilder::write_sst(Footer& footer) {
  if (status_ != Status::kOk) return status_;

  assert(table_options_.format_version == 3);
  assert(table_options_.checksum == kCRC32c);

  storage::WritableExtent& extent = rep_->extent_;
  storage::WritableExtent next_extent;
  Status s;

  int64_t next_eid = LAST_EXTENT;
  size_t footer_size = Footer::size(table_options_.format_version);
  size_t sst_file_size = sst_buf_.all_size() + footer_size;
  if (sst_file_size > EXTENT_SIZE) {
    __XENGINE_LOG(INFO, "sst size %lx larger than extent\n", sst_file_size);
  }

  size_t remain_size = sst_buf_.all_size();
  char* buf = const_cast<char*>(sst_buf_.all_data());
  // in case of sst larger than 2MB, an extra footer (PAGE_SIZE for direct i/o)
  // is needed for each extent in order to chain them. It's guaranteed that
  // enough room is available in the buffer, even for libaio. ebuf is the room
  // for these extra footers.
  char* ebuf = buf + sst_buf_.capacity() - PAGE_SIZE;
  while (remain_size > 0) {
    size_t this_count = 0;
    if (remain_size + footer_size > EXTENT_SIZE) {
      //must not enter this branch
      XENGINE_LOG(ERROR, "unexpected error, extent size more than 2M", K(remain_size), K(footer_size));
      abort();
      s = mtables_->space_manager->allocate(mtables_->table_space_id_,
                                            storage::HOT_EXTENT_SPACE,
                                            next_extent);
      if (!s.ok()) {
        __XENGINE_LOG(ERROR, "Could not allocate extent");
        return s.code();
      }
      next_eid = next_extent.get_extent_id().id();
      this_count = EXTENT_SIZE - PAGE_SIZE;
      remain_size -= this_count;
    } else {
      next_eid = LAST_EXTENT;
      this_count = remain_size;
      remain_size = 0;
    }

    footer.set_valid_size(this_count);
    footer.set_next_extent(next_eid);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);

    size_t padding_size = EXTENT_SIZE - footer_size - this_count;
    if (remain_size == 0) {  // last extent
      memcpy(buf + this_count + padding_size, footer_encoding.data(),
             footer_encoding.size());
      if (buf + EXTENT_SIZE > ebuf + PAGE_SIZE) {
        __XENGINE_LOG(ERROR, "buffer overflow");
        return Status::kCorruption;
      }

      if (!(s = extent.Append(Slice(buf, EXTENT_SIZE))).ok() ||
          !(s = extent.Sync()).ok()) {
        __XENGINE_LOG(ERROR, "Could not write out extent");
        return s.code();
      }
    } else {
      memcpy(ebuf + padding_size, footer_encoding.data(),
             footer_encoding.size());
      if (!(s = extent.Append(Slice(buf, this_count))).ok() ||
          !(s = extent.Append(Slice(ebuf, PAGE_SIZE))).ok() ||
          !(s = extent.Sync()).ok()) {
        __XENGINE_LOG(ERROR, "Could not write out extent");
        return s.code();
      }
      ebuf -= PAGE_SIZE;
      extent = next_extent;
    }
    buf += this_count;
  }

  rep_->meta.fd.file_size = sst_file_size;
  rep_->meta.marked_for_compaction = NeedCompact();
  rep_->meta.num_entries = rep_->props.num_entries;
  rep_->meta.num_deletions = rep_->props.num_deletes;
  assert(rep_->meta.fd.GetFileSize() > 0);
  offset_ += rep_->offset;
  num_entries_ += rep_->props.num_entries;
  mtables_->props.push_back(GetTableProperties());

  int ret = Status::kOk;
  storage::ExtentMeta extent_meta(storage::ExtentMeta::F_NORMAL_EXTENT,
      rep_->meta.fd.extent_id, rep_->meta, GetTableProperties(),
      mtables_->table_space_id_, storage::HOT_EXTENT_SPACE);
  if (FAILED(write_extent_meta(extent_meta, false /*is_large_object_extent*/))) {
    XENGINE_LOG(WARN, "fail to write meta", K(ret), K(extent_meta));
  } else {
    mtables_->metas.push_back(rep_->meta);
    not_flushed_normal_extent_id_.reset();
  }
  return ret;
}

int ExtentBasedTableBuilder::finish_one_sst() {
  if (status_ != Status::kOk) return status_;

  int ret = Status::kOk;

  bool empty_data_block = rep_->data_block.empty();
  if (!empty_data_block) {
    FAIL_RETURN_MSG(flush_data_block(),
                    "failed on flush_data_block(%d)", ret);
    block_stats_.actual_disk_size_ += rep_->offset - rep_->last_offset;
    rep_->index_builder->AddIndexEntry(&rep_->last_key, nullptr,
                                       rep_->pending_handle, block_stats_);
    sync_up_block_stats(block_stats_);
    ++rep_->props.num_data_blocks;
    block_stats_.reset();
  }
  rep_->props.data_size = rep_->offset;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle,
      compression_dict_block_handle;

  IndexBuilder::IndexBlocks index_blocks;
  Status index_builder_status = rep_->index_builder->Finish(&index_blocks);
  if (index_builder_status.IsIncomplete()) {
    // We we have more than one index partition then meta_blocks are not
    // supported for the index. Currently meta_blocks are used only by
    // HashIndexBuilder which is not multi-partition.
    assert(index_blocks.meta_blocks.empty());
  } else if (!index_builder_status.ok()) {
    return index_builder_status.code();
  }

  // Write meta blocks and metaindex block with the following order.
  //    1. [meta block: filter], *removed*
  //    2. [meta block: properties]
  //    3. [meta block: compression dictionary]
  //    4. [meta block: range deletion tombstone], *removed*
  //    5. [metaindex block]
  // write meta blocks
  MetaIndexBuilder meta_index_builder;
  for (const auto& item : index_blocks.meta_blocks) {
    BlockHandle block_handle;
    FAIL_RETURN_MSG(
        write_block(item.second, &block_handle,
                    false /* is_data_block */, false /* is_index_block */),
        "failed to write index meta block(%d)", ret);
    meta_index_builder.Add(item.first, block_handle);
  }

  // Write properties and compression dictionary blocks.
  PropertyBlockBuilder property_block_builder;
  BuildPropertyBlock(property_block_builder);
  BlockHandle properties_block_handle;
  FAIL_RETURN_MSG(write_raw_block(property_block_builder.Finish(),
                                  kNoCompression, &properties_block_handle,
                                  false /* is_data_block */,
                                  false /* is_index_block */),
                  "failed to write property block(%d)", ret);
  meta_index_builder.Add(kPropertiesBlock, properties_block_handle);

  // Write compression dictionary block
  if (compression_dict_ && compression_dict_->size()) {
    FAIL_RETURN_MSG(write_raw_block(*compression_dict_, kNoCompression,
                                    &compression_dict_block_handle,
                                    false /* is_data_block */,
                                    false /* is_index_block */),
                    "failed to write compression block(%d)", ret);
    meta_index_builder.Add(kCompressionDictBlock,
                           compression_dict_block_handle);
  }

  // flush the meta index block
  FAIL_RETURN_MSG(write_raw_block(meta_index_builder.Finish(), kNoCompression,
                                  &metaindex_block_handle,
                                  false /* is_data_block */,
                                  false /* is_index_block */),
                  "failed to write meta index block(%d)", ret);

  // Write index block
  const bool is_data_block = true;
  FAIL_RETURN_MSG(write_block(index_blocks.index_block_contents,
                              &index_block_handle, !is_data_block,
                              true /* is_index_block */),
                  "failed to write index block(%d)", ret);
  // If there are more index partitions, finish them and write them out
  Status& s = index_builder_status;
  while (s.IsIncomplete()) {
    s = rep_->index_builder->Finish(&index_blocks, index_block_handle);
    if (!s.ok() && !s.IsIncomplete()) {
      return s.code();
    }
    FAIL_RETURN_MSG(write_block(index_blocks.index_block_contents,
                                &index_block_handle, !is_data_block,
                                true /* is_index_block */),
                    "failed to write index partition(%d)", ret);
    // The last index_block_handle will be for the partition index block
  }

  Footer footer(kExtentBasedTableMagicNumber, table_options_.format_version);
  footer.set_metaindex_handle(metaindex_block_handle);
  footer.set_index_handle(index_block_handle);
  footer.set_checksum(table_options_.checksum);
  FAIL_RETURN_MSG(write_sst(footer), "failed to write sst(%d)", ret);

  return ret;
}

Status ExtentBasedTableBuilder::status() const { return status_; }

int ExtentBasedTableBuilder::insert_block_in_cache(const Slice& block_contents,
                                                   const CompressionType type,
                                                   const BlockHandle* handle,
                                                   bool is_data_block,
                                                   bool is_index_block) {
  Rep* r = rep_;
  if (nullptr == r) {
    return Status::kInvalidArgument;
  } else if (type == kNoCompression && table_options_.block_cache) {
    size_t size = block_contents.size();
    std::unique_ptr<char[], ptr_delete<char>> ubuf;
    char* buf = static_cast<char*>(
        base_malloc(size, is_index_block ? ModId::kIndexBlockCache :
                          is_data_block ? ModId::kDataBlockCache :
                          ModId::kDefaultBlockCache));
    if (size > 0 && nullptr == buf) {
      return Status::kMemoryLimit;
    } else {
      ubuf.reset(buf);
      memcpy(ubuf.get(), block_contents.data(), size);

      BlockContents results(std::move(ubuf), size, true, type);
      Block* block = MOD_NEW_OBJECT(ModId::kCache, Block,
          std::move(results), kDisableGlobalSequenceNumber);
//          new Block(std::move(results), kDisableGlobalSequenceNumber);
      if (!block) {
        return Status::kMemoryLimit;
      }

      // make cache key by appending the file offset to the cache prefix id
      char* end = EncodeVarint64(r->cache_key_prefix + r->cache_key_prefix_size,
                                 handle->offset());
      Slice key(r->cache_key_prefix,
                static_cast<size_t>(end - r->cache_key_prefix));
      size_t block_usable_size = block->usable_size();
      // Insert into block cache.
      table_options_.block_cache->Insert(
          key, block, block->usable_size(), &DeleteCachedBlock, nullptr,
          cache::Cache::Priority::LOW, false);
      if (is_index_block) {
        QUERY_COUNT(CountPoint::BLOCK_CACHE_INDEX_ADD);
        QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_INDEX_BYTES_INSERT,
                        block_usable_size);
      } else if (is_data_block){
        QUERY_COUNT(CountPoint::BLOCK_CACHE_DATA_ADD);
        QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_DATA_BYTES_INSERT,
                        block_usable_size);
      }
      QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD);
      QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_BYTES_WRITE,
                      block_usable_size);
    }
  } else if (type != kNoCompression && table_options_.block_cache_compressed) {
    size_t size = block_contents.size();
    std::unique_ptr<char[], ptr_delete<char>> ubuf;
    char* buf = static_cast<char*>(base_malloc(size + 1));
    if (nullptr == buf) {
      return Status::kMemoryLimit;
    } else {
      ubuf.reset(buf);
      memcpy(ubuf.get(), block_contents.data(), size);
      ubuf[size] = type;

      BlockContents results(std::move(ubuf), size, true, type);
      Block* block = MOD_NEW_OBJECT(ModId::kCache, Block,
          std::move(results), kDisableGlobalSequenceNumber);
//          new Block(std::move(results), kDisableGlobalSequenceNumber);
      if (!block) {
        return Status::kMemoryLimit;
      }

      char* end = EncodeVarint64(
          r->compressed_cache_key_prefix + r->compressed_cache_key_prefix_size,
          handle->offset());
      Slice key(r->compressed_cache_key_prefix,
                static_cast<size_t>(end - r->compressed_cache_key_prefix));
      table_options_.block_cache_compressed->Insert(
          key, block, block->usable_size(), &DeleteCachedBlock, nullptr,
          cache::Cache::Priority::LOW);
    }
  }

  return Status::kOk;
}

int ExtentBasedTableBuilder::Finish() {
  int ret = Status::kOk;

  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentBasedTableBuilder should been inited first", K(ret));
  } else if (Status::kOk != status_) {
    ret = status_;
    XENGINE_LOG(WARN, "ExtentBasedTableBuilder internal status is wrong", K(ret));
  } else if (FAILED(finish_one_sst())) {
    XENGINE_LOG(WARN, "fail to finish one sst", K(ret));
  }

  return ret;
}

int ExtentBasedTableBuilder::Abandon() {
  int ret = Status::kOk;
  storage::ExtentId extent_id;

  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentBasedTableBuilder should been inited first", K(ret));
  } else {
    /**recycle normal extent has flushed to disk*/
    for (uint32_t i = 0; SUCCED(ret) && i < mtables_->metas.size(); ++i) {
      extent_id = mtables_->metas[i].fd.extent_id;
      if (FAILED(mtables_->space_manager->recycle(
              mtables_->table_space_id_, storage::HOT_EXTENT_SPACE, extent_id))) {
        XENGINE_LOG(WARN, "fail to recycle flushed normal extent", K(ret),
                    K(extent_id));
      }
    }
    mtables_->metas.clear();

    /**recycle lob extent has flushed to disk*/
    if (SUCCED(ret)) {
      for (uint32_t i = 0; SUCCED(ret) && i < flushed_lob_extent_ids_.size();
           ++i) {
        extent_id = flushed_lob_extent_ids_.at(i);
        if (FAILED(mtables_->space_manager->recycle(mtables_->table_space_id_,
                                                    storage::HOT_EXTENT_SPACE,
                                                    extent_id))) {
          XENGINE_LOG(WARN, "fail to recycle flushed lob extent", K(ret),
                      K(extent_id));
        }
      }
    }
    flushed_lob_extent_ids_.clear();

    /**recycle not flushed normal extent*/
    if (SUCCED(ret) && 0 != not_flushed_normal_extent_id_.id()) {
      if (FAILED(mtables_->space_manager->recycle(
              mtables_->table_space_id_, storage::HOT_EXTENT_SPACE,
              not_flushed_normal_extent_id_, false /*no extent meta*/))) {
        XENGINE_LOG(WARN, "fail to recycle not flushed normal extent", K(ret),
                    K(not_flushed_normal_extent_id_));
      }
    }

    /**recycle not flushed lob extent*/
    if (SUCCED(ret) && 0 != not_flushed_lob_extent_id_.id()) {
      if (FAILED(mtables_->space_manager->recycle(
          mtables_->table_space_id_, storage::HOT_EXTENT_SPACE,
          not_flushed_lob_extent_id_, false /*no extent meta*/))) {
        XENGINE_LOG(WARN, "fail to recycle the extent", K(ret), K(not_flushed_lob_extent_id_));
      } else {
        XENGINE_LOG(INFO, "success to recycle thr not flushed lob extent", K_(not_flushed_lob_extent_id));
        not_flushed_lob_extent_id_.reset();
      }
    }
  }

  return ret;
}

uint64_t ExtentBasedTableBuilder::NumEntries() const {
  return num_entries_ + rep_->props.num_entries;
}

uint64_t ExtentBasedTableBuilder::FileSize() const {
  return offset_ + rep_->offset;
}

bool ExtentBasedTableBuilder::NeedCompact() const {
  for (const auto& collector : rep_->table_properties_collectors) {
    if (collector->NeedCompact()) {
      return true;
    }
  }
  return false;
}

TableProperties ExtentBasedTableBuilder::GetTableProperties() const {
  TableProperties ret = rep_->props;
  for (const auto& collector : rep_->table_properties_collectors) {
    for (const auto& prop : collector->GetReadableProperties()) {
      ret.readable_properties.insert(prop);
    }
    collector->Finish(&ret.user_collected_properties);
  }
  return ret;
}

int ExtentBasedTableBuilder::build_large_object_extent_meta(const common::Slice &lob_key,
                                                            const storage::ExtentId &extent_id,
                                                            const int64_t data_size,
                                                            storage::ExtentMeta &extent_meta)
{
  int ret = Status::kOk;
  ParsedInternalKey ikey;

  if (!ParseInternalKey(lob_key, &ikey)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to parse internal key", K(ret), K(extent_id));
  } else {
    extent_meta.attr_ = storage::ExtentMeta::F_LARGE_OBJECT_EXTENT;
    extent_meta.smallest_key_.DecodeFrom(lob_key);
    extent_meta.largest_key_.DecodeFrom(lob_key);
    extent_meta.extent_id_ = extent_id;
    extent_meta.smallest_seqno_ = ikey.sequence;
    extent_meta.largest_seqno_ = ikey.sequence;
    extent_meta.refs_ = 0;
    extent_meta.data_size_ = data_size;
    extent_meta.index_size_ = 0;
    extent_meta.num_data_blocks_ = 1;
    extent_meta.num_entries_ = 1;
    extent_meta.num_deletes_ = 0;
    extent_meta.table_space_id_ = mtables_->table_space_id_;
    extent_meta.extent_space_type_ = storage::HOT_EXTENT_SPACE;
  }

  return ret;
}

int ExtentBasedTableBuilder::write_extent_meta(const storage::ExtentMeta &extent_meta, bool is_large_object_extent)
{
  int ret = Status::kOk;

  if (FAILED(mtables_->space_manager->write_meta(extent_meta, true))) {
    XENGINE_LOG(WARN, "fail to write extent meta", K(ret));
  } else {
    if (is_large_object_extent) {
      if (FAILED(mtables_->change_info_->add_large_object_extent(extent_meta.extent_id_))) {
        XENGINE_LOG(WARN, "fail to add large object extent to change info", K(ret), K(extent_meta));
      } else {
        XENGINE_LOG(INFO, "sucess to flush large object extent", K_(column_family_id), K_(output_position), "extent_id", extent_meta.extent_id_);
      }
    } else {
      if (FAILED(mtables_->change_info_->add_extent(output_position_, extent_meta.extent_id_))) {
        XENGINE_LOG(WARN, "fail to add extent to change info", K(ret));
      } else {
        XENGINE_LOG(INFO, "success to flush normal extent", K_(column_family_id), K_(output_position), "extent_id", extent_meta.extent_id_);
      }
    }
  }

  return ret;
}

#ifndef NDEBUG
void ExtentBasedTableBuilder::TEST_inject_ignore_flush_data()
{
  test_ignore_flush_data_ = true;
}

bool ExtentBasedTableBuilder::TEST_is_ignore_flush_data()
{
  return test_ignore_flush_data_;
}
#endif

}  // namespace table
}  // namespace xengine
