// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "utilities/blob_db/blob_db.h"

#ifndef ROCKSDB_LITE
#include "db/write_batch_internal.h"
#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_builder.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "xengine/convenience.h"
#include "xengine/env.h"
#include "xengine/iterator.h"
#include "xengine/utilities/stackable_db.h"

using namespace xengine;
using namespace db;
using namespace common;
using namespace table;
using namespace monitor;

namespace xengine {
namespace util {

namespace {
int kBlockBasedTableVersionFormat = 2;
}  // namespace

class BlobDB : public StackableDB {
 public:
  using util::StackableDB::Put;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

  using util::StackableDB::Get;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;

  Status Open();

  explicit BlobDB(DB* db);
  ~BlobDB();
 private:
  std::string dbname_;
  ImmutableCFOptions ioptions_;
  InstrumentedMutex mutex_;
  RandomAccessFileReader *file_reader_;
  WritableFileWriter *file_writer_;
//  std::unique_ptr<RandomAccessFileReader, memory::ptr_destruct_delete<RandomAccessFileReader>> file_reader_;
//  std::unique_ptr<WritableFileWriter, memory::ptr_destruct_delete<WritableFileWriter>> file_writer_;
  size_t writer_offset_;
  size_t next_sync_offset_;

  static const std::string kFileName;
  static const size_t kBlockHeaderSize;
  static const size_t kBytesPerSync;
};

Status NewBlobDB(Options options, std::string dbname, DB** blob_db) {
  DB* db;
  Status s = DB::Open(options, dbname, &db);
  if (!s.ok()) {
    return s;
  }
  BlobDB* bdb = new BlobDB(db);
  s = bdb->Open();
  if (!s.ok()) {
    delete bdb;
  }
  *blob_db = bdb;
  return s;
}

const std::string BlobDB::kFileName = "blob_log";
const size_t BlobDB::kBlockHeaderSize = 8;
const size_t BlobDB::kBytesPerSync = 1024 * 1024 * 128;

BlobDB::BlobDB(DB* db)
    : StackableDB(db),
      ioptions_(db->GetOptions()),
      writer_offset_(0),
      next_sync_offset_(kBytesPerSync) {}

BlobDB::~BlobDB() {
  if (nullptr != file_writer_) {
    // todo delete file
  }
  MOD_DELETE_OBJECT(WritableFileWriter, file_writer_);
  MOD_DELETE_OBJECT(RandomAccessFileReader, file_reader_);
}

Status BlobDB::Open() {
//  unique_ptr<WritableFile> wfile;
  WritableFile *wfile = nullptr;
  EnvOptions env_options(db_->GetOptions());
  Status s = ioptions_.env->NewWritableFile(db_->GetName() + "/" + kFileName,
                                            wfile, env_options);
  if (!s.ok()) {
    return s;
  }
  file_writer_ = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, WritableFileWriter, wfile, env_options);
//  file_writer_.reset(new WritableFileWriter(wfile, env_options));

  // Write version
  std::string version;
  PutFixed64(&version, 0);
  s = file_writer_->Append(Slice(version));
  if (!s.ok()) {
    return s;
  }
  writer_offset_ += version.size();

//  std::unique_ptr<RandomAccessFile> rfile;
  RandomAccessFile *rfile = nullptr;
  s = ioptions_.env->NewRandomAccessFile(db_->GetName() + "/" + kFileName,
                                         rfile, env_options);
  if (!s.ok()) {
    return s;
  }
  file_reader_ = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, RandomAccessFileReader, rfile);
//  file_reader_.reset(new RandomAccessFileReader(rfile));
  return s;
}

Status BlobDB::Put(const WriteOptions& options, const Slice& key,
                   const Slice& value) {
  BlockBuilder block_builder(1, false);
  block_builder.Add(key, value);

  CompressionType compression = CompressionType::kLZ4Compression;
  CompressionOptions compression_opts;

  Slice block_contents;
  std::string compression_output;

  block_contents = CompressBlock(block_builder.Finish(), compression_opts,
                                 &compression, kBlockBasedTableVersionFormat,
                                 Slice() /* dictionary */, &compression_output);

  char header[kBlockHeaderSize];
  char trailer[kBlockTrailerSize];
  trailer[0] = compression;
  auto crc = crc32c::Value(block_contents.data(), block_contents.size());
  crc = crc32c::Extend(crc, trailer, 1);  // Extend to cover block type
  EncodeFixed32(trailer + 1, crc32c::Mask(crc));

  BlockHandle handle;
  std::string index_entry;
  Status s;
  {
    InstrumentedMutexLock l(&mutex_);
    auto raw_block_size = block_contents.size();
    EncodeFixed64(header, raw_block_size);
    s = file_writer_->Append(Slice(header, kBlockHeaderSize));
    writer_offset_ += kBlockHeaderSize;
    if (s.ok()) {
      handle.set_offset(writer_offset_);
      handle.set_size(raw_block_size);
      s = file_writer_->Append(block_contents);
    }
    if (s.ok()) {
      s = file_writer_->Append(Slice(trailer, kBlockTrailerSize));
    }
    if (s.ok()) {
      s = file_writer_->Flush();
    }
    if (s.ok() && writer_offset_ > next_sync_offset_) {
      // Sync every kBytesPerSync. This is a hacky way to limit unsynced data.
      next_sync_offset_ += kBytesPerSync;
      s = file_writer_->Sync(db_->GetOptions().use_fsync);
    }
    if (s.ok()) {
      writer_offset_ += block_contents.size() + kBlockTrailerSize;
      // Put file number
      PutVarint64(&index_entry, 0);
      handle.EncodeTo(&index_entry);
      s = db_->Put(options, key, index_entry);
    }
  }
  return s;
}

Status BlobDB::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  std::string index_entry;
  s = db_->Get(options, key, &index_entry);
  if (!s.ok()) {
    return s;
  }
  BlockHandle handle;
  Slice index_entry_slice(index_entry);
  uint64_t file_number;
  if (!GetVarint64(&index_entry_slice, &file_number)) {
    return Status::Corruption();
  }
  assert(file_number == 0);
  s = handle.DecodeFrom(&index_entry_slice);
  if (!s.ok()) {
    return s;
  }
  Footer footer(0, kBlockBasedTableVersionFormat);
  BlockContents contents;
  s = ReadBlockContents(file_reader_, footer, options, handle, &contents,
                        ioptions_);
  if (!s.ok()) {
    return s;
  }
  Block block(std::move(contents), kDisableGlobalSequenceNumber);
  BlockIter bit;
  InternalIterator* it = block.NewIterator(nullptr, &bit);
  it->SeekToFirst();
  if (!it->status().ok()) {
    return it->status();
  }
  *value = it->value().ToString();
  return s;
}
}  //  namespace util
}  //  namespace xengine
#else
namespace xengine {
namespace util {
Status NewBlobDB(Options options, std::string dbname, DB** blob_db) {
  return Status::NotSupported();
}
}  //  namespace util
}  //  namespace xengine
#endif  // ROCKSDB_LITE
