// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <algorithm>
#include <deque>
#include <string>
#include <vector>

#include "table/block_based_table_factory.h"
#include "table/internal_iterator.h"
#include "table/plain_table_factory.h"
#include "util/concurrent_direct_file_writer.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "xengine/compaction_filter.h"
#include "xengine/env.h"
#include "xengine/iterator.h"
#include "xengine/merge_operator.h"
#include "xengine/options.h"
#include "xengine/slice.h"
#include "xengine/table.h"

namespace xengine {

namespace util {
class SequentialFileReader;
class SequentialFile;

namespace test {

// Store in *dst a random string of length "len" and return a common::Slice that
// references the generated data.
extern common::Slice RandomString(Random* rnd, int len, std::string* dst);

extern std::string RandomHumanReadableString(Random* rnd, int len);

// Return a random key with the specified length that may contain interesting
// characters (e.g. \x00, \xff, etc.).
enum RandomKeyType : char { RANDOM, LARGEST, SMALLEST, MIDDLE };
extern std::string RandomKey(Random* rnd, int len,
                             RandomKeyType type = RandomKeyType::RANDOM);

// Store in *dst a string of length "len" that will compress to
// "N*compressed_fraction" bytes and return a common::Slice that references
// the generated data.
extern common::Slice CompressibleString(Random* rnd, double compressed_fraction,
                                        int len, std::string* dst);

// A wrapper that allows injection of errors.
class ErrorEnv : public util::EnvWrapper {
 public:
  bool writable_file_error_;
  int num_writable_file_errors_;

  ErrorEnv()
      : EnvWrapper(Env::Default()),
        writable_file_error_(false),
        num_writable_file_errors_(0) {}

  virtual common::Status NewWritableFile(const std::string& fname,
                                         WritableFile *&result,
                                         const EnvOptions& soptions) override {
//    result->reset();
    if (writable_file_error_) {
      ++num_writable_file_errors_;
      return common::Status::IOError(fname, "fake error");
    }
    return target()->NewWritableFile(fname, result, soptions);
  }
};

// An internal comparator that just forward comparing results from the
// user comparator in it. Can be used to test entities that have no dependency
// on internal key structure but consumes InternalKeyComparator, like
// BlockBasedTable.
class PlainInternalKeyComparator : public db::InternalKeyComparator {
 public:
  explicit PlainInternalKeyComparator(const util::Comparator* c)
      : InternalKeyComparator(c) {}

  virtual ~PlainInternalKeyComparator() {}

  virtual int Compare(const common::Slice& a,
                      const common::Slice& b) const override {
    return user_comparator()->Compare(a, b);
  }
  virtual void FindShortestSeparator(
      std::string* start, const common::Slice& limit) const override {
    user_comparator()->FindShortestSeparator(start, limit);
  }
  virtual void FindShortSuccessor(std::string* key) const override {
    user_comparator()->FindShortSuccessor(key);
  }
};

// A test comparator which compare two strings in this way:
// (1) first compare prefix of 8 bytes in alphabet order,
// (2) if two strings share the same prefix, sort the other part of the string
//     in the reverse alphabet order.
// This helps simulate the case of compounded key of [entity][timestamp] and
// latest timestamp first.
class SimpleSuffixReverseComparator : public util::Comparator {
 public:
  SimpleSuffixReverseComparator() {}

  virtual const char* Name() const override {
    return "SimpleSuffixReverseComparator";
  }

  virtual int Compare(const common::Slice& a,
                      const common::Slice& b) const override {
    common::Slice prefix_a = common::Slice(a.data(), 8);
    common::Slice prefix_b = common::Slice(b.data(), 8);
    int prefix_comp = prefix_a.compare(prefix_b);
    if (prefix_comp != 0) {
      return prefix_comp;
    } else {
      common::Slice suffix_a = common::Slice(a.data() + 8, a.size() - 8);
      common::Slice suffix_b = common::Slice(b.data() + 8, b.size() - 8);
      return -(suffix_a.compare(suffix_b));
    }
  }
  virtual void FindShortestSeparator(
      std::string* start, const common::Slice& limit) const override {}

  virtual void FindShortSuccessor(std::string* key) const override {}
};

// Returns a user key comparator that can be used for comparing two uint64_t
// slices. Instead of comparing slices byte-wise, it compares all the 8 bytes
// at once. Assumes same endian-ness is used though the database's lifetime.
// Symantics of comparison would differ from Bytewise comparator in little
// endian machines.
extern const util::Comparator* Uint64Comparator();

// Iterator over a vector of keys/values
class VectorIterator : public table::InternalIterator {
 public:
  explicit VectorIterator(const std::vector<std::string>& keys)
      : keys_(keys), current_(keys.size()) {
    std::sort(keys_.begin(), keys_.end());
    values_.resize(keys.size());
  }

  VectorIterator(const std::vector<std::string>& keys,
                 const std::vector<std::string>& values)
      : keys_(keys), values_(values), current_(keys.size()) {
    assert(keys_.size() == values_.size());
  }

  virtual bool Valid() const override { return current_ < keys_.size(); }

  virtual void SeekToFirst() override { current_ = 0; }
  virtual void SeekToLast() override { current_ = keys_.size() - 1; }

  virtual void Seek(const common::Slice& target) override {
    current_ = std::lower_bound(keys_.begin(), keys_.end(), target.ToString()) -
               keys_.begin();
  }

  virtual void SeekForPrev(const common::Slice& target) override {
    current_ = std::upper_bound(keys_.begin(), keys_.end(), target.ToString()) -
               keys_.begin();
    if (!Valid()) {
      SeekToLast();
    } else {
      Prev();
    }
  }

  virtual void Next() override { current_++; }
  virtual void Prev() override { current_--; }

  virtual common::Slice key() const override {
    return common::Slice(keys_[current_]);
  }
  virtual common::Slice value() const override {
    return common::Slice(values_[current_]);
  }

  virtual common::Status status() const override {
    return common::Status::OK();
  }

 private:
  std::vector<std::string> keys_;
  std::vector<std::string> values_;
  size_t current_;
};
extern WritableFileWriter* GetWritableFileWriter(WritableFile* wf);

extern util::ConcurrentDirectFileWriter* GetConcurrentDirectFileWriter(
    WritableFile* wf);

extern RandomAccessFileReader* GetRandomAccessFileReader(RandomAccessFile* raf);

extern util::SequentialFileReader* GetSequentialFileReader(SequentialFile* se);

class StringSink : public WritableFile {
 public:
  std::string contents_;

  explicit StringSink(common::Slice* reader_contents = nullptr)
      : WritableFile(),
        contents_(""),
        reader_contents_(reader_contents),
        last_flush_(0) {
    if (reader_contents_ != nullptr) {
      *reader_contents_ = common::Slice(contents_.data(), 0);
    }
  }

  const std::string& contents() const { return contents_; }

  virtual common::Status Truncate(uint64_t size) override {
    contents_.resize(static_cast<size_t>(size));
    return common::Status::OK();
  }
  virtual common::Status Close() override { return common::Status::OK(); }
  virtual common::Status Flush() override {
    if (reader_contents_ != nullptr) {
      assert(reader_contents_->size() <= last_flush_);
      size_t offset = last_flush_ - reader_contents_->size();
      *reader_contents_ =
          common::Slice(contents_.data() + offset, contents_.size() - offset);
      last_flush_ = contents_.size();
    }

    return common::Status::OK();
  }
  virtual common::Status Sync() override { return common::Status::OK(); }
  virtual common::Status Append(const common::Slice& slice) override {
    contents_.append(slice.data(), slice.size());
    return common::Status::OK();
  }
  virtual uint64_t GetFileSize() override { return contents_.size(); }
  void Drop(size_t bytes) {
    if (reader_contents_ != nullptr) {
      contents_.resize(contents_.size() - bytes);
      *reader_contents_ = common::Slice(reader_contents_->data(),
                                        reader_contents_->size() - bytes);
      last_flush_ = contents_.size();
    }
  }

 private:
  common::Slice* reader_contents_;
  size_t last_flush_;
};

// A wrapper around a StringSink to give it a RandomRWFile interface
class RandomRWStringSink : public RandomRWFile {
 public:
  explicit RandomRWStringSink(StringSink* ss) : ss_(ss) {}

  common::Status Write(uint64_t offset, const common::Slice& data) {
    if (offset + data.size() > ss_->contents_.size()) {
      ss_->contents_.resize(offset + data.size(), '\0');
    }

    char* pos = const_cast<char*>(ss_->contents_.data() + offset);
    memcpy(pos, data.data(), data.size());
    return common::Status::OK();
  }

  common::Status Read(uint64_t offset, size_t n, common::Slice* result,
                      char* scratch) const {
    *result = common::Slice(nullptr, 0);
    if (offset < ss_->contents_.size()) {
      size_t str_res_sz =
          std::min(static_cast<size_t>(ss_->contents_.size() - offset), n);
      *result = common::Slice(ss_->contents_.data() + offset, str_res_sz);
    }
    return common::Status::OK();
  }

  common::Status Flush() { return common::Status::OK(); }

  common::Status Sync() { return common::Status::OK(); }

  common::Status Close() { return common::Status::OK(); }

  const std::string& contents() const { return ss_->contents(); }

 private:
  StringSink* ss_;
};

// Like StringSink, this writes into a string.  Unlink StringSink, it
// has some initial content and overwrites it, just like a recycled
// log file.
class OverwritingStringSink : public WritableFile {
 public:
  explicit OverwritingStringSink(common::Slice* reader_contents)
      : WritableFile(),
        contents_(""),
        reader_contents_(reader_contents),
        last_flush_(0) {}

  const std::string& contents() const { return contents_; }

  virtual common::Status Truncate(uint64_t size) override {
    contents_.resize(static_cast<size_t>(size));
    return common::Status::OK();
  }
  virtual common::Status Close() override { return common::Status::OK(); }
  virtual common::Status Flush() override {
    if (last_flush_ < contents_.size()) {
      assert(reader_contents_->size() >= contents_.size());
      memcpy((char*)reader_contents_->data() + last_flush_,
             contents_.data() + last_flush_, contents_.size() - last_flush_);
      last_flush_ = contents_.size();
    }
    return common::Status::OK();
  }
  virtual common::Status Sync() override { return common::Status::OK(); }
  virtual common::Status Append(const common::Slice& slice) override {
    contents_.append(slice.data(), slice.size());
    return common::Status::OK();
  }
  void Drop(size_t bytes) {
    contents_.resize(contents_.size() - bytes);
    if (last_flush_ > contents_.size()) last_flush_ = contents_.size();
  }

 private:
  std::string contents_;
  common::Slice* reader_contents_;
  size_t last_flush_;
};

class StringSource : public RandomAccessFile {
 public:
  explicit StringSource(const common::Slice& contents, uint64_t uniq_id = 0,
                        bool mmap = false)
      : contents_(contents.data(), contents.size()),
        uniq_id_(uniq_id),
        mmap_(mmap),
        total_reads_(0) {}

  virtual ~StringSource() {}

  uint64_t Size() const { return contents_.size(); }

  virtual common::Status Read(uint64_t offset, size_t n, common::Slice* result,
                              char* scratch) const override {
    total_reads_++;
    if (offset > contents_.size()) {
      return common::Status::InvalidArgument("invalid Read offset");
    }
    if (offset + n > contents_.size()) {
      n = contents_.size() - static_cast<size_t>(offset);
    }
    if (!mmap_) {
      memcpy(scratch, &contents_[static_cast<size_t>(offset)], n);
      *result = common::Slice(scratch, n);
    } else {
      *result = common::Slice(&contents_[static_cast<size_t>(offset)], n);
    }
    return common::Status::OK();
  }

  virtual size_t GetUniqueId(char* id, size_t max_size) const override {
    if (max_size < 20) {
      return 0;
    }

    char* rid = id;
    rid = EncodeVarint64(rid, uniq_id_);
    rid = EncodeVarint64(rid, 0);
    return static_cast<size_t>(rid - id);
  }

  int total_reads() const { return total_reads_; }

  void set_total_reads(int tr) { total_reads_ = tr; }

 private:
  std::string contents_;
  uint64_t uniq_id_;
  bool mmap_;
  mutable int total_reads_;
};

class NullLogger : public Logger {
 public:
  using Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override {}
  virtual size_t GetLogFileSize() const override { return 0; }
};

// Corrupts key by changing the type
extern void CorruptKeyType(db::InternalKey* ikey);

extern std::string KeyStr(const std::string& user_key,
                          const common::SequenceNumber& seq,
                          const db::ValueType& t, bool corrupt = false);

class SleepingBackgroundTask {
 public:
  SleepingBackgroundTask()
      : bg_cv_(&mutex_),
        should_sleep_(true),
        done_with_sleep_(false),
        sleeping_(false) {}

  bool IsSleeping() {
    MutexLock l(&mutex_);
    return sleeping_;
  }
  void DoSleep() {
    MutexLock l(&mutex_);
    sleeping_ = true;
    bg_cv_.SignalAll();
    while (should_sleep_) {
      bg_cv_.Wait();
    }
    sleeping_ = false;
    done_with_sleep_ = true;
    bg_cv_.SignalAll();
  }
  void WaitUntilSleeping() {
    MutexLock l(&mutex_);
    while (!sleeping_ || !should_sleep_) {
      bg_cv_.Wait();
    }
  }
  void WakeUp() {
    MutexLock l(&mutex_);
    should_sleep_ = false;
    bg_cv_.SignalAll();
  }
  void WaitUntilDone() {
    MutexLock l(&mutex_);
    while (!done_with_sleep_) {
      bg_cv_.Wait();
    }
  }
  bool WokenUp() {
    MutexLock l(&mutex_);
    return should_sleep_ == false;
  }

  void Reset() {
    MutexLock l(&mutex_);
    should_sleep_ = true;
    done_with_sleep_ = false;
  }

  static void DoSleepTask(void* arg) {
    reinterpret_cast<SleepingBackgroundTask*>(arg)->DoSleep();
  }

 private:
  port::Mutex mutex_;
  port::CondVar bg_cv_;  // Signalled when background work finishes
  bool should_sleep_;
  bool done_with_sleep_;
  bool sleeping_;
};

// Filters merge operands and values that are equal to `num`.
class FilterNumber : public storage::CompactionFilter {
 public:
  explicit FilterNumber(uint64_t num) : num_(num) {}

  std::string last_merge_operand_key() { return last_merge_operand_key_; }

  bool Filter(int level, const common::Slice& key, const common::Slice& value,
              std::string* new_value, bool* value_changed) const override {
    if (value.size() == sizeof(uint64_t)) {
      return num_ == DecodeFixed64(value.data());
    }
    return true;
  }

  bool FilterMergeOperand(int level, const common::Slice& key,
                          const common::Slice& value) const override {
    last_merge_operand_key_ = key.ToString();
    if (value.size() == sizeof(uint64_t)) {
      return num_ == DecodeFixed64(value.data());
    }
    return true;
  }

  const char* Name() const override { return "FilterBadMergeOperand"; }

 private:
  mutable std::string last_merge_operand_key_;
  uint64_t num_;
};

inline std::string EncodeInt(uint64_t x) {
  std::string result;
  PutFixed64(&result, x);
  return result;
}

class StringEnv : public EnvWrapper {
 public:
  class SeqStringSource : public SequentialFile {
   public:
    explicit SeqStringSource(const std::string& data)
        : data_(data), offset_(0) {}
    ~SeqStringSource() {}
    common::Status Read(size_t n, common::Slice* result,
                        char* scratch) override {
      std::string output;
      if (offset_ < data_.size()) {
        n = std::min(data_.size() - offset_, n);
        memcpy(scratch, data_.data() + offset_, n);
        offset_ += n;
        *result = common::Slice(scratch, n);
      } else {
        return common::Status::InvalidArgument(
            "Attemp to read when it already reached eof.");
      }
      return common::Status::OK();
    }
    common::Status Skip(uint64_t n) override {
      if (offset_ >= data_.size()) {
        return common::Status::InvalidArgument(
            "Attemp to read when it already reached eof.");
      }
      // TODO(yhchiang): Currently doesn't handle the overflow case.
      offset_ += n;
      return common::Status::OK();
    }

   private:
    std::string data_;
    size_t offset_;
  };

  class StringSink : public WritableFile {
   public:
    explicit StringSink(std::string* contents)
        : WritableFile(), contents_(contents) {}
    virtual common::Status Truncate(uint64_t size) override {
      contents_->resize(size);
      return common::Status::OK();
    }
    virtual common::Status Close() override { return common::Status::OK(); }
    virtual common::Status Flush() override { return common::Status::OK(); }
    virtual common::Status Sync() override { return common::Status::OK(); }
    virtual common::Status Append(const common::Slice& slice) override {
      contents_->append(slice.data(), slice.size());
      return common::Status::OK();
    }
    virtual uint64_t GetFileSize() override { return contents_->size(); }

   private:
    std::string* contents_;
  };

  explicit StringEnv(Env* t) : EnvWrapper(t) {}
  virtual ~StringEnv() {}

  const std::string& GetContent(const std::string& f) { return files_[f]; }

  const common::Status WriteToNewFile(const std::string& file_name,
                                      const std::string& content) {
//    unique_ptr<WritableFile> r;
    WritableFile *r = nullptr;
    auto s = NewWritableFile(file_name, r, EnvOptions());
    if (!s.ok()) {
      return s;
    }
    r->Append(content);
    r->Flush();
    r->Close();
    assert(files_[file_name] == content);
    return common::Status::OK();
  }

  // The following text is boilerplate that forwards all methods to target()
  common::Status NewSequentialFile(const std::string& f,
                                   SequentialFile *&r,
                                   const EnvOptions& options) override {
    auto iter = files_.find(f);
    if (iter == files_.end()) {
      return common::Status::NotFound("The specified file does not exist", f);
    }
//    r->reset(new SeqStringSource(iter->second));
//    r = new SeqStringSource(iter->second);
    r = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, SeqStringSource, iter->second);
    return common::Status::OK();
  }
  common::Status NewRandomAccessFile(const std::string& f,
                                     RandomAccessFile *&r,
                                     const EnvOptions& options) override {
    return common::Status::NotSupported();
  }
  common::Status NewWritableFile(const std::string& f,
                                 WritableFile *&r,
                                 const EnvOptions& options) override {
    auto iter = files_.find(f);
    if (iter != files_.end()) {
      return common::Status::IOError("The specified file already exists", f);
    }
//    r->reset(new StringSink(&files_[f]));
    r = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, StringSink, &files_[f]);
    return common::Status::OK();
  }
  virtual common::Status NewDirectory(const std::string& name,
                                      Directory *&result) override {
    return common::Status::NotSupported();
  }
  common::Status FileExists(const std::string& f) override {
    if (files_.find(f) == files_.end()) {
      return common::Status::NotFound();
    }
    return common::Status::OK();
  }
  common::Status GetChildren(const std::string& dir,
                             std::vector<std::string>* r) override {
    return common::Status::NotSupported();
  }
  common::Status DeleteFile(const std::string& f) override {
    files_.erase(f);
    return common::Status::OK();
  }
  common::Status CreateDir(const std::string& d) override {
    return common::Status::NotSupported();
  }
  common::Status CreateDirIfMissing(const std::string& d) override {
    return common::Status::NotSupported();
  }
  common::Status DeleteDir(const std::string& d) override {
    return common::Status::NotSupported();
  }
  common::Status GetFileSize(const std::string& f, uint64_t* s) override {
    auto iter = files_.find(f);
    if (iter == files_.end()) {
      return common::Status::NotFound("The specified file does not exist:", f);
    }
    *s = iter->second.size();
    return common::Status::OK();
  }

  common::Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) override {
    return common::Status::NotSupported();
  }

  common::Status RenameFile(const std::string& s,
                            const std::string& t) override {
    return common::Status::NotSupported();
  }

  common::Status LinkFile(const std::string& s, const std::string& t) override {
    return common::Status::NotSupported();
  }

  common::Status LockFile(const std::string& f, FileLock** l) override {
    return common::Status::NotSupported();
  }

  common::Status UnlockFile(FileLock* l) override {
    return common::Status::NotSupported();
  }

 protected:
  std::unordered_map<std::string, std::string> files_;
};

// Randomly initialize the given DBOptions
void RandomInitDBOptions(common::DBOptions* db_opt, util::Random* rnd);

// Randomly initialize the given ColumnFamilyOptions
// Note that the caller is responsible for releasing non-null
// cf_opt->compaction_filter.
void RandomInitCFOptions(common::ColumnFamilyOptions* cf_opt,
                         util::Random* rnd);

// A dummy merge operator which can change its name
class ChanglingMergeOperator : public db::MergeOperator {
 public:
  explicit ChanglingMergeOperator(const std::string& name)
      : name_(name + "MergeOperator") {}
  ~ChanglingMergeOperator() {}

  void SetName(const std::string& name) { name_ = name; }

  virtual bool FullMergeV2(
      const db::MergeOperator::MergeOperationInput& merge_in,
      MergeOperationOutput* merge_out) const override {
    return false;
  }
  virtual bool PartialMergeMulti(const common::Slice& key,
                                 const std::deque<common::Slice>& operand_list,
                                 std::string* new_value) const override {
    return false;
  }
  virtual const char* Name() const override { return name_.c_str(); }

 protected:
  std::string name_;
};

// Returns a dummy merge operator with random name.
db::MergeOperator* RandomMergeOperator(Random* rnd);

// A dummy compaction filter which can change its name
class ChanglingCompactionFilter : public storage::CompactionFilter {
 public:
  explicit ChanglingCompactionFilter(const std::string& name)
      : name_(name + "storage::CompactionFilter") {}
  ~ChanglingCompactionFilter() {}

  void SetName(const std::string& name) { name_ = name; }

  bool Filter(int level, const common::Slice& key,
              const common::Slice& existing_value, std::string* new_value,
              bool* value_changed) const override {
    return false;
  }

  const char* Name() const override { return name_.c_str(); }

 private:
  std::string name_;
};

// Returns a dummy compaction filter with a random name.
storage::CompactionFilter* RandomCompactionFilter(Random* rnd);

// A dummy compaction filter factory which can change its name
class ChanglingCompactionFilterFactory
    : public storage::CompactionFilterFactory {
 public:
  explicit ChanglingCompactionFilterFactory(const std::string& name)
      : name_(name + "CompactionFilterFactory") {}
  ~ChanglingCompactionFilterFactory() {}

  void SetName(const std::string& name) { name_ = name; }

  std::unique_ptr<storage::CompactionFilter> CreateCompactionFilter(
      const storage::CompactionFilter::Context& context) override {
    return std::unique_ptr<storage::CompactionFilter>();
  }

  // Returns a name that identifies this compaction filter factory.
  const char* Name() const override { return name_.c_str(); }

 protected:
  std::string name_;
};

common::CompressionType RandomCompressionType(Random* rnd);

void RandomCompressionTypeVector(const size_t count,
                                 std::vector<common::CompressionType>* types,
                                 util::Random* rnd);

storage::CompactionFilterFactory* RandomCompactionFilterFactory(Random* rnd);

const common::SliceTransform* RandomSliceTransform(Random* rnd,
                                                   int pre_defined = -1);

table::TableFactory* RandomTableFactory(Random* rnd, int pre_defined = -1);

std::string RandomName(Random* rnd, const size_t len);

common::Status DestroyDir(Env* env, const std::string& dir);

}  // namespace test
}  // namespace util
}  // namespace xengine
