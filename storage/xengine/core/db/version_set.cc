// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdio.h>
#include <algorithm>
#include <climits>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/internal_stats.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "memory/base_malloc.h"
#include "storage/extent_space_manager.h"
#include "storage/storage_log_entry.h"
#include "storage/storage_logger.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/merging_iterator.h"
#include "table/meta_blocks.h"
#include "table/plain_table_factory.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "memory/mod_info.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "xengine/env.h"
#include "xengine/merge_operator.h"
#include "xengine/write_buffer_manager.h"
#include "xengine/xengine_constants.h"  // MAX_EXTENT_SIZE

using namespace xengine;
using namespace storage;
using namespace cache;
using namespace table;
using namespace util;
using namespace common;
using namespace monitor;

namespace xengine {
namespace db {
namespace {

// Find File in LevelFilesBrief data structure
// Within an index range defined by left and right
int FindFileInRange(const InternalKeyComparator& icmp,
                    const LevelFilesBrief& file_level, const Slice& key,
                    uint32_t left, uint32_t right) {
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FdWithKeyRange& f = file_level.files[mid];
    if (icmp.InternalKeyComparator::Compare(f.largest_key, key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

// Class to help choose the next file to search for the particular key.
// Searches and returns files level by level.
// We can search level-by-level since entries never hop across
// levels. Therefore we are guaranteed that if we find data
// in a smaller level, later levels are irrelevant (unless we
// are MergeInProgress).
class FilePicker {
 public:
  FilePicker(std::vector<FileMetaData*>* files, const Slice& user_key,
             const Slice& ikey, autovector<LevelFilesBrief>* file_levels,
             unsigned int num_levels, FileIndexer* file_indexer,
             const Comparator* user_comparator,
             const InternalKeyComparator* internal_comparator)
      : num_levels_(num_levels),
        curr_level_(static_cast<unsigned int>(-1)),
        returned_file_level_(static_cast<unsigned int>(-1)),
        hit_file_level_(static_cast<unsigned int>(-1)),
        search_left_bound_(0),
        search_right_bound_(FileIndexer::kLevelMaxIndex),
#ifndef NDEBUG
        files_(files),
#endif
        level_files_brief_(file_levels),
        is_hit_file_last_in_level_(false),
        user_key_(user_key),
        ikey_(ikey),
        file_indexer_(file_indexer),
        user_comparator_(user_comparator),
        internal_comparator_(internal_comparator) {
    // Setup member variables to search first level.
    search_ended_ = !PrepareNextLevel();
    if (!search_ended_) {
      // Prefetch Level 0 table data to avoid cache miss if possible.
      for (unsigned int i = 0; i < (*level_files_brief_)[0].num_files; ++i) {
        auto* r = (*level_files_brief_)[0].files[i].fd.table_reader;
        if (r) {
          r->Prepare(ikey);
        }
      }
    }
  }

  int GetCurrentLevel() { return returned_file_level_; }

  FdWithKeyRange* GetNextFile() {
    while (!search_ended_) {  // Loops over different levels.
      while (curr_index_in_curr_level_ < curr_file_level_->num_files) {
        // Loops over all files in current level.
        FdWithKeyRange* f = &curr_file_level_->files[curr_index_in_curr_level_];
        hit_file_level_ = curr_level_;
        is_hit_file_last_in_level_ =
            curr_index_in_curr_level_ == curr_file_level_->num_files - 1;
        int cmp_largest = -1;

        // Do key range filtering of files or/and fractional cascading if:
        // (1) not all the files are in level 0, or
        // (2) there are more than 3 Level 0 files
        // If there are only 3 or less level 0 files in the system, we skip
        // the key range filtering. In this case, more likely, the system is
        // highly tuned to minimize number of tables queried by each query,
        // so it is unlikely that key range filtering is more efficient than
        // querying the files.
        if (num_levels_ > 1 || curr_file_level_->num_files > 3) {
          // Check if key is within a file's range. If search left bound and
          // right bound point to the same find, we are sure key falls in
          // range.
          assert(curr_level_ == 0 ||
                 curr_index_in_curr_level_ == start_index_in_curr_level_ ||
                 user_comparator_->Compare(
                     user_key_, ExtractUserKey(f->smallest_key)) <= 0);

          int cmp_smallest = user_comparator_->Compare(
              user_key_, ExtractUserKey(f->smallest_key));
          if (cmp_smallest >= 0) {
            cmp_largest = user_comparator_->Compare(
                user_key_, ExtractUserKey(f->largest_key));
          }

          // Setup file search bound for the next level based on the
          // comparison results
          if (curr_level_ > 0) {
            file_indexer_->GetNextLevelIndex(
                curr_level_, curr_index_in_curr_level_, cmp_smallest,
                cmp_largest, &search_left_bound_, &search_right_bound_);
          }
          // Key falls out of current file's range
          if (cmp_smallest < 0 || cmp_largest > 0) {
            if (curr_level_ == 0) {
              ++curr_index_in_curr_level_;
              continue;
            } else {
              // Search next level.
              break;
            }
          }
        }
#ifndef NDEBUG
        // Sanity check to make sure that the files are correctly sorted
        if (prev_file_) {
          if (curr_level_ != 0) {
            int comp_sign = internal_comparator_->Compare(
                prev_file_->largest_key, f->smallest_key);
            assert(comp_sign < 0);
          } else {
            // level == 0, the current file cannot be newer than the previous
            // one. Use compressed data structure, has no attribute seqNo
            assert(curr_index_in_curr_level_ > 0);
            assert(
                !NewestFirstBySeqNo(files_[0][curr_index_in_curr_level_],
                                    files_[0][curr_index_in_curr_level_ - 1]));
          }
        }
        prev_file_ = f;
#endif
        returned_file_level_ = curr_level_;
        if (curr_level_ > 0 && cmp_largest < 0) {
          // No more files to search in this level.
          search_ended_ = !PrepareNextLevel();
        } else {
          ++curr_index_in_curr_level_;
        }
        return f;
      }
      // Start searching next level.
      search_ended_ = !PrepareNextLevel();
    }
    // Search ended.
    return nullptr;
  }

  // getter for current file level
  // for GET_HIT_L0, GET_HIT_L1 & GET_HIT_L2_AND_UP counts
  unsigned int GetHitFileLevel() { return hit_file_level_; }

  // Returns true if the most recent "hit file" (i.e., one returned by
  // GetNextFile()) is at the last index in its level.
  bool IsHitFileLastInLevel() { return is_hit_file_last_in_level_; }

 private:
  unsigned int num_levels_;
  unsigned int curr_level_;
  unsigned int returned_file_level_;
  unsigned int hit_file_level_;
  int32_t search_left_bound_;
  int32_t search_right_bound_;
#ifndef NDEBUG
  std::vector<FileMetaData*>* files_;
#endif
  autovector<LevelFilesBrief>* level_files_brief_;
  bool search_ended_;
  bool is_hit_file_last_in_level_;
  LevelFilesBrief* curr_file_level_;
  unsigned int curr_index_in_curr_level_;
  unsigned int start_index_in_curr_level_;
  Slice user_key_;
  Slice ikey_;
  FileIndexer* file_indexer_;
  const Comparator* user_comparator_;
  const InternalKeyComparator* internal_comparator_;
#ifndef NDEBUG
  FdWithKeyRange* prev_file_;
#endif

  // Setup local variables to search next level.
  // Returns false if there are no more levels to search.
  bool PrepareNextLevel() {
    curr_level_++;
    while (curr_level_ < num_levels_) {
      curr_file_level_ = &(*level_files_brief_)[curr_level_];
      if (curr_file_level_->num_files == 0) {
        // When current level is empty, the search bound generated from upper
        // level must be [0, -1] or [0, FileIndexer::kLevelMaxIndex] if it is
        // also empty.
        assert(search_left_bound_ == 0);
        assert(search_right_bound_ == -1 ||
               search_right_bound_ == FileIndexer::kLevelMaxIndex);
        // Since current level is empty, it will need to search all files in
        // the next level
        search_left_bound_ = 0;
        search_right_bound_ = FileIndexer::kLevelMaxIndex;
        curr_level_++;
        continue;
      }

      // Some files may overlap each other. We find
      // all files that overlap user_key and process them in order from
      // newest to oldest. In the context of merge-operator, this can occur at
      // any level. Otherwise, it only occurs at Level-0 (since Put/Deletes
      // are always compacted into a single entry).
      int32_t start_index;
      if (curr_level_ == 0) {
        // On Level-0, we read through all files to check for overlap.
        start_index = 0;
      } else {
        // On Level-n (n>=1), files are sorted. Binary search to find the
        // earliest file whose largest key >= ikey. Search left bound and
        // right bound are used to narrow the range.
        if (search_left_bound_ == search_right_bound_) {
          start_index = search_left_bound_;
        } else if (search_left_bound_ < search_right_bound_) {
          if (search_right_bound_ == FileIndexer::kLevelMaxIndex) {
            search_right_bound_ =
                static_cast<int32_t>(curr_file_level_->num_files) - 1;
          }
          start_index =
              FindFileInRange(*internal_comparator_, *curr_file_level_, ikey_,
                              static_cast<uint32_t>(search_left_bound_),
                              static_cast<uint32_t>(search_right_bound_));
        } else {
          // search_left_bound > search_right_bound, key does not exist in
          // this level. Since no comparison is done in this level, it will
          // need to search all files in the next level.
          search_left_bound_ = 0;
          search_right_bound_ = FileIndexer::kLevelMaxIndex;
          curr_level_++;
          continue;
        }
      }
      start_index_in_curr_level_ = start_index;
      curr_index_in_curr_level_ = start_index;
#ifndef NDEBUG
      prev_file_ = nullptr;
#endif
      return true;
    }
    // curr_level_ = num_levels_. So, no more levels to search.
    return false;
  }
};
}  // anonymous namespace

//This function moved from lagacy file db/compaction.cc is merely used in this file.
uint64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size() && nullptr != files[i]; i++) {
    sum += files[i]->fd.GetFileSize();
  }
  return sum;
}

int FindFile(const InternalKeyComparator& icmp,
             const LevelFilesBrief& file_level, const Slice& key) {
  return FindFileInRange(icmp, file_level, key, 0,
                         static_cast<uint32_t>(file_level.num_files));
}

void DoGenerateLevelFilesBrief(LevelFilesBrief* file_level,
                               const std::vector<FileMetaData*>& files,
                               Arena* arena) {
  assert(file_level);
  assert(arena);

  size_t num = files.size();
  file_level->num_files = num;
  char* mem = arena->AllocateAligned(num * sizeof(FdWithKeyRange));
  file_level->files = new (mem) FdWithKeyRange[num];

  for (size_t i = 0; i < num; i++) {
    Slice smallest_key = files[i]->smallest.Encode();
    Slice largest_key = files[i]->largest.Encode();

    // Copy key slice to sequential memory
    size_t smallest_size = smallest_key.size();
    size_t largest_size = largest_key.size();
    mem = arena->AllocateAligned(smallest_size + largest_size);
    memcpy(mem, smallest_key.data(), smallest_size);
    memcpy(mem + smallest_size, largest_key.data(), largest_size);

    FdWithKeyRange& f = file_level->files[i];
    f.fd = files[i]->fd;
    f.smallest_key = Slice(mem, smallest_size);
    f.largest_key = Slice(mem + smallest_size, largest_size);
  }
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FdWithKeyRange* f) {
  // nullptr user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, ExtractUserKey(f->largest_key)) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FdWithKeyRange* f) {
  // nullptr user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, ExtractUserKey(f->smallest_key)) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const LevelFilesBrief& file_level,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < file_level.num_files; i++) {
      const FdWithKeyRange* f = &(file_level.files[i]);
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small;
    small.SetMaxPossibleForUserKey(*smallest_user_key);
    index = FindFile(icmp, file_level, small.Encode());
  }

  if (index >= file_level.num_files) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, &file_level.files[index]);
}

namespace {

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class LevelFileNumIterator : public InternalIterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const LevelFilesBrief* flevel)
      : icmp_(icmp),
        flevel_(flevel),
        index_(static_cast<uint32_t>(flevel->num_files)),
        current_value_(0, 0, 0) {  // Marks as invalid
  }
  virtual bool Valid() const override { return index_ < flevel_->num_files; }
  virtual void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flevel_, target);
  }
  virtual void SeekForPrev(const Slice& target) override {
    SeekForPrevImpl(target, &icmp_);
  }

  virtual void SeekToFirst() override { index_ = 0; }
  virtual void SeekToLast() override {
    index_ = (flevel_->num_files == 0)
                 ? 0
                 : static_cast<uint32_t>(flevel_->num_files) - 1;
  }
  virtual void Next() override {
    assert(Valid());
    index_++;
  }
  virtual void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = static_cast<uint32_t>(flevel_->num_files);  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const override {
    assert(Valid());
    return flevel_->files[index_].largest_key;
  }
  Slice value() const override {
    assert(Valid());

    auto file_meta = flevel_->files[index_];
    current_value_ = file_meta.fd;
    return Slice(reinterpret_cast<const char*>(&current_value_),
                 sizeof(FileDescriptor));
  }
  virtual Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const LevelFilesBrief* flevel_;
  uint32_t index_;
  mutable FileDescriptor current_value_;
};

class LevelFileIteratorState : public TwoLevelIteratorState {
 public:
  // @param skip_filters Disables loading/accessing the filter block
  LevelFileIteratorState(TableCache* table_cache,
                         const ReadOptions& read_options,
                         const EnvOptions& env_options,
                         const InternalKeyComparator& icomparator,
                         HistogramImpl* file_read_hist, bool for_compaction,
                         bool prefix_enabled, bool skip_filters, int level,
                         RangeDelAggregator* range_del_agg)
      : TwoLevelIteratorState(prefix_enabled),
        table_cache_(table_cache),
        read_options_(read_options),
        env_options_(env_options),
        icomparator_(icomparator),
        file_read_hist_(file_read_hist),
        for_compaction_(for_compaction),
        skip_filters_(skip_filters),
        level_(level),
        range_del_agg_(range_del_agg) {}

  InternalIterator* NewSecondaryIterator(const Slice& meta_handle,
      uint64_t* add_blocks = nullptr) override {
    if (meta_handle.size() != sizeof(FileDescriptor)) {
      return NewErrorInternalIterator(
          Status::Corruption("FileReader invoked with unexpected value"));
    }
    const FileDescriptor* fd =
        reinterpret_cast<const FileDescriptor*>(meta_handle.data());
    return table_cache_->NewIterator(
        read_options_, env_options_, icomparator_, *fd, range_del_agg_,
        nullptr /* don't need reference to table */, file_read_hist_,
        for_compaction_, nullptr /* arena */, skip_filters_, level_);
  }

  bool PrefixMayMatch(const Slice& internal_key) override { return true; }

 private:
  TableCache* table_cache_;
  const ReadOptions read_options_;
  const EnvOptions& env_options_;
  const InternalKeyComparator& icomparator_;
  HistogramImpl* file_read_hist_;
  bool for_compaction_;
  bool skip_filters_;
  int level_;
  RangeDelAggregator* range_del_agg_;
};

// A wrapper of version builder which references the current version in
// constructor and unref it in the destructor.
// Both of the constructor and destructor need to be called inside DB Mutex.
/*
class BaseReferencedVersionBuilder {
 public:
  explicit BaseReferencedVersionBuilder(ColumnFamilyData* cfd)
      : version_builder_(new VersionBuilder(
            cfd->current()->version_set()->env_options(), cfd->table_cache())),
        version_(cfd->current()) {
    version_->Ref();
  }
  ~BaseReferencedVersionBuilder() {
    delete version_builder_;
    version_->Unref();
  }
  VersionBuilder* version_builder() { return version_builder_; }

 private:
  VersionBuilder* version_builder_;
  Version* version_;
};
*/
}  // anonymous namespace

namespace {

// used to sort files by size
struct Fsize {
  size_t index;
  FileMetaData* file;
};

// Compator that is used to sort files based on their size
// In normal mode: descending size
#if 0
bool CompareCompensatedSizeDescending(const Fsize& first, const Fsize& second) {
  return (first.file->compensated_file_size >
          second.file->compensated_file_size);
}
#endif
}  // anonymous namespace

namespace {

#if 0
// Sort `temp` based on ratio of overlapping size over file size
void SortFileByOverlappingRatio(
    const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files,
    const std::vector<FileMetaData*>& next_level_files,
    std::vector<Fsize>* temp) {
  std::unordered_map<uint64_t, uint64_t> file_to_order;
  auto next_level_it = next_level_files.begin();

  for (auto& file : files) {
    uint64_t overlapping_bytes = 0;
    // Skip files in next level that is smaller than current file
    while (next_level_it != next_level_files.end() &&
           icmp.Compare((*next_level_it)->largest, file->smallest) < 0) {
      next_level_it++;
    }

    while (next_level_it != next_level_files.end() &&
           icmp.Compare((*next_level_it)->smallest, file->largest) < 0) {
      overlapping_bytes += (*next_level_it)->fd.file_size;

      if (icmp.Compare((*next_level_it)->largest, file->largest) > 0) {
        // next level file cross large boundary of current file.
        break;
      }
      next_level_it++;
    }

    assert(file->fd.file_size != 0);
    file_to_order[file->fd.GetNumber()] =
        overlapping_bytes * 1024u / file->fd.file_size;
  }

  std::sort(temp->begin(), temp->end(),
            [&](const Fsize& f1, const Fsize& f2) -> bool {
              return file_to_order[f1.file->fd.GetNumber()] <
                     file_to_order[f2.file->fd.GetNumber()];
            });
}
#endif
}  // namespace

/*
void Version::Ref() { ++refs_; }

bool Version::Unref() {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
    return true;
  }
  return false;
}

std::string Version::DebugString(bool hex) const {
  std::string r;
  return r;
}
*/

int AllSubTable::DUMMY = 0;
void *const AllSubTable::kAllSubtableInUse = &AllSubTable::DUMMY;
void *const AllSubTable::kAllSubtableObsolete = nullptr;
AllSubTable::AllSubTable()
    : version_number_(0),
      refs_(0),
      all_sub_table_mutex_(nullptr),
      sub_table_map_()
{
}

AllSubTable::AllSubTable(SubTableMap &sub_table_map, std::mutex *mutex)
    : version_number_(0),
      refs_(0),
      all_sub_table_mutex_(mutex),
      sub_table_map_(sub_table_map)
{
}

AllSubTable::~AllSubTable()
{
}
void AllSubTable::reset()
{
  sub_table_map_.clear();
}
int AllSubTable::add_sub_table(int64_t index_id, SubTable *sub_table)
{
  int ret = Status::kOk;

  if (index_id < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret));
  } else {
    if (!(sub_table_map_.emplace(index_id, sub_table).second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "fail to emplace subtable", K(ret), K(index_id));
    } else {
      XENGINE_LOG(INFO, "map add sub_table", K(index_id));
    }
  }

  return ret;
}
int AllSubTable::remove_sub_table(int64_t index_id)
{
  int ret = Status::kOk;

  if (index_id < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(index_id));
  } else {
    if (1 != sub_table_map_.erase(index_id)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "fail to remove subtable", K(ret), K(index_id));
    }
  }

  return ret;
}

int AllSubTable::get_sub_table(int64_t index_id, SubTable *&sub_table)
{
  int ret = Status::kOk;

  if (index_id < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(index_id));
  } else {
    std::lock_guard<std::mutex> lock_guard(*all_sub_table_mutex_);
    auto iter = sub_table_map_.find(index_id);
    if (sub_table_map_.end() == iter) {
      XENGINE_LOG(DEBUG, "sub table not exist", K(index_id));
    } else if (nullptr == (sub_table = iter->second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "subtable must not nullptr", K(index_id));
    }
  }

  return ret;
}


AllSubTableGuard::AllSubTableGuard(GlobalContext *global_ctx) : global_ctx_(global_ctx), all_sub_table_(nullptr)
{
  global_ctx_->acquire_thread_local_all_sub_table(all_sub_table_);
  assert(nullptr != all_sub_table_);
}

AllSubTableGuard::~AllSubTableGuard()
{
  assert(nullptr != all_sub_table_);
  global_ctx_->release_thread_local_all_sub_table(all_sub_table_);
}
  
// this is used to batch writes to the manifest file
struct VersionSet::ManifestWriter {
  Status status;
  bool done;
  InstrumentedCondVar cv;
  ColumnFamilyData* cfd;
  //const autovector<VersionEdit*>& edit_list;

  //explicit ManifestWriter(InstrumentedMutex* mu, ColumnFamilyData* _cfd,
  //                        const autovector<VersionEdit*>& e)
  //    : done(false), cv(mu), cfd(_cfd), edit_list(e) {}
};

VersionSet::VersionSet(const std::string& dbname,
                       const ImmutableDBOptions* db_options,
                       const EnvOptions& storage_options, Cache* table_cache,
                       WriteBufferManager* write_buffer_manager,
                       WriteController* write_controller)
    : is_inited_(false),
      global_ctx_(nullptr),
      column_family_set_(nullptr),
      env_(db_options->env),
      dbname_(dbname),
      db_options_(db_options),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      pending_manifest_file_number_(0),
      last_sequence_(0),
      last_allocated_sequence_(0),
      prev_log_number_(0),
      current_version_number_(0),
      manifest_file_size_(0),
      env_options_(storage_options),
      env_options_compactions_(
          env_->OptimizeForCompactionTableRead(env_options_, *db_options_)),
      meta_log_number_(1)/* storage meta log number start from 1 */,
      storage_logger_(nullptr),
      meta_snapshots_(),
      last_manifest_file_size_(0),
      last_wal_file_size_(0),
      checkpoint_file_number_(0),
      purge_checkpoint_file_number_(0),
      purge_manifest_file_number_(0),
      file_number_(0),
      log_number_(0)
{
#ifndef NDEBUG
  write_checkpoint_failed_ = false;
#endif
}

void CloseTables(void* ptr, size_t) {
  TableReader* table_reader = reinterpret_cast<TableReader*>(ptr);
  table_reader->Close();
}

VersionSet::~VersionSet() {
  // we need to delete column_family_set_ because its destructor depends on
  // VersionSet
  if (is_inited_) {
    global_ctx_->cache_->ApplyToAllCacheEntries(&CloseTables, false);
    column_family_set_.reset();
    for (auto file : obsolete_files_) {
      delete file;
    }
    obsolete_files_.clear();
  }
  if (descriptor_log_) {
    descriptor_log_->delete_file_writer();
  }
}

int VersionSet::init(GlobalContext *global_ctx)
{
  int ret = Status::kOk;
  ColumnFamilySet *column_family_set = nullptr;

  if (is_inited_) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "VersionSet has been inited", K(ret));
  } else if (IS_NULL(global_ctx) || UNLIKELY(!global_ctx->is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(global_ctx));
  } else if (IS_NULL(column_family_set = MOD_NEW_OBJECT(memory::ModId::kVersionSet, ColumnFamilySet, global_ctx))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for column family set", K(ret));
  } else {
    global_ctx_ = global_ctx;
    column_family_set_.reset(column_family_set);
    is_inited_ = true;
  }
  return ret;
}

// must called after Revover for correct filenumaber
Status VersionSet::create_descriptor_log_writer() {
//  unique_ptr<WritableFile> descriptor_file;
  WritableFile *descriptor_file = nullptr;
  EnvOptions opt_env_opts = env_->OptimizeForManifestWrite(env_options_);
  Status s;
  // create by versionset for checkpoint
  pending_manifest_file_number_ = NewFileNumber();
  __XENGINE_LOG(INFO, "Creating manifest %" PRIu64 "\n", pending_manifest_file_number_);
  s = NewWritableFile(
      env_, DescriptorFileName(dbname_, pending_manifest_file_number_),
      descriptor_file, opt_env_opts);

  if (s.ok()) {
    descriptor_file->SetPreallocationBlockSize(
        db_options_->manifest_preallocation_size);

//    unique_ptr<util::ConcurrentDirectFileWriter> file_writer(
//        new util::ConcurrentDirectFileWriter(descriptor_file,
//                                             opt_env_opts));
    util::ConcurrentDirectFileWriter *file_writer =
        MOD_NEW_OBJECT(memory::ModId::kDefaultMod, ConcurrentDirectFileWriter, descriptor_file, opt_env_opts);
    s = file_writer->init_multi_buffer();
    if (s.ok()) {
//      descriptor_log_.reset(new log::Writer(file_writer, 0, false));
      descriptor_log_.reset(MOD_NEW_OBJECT(memory::ModId::kDefaultMod, log::Writer, file_writer, 0, false));
      if (nullptr == descriptor_log_) {
        __XENGINE_LOG(ERROR, "Create manifest file failed %d", Status::kMemoryLimit);
        s = Status(Status::kMemoryLimit);
      }
    } else {
      __XENGINE_LOG(ERROR, "init multi log buffer failed for ManifestWrite");
    }
  }

  return s;
}

// recycle storage manager meta with out lock
int VersionSet::recycle_storage_manager_meta(
  std::unordered_map<int32_t, SequenceNumber>* all,
  InstrumentedMutex &mu) const {
  ColumnFamilyData *cfd = nullptr;
  Status s;
  Arena arena;
  int ret = Status::kOk;
  for (std::pair<int32_t, SequenceNumber> min : *all) {
    cfd = column_family_set_->GetColumnFamily(min.first); 
    if (nullptr == cfd) {
      __XENGINE_LOG(ERROR, "Can't find the column family to recycle");
      return Status::kAborted;
    }
    mu.Lock();
    if (cfd->IsDropped() || cfd->is_bg_stopped()) {
      mu.Unlock();
      continue;
    }
    // avoid delete the cfd
    cfd->Ref();
    mu.Unlock();

    if (ret != Status::kOk) {
      __XENGINE_LOG(ERROR, "Recycle large object failed cfd id %d, error %d", min.first, ret);
    }

    if ((min.second - cfd->get_bg_recycled_version()) > 
                                                  META_VERSION_INTERVAL) {
      if (!s.ok()) {
        __XENGINE_LOG(ERROR, "Recycle useless extents failed cfd id %d, error %d", min.first, s.code());
        ret = s.code();
      } else {
        cfd->set_bg_recycled_version(min.second);
        // call recycle delete entry after recycle extent
        //ret = cfd->get_storage_manager()->recycle_delete_entries(min.second);
        if (ret != Status::kOk) {
          __XENGINE_LOG(ERROR, "Recycle delete meta entries failed cfd id %d, error %d", min.first, ret);
        }
      }
    }
    
    mu.Lock();
    if (cfd->Unref()) {
      delete cfd;
    }
    mu.Unlock();
  }
  
  return ret;
}

// call all the storage manager to do checkpoint
/*
Status VersionSet::write_storage_manager_checkpoint(
                                            InstrumentedMutex *mu,
                                            uint64_t file_number) {
  std::string checkpoint_path = StorageManager::checkpoint_name(dbname_, 
                                                           file_number);
  EnvOptions opt_env_opts = env_options_;
  opt_env_opts.use_mmap_writes = false;
  opt_env_opts.use_direct_writes = true;
  std::unique_ptr<WritableFile> checkpoint_write_;
  Status s = NewWritableFile(env_, checkpoint_path,
                      &checkpoint_write_, opt_env_opts);
  if (!s.ok()) {
    return s;
  }
  
  int64_t start = 0;
  const Snapshot *sn = nullptr;
  int ret = Status::kOk;
  // step two: checkpoint all the storage managers
  for (ColumnFamilyData *cfd : *column_family_set_.get()) {
    mu->Lock();
    if (cfd->IsDropped()) {
      mu->Unlock();
      continue;
    }
    cfd->Ref(); // prevent delete the cfd
    mu->Unlock();
    cfd->get_storage_manager()->set_checkpoint_writer(
                                   checkpoint_write_.get(), start);
    sn = cfd->get_meta_snapshot();
    if (nullptr == sn) {
      ret = Status::kCorruption;
      XENGINE_LOG(ERROR, "Get snapshot failed when create checkpoint", K(ret));
      mu->Lock();
      if (cfd->Unref()) {
        delete cfd;
      }
      mu->Unlock();
      return Status::Corruption();
    } 
    s = cfd->get_storage_manager()->write_checkpoint(sn, cfd->GetID(), &start); 
    cfd->release_meta_snapshot(sn);
    mu->Lock();
    if (cfd->Unref()) {
      delete cfd;
    }
    mu->Unlock();
    if (!s.ok()) {
      ret = s.code();
      XENGINE_LOG(ERROR, "Write checkpoint of column family failed", K(ret));
      return s;
    }
  }

  if (!s.ok()) {
    __XENGINE_LOG(ERROR, "Update CURRENT_CHECKPOINT file failed %d\n", s.code());
    return s;
  }

  // at last sync the extent space manager
  checkpoint_file_number_ = file_number;
  return column_family_set_->get_extent_space_manager()->sync();
}
*/

// using a empty change info to trigger a manual checkpoit
int VersionSet::do_manual_checkpoint(InstrumentedMutex *mu, bool update_current_file) {
  assert(false);
  int ret = Status::kOk;
  mu->AssertHeld();
  // use default column family
  ColumnFamilyData *cfd = column_family_set_->GetColumnFamily(0); 
  if (nullptr == cfd) {
    ret = Status::kCorruption;
    XENGINE_LOG(ERROR, "Can't get the default cfd to write checkpoint", K(ret));
    return ret;
  }
  
  //VersionEdit ve;
  ChangeInfo info; // empty info
  //ve.set_change_info(cfd->GetID(), &info, file_number);
  //ve.SetNextFile(file_number);

  //Status s = LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(), &ve,
  //                       mu, column_family_set_->get_db_dir(), 
  //                       true/* create checkpoint */, nullptr, update_current_file);
  //if (!s.ok()) {
  //  ret = s.code();
  //  XENGINE_LOG(ERROR, "Create manual checkpoint failed.", K(ret));
  //}
 
  return ret;
}
#if 0
// write one extent id to extent.inc file 
// then hot backup tool will parse it and
// backup the extent of the id
int backup_one_extent(int64_t extent_id, int dest_fd, 
                      char *buf, int32_t block_size, 
                      int32_t &offset, int64_t &file_offset) {
  Status s;
  int ret = Status::kOk;
  *(int64_t*)(buf + offset) = extent_id;
  offset += sizeof(int64_t);
  // need write to file
  if (offset >= block_size) {
    s = unintr_pwrite(dest_fd, buf, block_size, file_offset, false);
    if (!s.ok()) {
      ret = s.code();
      XENGINE_LOG(ERROR, "Write extent id to backup file failed", K(ret));
    }
    file_offset += block_size;
    memset(buf, 0, block_size);
    offset = 0;
  }
  
  return ret;
}
#endif
// stream the extens loged in manifest log from start to end point
int VersionSet::stream_log_extents(
    std::function<int(const char*, int, int64_t, int)> *stream_extent,
                                   int64_t start, int64_t end, int dest_fd) {
  int ret = Status::kOk;
  /*
  uint64_t manifest_number = 0; 
  //FAIL_RETURN_MSG_NEW(StorageManager::parse_current_file(*const_cast<Env*>(env_),
  //                 dbname_, manifest_number), "Current file corrupt %d\n", ret);
  std::unique_ptr<SequentialFile> manifest_file;
  Status s;
  // init the reader
  std::string manifest_file_path =
      DescriptorFileName(dbname_, manifest_number);
  s = env_->NewSequentialFile(manifest_file_path,
                              &manifest_file, env_options_);
  if (!s.ok()) {
    __XENGINE_LOG(FATAL, "Open the manifest log file failed %s",
                  s.ToString().c_str());
    return s.code();
  }

  std::unique_ptr<SequentialFileReader> manifest_file_reader(
      new SequentialFileReader(std::move(manifest_file)));
  if (nullptr == manifest_file_reader) {
    __XENGINE_LOG(FATAL, "Create manifest file reader failed");
    return Status::kAborted;
  }
  LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(NULL, std::move(manifest_file_reader), &reporter,
                     true*checksum, start initial_offset, 0);
  Slice record;
  std::string scratch;
  ChangeInfo info;
  size_t i = 0;
  //VersionEdit ve;
  
  std::unique_ptr<char[]> buf(new char[EXTENT_ID_BLOCK]);
  memset(buf.get(), 0, EXTENT_ID_BLOCK);
  int64_t extent_id = -1;
  int64_t file_offset = 0;
  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    // don't process records pass the end point 
    if (reader.LastRecordOffset() >= static_cast<uint64_t>(end)) {
      break;
    }
    //s = ve.DecodeFrom(record, &info);
    if (!s.ok()) {
      __XENGINE_LOG(ERROR, "Decode the storage manager meta log failed %s",
                    s.ToString().c_str());
      return s.code();
    }
    //if (ve.get_meta_log_number() <= 0) {
    //  continue; //not meta log 
    //}

    // backup extent
    for (i = 0; i < info.extent_meta_.size(); i++) {
      XENGINE_LOG(INFO, "backup extent id ", K(info.extent_meta_[i].extent_id_));
      extent_id = info.extent_meta_[i].extent_id_.id();
      s = unintr_pwrite(dest_fd, reinterpret_cast<char*>(&extent_id), 
                        sizeof(int64_t), file_offset, false);
      if (!s.ok()) {
        ret = s.code();
        XENGINE_LOG(ERROR, "Write extent id to backup file failed", K(ret));
        return ret;
      }
      file_offset += sizeof(int64_t);
    }
    // backup large object
    for (auto& lobj : info.large_objects_) {
      for (auto& extent : lobj.value_.oob_extents_) {
        XENGINE_LOG(INFO, "backup large object extent id ", K(extent));
        extent_id = extent.id();
        s = unintr_pwrite(dest_fd, reinterpret_cast<char*>(&extent_id), 
                          sizeof(int64_t), file_offset, false);
        if (!s.ok()) {
          ret = s.code();
          XENGINE_LOG(ERROR, "Write extent id to backup file failed", K(ret));
          return ret;
        }
        file_offset += sizeof(int64_t);
      }
    }
  }
*/
   
  return ret;
}

int VersionSet::recover_extent_space_manager()
{
  int ret = Status::kOk;

  if (FAILED(global_ctx_->extent_space_mgr_->open_all_data_file())) {
    XENGINE_LOG(ERROR, "fail to open all data file", K(ret));
  }

  if (SUCCED(ret)) {
  SubTable* sub_table = nullptr;
  SubTableMap& all_sub_tables = global_ctx_->all_sub_table_->sub_table_map_;
  for (auto iter = all_sub_tables.begin();
       Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
    if (nullptr == (sub_table = iter->second)) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
    } else if (sub_table->IsDropped()) {
      // do nothing
    } else if (FAILED(sub_table->recover_extent_space())) {
      XENGINE_LOG(WARN, "fail to recover extent space", K(ret), "index_id",
                  sub_table->GetID());
    }
  }
  }

  if (SUCCED(ret)) {
    if (FAILED(global_ctx_->extent_space_mgr_->rebuild())) {
      XENGINE_LOG(WARN, "fail to rebuild extent_space_mgr", K(ret));
    } else {
      XENGINE_LOG(INFO, "success to rebuild extent_space_mgr");
    }
  }

  return ret;
}

/*
Status VersionSet::ListColumnFamilies(std::vector<std::string>* column_families,
                                      const std::string& dbname, Env* env) {
  // these are just for performance reasons, not correcntes,
  // so we're fine using the defaults
  EnvOptions soptions;
  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env, CurrentFileName(dbname), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);
  std::string dscname;
  FileType type;
  uint64_t manifest_file = 0;
  if (ParseFileName(current, &manifest_file, &type)) {
    dscname = dbname + "/" + current;
  } else { // try new format
    uint64_t checkpoint_file = 0;
    uint64_t log_number = 0;
    int ret = Status::kOk;
    if ((ret = StorageManager::parse_current_file_new(*env, dbname,
                                                    manifest_file, checkpoint_file,
                                                    log_number)) != Status::kOk) {
      return Status::Corruption("CURRENT file corrupted");
    }
    dscname = DescriptorFileName(dbname, manifest_file);
  }

  unique_ptr<SequentialFileReader> file_reader;
  {
    unique_ptr<SequentialFile> file;
    s = env->NewSequentialFile(dscname, &file, soptions);
    if (!s.ok()) {
      return s;
    }
    file_reader.reset(new SequentialFileReader(std::move(file)));
  }

  std::map<uint32_t, std::string> column_family_names;
  // default column family is always implicitly there
  column_family_names.insert({0, kDefaultColumnFamilyName});
  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(NULL, std::move(file_reader), &reporter, true checksum,
                     0 initial_offset, 0);
  Slice record;
  std::string scratch;
  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }
    if (edit.is_column_family_add_) {
      if (column_family_names.find(edit.column_family_) !=
          column_family_names.end()) {
        s = Status::Corruption("Manifest adding the same column family twice");
        break;
      }
      column_family_names.insert(
          {edit.column_family_, edit.column_family_name_});
    } else if (edit.is_column_family_drop_) {
      if (column_family_names.find(edit.column_family_) ==
          column_family_names.end()) {
        s = Status::Corruption(
            "Manifest - dropping non-existing column family");
        break;
      }
      column_family_names.erase(edit.column_family_);
    }
  }

  column_families->clear();
  if (s.ok()) {
    for (const auto& iter : column_family_names) {
      column_families->push_back(iter.second);
    }
  }

  return s;
}
*/

void VersionSet::MarkFileNumberUsedDuringRecovery(uint64_t number) {
  // only called during recovery which is single threaded, so this works because
  // there can't be concurrent calls
  if (next_file_number_.load(std::memory_order_relaxed) <= number) {
    next_file_number_.store(number + 1, std::memory_order_relaxed);
  }
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // WARNING: This method doesn't hold a mutex!!

  // This is done without DB mutex lock held, but only within single-threaded
  // LogAndApply. Column family manipulations can only happen within LogAndApply
  // (the same single thread), so we're safe to iterate.
  for (auto cfd : *column_family_set_) {
    if (cfd->IsDropped()) {
      continue;
    }
    {
      // Store column family info
      //VersionEdit edit;
      if (cfd->GetID() != 0) {
        // default column family is always there,
        // no need to explicitly write it
        //edit.AddColumnFamily(cfd->GetName());
        //edit.SetColumnFamily(cfd->GetID());
      }
      //edit.SetComparatorName(
      //    cfd->internal_comparator().user_comparator()->Name());
      //std::string record;
      //if (!edit.EncodeTo(&record)) {
      //  return Status::Corruption("Unable to Encode VersionEdit:" +
      //                            edit.DebugString(true));
      //}
      //Status s = log->AddRecord(record);
      //if (!s.ok()) {
      //  return s;
      //}
    }

    {
      // Save files
      //VersionEdit edit;
      //edit.SetColumnFamily(cfd->GetID());

      //edit.SetLogNumber(cfd->GetLogNumber());
      //std::string record;
      //if (!edit.EncodeTo(&record)) {
      //  return Status::Corruption("Unable to Encode VersionEdit:" +
      //                            edit.DebugString(true));
      //}
      //Status s = log->AddRecord(record);
      //if (!s.ok()) {
      //  return s;
      //}
    }
  }

  return Status::OK();
}

// TODO(aekmekji): in CompactionJob::GenSubcompactionBoundaries(), this
// function is called repeatedly with consecutive pairs of slices. For example
// if the slice list is [a, b, c, d] this function is called with arguments
// (a,b) then (b,c) then (c,d). Knowing this, an optimization is possible where
// we avoid doing binary search for the keys b and c twice and instead somehow
// maintain state of where they first appear in the files.
uint64_t VersionSet::ApproximateSize(ColumnFamilyData* cfd,
                           const db::Snapshot *sn,
                           const common::Slice& start,
                           const common::Slice& end, int start_level,
                           int end_level,
                           int64_t estimate_cost_depth) {
  // pre-condition
  assert(cfd->internal_comparator().Compare(start, end) <= 0);
  assert(start_level <= end_level);

  return cfd->get_storage_manager()->approximate_size(cfd, start, end,
                                            start_level, end_level, sn, estimate_cost_depth);
}

/*
uint64_t VersionSet::ApproximateSizeLevel0(Version* v,
                                           const LevelFilesBrief& files_brief,
                                           const Slice& key_start,
                                           const Slice& key_end) {
  // level 0 files are not in sorted order, we need to iterate through
  // the list to compute the total bytes that require scanning
  uint64_t size = 0;
  for (size_t i = 0; i < files_brief.num_files; i++) {
    const uint64_t start = ApproximateSize(v, files_brief.files[i], key_start);
    const uint64_t end = ApproximateSize(v, files_brief.files[i], key_end);
    assert(end >= start);
    size += end - start;
  }
  return size;
}

uint64_t VersionSet::ApproximateSize(Version* v, const FdWithKeyRange& f,
                                     const Slice& key) {
  // pre-condition
  assert(v);

  uint64_t result = 0;
  if (v->cfd_->internal_comparator().Compare(f.largest_key, key) <= 0) {
    // Entire file is before "key", so just add the file size
    result = f.fd.GetFileSize();
  } else if (v->cfd_->internal_comparator().Compare(f.smallest_key, key) > 0) {
    // Entire file is after "key", so ignore
    result = 0;
  } else {
    // "key" falls in the range for this table.  Add the
    // approximate offset of "key" within the table.
    TableReader* table_reader_ptr;
    InternalIterator* iter = v->cfd_->table_cache()->NewIterator(
        ReadOptions(), env_options_, v->cfd_->internal_comparator(), f.fd,
        nullptr range_del_agg , &table_reader_ptr);
    if (table_reader_ptr != nullptr) {
      result = table_reader_ptr->ApproximateOffsetOf(key);
    }
    delete iter;
  }
  return result;
}
*/

// return the extent meta data
void VersionSet::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata,
                                      InstrumentedMutex *mu) {
  //TODO:yuanfeng
  /*
  ExtentId extent_id;
  Arena arena;
  std::unique_ptr<InternalIterator, memory::ptr_destruct<InternalIterator>> iter;
  ReadOptions read_options;

  for (auto cfd : *column_family_set_) {
    if (cfd->IsDropped()) {
      continue;
    }
    const Snapshot *sn = cfd->get_meta_snapshot();
    if (nullptr == sn) {
      continue;
    }

    StorageManager* storage_manager = cfd->get_storage_manager();
    for (int32_t level = 0; level < storage::MAX_TIER_COUNT; level++) {
      iter.reset(storage_manager->get_single_level_iterator(read_options, &arena, sn, level));
      if (nullptr == iter) {
        continue;
      }
      iter->SeekToFirst();
      while (iter->Valid()) {
        extent_id = storage_manager->get_extent_id_from_iterator(iter.get(), level);
        const ExtentMeta *extentmeta = storage_manager->get_extent_meta(extent_id);
        assert(extentmeta != nullptr);
        LiveFileMetaData filemetadata;
        filemetadata.column_family_name = cfd->GetName();
        filemetadata.db_path = db_options_->db_paths[0].path;
        filemetadata.name = MakeTableFileName("", extent_id.file_number);
        filemetadata.level = level;
        filemetadata.size = MAX_EXTENT_SIZE;
        filemetadata.smallestkey =
            extentmeta->smallest_key_.user_key().ToString();
        filemetadata.largestkey =
            extentmeta->largest_key_.user_key().ToString();
        filemetadata.smallest_seqno = extentmeta->smallest_seqno_;
        filemetadata.largest_seqno = extentmeta->largest_seqno_;
        metadata->push_back(filemetadata);
        iter->Next();
      }
    }
    cfd->release_meta_snapshot(sn, mu);
  }
  */
}

void VersionSet::GetObsoleteFiles(std::vector<FileMetaData*>* files,
                                  std::vector<std::string>* manifest_filenames,
                                  uint64_t min_pending_output) {
  assert(manifest_filenames->empty());
  obsolete_manifests_.swap(*manifest_filenames);
  std::vector<FileMetaData*> pending_files;
  for (auto f : obsolete_files_) {
    if (f->fd.GetNumber() < min_pending_output) {
      files->push_back(f);
    } else {
      pending_files.push_back(f);
    }
  }
  obsolete_files_.swap(pending_files);
}

/*
ColumnFamilyData* VersionSet::CreateColumnFamily(
    const ColumnFamilyOptions& cf_options, VersionEdit* edit) {
  assert(edit->is_column_family_add_);

  Version* dummy_versions = new Version(nullptr, this);
  // Ref() dummy version once so that later we can call Unref() to delete it
  // by avoiding calling "delete" explicitly (~Version is private)
  dummy_versions->Ref();
  auto new_cfd = column_family_set_->CreateColumnFamily(
      edit->column_family_name_, edit->column_family_, dummy_versions,
      cf_options);

  Version* v = new Version(new_cfd, this, current_version_number_++);

  AppendVersion(new_cfd, v);
  // GetLatestMutableCFOptions() is safe here without mutex since the
  // cfd is not available to client
  new_cfd->CreateNewMemtable(*new_cfd->GetLatestMutableCFOptions(),
                             LastSequence());
  new_cfd->SetLogNumber(edit->log_number_);
  return new_cfd;
}
*/

/*
uint64_t VersionSet::GetNumLiveVersions(Version* dummy_versions) {
  uint64_t count = 0;
  for (Version* v = dummy_versions->next_; v != dummy_versions; v = v->next_) {
    count++;
  }
  return count;
}
*/

// aquire all column family snapshot under global read lock
// avoid extent reuse after release the lock
int VersionSet::create_backup_snapshot(MetaSnapshotMap &meta_snapshots)
{
  int ret = Status::kOk;
  const Snapshot *sn = nullptr;
  AllSubTable *all_sub_table = nullptr;
  SubTable* sub_table = nullptr;

  if (FAILED(global_ctx_->acquire_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to acquire all subtable", K(ret));
  } else if (IS_NULL(all_sub_table)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, all subtable must not nullptr", K(ret));
  } else {
    SubTableMap &sub_table_map = all_sub_table->sub_table_map_;
    for (auto iter = sub_table_map.begin();
        Status::kOk == ret && iter != sub_table_map.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        XENGINE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else if (sub_table->IsDropped()) {
        // do nothing
      } else {
        sn = sub_table->get_meta_snapshot();
        if (sn != nullptr) {
          sub_table->Ref();
          meta_snapshots.emplace(sub_table, sn);
        } else {
          ret = Status::kCorruption;
          XENGINE_LOG(ERROR, "meta snapshot is null", K(ret), K(sub_table->GetID()));
          break;
        }
      }
    }
  }

  // do not overwrite ret
  int tmp_ret = Status::kOk;
  if (Status::kOk !=
      (tmp_ret = global_ctx_->release_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to release all subtable", K(tmp_ret));
  }

  return ret;
}

int VersionSet::add_sub_table(CreateSubTableArgs &args, bool write_log, bool is_replay, ColumnFamilyData *&sub_table)
{
  int ret = Status::kOk;
  ChangeSubTableLogEntry log_entry(args.index_id_, args.table_space_id_);

  AllSubTable *new_all_sub_table = nullptr;
  if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else if (write_log && FAILED(global_ctx_->storage_logger_->write_log(REDO_LOG_ADD_SSTABLE, log_entry))) {
    XENGINE_LOG(WARN, "fail to write add subtable log", K(ret), K(log_entry));
  } else if (FAILED(column_family_set_->CreateColumnFamily(args, sub_table))) {
    XENGINE_LOG(WARN, "fail to create ColumnFamiltData", K(ret), K(args));
  } else {
    if (is_replay) {
      //no need alloc new AllSubTable when recovery, optimize for many subtable
      if (FAILED(global_ctx_->all_sub_table_->add_sub_table(args.index_id_, sub_table))) {
        XENGINE_LOG(WARN, "fail to add subtable to all subtable", K(ret), K(args));
      }
    } else {
      std::lock_guard<std::mutex> guard(global_ctx_->all_sub_table_mutex_);
      if (nullptr == (new_all_sub_table = MOD_NEW_OBJECT(
                          memory::ModId::kAllSubTable, AllSubTable,
                          global_ctx_->all_sub_table_->sub_table_map_,
                          &(global_ctx_->all_sub_table_mutex_)))) {
        ret = Status::kMemoryLimit;
        XENGINE_LOG(WARN, "fail to allocate memory for new all sub table",
                    K(ret));
      } else if (FAILED(new_all_sub_table->add_sub_table(args.index_id_, sub_table))) {
          XENGINE_LOG(WARN, "fail to add sub table to new all sub table", K(ret),
                      "index_id", args.index_id_);
      } else if (FAILED(global_ctx_->install_new_all_sub_table(new_all_sub_table))) {
        XENGINE_LOG(WARN, "fail to install new all sub table", K(ret));
      }
    }
    XENGINE_LOG(DEBUG, "end to add subtable", K(ret), K(args));
  }

  return ret;
}

int VersionSet::remove_sub_table(ColumnFamilyData *sub_table, bool write_log, bool is_replay)
{
  int ret = Status::kOk;
  ChangeSubTableLogEntry log_entry(sub_table->GetID(), sub_table->get_table_space_id());

  AllSubTable *new_all_sub_table = nullptr;
  if (IS_NULL(sub_table)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(sub_table));
  } else if (write_log && FAILED(global_ctx_->storage_logger_->write_log(REDO_LOG_REMOVE_SSTABLE, log_entry))) {
    XENGINE_LOG(WARN, "fail to write remove subtable log", K(ret), K(log_entry));
  } else {
    sub_table->SetDropped();
//    TODO: yeti (#15350417)
    sub_table->set_bg_stopped(true);
    XENGINE_LOG(INFO, "success to remove subtable", K(sub_table->GetID()));
  }

  if (SUCCED(ret)) {
    int64_t index_id = sub_table->GetID();
    if (is_replay) {
      //no need alloc new AllSubTable when recovery, optimize for many subtable
      if (FAILED(global_ctx_->all_sub_table_->remove_sub_table(index_id))) {
        XENGINE_LOG(WARN, "fail to remove subtable", K(ret), K(index_id));
      }
    } else {
      std::lock_guard<std::mutex> guard(global_ctx_->all_sub_table_mutex_);
      if (nullptr == (new_all_sub_table = MOD_NEW_OBJECT(memory::ModId::kAllSubTable, AllSubTable, global_ctx_->all_sub_table_->sub_table_map_, &(global_ctx_->all_sub_table_mutex_)))) {
        ret = Status::kMemoryLimit;
        XENGINE_LOG(WARN, "fail to allocate memory for new all sub table", K(ret));
      } else if (FAILED(new_all_sub_table->remove_sub_table(index_id))) {
        XENGINE_LOG(WARN, "fail to remove subtable", K(ret), K(index_id));
      } else if (FAILED(global_ctx_->install_new_all_sub_table(new_all_sub_table))) {
        XENGINE_LOG(WARN, "fail to install new all sub table", K(ret));
      }
    }
    XENGINE_LOG(DEBUG, "end to remove subtable", K(ret), K(index_id));
  }

  //resource clean
  if (FAILED(ret)) {
    if (nullptr != new_all_sub_table) {
      MOD_DELETE_OBJECT(AllSubTable, new_all_sub_table);
    }
  }

  return ret;
}

int VersionSet::do_checkpoint(util::WritableFile *checkpoint_writer, CheckpointHeader *header)
{
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = DEFAULT_BUFFER_SIZE;
  int64_t offset = 0;
  int64_t size = 0;
  CheckpointBlockHeader *block_header = nullptr;
  ColumnFamilyData *sub_table = nullptr;
  AllSubTable *all_sub_table = nullptr;

#ifndef NDEBUG
  TEST_SYNC_POINT("VersionSet::do_checkpoint::inject_error");
  if (TEST_is_write_checkpoint_failed()) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "fail to do VersionSet checkpoint", K(ret));
    return ret;
  }
#endif
  if (IS_NULL(checkpoint_writer) || IS_NULL(header)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_writer), KP(header));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(memory::base_memalign(PAGE_SIZE, buf_size, memory::ModId::kVersionSet)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
  } else if (FAILED(reserve_checkpoint_block_header(buf, buf_size, offset, block_header))) {
    XENGINE_LOG(WARN, "fail to reserve check block header", K(ret));
  } else if (FAILED(global_ctx_->acquire_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to acquire all subtable", K(ret));
  } else if (IS_NULL(all_sub_table)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, all subtable must not nullptr", K(ret));
  } else {
    SubTableMap &sub_table_map = all_sub_table->sub_table_map_;
    header->sub_table_count_ = sub_table_map.size();
    header->sub_table_meta_block_offset_ = checkpoint_writer->GetFileSize();
    for (auto iter = sub_table_map.begin(); SUCCED(ret) && iter != sub_table_map.end(); ++iter) {
      if (IS_NULL(sub_table = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret));
      } else if (0 == sub_table->GetID()) {
        // skip default column family data
      } else if ((static_cast<int64_t>(buf_size - sizeof(CheckpointBlockHeader))) < (size = sub_table->get_serialize_size())) {
        XENGINE_LOG(INFO, "write big subtable", "index_id", iter->first, K(size));
        if (FAILED(write_big_subtable(checkpoint_writer, sub_table))) {
          XENGINE_LOG(WARN, "fail to write big subtable", K(ret), "index_id", iter->first);
        } else {
          ++(header->sub_table_meta_block_count_);
        }
      } else {
        if ((buf_size - offset) < size) {
          block_header->data_size_ = offset - block_header->data_offset_;
          if (FAILED(checkpoint_writer->PositionedAppend(Slice(buf, buf_size), checkpoint_writer->GetFileSize()).code())) {
            XENGINE_LOG(WARN, "fail to append buf to checkpoint writer", K(ret), K(size));
          } else if (FAILED(reserve_checkpoint_block_header(buf, buf_size, offset, block_header))) {
            XENGINE_LOG(WARN, "fail to reserve checkpoint block header", K(ret));
          } else if (FAILED(sub_table->serialize(buf, buf_size, offset))) {
            XENGINE_LOG(WARN, "fail to serialize subtable", K(ret));
          } else {
            ++(header->sub_table_meta_block_count_);
          }
        } else if (FAILED(sub_table->serialize(buf, buf_size, offset))) {
          XENGINE_LOG(WARN, "fail to serialize subtable", K(ret));
        }
        ++(block_header->entry_count_);
      }
    }

    if (SUCCED(ret) && block_header->entry_count_ > 0) {
      block_header->data_size_ = offset - block_header->data_offset_;
      if (FAILED(checkpoint_writer->PositionedAppend(Slice(buf, buf_size), checkpoint_writer->GetFileSize()).code())) {
        XENGINE_LOG(WARN, "fail to append buffer to checkpoint writer", K(ret));
      } else {
        ++(header->sub_table_meta_block_count_);
      }
    }
  }

  if (Status::kOk != (tmp_ret = global_ctx_->release_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to release all subtable", K(ret), K(tmp_ret));
  }

  //release resource
  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }

  return ret;
}

int VersionSet::load_checkpoint(util::RandomAccessFile *checkpoint_reader, storage::CheckpointHeader *header)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = DEFAULT_BUFFER_SIZE;
  int64_t pos = 0;
  char *sub_table_buf = nullptr;
  int64_t block_index = 0;
  int64_t offset = header->sub_table_meta_block_offset_;
  CheckpointBlockHeader *block_header = nullptr;
  ColumnFamilyData *sub_table = nullptr;
  common::ColumnFamilyOptions cf_options(global_ctx_->options_);
  CreateSubTableArgs dummy_args(1, cf_options);
  Slice result;

  XENGINE_LOG(INFO, "begin to load version set checkpoint");
  if (IS_NULL(checkpoint_reader) || IS_NULL(header)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_reader), KP(header));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(memory::base_memalign(PAGE_SIZE, buf_size, memory::ModId::kVersionSet)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
  } else {
    while (SUCCED(ret) && block_index < header->sub_table_meta_block_count_) {
      pos = 0;
      if (FAILED(checkpoint_reader->Read(offset, buf_size, &result, buf).code())) {
        XENGINE_LOG(WARN, "fail to read buf", K(ret), K(buf_size));
      } else {
        block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
        pos = block_header->data_offset_;
        if (block_header->block_size_ > buf_size) {
          if (FAILED(read_big_subtable(checkpoint_reader, block_header->block_size_, offset))) {
            XENGINE_LOG(WARN, "fail to read big subtable", K(ret), K(*block_header));
          }
        } else {
          for (int64_t i = 0; SUCCED(ret) && i < block_header->entry_count_; ++i) {
            if (IS_NULL(sub_table = MOD_NEW_OBJECT(memory::ModId::kColumnFamilySet, ColumnFamilyData, global_ctx_->options_))) {
              ret = Status::kErrorUnexpected;
              XENGINE_LOG(WARN, "fail to allocate memory for sub table", K(ret));
            } else if (FAILED(sub_table->init(dummy_args, global_ctx_, column_family_set_.get()))) {
              XENGINE_LOG(WARN, "fail to init subtable", K(ret));
            } else if (FAILED(sub_table->deserialize(buf, buf_size, pos))) {
              XENGINE_LOG(WARN, "fail to deserialize subtable", K(ret));
            } else if (FAILED(column_family_set_->add_sub_table(sub_table))) {
              XENGINE_LOG(WARN, "fail to add subtable to column family set", K(ret), "index_id", sub_table->GetID());
            } else if (FAILED(global_ctx_->extent_space_mgr_->open_table_space(sub_table->get_table_space_id()))) {
              XENGINE_LOG(WARN, "fail to create table space if not exist", K(ret), "table_space_id", sub_table->get_table_space_id());
            } else if (FAILED(global_ctx_->extent_space_mgr_->register_subtable(sub_table->get_table_space_id(), sub_table->GetID()))) {
              XENGINE_LOG(WARN, "fail to register subtable", K(ret), "table_space_id", sub_table->get_table_space_id(), "index_id", sub_table->GetID());
            } else if (FAILED(global_ctx_->all_sub_table_->add_sub_table(sub_table->GetID(), sub_table))) {
              XENGINE_LOG(WARN, "fail to add subtable to AllSubTable", K(ret), "index_id", sub_table->GetID());
            } else {
              XENGINE_LOG(INFO, "success to load subtable", K(ret), "index_id", sub_table->GetID());
            }
          }
          offset += buf_size;
        }
      }
      ++block_index;
    }
  }

  if (SUCCED(ret)) {
    //check result
    XENGINE_LOG(INFO, "success to load version set checkpoint");
  }
  //resource clean
  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }

  return ret;
}

int VersionSet::replay(int64_t log_type, char *log_data, int64_t log_len)
{
  int ret = Status::kOk;

  if (!is_partition_log(log_type) || nullptr == log_data || 0 >= log_len) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(log_type), K(log_len));
  } else {
    XENGINE_LOG(INFO, "replay one log entry", K(log_type), K(log_len));
    switch (log_type) {
      case REDO_LOG_ADD_SSTABLE:
        if (FAILED(replay_add_subtable_log(log_data, log_len))) {
          XENGINE_LOG(WARN, "fail to replay add subtable log", K(ret), K(log_type));
        }
        break;
      case REDO_LOG_REMOVE_SSTABLE:
        if (FAILED(replay_remove_subtable_log(log_data, log_len))) {
          XENGINE_LOG(WARN, "fail to replay remove sutable log", K(ret), K(log_type));
        }
        break;
      case REDO_LOG_MODIFY_SSTABLE:
        if (FAILED(replay_modify_subtable_log(log_data, log_len))) {
          XENGINE_LOG(WARN, "fail to replay extent meta log", K(ret), K(log_type));
        }
        break;
      default:
        ret = Status::kNotSupported;
        XENGINE_LOG(WARN, "unknow log type", K(ret), K(log_type));
    }
  }

  return ret;
}


int VersionSet::write_big_subtable(util::WritableFile *checkpoint_writer, ColumnFamilyData *sub_table)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = 0;
  int64_t offset = 0;
  int64_t size = 0;
  CheckpointBlockHeader *block_header = nullptr;

  if (IS_NULL(checkpoint_writer) || IS_NULL(sub_table)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_writer), KP(sub_table));
  } else {
    buf_size = ((sizeof(CheckpointBlockHeader) + sub_table->get_serialize_size() + DEFAULT_BUFFER_SIZE) / DEFAULT_BUFFER_SIZE) * DEFAULT_BUFFER_SIZE;
    if (nullptr == (buf = reinterpret_cast<char *>(memory::base_memalign(PAGE_SIZE, buf_size, memory::ModId::kVersionSet)))) {
      ret = Status::kMemoryLimit;
      XENGINE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
    } else if (FAILED(reserve_checkpoint_block_header(buf, buf_size, offset, block_header))){
      XENGINE_LOG(WARN, "fail to reserve checkpoint block header", K(ret));
    } else {
      block_header->data_size_ = sub_table->get_serialize_size();
      block_header->entry_count_ = 1;
      if (FAILED(sub_table->serialize(buf, buf_size, offset))) {
        XENGINE_LOG(WARN, "fail to serialize subtable", K(ret));
      } else if (FAILED(checkpoint_writer->PositionedAppend(Slice(buf, buf_size), checkpoint_writer->GetFileSize()).code())) {
        XENGINE_LOG(WARN, "fail to append buf to checkpoint writer", K(ret));
      }
    }
  }

  //release resource
  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }
  return ret;
}

int VersionSet::read_big_subtable(util::RandomAccessFile *checkpoint_reader,
                                   int64_t block_size,
                                   int64_t &file_offset)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = block_size;
  CheckpointBlockHeader *block_header = nullptr;
  ColumnFamilyData *sub_table = nullptr;
  char *sub_table_buf = nullptr;
  Slice result;
  int64_t pos = 0;
  common::ColumnFamilyOptions cf_options(global_ctx_->options_);
  CreateSubTableArgs dummy_args(1, cf_options);

  if (IS_NULL(checkpoint_reader) || block_size <=0 || file_offset <= 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_reader), K(block_size), K(file_offset));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(memory::base_memalign(PAGE_SIZE, buf_size, memory::ModId::kVersionSet)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
  } else if (FAILED(checkpoint_reader->Read(file_offset, buf_size, &result, buf).code())) {
    XENGINE_LOG(WARN, "fail to read buf", K(ret));
  } else {
    block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
    pos = block_header->data_offset_;
    if (1 != block_header->entry_count_) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "unexpected entry count", K(ret), K(block_header->entry_count_));
    } else if (IS_NULL(sub_table = MOD_NEW_OBJECT(memory::ModId::kColumnFamilySet, ColumnFamilyData, global_ctx_->options_))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "fail to allocate memory for sub table", K(ret));
    } else if (FAILED(sub_table->init(dummy_args, global_ctx_, column_family_set_.get()))) {
      XENGINE_LOG(WARN, "fail to deserialize subtable", K(ret));
    } else if (FAILED(sub_table->deserialize(buf, buf_size, pos))) {
      XENGINE_LOG(WARN, "fail to deserialize partition group", K(ret));
    } else if (FAILED(global_ctx_->extent_space_mgr_->open_table_space(sub_table->get_table_space_id()))) {
      XENGINE_LOG(WARN, "fail to open table space", K(ret), "table_space_id", sub_table->get_table_space_id());
    } else if (FAILED(global_ctx_->extent_space_mgr_->register_subtable(sub_table->get_table_space_id(), sub_table->GetID()))) {
      XENGINE_LOG(WARN, "fail to register subtable", K(ret), "table_space_id", sub_table->get_table_space_id(), "index_id", sub_table->GetID());
    } else if (FAILED(column_family_set_->add_sub_table(sub_table))) {
      XENGINE_LOG(WARN, "fail to add subtable to column family set", K(ret), "index_id", sub_table->GetID());
    } else if (FAILED(global_ctx_->all_sub_table_->add_sub_table(sub_table->GetID(), sub_table))) {
      XENGINE_LOG(WARN, "fail to add subtable to AllSubTable", K(ret), "index_id", sub_table->GetID());
    } else {
      file_offset += buf_size;
    }
  }

  //resource clean
  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }

  return ret;
}

int VersionSet::reserve_checkpoint_block_header(char *buf,
                                                int64_t buf_size,
                                                int64_t &offset,
                                                CheckpointBlockHeader *&block_header)
{
  int ret = Status::kOk;

  if (nullptr == buf || buf_size <= 0 || offset < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(buf_size), K(offset));
  } else {
    memset(buf, 0, buf_size);
    offset = 0;
    block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
    offset += sizeof(CheckpointBlockHeader);
    block_header->type_ = 1; //partition group meta block
    block_header->block_size_ = buf_size;
    block_header->entry_count_ = 0;
    block_header->data_size_ = 0;
    block_header->data_offset_ = offset;
  }

  return ret;
}

int VersionSet::replay_add_subtable_log(const char *log_data, int64_t log_length)
{
  int ret = Status::kOk;
  ChangeSubTableLogEntry log_entry;
  ColumnFamilyData *sub_table = nullptr;
  common::ColumnFamilyOptions cf_options(global_ctx_->options_);
  int64_t pos = 0;

  if (IS_NULL(log_data) || log_length <= 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(log_data), K(log_length));
  } else if (FAILED(log_entry.deserialize(log_data, log_length, pos))) {
    XENGINE_LOG(WARN, "fail to deserialize add subtable log entry", K(ret), K(log_length));
  } else {
    //TODO:yuanfeng, create extent space?
    CreateSubTableArgs args(log_entry.index_id_, cf_options, true, log_entry.table_space_id_);
    if (FAILED(add_sub_table(args, false /*write log*/, true /*is_replay*/, sub_table))) {
      XENGINE_LOG(WARN, "fail to add subtable", K(ret), K(log_entry), K(args));
    } else if (FAILED(global_ctx_->extent_space_mgr_->open_table_space(log_entry.table_space_id_))) {
      XENGINE_LOG(WARN, "fail to create table space if not exist", K(ret), K(log_entry));
    } else if (FAILED(global_ctx_->extent_space_mgr_->register_subtable(log_entry.table_space_id_, sub_table->GetID()))) {
      XENGINE_LOG(WARN, "fail to register subtable", K(ret), "table_space_id", log_entry.table_space_id_, "index_id", sub_table->GetID());
    } else {
      XENGINE_LOG(INFO, "success to replay add subtable", "index_id", sub_table->GetID());
    }
  }

  return ret;
}

int VersionSet::replay_remove_subtable_log(const char *log_data, int64_t log_length)
{
  int ret= Status::kOk;
  ChangeSubTableLogEntry log_entry;
  ColumnFamilyData *sub_table = nullptr;
  int64_t pos = 0;

  if (IS_NULL(log_data) || log_length < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(log_data), K(log_length));
  } else if (FAILED(log_entry.deserialize(log_data, log_length, pos))) {
    XENGINE_LOG(WARN, "fail to deserialize remove subtable log entry", K(ret));
  } else if (IS_NULL(sub_table = column_family_set_->GetColumnFamily(log_entry.index_id_))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret), K(log_entry));
  } else if (FAILED(remove_sub_table(sub_table, false /*write log*/, true /*is_replay*/))) {
    XENGINE_LOG(WARN, "fail to remove subtable", K(ret), K(log_entry));
  } else if (FAILED(global_ctx_->extent_space_mgr_->unregister_subtable(sub_table->get_table_space_id(), sub_table->GetID()))) {
    XENGINE_LOG(WARN, "fail to unregister subtbale", K(ret), "table_space_id", sub_table->get_table_space_id(), "index_id", sub_table->GetID());
  } else if (FAILED(sub_table->release_resource(true /*for_recovery*/))) {
    XENGINE_LOG(WARN, "fail to release subtable", K(ret));
  } else {
    if (sub_table->Unref()) {
      //ugly, ColumnFamilySet will been replaced by AllSubtableMap
      MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
      XENGINE_LOG(INFO, "success to replay remove subtable", "index_id", log_entry.index_id_);
    } else {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, subtable should not ref by other", K(ret), "index_id", log_entry.index_id_);
    }
  }

  return ret;
}

int VersionSet::replay_modify_subtable_log(const char *log_data, int64_t log_length)
{
  int ret = Status::kOk;
  ChangeInfo change_info;
  ModifySubTableLogEntry log_entry(change_info);
  ColumnFamilyData *sub_table = nullptr;
  int64_t pos = 0;

  if (IS_NULL(log_data) || log_length < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(log_data), KP(log_length));
  } else if (FAILED(log_entry.deserialize(log_data, log_length, pos))) {
    XENGINE_LOG(WARN, "fail to deserialize log entry", K(ret));
  } else if (IS_NULL(sub_table = column_family_set_->GetColumnFamily(log_entry.index_id_))) {
    if (column_family_set_->is_subtable_dropped(log_entry.index_id_)) {
      XENGINE_LOG(INFO, "subtable has been dropped, no need replay any more", K(log_entry));
    } else {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret), K(log_entry));
    }
  //} else if (TaskType::DUMP_TASK == change_info.task_type_ || TaskType::SWITCH_M02L0_TASK == change_info.task_type_) {
  //  if (FAILED(sub_table->apply_change_info_for_dump(log_entry.change_info_,
  //                                                   false /*write log*/,
  //                                                   true /*is replay*/,
  //                                                   &log_entry.recovery_point_))) {
  //    XENGINE_LOG(WARN, "failed to apply change info for dump", K(ret), K(log_entry));
  //  } else {
  //    XENGINE_LOG(INFO, "success to apply change info for dump", K(ret), K(log_entry));
  //  }
  } else if (FAILED(sub_table->apply_change_info(log_entry.change_info_,
                                                 false /*write log*/,
                                                 true /*is replay*/,
                                                 &log_entry.recovery_point_))) {
    XENGINE_LOG(WARN, "fail to replay apply change info", K(ret), K(log_entry));
  } else {
    XENGINE_LOG(INFO, "success to replay apply chang info", "index_id", log_entry.index_id_);
  }

  return ret;
}

int VersionSet::recover_M02L0() {
  int ret = Status::kOk;

  /*TODO:@yuanfeng, may restructure the subtable manager mechanism in future
   *for fix bug#31657618
   * recover_M0L0 called when recovery for move dump layer to level0, no race condition here,
   * so can use global_ctx_->all_sub_table_->sub_table_map_ directly , like recovery add/remove subtable. */
  //SubTable* sub_table_picked = nullptr;
  //AllSubTable* all_sub_table = nullptr;
  //if (FAILED(global_ctx_->acquire_thread_local_all_sub_table(all_sub_table))) {
  //  XENGINE_LOG(WARN, "fail to acquire all sub table", K(ret));
  //} else if (nullptr == all_sub_table) {
  //  ret = Status::kErrorUnexpected;
  //  XENGINE_LOG(ERROR, "unexpected error, all sub table must not nullptr",
  //              K(ret));
  //} else {
    SubTableMap& all_subtables = global_ctx_->all_sub_table_->sub_table_map_;
    SubTable* sub_table = nullptr;
    for (auto iter = all_subtables.begin();
         Status::kOk == ret && iter != all_subtables.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        XENGINE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else if (FAILED(sub_table->recover_m0_to_l0())) {
        // subtable's memtable is empty, do nothing
        XENGINE_LOG(INFO, "failed to recover m0 to l0", K(ret), K(iter->first));
      } else {
        XENGINE_LOG(INFO, "success to recover m0 to l0", K(ret),
                    K(iter->first));
      }
    }
  //}

  //int tmp_ret = ret;
  //if (nullptr != global_ctx_ &&
  //    FAILED(global_ctx_->release_thread_local_all_sub_table(all_sub_table))) {
  //  XENGINE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  //}

  return ret;
}

#ifndef NDEBUG
void VersionSet::TEST_inject_write_checkpoint_failed()
{
  write_checkpoint_failed_ = true;
}

bool VersionSet::TEST_is_write_checkpoint_failed()
{
  return write_checkpoint_failed_;
}
#endif

}  // namespace db
}  // namespace xegnine
