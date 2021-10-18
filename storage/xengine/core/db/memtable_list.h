// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <deque>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/range_del_aggregator.h"
#include "monitoring/instrumented_mutex.h"
#include "util/autovector.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "xengine/db.h"
#include "xengine/iterator.h"
#include "xengine/options.h"
#include "xengine/types.h"

namespace xengine {

namespace monitor {
class InstrumentedMutex;
}

namespace table {
class MergeIteratorBuilder;
}

namespace db {

class ColumnFamilyData;
class InternalKeyComparator;

// keeps a list of immutable memtables in a vector. the list is immutable
// if refcount is bigger than one. It is used as a state for Get() and
// Iterator code paths
//
// This class is not thread-safe.  External synchronization is required
// (such as holding the db mutex or being on the write thread).
class MemTableListVersion {
 public:
  explicit MemTableListVersion(size_t* parent_memtable_list_memory_usage,
                               MemTableListVersion* old = nullptr);
  explicit MemTableListVersion(size_t* parent_memtable_list_memory_usage,
                               int max_write_buffer_number_to_maintain);

  void Ref();
  void Unref(util::autovector<MemTable*>* to_delete = nullptr);

  // Search all the memtables starting from the most recent one.
  // Return the most recent value found, if any.
  //
  // If any operation was found for this key, its most recent sequence number
  // will be stored in *seq on success (regardless of whether true/false is
  // returned).  Otherwise, *seq will be set to kMaxSequenceNumber.
  bool Get(LookupKey& key, std::string* value, common::Status* s,
           MergeContext* merge_context, RangeDelAggregator* range_del_agg,
           common::SequenceNumber* seq, const common::ReadOptions& read_opts);

  bool Get(LookupKey& key, std::string* value, common::Status* s,
           MergeContext* merge_context, RangeDelAggregator* range_del_agg,
           const common::ReadOptions& read_opts) {
    common::SequenceNumber seq;
    return Get(key, value, s, merge_context, range_del_agg, &seq, read_opts);
  }

  // Similar to Get(), but searches the Memtable history of memtables that
  // have already been flushed.  Should only be used from in-memory only
  // queries (such as Transaction validation) as the history may contain
  // writes that are also present in the SST files.
  bool GetFromHistory(LookupKey& key, std::string* value, common::Status* s,
                      MergeContext* merge_context,
                      RangeDelAggregator* range_del_agg,
                      common::SequenceNumber* seq,
                      const common::ReadOptions& read_opts);
  bool GetFromHistory(LookupKey& key, std::string* value, common::Status* s,
                      MergeContext* merge_context,
                      RangeDelAggregator* range_del_agg,
                      const common::ReadOptions& read_opts) {
    common::SequenceNumber seq;
    return GetFromHistory(key, value, s, merge_context, range_del_agg, &seq,
                          read_opts);
  }

  common::Status AddRangeTombstoneIterators(
      const common::ReadOptions& read_opts, util::Arena* arena,
      RangeDelAggregator* range_del_agg);

  void AddIterators(const common::ReadOptions& options,
                    std::vector<table::InternalIterator*>* iterator_list,
                    util::Arena* arena);

  void AddIterators(const common::ReadOptions& options,
                    table::MergeIteratorBuilder* merge_iter_builder);

  uint64_t GetTotalNumEntries() const;

  uint64_t GetTotalNumDeletes() const;

  MemTable::MemTableStats ApproximateStats(const common::Slice& start_ikey,
                                           const common::Slice& end_ikey);

  // Returns the value of MemTable::GetEarliestSequenceNumber() on the most
  // recent MemTable in this list or kMaxSequenceNumber if the list is empty.
  // If include_history=true, will also search Memtables in MemTableList
  // History.
  common::SequenceNumber GetEarliestSequenceNumber(
      bool include_history = false) const;

 private:
  // REQUIRE: m is an immutable memtable
  void Add(MemTable* m, util::autovector<MemTable*>* to_delete);
  // REQUIRE: m is an immutable memtable
  void Remove(MemTable* m, util::autovector<MemTable*>* to_delete);

  void TrimHistory(util::autovector<MemTable*>* to_delete);
  void trim_all_history(util::autovector<MemTable*>* to_delete);
  size_t TrimHistoryOlderThan(util::autovector<MemTable*>* to_delete,
                              common::SequenceNumber seqno);

  bool GetFromList(std::list<MemTable*>* list, LookupKey& key,
                   std::string* value, common::Status* s,
                   MergeContext* merge_context,
                   RangeDelAggregator* range_del_agg,
                   common::SequenceNumber* seq,
                   const common::ReadOptions& read_opts);

  void AddMemTable(MemTable* m);

  void UnrefMemTable(util::autovector<MemTable*>* to_delete, MemTable* m);

  friend class MemTableList;

  // Immutable MemTables that have not yet been flushed.
  std::list<MemTable*> memlist_;

  // MemTables that have already been flushed
  // (used during Transaction validation)
  std::list<MemTable*> memlist_history_;

  // Maximum number of MemTables to keep in memory (including both flushed
  // and not-yet-flushed tables).
  const int max_write_buffer_number_to_maintain_;

  int refs_ = 0;

  size_t* parent_memtable_list_memory_usage_;
};

// This class stores references to all the immutable memtables.
// The memtables are flushed to L0 as soon as possible and in
// any order. If there are more than one immutable memtable, their
// flushes can occur concurrently.  However, they are 'committed'
// to the manifest in FIFO order to maintain correctness and
// recoverability from a crash.
//
//
// Other than imm_flush_needed, this class is not thread-safe and requires
// external synchronization (such as holding the db mutex or being on the
// write thread.)
class MemTableList {
 public:
  // A list of memtables.
  explicit MemTableList(int min_write_buffer_number_to_merge,
                        int max_write_buffer_number_to_maintain)
      : imm_flush_needed(false),
        min_write_buffer_number_to_merge_(min_write_buffer_number_to_merge),
//        current_(new MemTableListVersion(&current_memory_usage_,
//                                         max_write_buffer_number_to_maintain)),
        current_(MOD_NEW_OBJECT(memory::ModId::kDefaultMod, MemTableListVersion,
            &current_memory_usage_, max_write_buffer_number_to_maintain)),
        num_flush_not_started_(0),
        commit_in_progress_(false),
        flush_requested_(false) {
    current_->Ref();
    current_memory_usage_ = 0;
  }

  // Should not delete MemTableList without making sure MemTableList::current()
  // is Unref()'d.
  ~MemTableList() {}

  MemTableListVersion* current() { return current_; }

  // so that background threads can detect non-nullptr pointer to
  // determine whether there is anything more to start flushing.
  std::atomic<bool> imm_flush_needed;

  // Returns the total number of memtables in the list that haven't yet
  // been flushed and logged.
  int NumNotFlushed() const;

  // Returns total number of memtables in the list that have been
  // completely flushed and logged.
  int NumFlushed() const;

  // Returns true if there is at least one memtable on which flush has
  // not yet started.
  bool IsFlushPending() const;

  // Return the delete info and data size of memtables
  void calc_flush_info(bool &delete_trigger, int64_t &size, int64_t &cnt);

  // Returns the earliest memtables that needs to be flushed. The returned
  // memtables are guaranteed to be in the ascending order of created time.
  void PickMemtablesToFlush(util::autovector<MemTable*>* mems);

  // Reset status of the given memtable list back to pending state so that
  // they can get picked up again on the next round of flush.
  void RollbackMemtableFlush(const util::autovector<MemTable*>& mems,
                             uint64_t file_number);

  // Commit a successful flush in the manifest file
  common::Status InstallMemtableFlushResults(
      ColumnFamilyData* cfd, const common::MutableCFOptions& mutable_cf_options,
      const util::autovector<MemTable*>& m, VersionSet* vset,
      monitor::InstrumentedMutex* mu, uint64_t file_number,
      util::autovector<MemTable*>* to_delete, util::Directory* db_directory,
      util::LogBuffer* log_buffer, db::MiniTables &mtables);

  // New memtables are inserted at the front of the list.
  // Takes ownership of the referenced held on *m by the caller of Add().
  void Add(MemTable* m, util::autovector<MemTable*>* to_delete);

  size_t TrimOlderThan(util::autovector<MemTable*>* to_delete,
                       common::SequenceNumber seqno);

  // Returns an estimate of the number of bytes of data in use.
  size_t ApproximateMemoryUsage();

  size_t ApproximateUnflushedMemTablesMemoryAllocated();
  // Returns an estimate of the number of bytes of data used by
  // the unflushed mem-tables.
  size_t ApproximateUnflushedMemTablesMemoryUsage();

  // Request a flush of all existing memtables to storage.  This will
  // cause future calls to IsFlushPending() to return true if this list is
  // non-empty (regardless of the min_write_buffer_number_to_merge
  // parameter). This flush request will persist until the next time
  // PickMemtablesToFlush() is called.
  void FlushRequested() { flush_requested_ = true; }

  bool HasFlushRequested() { return flush_requested_; }

  // Copying allowed
  // MemTableList(const MemTableList&);
  // void operator=(const MemTableList&);

  size_t* current_memory_usage() { return &current_memory_usage_; }

  uint64_t GetMinLogContainingPrepSection();
  
  uint64_t get_largest_lognumber();

  int purge_flushed_memtable(const util::autovector<MemTable *> &flushed_memtables, util::autovector<MemTable *> *to_delete);

 private:
  // DB mutex held
  void InstallNewVersion();

  const int min_write_buffer_number_to_merge_;

  MemTableListVersion* current_;

  // the number of elements that still need flushing
  int num_flush_not_started_;

  // committing in progress
  bool commit_in_progress_;

  // Requested a flush of all memtables to storage
  bool flush_requested_;

  // The current memory usage.
  size_t current_memory_usage_;
};
}
}  // namespace xengine
