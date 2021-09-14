// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_impl.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>
#include <thread>

#include "db/builder.h"
#include "db/debug_info.h"
#include "db/replay_task.h"
#include "db/replay_thread_pool.h"
#include "options/options_helper.h"
#include "table/extent_table_factory.h"
#include "table/filter_manager.h"
#include "memory/mod_info.h"
#include "util/sst_file_manager_impl.h"
#include "util/sync_point.h"
#include "xengine/wal_filter.h"
#include "storage/storage_logger.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace table;
using namespace monitor;
using namespace storage;
using namespace memory;

namespace xengine {
namespace db {
Options SanitizeOptions(const std::string& dbname, const Options& src) {
  auto db_options = SanitizeOptions(dbname, DBOptions(src));
  ImmutableDBOptions immutable_db_options(db_options);
  auto cf_options =
      SanitizeOptions(immutable_db_options, ColumnFamilyOptions(src));
  return Options(db_options, cf_options);
}

DBOptions SanitizeOptions(const std::string& dbname, const DBOptions& src) {
  DBOptions result(src);

  // result.max_open_files means an "infinite" open files.
  if (result.max_open_files != -1) {
    int max_max_open_files = port::GetMaxOpenFiles();
    if (max_max_open_files == -1) {
      max_max_open_files = 1000000;
    }
    ClipToRange(&result.max_open_files, 20, max_max_open_files);
  }

  if (!result.write_buffer_manager) {
    result.write_buffer_manager.reset(new WriteBufferManager(
        result.db_write_buffer_size, result.db_total_write_buffer_size));
  }
  if (result.base_background_compactions == -1) {
    result.base_background_compactions = result.max_background_compactions;
  }
  if (result.base_background_compactions > result.max_background_compactions) {
    result.base_background_compactions = result.max_background_compactions;
  }

  if (result.compaction_type > 1) {
    result.compaction_type = 0;
  }
  if (result.cpu_compaction_thread_num < (uint64_t)result.max_background_compactions) {
    result.cpu_compaction_thread_num = (uint64_t)result.max_background_compactions; 
  }

  result.env->IncBackgroundThreadsIfNeeded(src.max_background_compactions,
                                           Env::Priority::LOW);
  result.env->IncBackgroundThreadsIfNeeded(src.max_background_flushes,
                                           Env::Priority::HIGH);

  if (result.rate_limiter.get() != nullptr) {
    if (result.bytes_per_sync == 0) {
      result.bytes_per_sync = 1024 * 1024;
    }
  }

  if (result.WAL_ttl_seconds > 0 || result.WAL_size_limit_MB > 0) {
    result.recycle_log_file_num = false;
  }

  if (result.recycle_log_file_num &&
      (result.wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery ||
       result.wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency)) {
    // kPointInTimeRecovery is indistinguishable from
    // kTolerateCorruptedTailRecords in recycle mode since we define
    // the "end" of the log as the first corrupt record we encounter.
    // kAbsoluteConsistency doesn't make sense because even a clean
    // shutdown leaves old junk at the end of the log file.
    result.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  }
  if (result.parallel_wal_recovery &&
      result.wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
    result.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  }

  if (result.wal_dir.empty()) {
    // Use dbname as default
    result.wal_dir = dbname;
  }
  if (result.wal_dir.back() == '/') {
    result.wal_dir = result.wal_dir.substr(0, result.wal_dir.size() - 1);
  }

  if (result.db_paths.size() == 0) {
    result.db_paths.emplace_back(dbname, std::numeric_limits<uint64_t>::max());
  }

  if (result.use_direct_reads && result.compaction_readahead_size == 0) {
    result.compaction_readahead_size = 1024 * 1024 * 2;
  }

  if (result.compaction_readahead_size > 0 ||
      result.use_direct_io_for_flush_and_compaction) {
    result.new_table_reader_for_compaction_inputs = true;
  }

  return result;
}

namespace {

Status SanitizeOptionsByTable(
    const DBOptions& db_opts,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (auto cf : column_families) {
    s = cf.options.table_factory->SanitizeOptions(db_opts, cf.options);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

static Status ValidateOptions(
    const DBOptions& db_options,
    const std::string &dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;

  for (auto& cfd : column_families) {
    s = CheckCompressionSupported(cfd.options);
    if (s.ok() && db_options.allow_concurrent_memtable_write) {
      s = CheckConcurrentWritesSupported(cfd.options);
    }
    if (!s.ok()) {
      return s;
    }
    if (db_options.db_paths.size() > 1) {
      if ((cfd.options.compaction_style != kCompactionStyleUniversal) &&
          (cfd.options.compaction_style != kCompactionStyleLevel)) {
        return Status::NotSupported(
            "More than one DB paths are only supported in "
            "universal and level compaction styles. ");
      }
    }
  }

  if (db_options.db_paths.size() > 4) {
    return Status::NotSupported(
        "More than four DB paths are not supported yet. ");
  }

  if (db_options.allow_mmap_reads && db_options.use_direct_reads) {
    // Protect against assert in PosixMMapReadableFile constructor
    return Status::NotSupported(
        "If memory mapped reads (allow_mmap_reads) are enabled "
        "then direct I/O reads (use_direct_reads) must be disabled. ");
  }

  if (db_options.allow_mmap_writes &&
      db_options.use_direct_io_for_flush_and_compaction) {
    return Status::NotSupported(
        "If memory mapped writes (allow_mmap_writes) are enabled "
        "then direct I/O writes (use_direct_io_for_flush_and_compaction) must "
        "be disabled. ");
  }

  if (db_options.keep_log_file_num == 0) {
    return Status::InvalidArgument("keep_log_file_num must be greater than 0");
  }

  return Status::OK();
}
}  // namespace
Status DBImpl::NewDB() {
  /*
  VersionEdit new_db;
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);
  */

  Status s;

  /*
  __XENGINE_LOG(INFO, "Creating manifest 1 \n");
  const std::string manifest = DescriptorFileName(dbname_, 1);
  {
    unique_ptr<WritableFile> file;
    EnvOptions env_options = env_->OptimizeForManifestWrite(env_options_);
    s = NewWritableFile(env_, manifest, &file, env_options);
    if (!s.ok()) {
      return s;
    }
    file->SetPreallocationBlockSize(
        immutable_db_options_.manifest_preallocation_size);
    unique_ptr<util::ConcurrentDirectFileWriter> file_writer(
        new util::ConcurrentDirectFileWriter(std::move(file), env_options));
    s = file_writer->init_multi_buffer();
    if (!s.ok()) {
      __XENGINE_LOG(ERROR, "init multi log buffer failed for ManifestWrite");
      return s;
    }
    log::Writer log(std::move(file_writer), 0, false);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = SyncManifest(env_, &immutable_db_options_, log.file());
    }
  }
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1, directories_.GetDbDir(), 0, 0);
  } else {
    env_->DeleteFile(manifest);
  }
  */
  return s;
}

Status DBImpl::Directories::CreateAndNewDirectory(
    Env* env, const std::string& dirname,
    Directory *&directory) const {
  // We call CreateDirIfMissing() as the directory may already exist (if we
  // are reopening a DB), when this happens we don't want creating the
  // directory to cause an error. However, we need to check if creating the
  // directory fails or else we may get an obscure message about the lock
  // file not existing. One real-world example of this occurring is if
  // env->CreateDirIfMissing() doesn't create intermediate directories, e.g.
  // when dbname_ is "dir/db" but when "dir" doesn't exist.
  Status s = env->CreateDirIfMissing(dirname);
  if (!s.ok()) {
    return s;
  }
  return env->NewDirectory(dirname, directory);
}

Status DBImpl::Directories::SetDirectories(
    Env* env, const std::string& dbname, const std::string& wal_dir,
    const std::vector<DbPath>& data_paths) {
  util::Directory *ptr = nullptr;
  Status s = CreateAndNewDirectory(env, dbname, ptr);
  db_dir_.reset(ptr);
  if (!s.ok()) {
    return s;
  }
  if (!wal_dir.empty() && dbname != wal_dir) {
    util::Directory *wal_ptr = nullptr;
    s = CreateAndNewDirectory(env, wal_dir, wal_ptr);
    wal_dir_.reset(wal_ptr);
    if (!s.ok()) {
      return s;
    }
  }

  data_dirs_.clear();
  for (auto& p : data_paths) {
    const std::string db_path = p.path;
    if (db_path == dbname) {
      data_dirs_.emplace_back(nullptr);
    } else {
//      std::unique_ptr<Directory> path_directory;
      Directory *path_directory = nullptr;
      s = CreateAndNewDirectory(env, db_path, path_directory);
      if (!s.ok()) {
        return s;
      }
      data_dirs_.emplace_back(path_directory);
    }
  }
  assert(data_dirs_.size() == data_paths.size());
  return Status::OK();
}

Status DBImpl::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    const ColumnFamilyOptions &cf_options, bool read_only,
    bool error_if_log_file_exist, bool error_if_data_exists_in_logs) {
  mutex_.AssertHeld();

  int ret = Status::kOk;
  assert(db_lock_ == nullptr);

  if (FAILED(prepare_recovery(read_only, cf_options))) {
    XENGINE_LOG(ERROR, "fail to prepae recovery", K(ret));
  } else if (FAILED(recovery())) {
    XENGINE_LOG(ERROR, "fail to replay manifest log or wal", K(ret));
  } else if (FAILED(after_recovery())) {
    XENGINE_LOG(ERROR, "fail to do something after recovery", K(ret));
  } else {
    XENGINE_LOG(INFO, "success to recovery xengine log");
  }

  return ret;
}


//now not called by others
#if 0
// REQUIRES: log_numbers are sorted in ascending order
Status DBImpl::RecoverLogFiles(const std::vector<uint64_t>& log_numbers,
                               SequenceNumber* next_sequence, bool read_only) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    const char* fname;
    Status* status;  // nullptr if immutable_db_options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) override {
      __XENGINE_LOG(WARN, "%s%s: dropping %d bytes; %s",
                     (this->status == nullptr ? "(ignoring error) " : ""),
                     fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) {
        *this->status = s;
      }
    }
  };

  mutex_.AssertHeld();
  Status status;
  //std::unordered_map<int, VersionEdit> version_edits;
  //std::unordered_map<int, storage::ChangeInfo> change_infos;
  // no need to refcount because iteration is under mutex
  /*
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    version_edits.insert({cfd->GetID(), edit});
    storage::ChangeInfo change_info;
    change_infos.insert({cfd->GetID(), change_info});
  }
  */
  int job_id = next_job_id_.fetch_add(1);
  {
    auto stream = EventLogger::GetLoggerStream();
    stream << "job" << job_id << "event"
           << "recovery_started";
    stream << "log_files";
    stream.StartArray();
    for (auto log_number : log_numbers) {
      stream << log_number;
    }
    stream.EndArray();
  }

#ifndef ROCKSDB_LITE
  if (immutable_db_options_.wal_filter != nullptr) {
    std::map<std::string, uint32_t> cf_name_id_map;
    std::map<uint32_t, uint64_t> cf_lognumber_map;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      cf_name_id_map.insert(std::make_pair(cfd->GetName(), cfd->GetID()));
      cf_lognumber_map.insert(
          std::make_pair(cfd->GetID(), cfd->get_recovery_point().log_file_number_));
    }

    immutable_db_options_.wal_filter->ColumnFamilyLogNumberMap(cf_lognumber_map,
                                                               cf_name_id_map);
  }
#endif

  bool stop_replay_by_wal_filter = false;
  bool stop_replay_for_corruption = false;
  bool flushed = false;
  WriteContext write_context; // switch memtable release resource
  if (immutable_db_options_.avoid_flush_during_recovery) {
    // set no flush in recovery, don't switch memtable 
    for (ColumnFamilyData *cfd : *versions_->GetColumnFamilySet()) {
      cfd->mem()->set_no_flush(true);
    }
  }
  for (auto log_number : log_numbers) {
    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsedDuringRecovery(log_number);
    // Open the log file
    std::string fname = LogFileName(immutable_db_options_.wal_dir, log_number);

    __XENGINE_LOG(INFO, "Recovering log #%" PRIu64 " mode %d", log_number,
        immutable_db_options_.wal_recovery_mode);
    auto logFileDropped = [this, &fname]() {
      uint64_t bytes;
      if (env_->GetFileSize(fname, &bytes).ok()) {
        __XENGINE_LOG(WARN, "%s: dropping %d bytes",
                      fname.c_str(), static_cast<int>(bytes));
      }
    };
    if (stop_replay_by_wal_filter) {
      logFileDropped();
      continue;
    }

    unique_ptr<SequentialFileReader> file_reader;
    {
      unique_ptr<SequentialFile> file;
      status = env_->NewSequentialFile(fname, &file, env_options_);
      if (!status.ok()) {
        MaybeIgnoreError(&status);
        if (!status.ok()) {
          return status;
        } else {
          // Fail with one log file, but that's ok.
          // Try next one.
          continue;
        }
      }
      file_reader.reset(new SequentialFileReader(std::move(file)));
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.fname = fname.c_str();
    if (!immutable_db_options_.paranoid_checks ||
        immutable_db_options_.wal_recovery_mode ==
            WALRecoveryMode::kSkipAnyCorruptedRecords) {
      reporter.status = nullptr;
    } else {
      reporter.status = &status;
    }
    // We intentially make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::Reader reader(std::move(file_reader), &reporter,
                       true /*checksum*/, 0 /*initial_offset*/, log_number);

    // Determine if we should tolerate incomplete records at the tail end of the
    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;

    while (!stop_replay_by_wal_filter &&
           reader.ReadRecord(&record, &scratch,
                             immutable_db_options_.wal_recovery_mode) &&
           status.ok()) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter.Corruption(record.size(),
                            Status::Corruption("log record too small"));
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      SequenceNumber sequence = WriteBatchInternal::Sequence(&batch);

      if (immutable_db_options_.wal_recovery_mode ==
          WALRecoveryMode::kPointInTimeRecovery) {
        // In point-in-time recovery mode, if sequence id of log files are
        // consecutive, we continue recovery despite corruption. This could
        // happen when we open and write to a corrupted DB, where sequence id
        // will start from the last sequence id we recovered.
        if (sequence == *next_sequence) {
          stop_replay_for_corruption = false;
        }
        if (stop_replay_for_corruption) {
          logFileDropped();
          break;
        }
      }

#ifndef ROCKSDB_LITE
      if (immutable_db_options_.wal_filter != nullptr) {
        WriteBatch new_batch;
        bool batch_changed = false;

        WalFilter::WalProcessingOption wal_processing_option =
            immutable_db_options_.wal_filter->LogRecordFound(
                log_number, fname, batch, &new_batch, &batch_changed);

        switch (wal_processing_option) {
          case WalFilter::WalProcessingOption::kContinueProcessing:
            // do nothing, proceeed normally
            break;
          case WalFilter::WalProcessingOption::kIgnoreCurrentRecord:
            // skip current record
            continue;
          case WalFilter::WalProcessingOption::kStopReplay:
            // skip current record and stop replay
            stop_replay_by_wal_filter = true;
            continue;
          case WalFilter::WalProcessingOption::kCorruptedRecord: {
            status =
                Status::Corruption("Corruption reported by Wal Filter ",
                                   immutable_db_options_.wal_filter->Name());
            MaybeIgnoreError(&status);
            if (!status.ok()) {
              reporter.Corruption(record.size(), status);
              continue;
            }
            break;
          }
          default: {
            assert(false);  // unhandled case
            status = Status::NotSupported(
                "Unknown WalProcessingOption returned"
                " by Wal Filter ",
                immutable_db_options_.wal_filter->Name());
            MaybeIgnoreError(&status);
            if (!status.ok()) {
              return status;
            } else {
              // Ignore the error with current record processing.
              continue;
            }
          }
        }

        if (batch_changed) {
          // Make sure that the count in the new batch is
          // within the orignal count.
          int new_count = WriteBatchInternal::Count(&new_batch);
          int original_count = WriteBatchInternal::Count(&batch);
          if (new_count > original_count) {
            __XENGINE_LOG(ERROR,
                "Recovering log #%" PRIu64
                " mode %d log filter %s returned "
                "more records (%d) than original (%d) which is not allowed. "
                "Aborting recovery.",
                log_number, immutable_db_options_.wal_recovery_mode,
                immutable_db_options_.wal_filter->Name(), new_count,
                original_count);
            status = Status::NotSupported(
                "More than original # of records "
                "returned by Wal Filter ",
                immutable_db_options_.wal_filter->Name());
            return status;
          }
          // Set the same sequence number in the new_batch
          // as the original batch.
          WriteBatchInternal::SetSequence(&new_batch,
                                          WriteBatchInternal::Sequence(&batch));
          batch = new_batch;
        }
      }
#endif  // ROCKSDB_LITE

      // If column family was not found, it might mean that the WAL write
      // batch references to the column family that was dropped after the
      // insert. We don't want to fail the whole write batch in that case --
      // we just ignore the update.
      // That's why we set ignore missing column families to true
      bool has_valid_writes = false;
      ColumnFamilyMemTablesImpl column_family_memtables(versions_->global_ctx_->all_sub_table_->sub_table_map_, versions_->GetColumnFamilySet());
      status = WriteBatchInternal::InsertInto(
          &batch, &column_family_memtables, &flush_scheduler_, true,
          log_number, this, false /* concurrent_memtable_writes */,
          next_sequence, &has_valid_writes);
      MaybeIgnoreError(&status);
      if (!status.ok()) {
        // We are treating this as a failure while reading since we read valid
        // blocks that do not form coherent data
        reporter.Corruption(record.size(), status);
        continue;
      }

      if (has_valid_writes && !read_only) {
        // we can do this because this is called before client has access to the
        // DB and there is only a single thread operating on DB

        ColumnFamilyData* cfd;

        while ((cfd = flush_scheduler_.TakeNextColumnFamily()) != nullptr) {
          cfd->Unref();
          if (!immutable_db_options_.avoid_flush_during_recovery) {
            // If this asserts, it means that InsertInto failed in
            // filtering updates to already-flushed column families
            assert(static_cast<uint64_t>(cfd->get_recovery_point().log_file_number_) <= log_number);
            //auto iter = version_edits.find(cfd->GetID());
            //assert(iter != version_edits.end());
            //VersionEdit* edit = &iter->second;
            //auto iter_info = change_infos.find(cfd->GetID());
            //assert(iter_info != change_infos.end());
            //status = WriteLevel0TableForRecovery(job_id, cfd, cfd->mem(), edit/*,
            //                                     iter_info->second*/);
            //if (!status.ok()) {
              // Reflect errors immediately so that conditions like full
              // file-systems cause the DB::Open() to fail.
              //return status;
            //}
            //flushed = true;

            //cfd->CreateNewMemtable(*cfd->GetLatestMutableCFOptions(),
            //                       *next_sequence);
          } else { // no memtable memory usage limit, this can't happen
            assert(false);
          }
        }
      }
    }

    if (!status.ok()) {
      if (immutable_db_options_.wal_recovery_mode ==
          WALRecoveryMode::kSkipAnyCorruptedRecords) {
        // We should ignore all errors unconditionally
        status = Status::OK();
      } else if (immutable_db_options_.wal_recovery_mode ==
                 WALRecoveryMode::kPointInTimeRecovery) {
        // We should ignore the error but not continue replaying
        status = Status::OK();
        stop_replay_for_corruption = true;
        __XENGINE_LOG(INFO, "Point in time recovered to log #%" PRIu64 " seq #%" PRIu64, log_number, *next_sequence);
      } else {
        assert(immutable_db_options_.wal_recovery_mode ==
                   WALRecoveryMode::kTolerateCorruptedTailRecords ||
               immutable_db_options_.wal_recovery_mode ==
                   WALRecoveryMode::kAbsoluteConsistency);
        return status;
      }
    }

    flush_scheduler_.Clear();
    auto last_sequence = *next_sequence - 1;
    if ((*next_sequence != kMaxSequenceNumber) &&
        (versions_->LastSequence() <= last_sequence)) {
      versions_->SetLastSequence(last_sequence);
      versions_->SetLastAllocatedSequence(last_sequence);
    }
  }

  // True if there's any data in the WALs; if not, we can skip re-processing
  // them later
  bool data_seen = false;
  if (!read_only) {
    // no need to refcount since client still doesn't have access
    // to the DB and can not drop column families while we iterate
    auto max_log_number = log_numbers.back();
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      //auto iter = version_edits.find(cfd->GetID());
      //assert(iter != version_edits.end());
      //VersionEdit* edit = &iter->second;
      //auto iter_info = change_infos.find(cfd->GetID());
      //assert(iter_info != change_infos.end());

      if (static_cast<uint64_t>(cfd->get_recovery_point().log_file_number_) > max_log_number) {
        // Column family cfd has already flushed the data
        // from all logs. Memtable has to be empty because
        // we filter the updates based on log_number
        // (in WriteBatch::InsertInto)
        assert(cfd->mem()->GetFirstSequenceNumber() == 0);
        continue;
      }

      // flush the final memtable (if non-empty)
      //TODO:yuanfeng
      /*
      if (cfd->mem()->GetFirstSequenceNumber() != 0) {
        // If flush happened in the middle of recovery (e.g. due to memtable
        // being full), we flush at the end. Otherwise we'll need to record
        // where we were on last flush, which make the logic complicated.
        if (!immutable_db_options_.avoid_flush_during_recovery) {
          //status = WriteLevel0TableForRecovery(job_id, cfd, cfd->mem(), edit,
          //                                     iter_info->second);
          if (!status.ok()) {
            // Recovery failed
            break;
          }
          flushed = true;

          cfd->CreateNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                 versions_->LastSequence());
        } else {
          assert(!flushed); // no flush happened
        }
        data_seen = true;
      }
      */

      if (!immutable_db_options_.avoid_flush_during_recovery) {
        // write MANIFEST with update
        // writing log_number in the manifest means that any log file
        // with number strongly less than (log_number + 1) is already
        // recovered and should be ignored on next reincarnation.
        // Since we already recovered max_log_number, we want all logs
        // with numbers `<= max_log_number` (includes this one) to be ignored
        if (flushed || cfd->mem()->GetFirstSequenceNumber() == 0) {
          //edit->SetLogNumber(max_log_number + 1);
        }
        // we must mark the next log number as used, even though it's
        // not actually used. that is because VersionSet assumes
        // VersionSet::next_file_number_ always to be strictly greater than any
        // log number
        versions_->MarkFileNumberUsedDuringRecovery(max_log_number + 1);
        // update current after all log applied
        //status = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
        //                              edit, &mutex_, nullptr, false, nullptr, false);
        if (!status.ok()) {
          // Recovery failed
          break;
        }
        //status = install_flush_result(cfd, iter_info->second, false);
        if (!status.ok()) {
          break;
        }
      } 
    }
  }

  if (status.ok()) {
    // do a storage manager checkpoint
    if (immutable_db_options_.avoid_flush_during_recovery) {
      assert(!flushed);
      // after recovery reset the memtable memory usage limit
      for (ColumnFamilyData *cfd : *versions_->GetColumnFamilySet()) {
        cfd->mem()->set_no_flush(false);
      }
      int ret = versions_->do_manual_checkpoint(&mutex_);
      if (ret != Status::kOk) {
        __XENGINE_LOG(ERROR, "Do storage manager checkpoint failed %d", ret);
        return Status(static_cast<Status::Code>(ret));
      }
    } else {
      //TODO:yuanfeng
      //status = versions_->set_current_file(versions_->GetColumnFamilySet()->get_db_dir());
      if (!status.ok()) {
        int ret = status.code();
        XENGINE_LOG(ERROR, "Update current file failed", K(ret));
        return status;
      }
    }  

    if (data_seen && !flushed) {
      // Mark these as alive so they'll be considered for deletion later by
      // FindObsoleteFiles()
      for (auto log_number : log_numbers) {
        alive_log_files_.push_back(LogFileNumberSize(log_number));
      }
    }
  }

  auto stream = EventLogger::GetLoggerStream();
  stream << "job" << job_id << "event"
         << (status.ok() ? "recovery_finished" : "recovery_faild");

  return status;
}
#endif

int DBImpl::prepare_recovery(bool read_only, const ColumnFamilyOptions &cf_options)
{
  int ret = Status::kOk;
  const uint64_t t1 = env_->NowMicros();
  if (!read_only) {
    if (FAILED(directories_.SetDirectories(env_, dbname_,
                                           immutable_db_options_.wal_dir,
                                           immutable_db_options_.db_paths).code())) {
      XENGINE_LOG(INFO, "fail to set directories", K(ret), K_(dbname), "wal_dir", immutable_db_options_.wal_dir);
    } else if (FAILED(env_->LockFile(LockFileName(dbname_), &db_lock_).code())) {
      XENGINE_LOG(INFO, "fail to lock file", K(ret), K_(dbname));
    } else {
      if (FAILED(env_->FileExists(CurrentFileName(dbname_)).code())) {
        if (Status::kNotFound != ret) {
          XENGINE_LOG(WARN, "unexpected error, when check current file exist or not", K(ret), K_(dbname));
        } else {
          if (immutable_db_options_.create_if_missing) {
            //overwrite ret as design
            const uint64_t t2 = env_->NowMicros();
            if (FAILED(storage_logger_->external_write_checkpoint())) {
              XENGINE_LOG(WARN, "fail to external write checkpoint", K(ret), K_(dbname));
            }
            const uint64_t t3 = env_->NowMicros();
            recovery_debug_info_.prepare_external_write_ckpt = t3 - t2;
          }
        }
      } else {
        if (immutable_db_options_.error_if_exists) {
          ret = Status::kErrorUnexpected;
          XENGINE_LOG(WARN, "unexpected error, the dir should not exist as expected", K(ret), K_(dbname));
        }
      }

      if (SUCCED(ret)) {
        if (FAILED(env_->FileExists(IdentityFileName(dbname_)).code())) {
          if (Status::kNotFound != ret) {
            XENGINE_LOG(WARN, "unexpected error, when check identity file exist", K(ret));
          } else {
            //overwite ret as disign
            if (FAILED(SetIdentityFile(env_, dbname_).code())) {
              XENGINE_LOG(WARN, "fail to set identity file", K(ret), K_(dbname));
            }
          }
        }
      }
    }
  }

  if (SUCCED(ret)) {
    const uint64_t t4 = env_->NowMicros();
    if (FAILED(create_default_subtbale(cf_options))) {
      XENGINE_LOG(WARN, "fail to create default subtable", K(ret));
    }
    const uint64_t t5 = env_->NowMicros();
    recovery_debug_info_.prepare_create_default_subtable = t5 - t4;
  }

  const uint64_t t6 = env_->NowMicros();
  recovery_debug_info_.prepare = t6 - t1;
  return ret;
}

int DBImpl::after_recovery()
{
  return calc_max_total_in_memory_state();
}

int DBImpl::set_compaction_need_info() {
  GlobalContext* global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  int ret = 0;
  if (FAILED(get_all_sub_table(all_sub_table, global_ctx))) {
    XENGINE_LOG(WARN, "get all subtable failed", K(ret));
  } else {
    SubTableMap& all_subtables = all_sub_table->sub_table_map_;
    SubTable *sub_table = nullptr;
    for (auto iter = all_subtables.begin();
         Status::kOk == ret && iter != all_subtables.end(); ++iter) {
      if (IS_NULL(sub_table = iter->second)) {
        ret = Status::kCorruption;
        XENGINE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else if (sub_table->IsDropped()) {
        XENGINE_LOG(INFO, "subtable has been dropped", K(iter->first));
      } else {
        if(FAILED(sub_table->set_compaction_check_info(&mutex_))) {
          XENGINE_LOG(WARN, "failed to set compaction check info", K(ret));
        }
      }
    }
  }
  int tmp_ret = ret;
  if (nullptr != global_ctx &&
      FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }
  return ret;
}

int DBImpl::recovery()
{
  int ret = Status::kOk;
  ArenaAllocator recovery_arena(CharArena::DEFAULT_PAGE_SIZE, ModId::kRecovery);

  const uint64_t t1 = env_->NowMicros();
  if (FAILED(storage_logger_->replay(recovery_arena))) {
    XENGINE_LOG(ERROR, "fail to replay manifest log", K(ret));
  } else if (FAILED(set_compaction_need_info())){ // set compaction info
    XENGINE_LOG(WARN, "failed to set compaction need info", K(ret));
  }
  const uint64_t t2 = env_->NowMicros();
  if (SUCC(ret)) {
    if (FAILED(recovery_wal(recovery_arena))) {
      XENGINE_LOG(WARN, "fail to replay wal", K(ret));
    }
  }
  const uint64_t t3 = env_->NowMicros();
  recovery_debug_info_.recovery = t3 - t1;
  recovery_debug_info_.recovery_slog_replay = t2 - t1;
  recovery_debug_info_.recovery_wal = t3 - t2;

  DebugInfoEntry entry;
  entry.key_desc_ = "breakdown recovery time consumed";
  entry.item_id_ = 0;
  entry.timestamp_ = env_->NowMicros();
  entry.debug_info_1_ = recovery_debug_info_.show();
  const std::string recovery_debug_key = "RECOVERY_PERF";
  DebugInfoStation::get_instance()->replace_entry(recovery_debug_key, entry);
  return ret;
}

int DBImpl::create_default_subtbale(const ColumnFamilyOptions &cf_options)
{
  int ret = Status::kOk;
  CreateSubTableArgs args(0, cf_options, true, 0);
  ColumnFamilyData *sub_table = nullptr;

  if (FAILED(versions_->add_sub_table(args, false, true /*is replay*/, sub_table))) {
    XENGINE_LOG(WARN, "fail to create default subtable", K(ret));
  } else if IS_NULL(default_cf_handle_ = MOD_NEW_OBJECT(
      ModId::kDBImpl, ColumnFamilyHandleImpl,sub_table, this, &mutex_)) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for default_cf_handle", K(ret));
  } else if (FAILED(extent_space_manager_->create_table_space(args.table_space_id_))) {
    XENGINE_LOG(WARN, "fail to create table space", K(ret), K(args));
  } else {
    default_cf_internal_stats_ = default_cf_handle_->cfd()->internal_stats();
    single_column_family_mode_ = false;
  }

  return ret;
}

int DBImpl::calc_max_total_in_memory_state()
{
  int ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  ColumnFamilyData *sub_table = nullptr;

  //used when recovery, no need to use threaa local AllSubtableMap
  if (IS_NULL(global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
  } else if (IS_NULL(all_sub_table = global_ctx->all_sub_table_)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, AllSubTable must not nullptr", K(ret));
  } else {
    SubTableMap &sub_table_map = all_sub_table->sub_table_map_;
    for (auto iter = sub_table_map.begin(); SUCCED(ret) && iter != sub_table_map.end(); ++iter) {
      if (IS_NULL(sub_table = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret), "index_id", iter->first);
      } else {
        max_total_in_memory_state_ += sub_table->GetLatestMutableCFOptions()->write_buffer_size *
            sub_table->GetLatestMutableCFOptions()->max_write_buffer_number;
      }
    }
  }

  return ret;
}

int DBImpl::recovery_wal(ArenaAllocator &arena)
{
  int ret = Status::kOk;

  const uint64_t t1 = env_->NowMicros();
  if (FAILED(before_replay_wal_files())) {
    XENGINE_LOG(WARN, "fail to do something before replay wal", K(ret));
  }
  const uint64_t t2 = env_->NowMicros();
  if (SUCC(ret)) {
    if (immutable_db_options_.parallel_wal_recovery) {
      XENGINE_LOG(INFO, "start replaying wal files in parallel");
      ret = parallel_replay_wal_files(arena);
    } else {
      XENGINE_LOG(INFO, "start replaying wal files");
      ret = replay_wal_files(arena);
    }
    if (FAILED(ret)) {
      XENGINE_LOG(WARN, "fail to replay wal", K(ret));
    }
  }
  const uint64_t t3 = env_->NowMicros();
  if (SUCC(ret)) {
    if (FAILED(after_replay_wal_files(arena))) {
      XENGINE_LOG(WARN, "fail to do something after replay wal files", K(ret));
    }
  }
  const uint64_t t4 = env_->NowMicros();
  recovery_debug_info_.recoverywal = t4 - t1;
  recovery_debug_info_.recoverywal_before = t2 - t1;
  recovery_debug_info_.recoverywal_wal_files = t3 - t2;
  recovery_debug_info_.recoverywal_after = t4 - t3;
  return ret;
}

int DBImpl::before_replay_wal_files()
{
  int ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  SubTable *sub_table = nullptr;

  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kCorruption;
    XENGINE_LOG(WARN, "global ctx must not nullptr", K(ret));
  } else {
    SubTableMap &all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
    for (auto iter = all_sub_tables.begin(); Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        XENGINE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else {
        if (sub_table->get_recovery_point().seq_ > max_seq_in_rp_) {
          max_seq_in_rp_ = sub_table->get_recovery_point().seq_;
        }
      }
    }
  }

  XENGINE_LOG(INFO, "max sequence number in recovery point", K_(max_seq_in_rp));

  if (immutable_db_options_.avoid_flush_during_recovery) {
    if (nullptr == (global_ctx = versions_->get_global_ctx())) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "global ctx must not nullptr", K(ret));
    } else {
      SubTableMap& all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
      for (auto iter = all_sub_tables.begin();
           Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
        if (nullptr == (sub_table = iter->second)) {
          ret = Status::kCorruption;
          XENGINE_LOG(WARN, "subtable must not nullptr", K(ret),
                      K(iter->first));
        } else {
          sub_table->mem()->set_no_flush(true);
        }
      }
    }
  }

  versions_->GetColumnFamilySet()->set_during_repaly_wal(true);

  return ret;
}

// this method can only be called when all replay threads are suspend or stopped
void DBImpl::update_last_sequence_during_recovery() {
  mutex_.AssertHeld();
  if (max_sequence_during_recovery_.load() > versions_->LastSequence()) {
    versions_->SetLastSequence(max_sequence_during_recovery_.load());
    versions_->SetLastAllocatedSequence(max_sequence_during_recovery_.load());
    XENGINE_LOG(INFO, "set last sequence", K(max_sequence_during_recovery_.load()),
        "last_sequence", versions_->LastSequence());
  }
  if (max_log_file_number_during_recovery_.load() > logfile_number_) {
    logfile_number_ = max_log_file_number_during_recovery_.load();
    XENGINE_LOG(INFO, "set current replayed logfile_number", K(logfile_number_));
  }
}

int DBImpl::parallel_replay_wal_files(ArenaAllocator &arena) {
  assert(WALRecoveryMode::kPointInTimeRecovery != immutable_db_options_.wal_recovery_mode);
  int ret = Status::kOk;
  uint64_t thread_num = 0;
  if (immutable_db_options_.parallel_recovery_thread_num == 0) {
    auto num_cpus = std::thread::hardware_concurrency();
    thread_num = num_cpus < 4 ? 2 : (num_cpus >> 1);
    thread_num = thread_num > 1024 ? 1024 : thread_num;
  } else {
    thread_num = immutable_db_options_.parallel_recovery_thread_num;
  }
  auto start_t = env_->NowMicros();
  ReplayThreadPool replay_thread_pool(thread_num, this);
  uint64_t read_time = 0;
  uint64_t parse_time = 0;
  uint64_t submit_time = 0;
  std::vector<uint64_t> log_file_numbers;
  if (FAILED(replay_thread_pool.init())) {
    XENGINE_LOG(WARN, "fail to init replay thread pool", K(ret), K(thread_num));
  } else if (FAILED(collect_sorted_wal_file_number(log_file_numbers))) {
      XENGINE_LOG(WARN, "fail to collect sorted wal file numer", K(ret));
  } else {
    uint64_t log_file_number = 0;
    uint64_t next_log_file_number = 0;
    bool last_file = false;
    uint64_t slowest = 0;
    uint64_t logfile_count = log_file_numbers.size();
    for (uint32_t i = 0; SUCC(ret) && i < logfile_count; ++i) {
      const uint64_t t1 = env_->NowMicros();
      log_file_number = log_file_numbers.at(i);
      next_log_file_number = (i < (logfile_count - 1)) ?
          log_file_numbers.at(i + 1) : (log_file_number + 1);
      last_file = ((logfile_count -1) == i);
      if (FAILED(before_replay_one_wal_file(log_file_number))) {
        XENGINE_LOG(WARN, "fail to do something before replay one wal file",
            K(ret), K(i), K(logfile_count), K(log_file_number), K(next_log_file_number));
      } else if (FAILED(parallel_replay_one_wal_file(log_file_number,
              next_log_file_number, last_file, replay_thread_pool, arena,
              &read_time, &parse_time, &submit_time))) {
        XENGINE_LOG(WARN, "fail to replay wal file", K(ret),
            K(logfile_count), K(log_file_number), K(next_log_file_number));
        replay_thread_pool.set_error(); // replay threads will stop if error
      } else {
        XENGINE_LOG(INFO, "success to read one wal file", K(log_file_number), K(next_log_file_number));
      }
      const uint64_t t2 = env_->NowMicros();
      if (t2 - t1 > slowest) {
        slowest = t2 - t1;
      }
    }
    auto replay_last_time = env_->NowMicros() - start_t;
    recovery_debug_info_.recoverywal_file_count = logfile_count;
    recovery_debug_info_.recoverywal_slowest = slowest;
    if (SUCC(ret)) {
      if (FAILED(finish_parallel_replay_wal_files(replay_thread_pool))) {
        XENGINE_LOG(ERROR, "finish_parallel_replay_wal_files failed", K(ret),
            K(max_sequence_during_recovery_), K(max_log_file_number_during_recovery_));
      }
    } else {
       XENGINE_LOG(ERROR, "parallel_replay_wal_files failed", K(ret),
            K(max_sequence_during_recovery_), K(max_log_file_number_during_recovery_));
    }
    auto finish_parallel_replay_wal_files_t = env_->NowMicros() - start_t;
    XENGINE_LOG(INFO, "finish parallel_replay_wal_files", K(replay_last_time), K(read_time),
              K(parse_time), K(submit_time), K(finish_parallel_replay_wal_files_t));
  }
  return ret;
}

int DBImpl::finish_parallel_replay_wal_files(ReplayThreadPool &replay_thread_pool) {
  int ret = Status::kOk;
  if (FAILED(replay_thread_pool.stop())) {
    XENGINE_LOG(WARN, "fail to stop thread pool", K(ret));
  } else if (FAILED(replay_thread_pool.wait_for_all_threads_stopped())) {
    XENGINE_LOG(WARN, "fail to waiting for all replay tasks done", K(ret));
    replay_thread_pool.set_error();
  } else if (FAILED(replay_thread_pool.destroy())) {
    XENGINE_LOG(WARN, "failed to destroy replay_thread_pool", K(ret));
  } else {
    update_last_sequence_during_recovery();
    if (check_if_need_switch_memtable()) { // consume tasks in flush_scheduler_
        std::list<SubTable*> switched_sub_tables;
        if (FAILED(switch_memtable_during_parallel_recovery(switched_sub_tables))) {
          XENGINE_LOG(WARN, "switch memtables failed", K(ret));
        } else if (FAILED(flush_memtable_during_parallel_recovery(switched_sub_tables))) {
          XENGINE_LOG(WARN, "flush memtables failed", K(ret));
        }
    }
  }
  return ret;
}

int DBImpl::replay_wal_files(ArenaAllocator &arena)
{
  int ret = Status::kOk;
  std::vector<uint64_t> log_file_numbers;
  uint64_t log_file_number = 0;
  SequenceNumber next_allocate_sequence = kMaxSequenceNumber;
  bool stop_replay = false;
  uint64_t next_log_file_number = 0;
  uint32_t logfile_count = 0;
  bool last_file = false;
  if (FAILED(collect_sorted_wal_file_number(log_file_numbers))) {
    XENGINE_LOG(WARN, "fail to collect sorted wal file numer", K(ret));
  } else {
    logfile_count = log_file_numbers.size();
    uint64_t slowest = 0;
    for (uint32_t i = 0; SUCCED(ret) && i < logfile_count; ++i) {
      const uint64_t t1 = env_->NowMicros();
      log_file_number = log_file_numbers.at(i);
      next_log_file_number = (i < (logfile_count - 1)) ? log_file_numbers.at(i + 1) : (log_file_number + 1);
      last_file = ((logfile_count -1) == i);
      if (FAILED(before_replay_one_wal_file(log_file_number))) {
        XENGINE_LOG(WARN, "fail to do something before replay one wal file", K(ret), K(i), K(logfile_count), K(log_file_number),
            K(next_log_file_number), K(next_allocate_sequence));
      } else if (FAILED(replay_one_wal_file(log_file_number, last_file, next_allocate_sequence, stop_replay, arena))) {
        XENGINE_LOG(WARN, "fail to replay wal file", K(ret), K(i), K(logfile_count), K(log_file_number), K(next_log_file_number),
            K(next_allocate_sequence));
      } else if (FAILED(after_replay_one_wal_file(next_log_file_number, next_allocate_sequence))) {
        XENGINE_LOG(WARN, "fail to do something after replay one wal file", K(ret), K(i), K(logfile_count), K(log_file_number),
            K(next_log_file_number), K(next_allocate_sequence));
      } else {
        XENGINE_LOG(INFO, "success to replay one wal file", K(log_file_number), K(next_log_file_number));
      }
      XENGINE_LOG(INFO, "replay one log file", K(ret), K(log_file_number), K(next_log_file_number));
      const uint64_t t2 = env_->NowMicros();
      if (t2 - t1 > slowest) {
        slowest = t2 - t1;
      }
    }
    recovery_debug_info_.recoverywal_file_count = logfile_count;
    recovery_debug_info_.recoverywal_slowest = slowest;
  }

  return ret;
}

int DBImpl::after_replay_wal_files(ArenaAllocator &arena)
{
  int ret = Status::kOk;
  GlobalContext* global_ctx = nullptr;
  SubTable* sub_table = nullptr;

  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kCorruption;
    XENGINE_LOG(WARN, "global ctx must not nullptr", K(ret));
  } else {
    SubTableMap& all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
    for (auto iter = all_sub_tables.begin();
         Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        XENGINE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else {
        sub_table->mem()->set_no_flush(false);
      }
    }
  }

  //update the sequence, if the data with max sequence has flush to sst
  if (max_seq_in_rp_ > versions_->LastSequence()) {
    versions_->SetLastSequence(max_seq_in_rp_);
    versions_->SetLastAllocatedSequence(max_seq_in_rp_);
  }

  if (FAILED(create_new_log_writer(arena))) {
    XENGINE_LOG(WARN, "fail to create new log writer", K(ret));
//  } else if (FAILED(init_gc_timer())) {
//    XENGINE_LOG(WARN, "fail to init gc timer", K(ret));
//  } else if (FAILED(init_shrink_timer())) {
//    XENGINE_LOG(WARN, "fail to init shrink timer", K(ret));
  } else {
    versions_->GetColumnFamilySet()->set_during_repaly_wal(false);
  }
  return ret;
}

int DBImpl::collect_sorted_wal_file_number(std::vector<uint64_t> &log_file_numbers)
{
  int ret = Status::kOk;
  std::vector<std::string> file_names;
  uint64_t file_number = 0;
  FileType file_type;

  if (FAILED(env_->GetChildren(immutable_db_options_.wal_dir, &file_names).code())) {
    XENGINE_LOG(WARN, "fail to get files in wal dir", K(ret), "wal_dir", immutable_db_options_.wal_dir);
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < file_names.size(); ++i) {
      if (ParseFileName(file_names[i], &file_number, &file_type) && kLogFile == file_type) {
        log_file_numbers.push_back(file_number);
      }
    }

    if (SUCCED(ret)) {
      std::sort(log_file_numbers.begin(), log_file_numbers.end());
    }
  }

  return ret;
}

int DBImpl::before_replay_one_wal_file(uint64_t current_log_file_number)
{
  int ret = Status::kOk;
  versions_->MarkFileNumberUsedDuringRecovery(current_log_file_number);
  alive_log_files_.push_back(LogFileNumberSize(current_log_file_number));
  return ret;
}

int DBImpl::after_replay_one_wal_file(uint64_t next_log_file_number, SequenceNumber next_allocate_sequence)
{
  int ret = Status::kOk;

  if (next_log_file_number > logfile_number_) {
    logfile_number_ = next_log_file_number;
  }
  if (kMaxSequenceNumber != next_allocate_sequence && next_allocate_sequence > versions_->LastSequence()) {
    versions_->SetLastSequence(next_allocate_sequence - 1);
    versions_->SetLastAllocatedSequence(next_allocate_sequence - 1);
    XENGINE_LOG(INFO, "set last sequence", K(next_allocate_sequence), "last_sequence", versions_->LastSequence());
  }

  if (FAILED(consume_flush_scheduler_task())) {
    XENGINE_LOG(WARN, "fail to consume_flush_scheduler_task", K(ret));
  }
  return ret;
}

int DBImpl::parallel_replay_one_wal_file(uint64_t file_number,
                                         uint64_t next_file_number,
                                         bool last_file,
                                         ReplayThreadPool &replay_thread_pool,
                                         ArenaAllocator &arena,
                                         uint64_t *read_time,
                                         uint64_t *parse_time,
                                         uint64_t *submit_time) {
  assert(WALRecoveryMode::kPointInTimeRecovery != immutable_db_options_.wal_recovery_mode);
  int ret = Status::kOk;
  std::string file_name = LogFileName(immutable_db_options_.wal_dir, file_number);
  XENGINE_LOG(INFO, "begin to read the wal file", K(file_name), "aio_read", immutable_db_options_.enable_aio_wal_reader);
  SequentialFile *file = nullptr;
  EnvOptions tmp_options = env_options_;
  tmp_options.arena = &arena;
  uint64_t file_size;
  if (FAILED(env_->NewSequentialFile(file_name, file, tmp_options).code())) {
    XENGINE_LOG(WARN, "fail to open file", K(ret), K(file_name));
  } else if (FAILED(env_->GetFileSize(file_name, &file_size).code())) {
    XENGINE_LOG(WARN, "fail to get file size", K(ret), K(file_name));
  } else {
    uint64_t last_record_end_pos = 0;
    bool last_record = false;
    Status status;
    LogReporter log_reporter(env_, file_name.c_str(), &status);
    auto file_reader = ALLOC_OBJECT(SequentialFileReader, arena, file, true);
    auto log_reader = ALLOC_OBJECT(log::Reader, arena, file_reader, &log_reporter,
        true/*checksum*/, 0 /*initial_offset*/, file_number, true,
        immutable_db_options_.enable_aio_wal_reader/*use aio*/, file_size);
    auto start_t = env_->NowMicros();
    uint64_t current_t = 0;
#ifndef NDEBUG
      uint64_t read_error_offset = UINT64_MAX;
      TEST_SYNC_POINT_CALLBACK("DBImpl::read_log::read_error", &read_error_offset);
#endif
    while (SUCCED(ret) && status.ok()) {
      Slice record;
      std::string scratch;
      uint32_t record_crc;
      last_record_end_pos = log_reader->get_last_record_end_pos();
      if (!log_reader->ReadRecord(&record, &scratch,
                                immutable_db_options_.wal_recovery_mode,
                                &record_crc)) {
        break; // read current log file finished
      }
#ifndef NDEBUG
      if (last_record_end_pos >= read_error_offset) {
        log_reporter.Corruption(record.size(), Status::Corruption("test error"));
        break;
      }
#endif
      current_t = env_->NowMicros();
      (*read_time) += current_t - start_t;
      start_t = current_t;
      if (WriteBatchInternal::kHeader > record.size()) {
        log_reporter.Corruption(record.size(), Status::Corruption("log record too small"));
        continue;
      } else {
        auto record_size = record.size();
        WriteBatch *replay_write_batch = nullptr;
        Status parse_status = ReplayTaskParser::parse_replay_writebatch_from_record(
                                  record, record_crc, this, &replay_write_batch,
                                  immutable_db_options_.allow_2pc);
        current_t = env_->NowMicros();
        (*parse_time) += current_t - start_t;
        start_t = current_t;
        if (!parse_status.ok()) {
          ret = parse_status.code();
          XENGINE_LOG(ERROR, "parse and submit replay task failed", K(ret),
                      "error msg", parse_status.getState());
          log_reporter.Corruption(record_size, parse_status);
        } else if (FAILED(replay_thread_pool.build_and_submit_task(replay_write_batch,
                          file_number, last_file && log_reader->is_real_eof(),
                          last_record_end_pos, arena))) {
          XENGINE_LOG(ERROR, "submit replay task to thread pool failed", K(ret));
        }
        current_t = env_->NowMicros();
        (*submit_time) += current_t - start_t;
        start_t = current_t;
      }
    }
    if (!status.ok()) {
      last_record = last_file && log_reader->IsEOF();
      bool stop_replay = false; // useless
      ret = deal_with_log_record_corrution(immutable_db_options_.wal_recovery_mode,
                                           file_name,
                                           last_record,
                                           last_record_end_pos,
                                           stop_replay);
    }
    FREE_OBJECT(Reader, arena, log_reader);
    FREE_OBJECT(SequentialFileReader, arena, file_reader); // file_reader won't be deleted by log_reader if use allocator
  }
  return ret;
}

int DBImpl::replay_one_wal_file(uint64_t file_number,
                                bool last_file,
                                SequenceNumber &next_allocate_sequence,
                                bool &stop_replay,
                                ArenaAllocator &arena)
{
  int ret = Status::kOk;
  Status status;
  std::string file_name = LogFileName(immutable_db_options_.wal_dir, file_number);
  std::string scratch;
  Slice record;
  WriteBatch batch;
//  unique_ptr<SequentialFile> file;
  SequentialFile *file = nullptr;
  SequentialFileReader *file_reader = nullptr;
  log::Reader *log_reader = nullptr;
  LogReporter log_reporter(env_, file_name.c_str(), &status);
  SequenceNumber first_seq_in_write_batch = 0;
  uint64_t last_record_end_pos = 0;
  bool is_last_record = false; 

  XENGINE_LOG(INFO, "begin replay the wal file", K(file_name));
  EnvOptions tmp_options = env_options_;
  tmp_options.arena = &arena;
  uint64_t file_size;
  if (FAILED(env_->NewSequentialFile(file_name, file, tmp_options).code())) {
    XENGINE_LOG(WARN, "fail to open file", K(ret), K(file_name));
  } else if (FAILED(env_->GetFileSize(file_name, &file_size).code())) {
    XENGINE_LOG(WARN, "fail to get file size", K(ret), K(file_name));
  }  else {
    file_reader = ALLOC_OBJECT(SequentialFileReader, arena, file, true);
    log_reader = ALLOC_OBJECT(log::Reader, arena, file_reader, &log_reporter,
        true /*checksum*/, 0 /*initial_offset*/, file_number, true,
        immutable_db_options_.enable_aio_wal_reader, file_size);
    while (SUCCED(ret)
           && log_reader->ReadRecord(&record, &scratch, immutable_db_options_.wal_recovery_mode)
           && status.ok()) {
      last_record_end_pos = log_reader->get_last_record_end_pos();
      if (WriteBatchInternal::kHeader > record.size()) {
        //TODO:yuanfeng
        log_reporter.Corruption(record.size(), Status::Corruption("log record too small"));
        continue;
      } else if (FAILED(WriteBatchInternal::SetContents(&batch, record).code())) {
        XENGINE_LOG(WARN, "fail to set batch contents", K(ret))	;
      } else {
        //check if continue to replay or not, when under kPointInTimeRecovery recovery mode
        if (stop_replay && WALRecoveryMode::kPointInTimeRecovery == immutable_db_options_.wal_recovery_mode) {
          if (next_allocate_sequence == WriteBatchInternal::Sequence(&batch)) {
            stop_replay = false;
          } else {
            break;
          }
        }
        if (FAILED(recovery_write_memtable(batch, file_number, next_allocate_sequence))) {
          XENGINE_LOG(WARN, "fail to replay write memtable", K(ret), K(file_number), K(next_allocate_sequence));
        }
      }
    }

    if (!status.ok()) {
      is_last_record = last_file && log_reader->IsEOF();
      ret = deal_with_log_record_corrution(immutable_db_options_.wal_recovery_mode,
                                           file_name,
                                           is_last_record,
                                           last_record_end_pos,
                                           stop_replay);
    }
    FREE_OBJECT(SequentialFileReader, arena, file_reader);
    FREE_OBJECT(Reader, arena, log_reader);
  }
  return ret;
}

int DBImpl::consume_flush_scheduler_task(int *schedule_flush_num) {
  mutex_.AssertHeld();
  int ret = Status::kOk;
  ColumnFamilyData *sub_table = nullptr;

  while(SUCCED(ret) && (nullptr != (sub_table = flush_scheduler_.TakeNextColumnFamily()))) {
    if (!immutable_db_options_.avoid_flush_during_recovery) {
      ret = recovery_switch_and_flush_memtable(sub_table);
      if (SUCCED(ret) && schedule_flush_num != nullptr) {
        ++(*schedule_flush_num);
      }
      if (sub_table->Unref()) {
        MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
      }
    }
  }
  if (SUCCED(ret)) {
    flush_scheduler_.Clear();
  }

  return ret;
}

int DBImpl::recovery_switch_memtable(ColumnFamilyData *sub_table)
{
  int ret = Status::kOk;
  MemTable *new_mem = nullptr;
  WriteContext write_context;
  RecoveryPoint recovery_point(static_cast<int64_t>(logfile_number_), versions_->LastSequence());

  if (IS_NULL(sub_table)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(sub_table));
  } else if (IS_NULL(new_mem = sub_table->ConstructNewMemtable(*sub_table->GetLatestMutableCFOptions(), versions_->LastSequence()))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "fail to construct new memtable", K(ret));
  } else {
    sub_table->mem()->set_recovery_point(recovery_point);
    sub_table->imm()->Add(sub_table->mem(), &write_context.memtables_to_free_);
    sub_table->set_imm_largest_seq(sub_table->mem()->get_last_sequence_number());
    new_mem->Ref();
    sub_table->SetMemtable(new_mem);
  }

  return ret;
}

int DBImpl::recovery_write_memtable(WriteBatch &batch, uint64_t current_log_file_number, SequenceNumber &next_allocate_sequence)
{
  int ret = Status::kOk;
  SequenceNumber first_seq_in_write_batch = 0;

  ColumnFamilyMemTablesImpl column_family_memtables(versions_->global_ctx_->all_sub_table_->sub_table_map_, versions_->GetColumnFamilySet());

  if (WALRecoveryMode::kPointInTimeRecovery == immutable_db_options_.wal_recovery_mode
      && kMaxSequenceNumber != next_allocate_sequence
      && next_allocate_sequence != (first_seq_in_write_batch = WriteBatchInternal::Sequence(&batch))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, seq not continuously", K(ret), K(next_allocate_sequence), K(first_seq_in_write_batch));
  } else if (FAILED(WriteBatchInternal::InsertInto(&batch, &column_family_memtables, &flush_scheduler_, true,
          current_log_file_number, this, false /*concurrent_memtable_writes*/,
          &next_allocate_sequence, nullptr /*has_valid_write*/, &missing_subtable_during_recovery_).code())) {
    XENGINE_LOG(WARN, "fail to replay write memtable", K(ret), K(current_log_file_number), K(next_allocate_sequence));
  }

   return ret;
}

int DBImpl::recovery_flush(ColumnFamilyData *sub_table)
{
  int ret = Status::kOk;
  const MutableCFOptions mutable_cf_options = *sub_table->GetLatestMutableCFOptions();
  JobContext job_context(next_job_id_.fetch_add(1), storage_logger_, true);
  STFlushJob st_flush_job(sub_table, nullptr, FLUSH_TASK, true /*need_check_snapshot*/);
 
  if (FAILED(FlushMemTableToOutputFile(st_flush_job, mutable_cf_options, nullptr, job_context).code())) {
    XENGINE_LOG(WARN, "fail to flush memtable", K(ret));
  } else {
    job_context.Clean();
  }

  return ret;
}

bool DBImpl::check_if_need_switch_memtable() {
  bool ret = false;
#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("DBImpl::check_if_need_switch_memtable::trigger_switch_memtable", this);
  if (TEST_trigger_switch_memtable_) {
    return true;
  }
#endif
  if (immutable_db_options_.avoid_flush_during_recovery) {
    ret = false;
  } else if (check_switch_memtable_now()) {
    if (flush_scheduler_.size() >= MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME ||
        write_buffer_manager_->should_trim()) {
      no_switch_round_ = 0;
      ret = true;
    } else if (!flush_scheduler_.Empty()) {
      if (++no_switch_round_ >= MAX_NO_SWITCH_ROUND) {
        no_switch_round_ = 0;
        ret = true;
      }
    }
  }
  return ret;
}

// this method can only be called by recovery main thread
bool DBImpl::check_switch_memtable_now() {
  mutex_.AssertHeld();
  if (last_check_time_during_recovery_ == 0) {
    last_check_time_during_recovery_ = env_->NowMicros();
  }
  bool ret = false;
  auto current_timestamp = env_->NowMicros();
  if (current_timestamp > last_check_time_during_recovery_ + CHECK_NEED_SWITCH_DELTA) {
    last_check_time_during_recovery_ = current_timestamp;
    ret = true;
  }
  return ret;
}

int DBImpl::switch_memtable_during_parallel_recovery(std::list<SubTable*>& switched_sub_tables) {
  mutex_.AssertHeld();
  XENGINE_LOG(INFO, "start switching memtable during recovery", "total_memory_usage",
      write_buffer_manager_->total_memory_usage());
  int ret = Status::kOk;
  if (immutable_db_options_.avoid_flush_during_recovery) {
    return ret;
  }

  // since all replay threads are suspend, we can safely update last_sequence_
  // here to advance recovery point during switching memtable
  update_last_sequence_during_recovery();

  // 1. consume flush tasks in flush_scheduler
  int schedule_switch_num = 0;
  ColumnFamilyData *sub_table = nullptr;
  while(SUCCED(ret) && (nullptr != (sub_table = flush_scheduler_.TakeNextColumnFamily()))) {
    ret = recovery_switch_memtable(sub_table);
    if (SUCCED(ret)) {
      switched_sub_tables.push_back(sub_table);
      ++schedule_switch_num;
    } else {
      if (sub_table->Unref()) {
        MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
      }
    }
  }
  if (FAILED(ret)) {
    XENGINE_LOG(WARN, "fail to consume flush scheduler task", K(ret));
    return ret;
  }
  flush_scheduler_.Clear();
  if (schedule_switch_num >= MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME) {
    // we have already flush enough subtables
    XENGINE_LOG(INFO, "finished switching", K(schedule_switch_num));
    return ret;
  }

  // 2. switch subtables with the largest memory usage
  uint64_t expected_pick_num = MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME - schedule_switch_num;
  GlobalContext *global_ctx = versions_->get_global_ctx();
  sub_table = nullptr;
  std::set<uint32_t> picked_cf_ids;
  uint64_t picked_switch_num = 0;
  if (nullptr == global_ctx || nullptr == global_ctx->all_sub_table_) {
    ret = Status::kCorruption;
    XENGINE_LOG(WARN, "global ctx must not nullptr", K(ret));
  } else {
    SubTableMap &all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
    ReplayThreadPool::SubtableMemoryUsageComparor mem_cmp;
    BinaryHeap<SubTable*, ReplayThreadPool::SubtableMemoryUsageComparor> max_sub_tables;
    if (FAILED(pick_and_switch_subtables(all_sub_tables, picked_cf_ids,
                                        expected_pick_num, mem_cmp,
                                        &picked_switch_num, &max_sub_tables,
                                        switched_sub_tables))) {
      XENGINE_LOG(WARN, "fail to switch subtables with largest memory usage", K(ret));
      return ret;
    }
  }
  schedule_switch_num += picked_switch_num;
  if (schedule_switch_num >= MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME) {
    // we have already flush enough subtables
    XENGINE_LOG(INFO, "finished switching", K(schedule_switch_num));
    return ret;
  }

  // 3. switch oldest subtale
  SubTableMap &all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
  expected_pick_num = MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME - schedule_switch_num;
  ReplayThreadPool::SubtableSeqComparor seq_cmp;
  BinaryHeap<SubTable*, ReplayThreadPool::SubtableSeqComparor> oldest_sub_tables;
  if (FAILED(pick_and_switch_subtables(all_sub_tables, picked_cf_ids,
                                      expected_pick_num, seq_cmp,
                                      &picked_switch_num, &oldest_sub_tables,
                                      switched_sub_tables))) {
    XENGINE_LOG(WARN, "fail to switch oldest subtables", K(ret));
    return ret;
  }
  schedule_switch_num += picked_switch_num;
  XENGINE_LOG(INFO, "finished switching", K(schedule_switch_num));
  return ret;
}

template <typename Compare>
int DBImpl::pick_and_switch_subtables(const SubTableMap& all_sub_tables,
                                     std::set<uint32_t>& picked_cf_ids,
                                     uint64_t expected_pick_num,
                                     Compare& cmp,
                                     uint64_t *picked_num,
                                     BinaryHeap<SubTable*, Compare> *picked_sub_tables,
                                     std::list<SubTable*>& switched_sub_tables) {
  int ret = Status::kOk;
  *picked_num = 0;
  SubTable *sub_table = nullptr;
  for (auto iter = all_sub_tables.begin();
      SUCCED(ret) && iter != all_sub_tables.end(); ++iter) {
    if (UNLIKELY(nullptr == (sub_table = iter->second) || nullptr == sub_table->mem())) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "subtable must not be nullptr", K(ret), K(iter->first), KP(sub_table), K(*sub_table));
    } else {
      if (sub_table->mem()->IsEmpty()) {
        continue;
      }
      if (picked_cf_ids.find(sub_table->GetID()) != picked_cf_ids.end()) {
        continue;
      }
      if (picked_sub_tables->size() < expected_pick_num) {
        picked_sub_tables->push(sub_table);
      } else {
        if (cmp(sub_table, picked_sub_tables->top())) {
          picked_sub_tables->replace_top(sub_table);
        }
      }
    }
  }
#ifndef NDEBUG
  if (TEST_avoid_flush_) {
    return ret;
  }
#endif
  while(SUCCED(ret) && !picked_sub_tables->empty()) {
    SubTable *picked_sub_table = picked_sub_tables->top();
    picked_sub_tables->pop();
    auto sub_table_id = picked_sub_table->GetID();
    auto mem_usage = picked_sub_table->mem()->ApproximateMemoryUsage();
    auto first_seq = picked_sub_table->mem()->GetFirstSequenceNumber();
    ret = recovery_switch_memtable(picked_sub_table);
    XENGINE_LOG(DEBUG, "switched subtable", K(sub_table_id), K(mem_usage), K(first_seq), K(ret));
    if (SUCCED(ret)) {
      picked_cf_ids.insert(sub_table_id);
      picked_sub_table->Ref();
      switched_sub_tables.push_back(picked_sub_table);
      ++(*picked_num);
    } else {
      XENGINE_LOG(ERROR, "failed to switch memtable during recovery", K(sub_table_id), K(ret));
    }
  }
  picked_sub_tables->clear();
  return ret;
}

int DBImpl::flush_memtable_during_parallel_recovery(std::list<SubTable*>& switched_sub_tables) {
  int ret = Status::kOk;
  if (!write_buffer_manager_->should_trim()) {
    for (auto sub_table : switched_sub_tables) {
      if (sub_table->Unref()) {
        MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
      }
    }
    switched_sub_tables.clear();
    return ret; // no need to flush memtable during parallel recovery
  }
  for (auto sub_table : switched_sub_tables) {
    if (SUCCED(ret)) {
      ret = recovery_flush(sub_table);
    }
    if (FAILED(ret)) {
      XENGINE_LOG(WARN, "failed to flush subtable", "sub_table_id", sub_table->GetID());
    }
    if (sub_table->Unref()) {
      MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
    }
  }
  XENGINE_LOG(INFO, "finish flushing subtables during recovery", "schedule_flush_num",
              switched_sub_tables.size(), "total_memory_usage",
              write_buffer_manager_->total_memory_usage());
  switched_sub_tables.clear();
  return ret;
}

int DBImpl::recovery_switch_and_flush_memtable(ColumnFamilyData *sub_table) {
  int ret = Status::kOk;
  if (FAILED(recovery_switch_memtable(sub_table))) {
    XENGINE_LOG(WARN, "fail to replay switch memtable", K(ret), "index_id", sub_table->GetID());
  } else if (FAILED(recovery_flush(sub_table))) {
    XENGINE_LOG(WARN, "fail to recovery flush", K(ret), "index_id", sub_table->GetID());
  }
  return ret;
}

int DBImpl::parallel_recovery_write_memtable(WriteBatch &batch,
    uint64_t current_log_file_number, uint64_t* next_allocate_sequence) {
  int ret = Status::kOk;
  GlobalContext* global_ctx = nullptr;
  AllSubTable* all_sub_table = nullptr;
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
  } else {
    ColumnFamilyMemTablesImpl column_family_memtables(all_sub_table->sub_table_map_,
        versions_->GetColumnFamilySet());
    Status s = WriteBatchInternal::InsertInto(&batch, &column_family_memtables,
        &flush_scheduler_, true, current_log_file_number, this,
        true/*concurrent_memtable_writes*/, next_allocate_sequence,
        nullptr /*has_valid_write*/, &missing_subtable_during_recovery_);
    if (FAILED(s.code())) {
      XENGINE_LOG(WARN, "fail to replay write memtable", K(ret),
          K(current_log_file_number), K(*next_allocate_sequence),
          "errormsg", s.getState());
    }
  }
  if (nullptr != global_ctx) {
    global_ctx->release_thread_local_all_sub_table(all_sub_table);
  }
  return ret;
}

int DBImpl::update_max_sequence_and_log_number(uint64_t next_allocate_sequence,
    uint64_t current_log_file_number) {
  int ret = Status::kOk;
  uint64_t current_max_sequence = next_allocate_sequence - 1;
  while(true) {
    uint64_t old_max_sequence = max_sequence_during_recovery_.load();
    if (current_max_sequence > old_max_sequence) {
      if (max_sequence_during_recovery_.compare_exchange_strong(
                old_max_sequence, current_max_sequence)) {
        break;
      }
    } else {
      break;
    }
  }
  while(true) {
    uint64_t old_logfile_number = max_log_file_number_during_recovery_.load();
    if (current_log_file_number > old_logfile_number) {
      if (max_log_file_number_during_recovery_.compare_exchange_strong(
            old_logfile_number, current_log_file_number)) {
        break;
      }
    } else {
      break;
    }
  }
  return ret;
}

int DBImpl::deal_with_log_record_corrution(WALRecoveryMode recovery_mode,
                                           const std::string file_name,
                                           bool is_last_record,
                                           uint64_t last_record_end_pos,
                                           bool &stop_replay) {
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  uint64_t origin_file_size = 0;

  //we support three recovery mode and use kAbsoluteConsistency as default mode:
  //kAbsoluteConsistency : absolute consistency mode. require all wal files are correct, recovery to the state before shutdown.
  //                       stop replay and system exit, when find incorrect wal file
  //kPointInTimeRecovery: maximize consistency mode. recovery to point-in-time consistency
  //                      stop replay and system start, when find incorrect wal file
  //kSkipAnyCorruptedRecords: maximize recovery data mode. recovery as much data as possible.
  //                          continue replay and ignore the incorrect wal file
  if (WALRecoveryMode::kAbsoluteConsistency == recovery_mode) {
    if (is_last_record) {
      util::MutexLock lock_guard(&deal_last_record_error_mutex_);
      if (FAILED(env_->GetFileSize(file_name, &origin_file_size).code())) {
        XENGINE_LOG(WARN, "fail get file_size", K(ret), K(origin_file_size));
      } else if (origin_file_size < last_record_end_pos) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "can't truncate current file", K(ret), K(origin_file_size), K(last_record_end_pos));
      } else if (0 != (tmp_ret = truncate(file_name.c_str(), last_record_end_pos))) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "fail to truncate wal file", K(tmp_ret), K(file_name), K(origin_file_size), K(last_record_end_pos),
            "errno", errno, "err_msg", strerror(errno));
      } else {
        XENGINE_LOG(INFO, "success to truncate wal file", K(file_name), K(origin_file_size), K(last_record_end_pos));
      }
    } else {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, only last record may corruption under kAbsoluteConsistency recovery mode",
          K(ret), K(file_name), K(last_record_end_pos));
    }
  } else if (WALRecoveryMode::kPointInTimeRecovery == recovery_mode) {
    //kPointInTimeRecovery only attention consistency, don't care log record corruption
    //if the sequence is continuously with the next wal file, reset stop_replay to false, and continue to replay
    stop_replay = true;
  } else if (WALRecoveryMode::kSkipAnyCorruptedRecords == recovery_mode) {
    //kSkipAnyCorruptedRecords ignore any log record corruption
  } else {
    ret = Status::kNotSupported;
    XENGINE_LOG(WARN, "the recovery mode is not supported", K(ret), KE(immutable_db_options_.wal_recovery_mode));
  }

  return ret;
}

int DBImpl::create_new_log_writer(ArenaAllocator &arena)
{
  int ret = Status::kOk;
  UNUSED(arena);
  uint64_t new_log_number = versions_->NewFileNumber();
  std::string log_file_name =  LogFileName(immutable_db_options_.wal_dir, new_log_number);
  EnvOptions env_options(initial_db_options_);
  EnvOptions opt_env_options = immutable_db_options_.env->OptimizeForLogWrite(
      env_options, BuildDBOptions(immutable_db_options_, mutable_db_options_));
  //  unique_ptr<WritableFile> write_file;
  //  unique_ptr<util::ConcurrentDirectFileWriter> concurrent_file_writer;
  WritableFile *write_file = nullptr;
  ConcurrentDirectFileWriter *concurrent_file_writer = nullptr;
  log::Writer *log_writer = nullptr;

  if (FAILED(NewWritableFile(immutable_db_options_.env, log_file_name, write_file, opt_env_options).code())) {
    XENGINE_LOG(WARN, "fail to create write file", K(ret), K(new_log_number), K(log_file_name));
  } else {
    write_file->SetPreallocationBlockSize(GetWalPreallocateBlockSize(32 * 1024));
    //    concurrent_file_writer.reset(new util::ConcurrentDirectFileWriter(std::move(write_file), opt_env_options));
    concurrent_file_writer = MOD_NEW_OBJECT(ModId::kDBImplWrite, ConcurrentDirectFileWriter, write_file, opt_env_options);
    if (FAILED(concurrent_file_writer->init_multi_buffer().code())) {
      XENGINE_LOG(WARN, "fail to init multi buffer", K(ret), K(new_log_number), K(log_file_name));
    } else if (IS_NULL(log_writer = MOD_NEW_OBJECT(ModId::kDBImplWrite, log::Writer, concurrent_file_writer,
            new_log_number, immutable_db_options_.recycle_log_file_num > 0, false/*not free mem*/))) {
      ret = Status::kMemoryLimit;
      XENGINE_LOG(WARN, "fail to allocate memory for log_writer", K(ret));
    } else {
      logfile_number_ = new_log_number;
      logs_.emplace_back(new_log_number, log_writer);
      alive_log_files_.push_back(LogFileNumberSize(logfile_number_));
    }
  }

  return ret;
}

//int DBImpl::init_gc_timer()
//{
//  int ret = Status::kOk;
//  const uint64_t GC_SCHEDULER_INTERVAL = 5 * 60 * 1000; //5 minute
//  std::function<int(void)> schedule_dump_func = std::bind(&DBImpl::schedule_gc, this);
//
//  if (IS_NULL(gc_timer_ = MOD_NEW_OBJECT(ModId::kDefaultMod,
//                                         Timer,
//                                         timer_service_,
//                                         GC_SCHEDULER_INTERVAL,
//                                         Timer::Repeatable,
//                                         schedule_dump_func))) {
//    ret = Status::kMemoryLimit;
//    XENGINE_LOG(WARN, "fail to allocate memory for gc timer", K(ret));
//    assert(false);
//  } else {
//    gc_timer_->start();
//    XENGINE_LOG(INFO, "success to init gc timer");
//  }
//  return ret;
//}
//
//int DBImpl::init_shrink_timer()
//{
//  int ret = Status::kOk;
//  const uint64_t SHRINK_SCHEDULER_INTERVAL = 10 * 60 * 1000; //10 minute
//  std::function<int(void)> schedule_shrink_func = std::bind(&DBImpl::schedule_shrink, this);
//
//  if (IS_NULL(shrink_timer_ = MOD_NEW_OBJECT(ModId::kDefaultMod,
//                                             Timer,
//                                             timer_service_,
//                                             SHRINK_SCHEDULER_INTERVAL,
//                                             Timer::Repeatable,
//                                             schedule_shrink_func))) {
//    ret = Status::kMemoryLimit;
//    XENGINE_LOG(WARN, "fail to allocate memory for shrink timer", K(ret));
//  } else {
//    shrink_timer_->start();
//    XENGINE_LOG(INFO, "success to init shrink timer");
//  }
//
//  return ret;
//}

/*
Status DBImpl::WriteLevel0TableForRecovery(int job_id, ColumnFamilyData* cfd, MemTable* mem) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  uint64_t bytes_written = 0;
  MiniTables mtables;
  mtables.space_manager = cfd->get_extent_space_manager();
  auto pending_outputs_inserted_elem =
      CaptureCurrentFileNumberInPendingOutputs();
  ReadOptions ro;
  ro.total_order_seek = true;
  Arena arena;
  Status s;
  TableProperties table_properties;
  {
    ScopedArenaIterator iter(mem->NewIterator(ro, &arena));
    __XENGINE_LOG(DEBUG, "[%s] [WriteLevel0TableForRecovery]: started", cfd->GetName().c_str());

    // Get the latest mutable cf options while the mutex is still locked
    const MutableCFOptions mutable_cf_options =
        *cfd->GetLatestMutableCFOptions();
    bool paranoid_file_checks =
        cfd->GetLatestMutableCFOptions()->paranoid_file_checks;
    {
      mutex_.Unlock();

      SequenceNumber earliest_write_conflict_snapshot;
      std::vector<SequenceNumber> snapshot_seqs =
          GetAll(&earliest_write_conflict_snapshot);

      EnvOptions optimized_env_options = env_->OptimizeForCompactionTableWrite(
          env_options_, immutable_db_options_);
      s = BuildTable(
          dbname_, env_, *cfd->ioptions(), mutable_cf_options,
          optimized_env_options, cfd->table_cache(), iter.get(),
          std::unique_ptr<InternalIterator>(mem->NewRangeTombstoneIterator(ro)),
          &mtables, cfd->internal_comparator(),
          cfd->int_tbl_prop_collector_factories(), cfd->GetID(), cfd->GetName(),
          snapshot_seqs, earliest_write_conflict_snapshot,
          GetCompressionFlush(*cfd->ioptions(), mutable_cf_options, 0),
          cfd->ioptions()->compression_opts, paranoid_file_checks,
          cfd->internal_stats(), TableFileCreationReason::kRecovery, job_id);
      // LogFlush(immutable_db_options_.info_log);
      if (s.ok()) {
        for (auto& meta : mtables.metas) bytes_written += meta.fd.GetFileSize();
        __XENGINE_LOG(INFO,
                      "[%s] [WriteLevel0TableForRecovery]"
                      " to %" PRIu64
                      " mini tables, "
                      "totally %" PRIu64 " bytes: %s",
                      cfd->GetName().c_str(), mtables.metas.size(),
                      bytes_written, s.ToString().c_str());
      } else {
        __XENGINE_LOG(INFO, "[%s] [WriteLevel0TableForRecovery]: failed", cfd->GetName().c_str());
      }

      mutex_.Lock();
    }
  }
  ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok()) {
    // record the change meta
    //s = change_info.add(static_cast<int32_t>(cfd->GetID()), 0, mtables);
  }
  InternalStats::CompactionStats stats(1);
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = bytes_written;
  stats.num_output_files = mtables.metas.size();
  cfd->internal_stats()->AddCompactionStats(level, stats);
  cfd->internal_stats()->AddCFStats(InternalStats::BYTES_FLUSHED,
                                    bytes_written);
  return s;
}
*/

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DB::Open(db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
//    delete handles[0];
    MOD_DELETE_OBJECT(ColumnFamilyHandle, handles[0]);
  }
  return s;
}

Status DB::Open(const DBOptions& db_options, const std::string& dbname,
                const std::vector<ColumnFamilyDescriptor>& column_families,
                std::vector<ColumnFamilyHandle*>* handles, DB** dbptr) {
  Status s = SanitizeOptionsByTable(db_options, column_families);
  if (!s.ok()) {
    return s;
  }

  s = ValidateOptions(db_options, dbname, column_families);
  if (!s.ok()) {
    return s;
  }

  *dbptr = nullptr;
  handles->clear();

  size_t max_write_buffer_size = 0;
  for (auto cf : column_families) {
    max_write_buffer_size =
        std::max(max_write_buffer_size, cf.options.write_buffer_size);
  }

//  DBImpl* impl = new DBImpl(db_options, dbname);
  DBImpl *impl = MOD_NEW_OBJECT(ModId::kDBImpl, DBImpl, db_options, dbname);
  s = impl->env_->CreateDirIfMissing(impl->immutable_db_options_.wal_dir);
  if (s.ok()) {
    for (auto db_path : impl->immutable_db_options_.db_paths) {
      s = impl->env_->CreateDirIfMissing(db_path.path);
      if (!s.ok()) {
        break;
      }
    }
  }

  if (!s.ok()) {
//    delete impl;
    MOD_DELETE_OBJECT(DBImpl, impl);
    return s;
  }
  AllocMgr *alloc = AllocMgr::get_instance();
  alloc->set_mod_size_limit(ModId::kMemtable, impl->immutable_db_options_.db_total_write_buffer_size);
  int64_t task_mem_limit = 128 * 1024 * 1024L;
  alloc->set_mod_size_limit(ModId::kCompaction, impl->mutable_db_options_.max_background_compactions * task_mem_limit);
  //TODO:@yuanfeng, init in the handler layer
  if (!logger::Logger::get_log().is_inited()) {
    std::string info_log_path = dbname + "/Log";
    logger::Logger::get_log().init(info_log_path.c_str(),
        logger::InfoLogLevel::INFO_LEVEL,
        256 * 1024 * 1024);
  }

  int64_t ret = Status::kOk;
  char *tmp_buf = nullptr;
  ColumnFamilyOptions cf_options = column_families.back().options;
  Options options(db_options, cf_options);
  GlobalContext *gctx = nullptr;
  if (nullptr != cf_options.table_factory) {
    cache::Cache *block_cache =
        static_cast<ExtentBasedTableFactory *>(cf_options.table_factory.get())->table_options().block_cache.get();
    if (nullptr != block_cache) {
      alloc->set_mod_size_limit(ModId::kDataBlockCache, block_cache->GetCapacity());
    }
  }
  if (IS_NULL(impl->table_cache_.get())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, table cache must not nullptr", K(ret));
  } else if (IS_NULL(impl->storage_logger_ = MOD_NEW_OBJECT(ModId::kStorageLogger, StorageLogger))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for StorageLogger", K(ret));
  } else if (IS_NULL(impl->extent_space_manager_ = MOD_NEW_OBJECT(ModId::kExtentSpaceMgr,
                                                                  ExtentSpaceManager,
                                                                  impl->env_,
                                                                  impl->env_options_,
                                                                  impl->initial_db_options_))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for ExtentSpaceManager", K(ret));
  } else if (IS_NULL(gctx = MOD_NEW_OBJECT(ModId::kDefaultMod, GlobalContext, dbname, options))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for GlobalContext", K(ret));
  } else if (FAILED(impl->storage_logger_->init(impl->env_,
                                                dbname,
                                                impl->env_options_,
                                                impl->immutable_db_options_,
                                                impl->versions_.get(),
                                                impl->extent_space_manager_,
                                                64 * 1024 * 1024))) {
    XENGINE_LOG(WARN, "fail to init StorageLogger", K(ret));
  } else if (FAILED(impl->extent_space_manager_->init(impl->storage_logger_))) {
    XENGINE_LOG(WARN, "fail to init ExtentSpaceMgr", K(ret));
  } else {
    gctx->options_ = options;
    gctx->env_options_ = impl->env_options_;
    gctx->env_ = impl->env_;
    gctx->cache_ = impl->table_cache_.get();
    gctx->write_buf_mgr_ = impl->write_buffer_manager_;
    gctx->storage_logger_ = impl->storage_logger_;
    gctx->extent_space_mgr_ = impl->extent_space_manager_;
    gctx->reset_thread_local_all_sub_table();
    gctx->db_dir_ = impl->directories_.GetDbDir();
    if (FAILED(impl->versions_->init(gctx))) {
      XENGINE_LOG(WARN, "fail to init versions", K(ret));
    }
  }
  s = Status(ret);
  if (!s.ok()) {
    MOD_DELETE_OBJECT(DBImpl, impl);
    return s;
  }

  s = impl->CreateArchivalDirectory();
  if (!s.ok()) {
//    delete impl;
    MOD_DELETE_OBJECT(DBImpl, impl);
    return s;
  }
  impl->mutex_.Lock();
  // Handles create_if_missing, error_if_exists
  s = impl->Recover(column_families, cf_options);
  if (s.ok()) {
    SubTable* sub_table = nullptr;
    for (auto iter :
         impl->versions_->get_global_ctx()->all_sub_table_->sub_table_map_) {
      handles->push_back(
          MOD_NEW_OBJECT(ModId::kColumnFamilySet, ColumnFamilyHandleImpl, iter.second, impl, &impl->mutex_));
//          new ColumnFamilyHandleImpl(iter.second, impl, &impl->mutex_));
      sub_table = iter.second;
      impl->NewThreadStatusCfInfo(sub_table);
      // no compaction schedule in recovery
      SuperVersion *old_sv = sub_table->InstallSuperVersion(
          MOD_NEW_OBJECT(ModId::kSuperVersion, SuperVersion), &impl->mutex_,
          *sub_table->GetLatestMutableCFOptions());
      MOD_DELETE_OBJECT(SuperVersion, old_sv);
//      delete sub_table->InstallSuperVersion(
//          new SuperVersion(), &impl->mutex_,
//          *sub_table->GetLatestMutableCFOptions());
    }
    impl->alive_log_files_.push_back(
        DBImpl::LogFileNumberSize(impl->logfile_number_));
    impl->DeleteObsoleteFiles();
    s = impl->directories_.GetDbDir()->Fsync();
  }
  // init BatchGroupManager;
  impl->batch_group_manager_.init();

  if (0 != impl->pipline_manager_.init_pipline_queue()) {
    __XENGINE_LOG(ERROR, "Failed to init pipline queue, probably memory limit");
    s = Status::MemoryLimit("init pipline queue failed");
    assert(s.ok());  // failed directly in dbug mode
  }

  if (s.ok()) {
    SubTable* sub_table = nullptr;
    for (auto iter :
         impl->versions_->get_global_ctx()->all_sub_table_->sub_table_map_) {
      sub_table = iter.second;

      auto* table_factory = dynamic_cast<table::ExtentBasedTableFactory*>(
          sub_table->ioptions()->table_factory);
      if (table_factory != nullptr) {
        BlockBasedTableOptions table_opts = table_factory->table_options();
        sub_table->ioptions()->filter_manager->start_build_thread(
            sub_table, impl, &(impl->mutex_), &(impl->versions_->env_options()),
            impl->env_, table_opts.filter_policy, table_opts.block_cache,
            table_opts.whole_key_filtering,
            table_opts.cache_index_and_filter_blocks_with_high_priority,
            db_options.filter_queue_stripes, db_options.filter_building_threads,
            &(impl->filter_build_quota_));
      }
      if (sub_table->ioptions()->compaction_style == kCompactionStyleFIFO) {
      }
      if (!sub_table->mem()->IsSnapshotSupported()) {
        impl->is_snapshot_supported_ = false;
      }
      if (sub_table->ioptions()->merge_operator != nullptr &&
          !sub_table->mem()->IsMergeOperatorSupported()) {
        s = Status::InvalidArgument(
            "The memtable of sub table(%s) does not support merge operator "
            "its options.merge_operator is non-null",
            sub_table->GetName().c_str());
      }
      if (!s.ok()) {
        break;
      }
    }
  }

  TEST_SYNC_POINT("DBImpl::Open:Opened");
  //Status persist_options_status;
  if (s.ok()) {
    // Persist Options before scheduling the compaction.
    // The WriteOptionsFile() will release and lock the mutex internally.
    //persist_options_status = impl->WriteOptionsFile();

    *dbptr = impl;
    impl->opened_successfully_ = true;
    impl->MaybeScheduleFlushOrCompaction();
    impl->schedule_master_thread();
//    if (FAILED(impl->init_cache_purge_timer())) {
//      s = Status(ret);
//      XENGINE_LOG(WARN, "failed init cache purge timer", K(ret));
//    }
  }
  impl->mutex_.Unlock();
  assert(impl->versions_->LastSequence() ==
         impl->versions_->LastAllocatedSequence());

#ifndef ROCKSDB_LITE
  auto sfm = static_cast<SstFileManagerImpl*>(
      impl->immutable_db_options_.sst_file_manager.get());
  if (s.ok() && sfm) {
    // Notify SstFileManager about all sst files that already exist in
    // db_paths[0] when the DB is opened.
    auto& db_path = impl->immutable_db_options_.db_paths[0];
    std::vector<std::string> existing_files;
    impl->immutable_db_options_.env->GetChildren(db_path.path, &existing_files);
    for (auto& file_name : existing_files) {
      uint64_t file_number;
      FileType file_type;
      std::string file_path = db_path.path + "/" + file_name;
      if (ParseFileName(file_name, &file_number, &file_type) &&
          file_type == kTableFile) {
        sfm->OnAddFile(file_path);
      }
    }
  }
#endif  // !ROCKSDB_LITE

  if (s.ok()) {
    __XENGINE_LOG(INFO, "DB pointer %p", impl);
    // LogFlush(impl->immutable_db_options_.info_log);
    //if (!persist_options_status.ok()) {
    //  if (db_options.fail_if_options_file_error) {
    //    s = Status::IOError(
    //        "DB::Open() failed --- Unable to persist Options file",
    //        persist_options_status.ToString());
    //  }
    //  __XENGINE_LOG(WARN, "Unable to persist options in DB::Open() -- %s",
    //                persist_options_status.ToString().c_str());
    //}
  }
  if (!s.ok()) {
    for (auto* h : *handles) {
//      delete h;
      MOD_DELETE_OBJECT(ColumnFamilyHandle, h);
    }
    handles->clear();
//    delete impl;
    MOD_DELETE_OBJECT(DBImpl, impl);
    *dbptr = nullptr;
  }
  return s;
}

std::string RecoveryDebugInfo::show()
{
  std::stringstream ss;
  ss << "P=" << prepare << ",ewc=" << prepare_external_write_ckpt << ",cds=" << prepare_create_default_subtable << ";"
     << "R=" << recovery << ",sr=" << recovery_slog_replay << ",w=" << recovery_wal << ";"
     << "W=" << recoverywal << ",b=" << recoverywal_before << ",wf=" << recoverywal_wal_files
             << ",a=" << recoverywal_after << ",fc=" << recoverywal_file_count << ",s=" << recoverywal_slowest;
  return ss.str();
}

}
}  // namespace xengine
