//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "xengine/utilities/ldb_cmd.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace xengine {
namespace tools {

class CompactorCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "compact"; }

  CompactorCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;

 private:
  bool null_from_;
  std::string from_;
  bool null_to_;
  std::string to_;
};

class DBFileDumperCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "dump_live_files"; }

  DBFileDumperCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;
};

class DBDumperCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "dump"; }

  DBDumperCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;

 private:
  /**
   * Extract file name from the full path. We handle both the forward slash (/)
   * and backslash (\) to make sure that different OS-s are supported.
  */
  static std::string GetFileNameFromPath(const std::string& s) {
    std::size_t n = s.find_last_of("/\\");

    if (std::string::npos == n) {
      return s;
    } else {
      return s.substr(n + 1);
    }
  }

  void DoDumpCommand();

  bool null_from_;
  std::string from_;
  bool null_to_;
  std::string to_;
  int max_keys_;
  std::string delim_;
  bool count_only_;
  bool count_delim_;
  bool print_stats_;
  std::string path_;

  static const std::string ARG_COUNT_ONLY;
  static const std::string ARG_COUNT_DELIM;
  static const std::string ARG_STATS;
  static const std::string ARG_TTL_BUCKET;
};

class InternalDumpCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "idump"; }

  InternalDumpCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;

 private:
  bool has_from_;
  std::string from_;
  bool has_to_;
  std::string to_;
  int max_keys_;
  std::string delim_;
  bool count_only_;
  bool count_delim_;
  bool print_stats_;
  bool is_input_key_hex_;

  static const std::string ARG_DELIM;
  static const std::string ARG_COUNT_ONLY;
  static const std::string ARG_COUNT_DELIM;
  static const std::string ARG_STATS;
  static const std::string ARG_INPUT_KEY_HEX;
};

class DBLoaderCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "load"; }

  DBLoaderCommand(std::string& db_name, std::vector<std::string>& args);

  DBLoaderCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual common::Options PrepareOptionsForOpenDB() override;

 private:
  bool create_if_missing_;
  bool disable_wal_;
  bool bulk_load_;
  bool compact_;

  static const std::string ARG_DISABLE_WAL;
  static const std::string ARG_BULK_LOAD;
  static const std::string ARG_COMPACT;
};

struct SliceHasher {
  size_t operator() (const common::Slice &s) const {
    return std::hash<std::string>()(std::string(s.data(), s.size()));
  }
};

struct SliceComparator {
  bool operator()(const common::Slice &lhs, const common::Slice &rhs) const {
    if (lhs.size() == rhs.size() &&
        !memcmp(lhs.data(), rhs.data(), lhs.size())) {
      return true;
    } else {
      return false;
    }
  }
};


class ManifestDumpCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "manifest_dump"; }

  ManifestDumpCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }
  // for backup read the increment extents 
  //static int dump_manifest_file(std::string ckpname, bool verbose, bool hex, bool json,
  //                     std::unordered_map<common::Slice,
  //std::pair<common::Slice, int64_t>, SliceHasher, SliceComparator> *extents = nullptr);

 private:
  bool verbose_;
  bool json_;
  std::string path_;

  static const std::string ARG_VERBOSE;
  static const std::string ARG_JSON;
  static const std::string ARG_PATH;
};

class CheckpointDumpCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "checkpoint_dump"; }

  CheckpointDumpCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }
  // for backup read the last version meta
  //TODO:yuanfeng
  //static int dump_checkpoint_file(std::string ckpname, bool verbose, bool hex, bool json,
  //                     std::unordered_map<common::Slice,
  //std::pair<common::Slice, int64_t>, SliceHasher, SliceComparator> *extents = nullptr);
  int dump_checkpoint_file();
 private:
  int dump_extent_space_mgr_checkpoint(util::RandomAccessFile *checkpoint_reader, CheckpointHeader *header);
  int dump_version_set_checkpoint(util::RandomAccessFile *checkpoint_reader, CheckpointHeader *header);
  int read_big_subtable(util::RandomAccessFile *checkpoint_reader, int64_t block_size, int64_t &file_offset);

 private:
  static const int64_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;
  static const int64_t DEFAULT_TO_STRING_SIZE = 16 * 1024 * 1024;
 private:
  bool verbose_;
  bool json_;
  std::string path_;

  static const std::string ARG_VERBOSE;
  static const std::string ARG_JSON;
  static const std::string ARG_PATH;
};

class ListColumnFamiliesCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "list_column_families"; }

  ListColumnFamiliesCommand(const std::vector<std::string>& params,
                            const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

 private:
  std::string dbname_;
};

class CreateColumnFamilyCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "create_column_family"; }

  CreateColumnFamilyCommand(const std::vector<std::string>& params,
                            const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return false; }

 private:
  std::string new_cf_name_;
};

class ReduceDBLevelsCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "reduce_levels"; }

  ReduceDBLevelsCommand(const std::vector<std::string>& params,
                        const std::map<std::string, std::string>& options,
                        const std::vector<std::string>& flags);

  virtual common::Options PrepareOptionsForOpenDB() override;

  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

  static void Help(std::string& msg);

  static std::vector<std::string> PrepareArgs(const std::string& db_path,
                                              int new_levels,
                                              bool print_old_level = false);

 private:
  int old_levels_;
  int new_levels_;
  bool print_old_levels_;

  static const std::string ARG_NEW_LEVELS;
  static const std::string ARG_PRINT_OLD_LEVELS;

  common::Status GetOldNumOfLevels(common::Options& opt, int* levels);
};

class ChangeCompactionStyleCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "change_compaction_style"; }

  ChangeCompactionStyleCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual common::Options PrepareOptionsForOpenDB() override;

  virtual void DoCommand() override;

  static void Help(std::string& msg);

 private:
  int old_compaction_style_;
  int new_compaction_style_;

  static const std::string ARG_OLD_COMPACTION_STYLE;
  static const std::string ARG_NEW_COMPACTION_STYLE;
};

class WALDumperCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "dump_wal"; }

  WALDumperCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  virtual bool NoDBOpen() override { return true; }

  static void Help(std::string& ret);
  virtual void DoCommand() override;

 private:
  bool print_header_;
  std::string wal_file_;
  bool print_values_;

  static const std::string ARG_WAL_FILE;
  static const std::string ARG_PRINT_HEADER;
  static const std::string ARG_PRINT_VALUE;
};

class GetCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "get"; }

  GetCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string key_;
};

class ApproxSizeCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "approxsize"; }

  ApproxSizeCommand(const std::vector<std::string>& params,
                    const std::map<std::string, std::string>& options,
                    const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string start_key_;
  std::string end_key_;
};

class BatchPutCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "batchput"; }

  BatchPutCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

  virtual common::Options PrepareOptionsForOpenDB() override;

 private:
  /**
   * The key-values to be inserted.
   */
  std::vector<std::pair<std::string, std::string>> key_values_;
};

class ScanCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "scan"; }

  ScanCommand(const std::vector<std::string>& params,
              const std::map<std::string, std::string>& options,
              const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string start_key_;
  std::string end_key_;
  bool start_key_specified_;
  bool end_key_specified_;
  int max_keys_scanned_;
  bool no_value_;
};

class DeleteCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "delete"; }

  DeleteCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string key_;
};

class DeleteRangeCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "deleterange"; }

  DeleteRangeCommand(const std::vector<std::string>& params,
                     const std::map<std::string, std::string>& options,
                     const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string begin_key_;
  std::string end_key_;
};

class PutCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "put"; }

  PutCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

  virtual common::Options PrepareOptionsForOpenDB() override;

 private:
  std::string key_;
  std::string value_;
};

/**
 * Command that starts up a REPL shell that allows
 * get/put/delete.
 */
class DBQuerierCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "query"; }

  DBQuerierCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;

 private:
  static const char* HELP_CMD;
  static const char* GET_CMD;
  static const char* PUT_CMD;
  static const char* DELETE_CMD;
};

class CheckConsistencyCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "checkconsistency"; }

  CheckConsistencyCommand(const std::vector<std::string>& params,
                          const std::map<std::string, std::string>& options,
                          const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

  static void Help(std::string& ret);
};

class CheckPointCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "checkpoint"; }

  CheckPointCommand(const std::vector<std::string>& params,
                    const std::map<std::string, std::string>& options,
                    const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

  std::string checkpoint_dir_;

 private:
  static const std::string ARG_CHECKPOINT_DIR;
};

class RepairCommand : public tools::LDBCommand {
 public:
  static std::string Name() { return "repair"; }

  RepairCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

  static void Help(std::string& ret);
};

class BackupableCommand : public tools::LDBCommand {
 public:
  BackupableCommand(const std::vector<std::string>& params,
                    const std::map<std::string, std::string>& options,
                    const std::vector<std::string>& flags);

 protected:
  static void Help(const std::string& name, std::string& ret);
  std::string backup_env_uri_;
  std::string backup_dir_;
  int num_threads_;

 private:
  static const std::string ARG_BACKUP_DIR;
  static const std::string ARG_BACKUP_ENV_URI;
  static const std::string ARG_NUM_THREADS;
  static const std::string ARG_STDERR_LOG_LEVEL;
};

class BackupCommand : public BackupableCommand {
 public:
  static std::string Name() { return "backup"; }
  BackupCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);
  virtual void DoCommand() override;
  static void Help(std::string& ret);
};

class RestoreCommand : public BackupableCommand {
 public:
  static std::string Name() { return "restore"; }
  RestoreCommand(const std::vector<std::string>& params,
                 const std::map<std::string, std::string>& options,
                 const std::vector<std::string>& flags);
  virtual void DoCommand() override;
  virtual bool NoDBOpen() override { return true; }
  static void Help(std::string& ret);
};
}
}  // namespace xengine
