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
#pragma once
#ifndef ROCKSDB_LITE

namespace xengine
{
namespace db
{
class DB;
}
namespace util
{

static const char *const BACKUP_TMP_DIR = "/hotbackup_tmp";
static const char *const BACKUP_EXTENT_IDS_FILE = "/extent_ids.inc";
static const char *const BACKUP_EXTENTS_FILE = "/extent.inc";

class BackupSnapshot
{
public:
  // Create a backup snapshot instance
  static int create(BackupSnapshot *&backup_instance);
  // Check backup job and do init
  virtual int init(db::DB *db, const char *backup_tmp_dir_path = nullptr);
  // Do a manual checkpoint and flush memtable
  virtual int do_checkpoint(db::DB *db);
  // Acquire snapshots and hard-link/copy MANIFEST files
  virtual int acquire_snapshots(db::DB *db);
  // Parse incremental MANIFEST files and record the modified extent ids
  virtual int record_incremental_extent_ids(db::DB *db);
  // Release the snapshots
  virtual int release_snapshots(db::DB *db);

protected:
  BackupSnapshot() {}
  virtual ~BackupSnapshot() {}
};

} // namespace util
} // namespace xengien

#endif
