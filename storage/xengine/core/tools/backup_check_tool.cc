/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * 3 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "xengine/backup_check.h"
#include "stdio.h"
#include <cstring>
#include <string>
#include "ldb_cmd_impl.h"
#include "xengine/utilities/ldb_cmd.h"
#include "storage/io_extent.h"
#include "xengine/slice.h"
#include "sst_dump_tool_imp.h"
#include "table/table_reader.h"
#include "xengine/backup_check.h"

using namespace std;
using namespace xengine;
using namespace storage;
using namespace common;
using namespace table;

namespace xengine {
namespace tools {

void print_help() {
  fprintf(stderr, "\
    --checkpoint=checkpoint_file checkpoint file name.\n\
    --manifest=manifest_file manifest file name.\n\
    --data_dir= data dirctory.\n"); 
}

int BackupCheckTool::check_one_extent(ExtentId &extent, const Slice &begin, 
                                      const Slice &end, bool &check_result) {
  int ret = Status::kOk;
  char sst_name[50];
  std::unique_ptr<SstFileReader> extent_reader(nullptr);
  Options options;
  options.db_paths.emplace_back(data_dir,
                                std::numeric_limits<uint64_t>::max());
  snprintf(sst_name, sizeof(sst_name), "%06d.sst", extent.file_number);
  std::string filename = data_dir + "/" + sst_name;
  extent_reader.reset(new SstFileReader(filename, true, true, extent.offset, options));
  if (!extent_reader.get() || !extent_reader.get()->getStatus().ok()) { 
    if (!extent_reader.get()) {
      fprintf(stderr, "create the extent reader failed\n");
    } else {
      fprintf(stderr, "%s: %s\n", filename.c_str(), 
              extent_reader.get()->getStatus().ToString().c_str());
      return extent_reader.get()->getStatus().code();
    }
  }

  
  Status s = extent_reader->get_table_reader()->check_range(begin, end, check_result);
  if (!s.ok()) {
    fprintf(stderr, "check extent return error %d", s.code());
    return s.code();
  } else if (!check_result) {
    fprintf(stderr, "the extent content and meta are not matching\n");
  }
  return ret;
}

int BackupCheckTool::run(int argc, char** argv) {
  std::string checkpoint_file;
  std::string manifest_file;
  
  // the hash table store all the extents
  // extent id and the range of it
  std::unordered_map<Slice, std::pair<Slice, int64_t>, SliceHasher, SliceComparator> all_extents; 
  int ret = Status::kOk;

  if (argc != 4) {
    ret = Status::kInvalidArgument;
    fprintf(stderr, "invalid argument\n");
    print_help();
    return ret;
  }

  for (int i = 1; i < argc; i++) {
    if (!strncmp(argv[i], "--checkpoint=", strlen("--checkpoint="))) {
      checkpoint_file = argv[i] + strlen("--checkpoint="); 
    } else if (!strncmp(argv[i], "--manifest=", strlen("--manifest="))) {
      manifest_file = argv[i] + strlen("--manifest=");
    } else if (!strncmp(argv[i], "--data_dir=", strlen("--data_dir="))) {
      data_dir = argv[i] + strlen("--data_dir=");
    } else {
      ret = Status::kInvalidArgument;
      fprintf(stderr, "invalid argument\n");
      print_help();
      return ret;
    }
  }
  // read extents of checkpoint
  //TODO:yuanfeng
  /*
  if (FAILED(CheckpointDumpCommand::dump_checkpoint_file(checkpoint_file, 
                                         false, false, false, &all_extents))) {
    fprintf(stderr, "get the last meta version filed %d\n", ret);
    return ret;
  }
  */
  // read extents of manifest
  /*
  if (FAILED(ManifestDumpCommand::dump_manifest_file(manifest_file,
                                         false, false, false, &all_extents))) {
    fprintf(stderr, "read manifest filed %d\n", ret);
    return ret; 
  }
  */

  // iterate the hash table to check all the valid extents
  ExtentId extent_id;
  bool check_result = true;
  int32_t extents = 0;
  int32_t failed_extents = 0;
  for (auto extent : all_extents) {
    extent_id = extent.second.second;
    fprintf(stdout, "check extent [%d, %d]", extent_id.file_number, extent_id.offset);
    check_result = true;
    extents++;
    if (FAILED(check_one_extent(extent_id, extent.second.first, extent.first, check_result))) {
      fprintf(stderr, " error happend %d\n", ret);
      break;
    } else if (check_result) {
      fprintf(stdout, " ...... ok\n");
    } else {
      failed_extents++;
      fprintf(stdout, " ...... failed\n");
    }
  }

  fprintf(stdout, "Check Backup Data Directory Done Checked %d Failed %d\n", 
          extents, failed_extents);

  return ret;
}

}
}
