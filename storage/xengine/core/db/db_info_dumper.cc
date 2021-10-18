// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "db/db_info_dumper.h"

#include <inttypes.h>
#include <stdio.h>
#include <algorithm>
#include <string>
#include <vector>

#include "util/filename.h"
#include "logger/logger.h"
#include "xengine/env.h"

using namespace xengine;
using namespace common;
using namespace util;

namespace xengine {
namespace db {

void DumpDBFileSummary(const ImmutableDBOptions& options,
                       const std::string& dbname) {
  auto* env = options.env;
  uint64_t number = 0;
  FileType type = kInfoLogFile;

  std::vector<std::string> files;
  uint64_t file_num = 0;
  uint64_t file_size;
  std::string file_info, wal_info;

  __XENGINE_LOG(INFO, "DB SUMMARY");
  // Get files in dbname dir
  Status s = env->GetChildren(dbname, &files);
  if (!s.ok()) {
    if (!s.IsNotFound()) {
      __XENGINE_LOG(ERROR, "Error happened when reading directory:%s, status=%s",
                    dbname.c_str(), s.ToString().c_str());
    }
    return ;
  }
  std::sort(files.begin(), files.end());
  for (std::string file : files) {
    if (!ParseFileName(file, &number, &type)) {
      continue;
    }
    switch (type) {
      case kCurrentFile:
        __XENGINE_LOG(INFO, "CURRENT file:  %s", file.c_str());
        break;
      case kIdentityFile:
        __XENGINE_LOG(INFO, "IDENTITY file:  %s", file.c_str());
        break;
      case kDescriptorFile:
        env->GetFileSize(dbname + "/" + file, &file_size);
        __XENGINE_LOG(INFO, "MANIFEST file:  %s size: %" PRIu64 " Bytes",
                      file.c_str(), file_size);
        break;
      case kLogFile:
        env->GetFileSize(dbname + "/" + file, &file_size);
        char str[16];
        snprintf(str, sizeof(str), "%" PRIu64, file_size);
        wal_info.append(file).append(" size: ").append(str).append(" ; ");
        break;
      case kTableFile:
        if (++file_num < 10) {
          file_info.append(file).append(" ");
        }
        break;
      default:
        break;
    }
  }

  // Get sst files in db_path dir
  for (auto& db_path : options.db_paths) {
    if (dbname.compare(db_path.path) != 0) {
      s = env->GetChildren(db_path.path, &files);
      if (!s.ok()) {
        __XENGINE_LOG(ERROR, "Error happened when reading directory %s, status=%s",
                      db_path.path.c_str(), s.ToString().c_str());
        continue;
      }
      std::sort(files.begin(), files.end());
      for (std::string file : files) {
        if (ParseFileName(file, &number, &type)) {
          if (type == kTableFile && ++file_num < 10) {
            file_info.append(file).append(" ");
          }
        }
      }
    }
    __XENGINE_LOG(INFO, "SST files in %s dir, Total Num: %" PRIu64 ", files: %s\n",
                  db_path.path.c_str(), file_num, file_info.c_str());
    file_num = 0;
    file_info.clear();
  }

  // Get wal file in wal_dir
  if (dbname.compare(options.wal_dir) != 0) {
    s = env->GetChildren(options.wal_dir, &files);
    if (!s.ok()) {
      __XENGINE_LOG(ERROR, "Error happened when reading directory %s, status=%s",
                    options.wal_dir.c_str(), s.ToString().c_str());
      return;
    }
    wal_info.clear();
    for (std::string file : files) {
      if (ParseFileName(file, &number, &type)) {
        if (type == kLogFile) {
          env->GetFileSize(options.wal_dir + "/" + file, &file_size);
          char str[16];
          snprintf(str, sizeof(str), "%" PRIu64, file_size);
          wal_info.append(file).append(" size: ").append(str).append(" ; ");
        }
      }
    }
  }
  __XENGINE_LOG(INFO, "Write Ahead Log file in %s: %s",
                options.wal_dir.c_str(), wal_info.c_str());
}
}
}  // namespace xengine
