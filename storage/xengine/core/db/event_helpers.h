// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/version_edit.h"
#include "util/event_logger.h"
#include "xengine/listener.h"
#include "xengine/table_properties.h"

namespace xengine {
namespace db {

class EventHelpers {
 public:
  static void AppendCurrentTime(util::JSONWriter* json_writer);
#ifndef ROCKSDB_LITE
  static void NotifyTableFileCreationStarted(
      const std::vector<std::shared_ptr<common::EventListener>>& listeners,
      const std::string& db_name, const std::string& cf_name,
      const std::string& file_path, int job_id,
      common::TableFileCreationReason reason);
#endif  // !ROCKSDB_LITE
  static void LogAndNotifyTableFileCreationFinished(
      const std::vector<std::shared_ptr<common::EventListener>>& listeners,
      const std::string& db_name, const std::string& cf_name,
      const std::string& file_path, int job_id, const FileDescriptor& fd,
      const table::TableProperties& table_properties,
      common::TableFileCreationReason reason, const common::Status& s);
  static void LogAndNotifyTableFileDeletion(
      int job_id, uint64_t file_number,
      const std::string& file_path, const common::Status& status,
      const std::string& db_name,
      const std::vector<std::shared_ptr<common::EventListener>>& listeners);
};
}
}  // namespace xengine
