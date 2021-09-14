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
#include <vector>

#include "db/column_family.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "xengine/status.h"

using namespace xengine;
using namespace common;
using namespace monitor;

namespace xengine {
namespace db {

#ifndef ROCKSDB_LITE
Status DBImpl::SuggestCompactRange(ColumnFamilyHandle* column_family,
                                   const Slice* begin, const Slice* end) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  InternalKey start_key, end_key;
  if (begin != nullptr) {
    start_key.SetMaxPossibleForUserKey(*begin);
  }
  if (end != nullptr) {
    end_key.SetMinPossibleForUserKey(*end);
  }
  {
    InstrumentedMutexLock l(&mutex_);
    SchedulePendingCompaction(cfd);
    MaybeScheduleFlushOrCompaction();
  }
  return Status::OK();
}

Status DBImpl::PromoteL0(ColumnFamilyHandle* column_family, int target_level) {
  assert(false);
  assert(column_family);

  if (target_level < 1) {
    __XENGINE_LOG(INFO, "PromoteL0 FAILED. Invalid target level %d\n", target_level);
    return Status::InvalidArgument("Invalid target level");
  }

  Status status;
  //VersionEdit edit;
  JobContext job_context(next_job_id_.fetch_add(1), nullptr, true);
  {
    InstrumentedMutexLock l(&mutex_);
    auto* cfd = static_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();

    // Sort L0 files by range.
    const InternalKeyComparator* icmp = &cfd->internal_comparator();

    //edit.SetColumnFamily(cfd->GetID());

    //status = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
    //                                &edit, &mutex_, directories_.GetDbDir());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWorkWrapper(
          cfd, &job_context, *cfd->GetLatestMutableCFOptions());
    }
  }  // lock released here
  // LogFlush(immutable_db_options_.info_log);
  job_context.Clean();

  return status;
}
#endif  // ROCKSDB_LITE
}
}  // namespace xengine
