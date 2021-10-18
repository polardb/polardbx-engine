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

#include "util/aio_wrapper.h"
#include "util/time_interval.h"
#include "util/misc_utility.h"

namespace xengine
{
namespace util
{

void AIOReq::reset()
{
  if (common::Status::kBusy == status_) {
    // The status of an AIOReq is kBusy, which means this req has been
    // submitted but not reaped, should NOT happen!
    // We should reap it before release the aio buf, otherwise the memory
    // allocated by aio buf might be overwrite by aio.
    if (aio_info_.size_ == storage::MAX_EXTENT_SIZE) {
      XENGINE_LOG(INFO, "this req is NOT reaped", KP(this), K(aio_info_), K(aio_buf_));
    } else if (reach_time_interval(600 * 1000 * 1000ULL)) { // 10 min
      XENGINE_LOG(WARN, "this req is NOT reaped", KP(this), K(aio_info_), K(aio_buf_));
      BACKTRACE(ERROR, "this req is NOT reaped");
    }
    AIO::get_instance()->reap(this);
  }
  aio_info_.reset();
  aio_buf_.reset();
  memset(&iocb_, 0, sizeof(iocb));
  status_ = common::Status::kInvalidArgument;
  aio_data_ = nullptr;
}


} // namespace util
} // namespace xengine
