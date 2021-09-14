/************************************************************************
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 * $Id:  error.h,v 1.0 01/09/2018 08:28:02 PM
 *
 ************************************************************************/

/**
 * @file error.h
 * @date 01/09/2018 08:28:02 PM
 * @version 1.0
 * @brief
 *
 **/

#pragma once
namespace xengine {
namespace util {
enum class Error {
  OK = 0,
  Timeout,
  OOM,
  UDF1 = 98,
  UDF2 = 99,
  Unknown = 100,
};
}
}  // namespace is
