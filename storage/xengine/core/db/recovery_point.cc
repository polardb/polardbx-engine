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

#include "recovery_point.h"

namespace xengine
{
namespace db
{
DEFINE_TO_STRING(RecoveryPoint, KV_(log_file_number), KV_(seq));
DEFINE_COMPACTIPLE_SERIALIZATION(RecoveryPoint, log_file_number_, seq_);

} //namespace db
} //namespace xengine
