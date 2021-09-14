# Copyright (c) 2020, Alibaba Group Holding Limited
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# 3 Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BASEPATH=$(cd `dirname $0`; pwd)
cd $BASEPATH
source ./config.sh

if [[ ! $WITH_SQLCHART ]]; then
  CUR_BACKUP_DIR=`date '+%Y%m%d-%H%M%S'`
  echo SYSBENCH_HISTORY_DIR=$SYSBENCH_HISTORY_DIR
  mkdir $SYSBENCH_HISTORY_DIR/$CUR_BACKUP_DIR
  cp cpu.out   $SYSBENCH_HISTORY_DIR/$CUR_BACKUP_DIR
  cp io.out    $SYSBENCH_HISTORY_DIR/$CUR_BACKUP_DIR
  cp sysbench_result.out   $SYSBENCH_HISTORY_DIR/$CUR_BACKUP_DIR
  cp sysbench_result_show.out  $SYSBENCH_HISTORY_DIR/$CUR_BACKUP_DIR
fi
