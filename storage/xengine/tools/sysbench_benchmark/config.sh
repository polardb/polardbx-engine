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

# mysqld config
SVR_IP=127.0.0.1
SVR_PORT=9909

INSTALL_PATH=sysbench_benchmark/install
DATA_PATH=sysbench_benchmark/data

RUNNING_CONFIG_FILE=$DATA_PATH/running.conf

# sysbench config
SYSBENCH_BIN_PATH=sysbench_benchmark/sysbench_install/src
SYSBENCH_MAX_TIME=1800
SYSBENCH_ENGINE="XENGINE"
SYSBENCH_TABLE_SIZE=1000000
SYSBENCH_TABLE_NUM=100
SYSBENCH_THREAD_NUM=512

# backup sysbench test history
SYSBENCH_HISTORY_DIR=sysbench_benchmark/sysbench_history

PIPE_NAME=/tmp/pipe_for_sqlchart_sysbench

