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
source ./check_result.sh

# check if the server is not exist
n=$(lsof -i:$SVR_PORT | wc -l)
if [ $n -ne 0 ]; then
  echo "Error, there has a running server"
  exit 1
fi

# copy the my.cnf file
cp $BASEPATH/rds_my.cnf $DATA_PATH

# start server
export MALLOC_CONF=dirty_decay_ms:0,muzzy_decay_ms:0 # set jemalloc env variables

JEMALLOC_LIB_PATH=/usr/lib64/libjemalloc.so.2
LD_PRELOAD=$JEMALLOC_LIB_PATH $INSTALL_PATH/bin/mysqld --defaults-file=$DATA_PATH/rds_my.cnf --recovery-inconsistency-check=off --socket=$DATA_PATH/mysql1.sock --port=$SVR_PORT --datadir=$DATA_PATH/data --log-error=$DATA_PATH/log/mysql.err &

export MYSQLD_PROCESS_ID=$!
echo MYSQLD_PROCESS_ID=$MYSQLD_PROCESS_ID

echo start command:$INSTALL_PATH/bin/mysqld --defaults-file=$DATA_PATH/rds_my.cnf --recovery-inconsistency-check=off --socket=$DATA_PATH/mysql1.sock --port=$SVR_PORT --datadir=$DATA_PATH/data --log-error=$DATA_PATH/log/mysql.err

check_result "Error, start mysqld server error, in sysbench_benchmark/start_server.sh"

echo "wait server start"
sleep 2

process=$(ps -elf | grep mysqld | grep "$DATA_PATH/mysql1.sock" | wc -l)
if [ $process -ne 1 ]; then
  echo "process=$process"
  echo "Error, server start error"
  exit 1
fi

echo "start server succ"
