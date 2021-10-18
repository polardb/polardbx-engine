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

export LD_LIBRARY_PATH=/usr/local/gcc-5.3.0/lib64/:$INSTALL_PATH/lib:$LD_LIBRARY_PATH

pkill mysqld
sleep 5
pkill mysqld
sleep 5
pkill mysqld
sleep 5
# stop the server whatever
$INSTALL_PATH/bin/mysqladmin -u root --socket=$DATA_PATH/mysql1.sock shutdown 1> /dev/null 2>&1

# check if the server is not exist
n=$(lsof -i:$SVR_PORT | wc -l)
if [ $n -ne 0 ]; then
  echo "Error, there has a running server"
  exit 1
fi

# clean data directory
echo "clean data directory"
rm -rf $DATA_PATH/data/.xengine/
rm -rf $DATA_PATH/data
rm -rf $DATA_PATH/log
rm -rf $DATA_PATH/tmp
rm -rf $DATA_PATH/repl
echo "clean data directory done"

mkdir -p $DATA_PATH/data
mkdir -p $DATA_PATH/log
mkdir -p $DATA_PATH/tmp
mkdir -p $DATA_PATH/repl

# copy the my.cnf file
cp $BASEPATH/rds_my.cnf $DATA_PATH

# bootstrap the server
$INSTALL_PATH/bin/mysqld --defaults-file=$DATA_PATH/rds_my.cnf --recovery-inconsistency-check=off --datadir=$DATA_PATH/data --log-error=$DATA_PATH/log/mysql.err --initialize-insecure 
echo bootstrap command:$INSTALL_PATH/bin/mysqld --defaults-file=$DATA_PATH/rds_my.cnf --recovery-inconsistency-check=off --datadir=$DATA_PATH/data --initialize-insecure --log-error=$DATA_PATH/log/mysql.err

check_result "Error, bootstrap mysqld server error, in sysbench_benchmark/bootstrap_db.sh"

echo "bootstrap succ"
