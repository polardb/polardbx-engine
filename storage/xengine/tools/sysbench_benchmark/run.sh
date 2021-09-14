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

export SYSBENCH_RESULT_FILE_NAME=$BASEPATH/sysbench_result.out
export SYSBENCH_RESULT_SHOW_FILE_NAME=$BASEPATH/sysbench_result_show.out

rm $SYSBENCH_RESULT_FILE_NAME
rm $SYSBENCH_RESULT_SHOW_FILE_NAME

echo "Start run sysbench test"
sysbench_test_start_time=`date`
echo "test start at $sysbench_test_start_time"
if [ -z ${LOCAL_TEST+x} ]; then
  echo "LOCAL_TEST is unset, use jekins config"
else
  echo "LOCAL_TEST is set, use local config"
fi
echo "================ Config ================"
echo SVR_IP=$SVR_IP
echo SVR_PORT=$SVR_PORT
echo INSTALL_PATH=$INSTALL_PATH
echo DATA_PATH=$DATA_PATH
echo SYSBENCH_BIN_PATH=$SYSBENCH_BIN_PATH
echo SYSBENCH_THREAD_NUM=$SYSBENCH_THREAD_NUM
echo SYSBENCH_MAX_TIME=$SYSBENCH_MAX_TIME
echo SYSBENCH_TABLE_SIZE=$SYSBENCH_TABLE_SIZE
echo SYSBENCH_TABLE_NUM=$SYSBENCH_TABLE_NUM
echo SYSBENCH_HISTORY_DIR=$SYSBENCH_HISTORY_DIR
echo "================ Config ================"


run()
{
  echo "try bootstrap_db"
  sh bootstrap_db.sh
  echo "bootstrap_db done"

  echo "try start_server"
  sh start_server.sh
  echo "start_server done"

  echo "try wait_server_ready"
  sh wait_server_ready.sh
  echo "wait_server_ready done"

  echo "try run_sysbench"
  sh run_sysbench.sh
  echo "run_sysbench done"
}

CUR_BACKUP_DIR=`date '+%Y%m%d-%H%M%S'`
echo "Full Result In $SYSBENCH_HISTORY_DIR/$CUR_BACKUP_DIR/" >> $SYSBENCH_RESULT_SHOW_FILE_NAME

for oltp_case_name_iter in oltp_insert.lua
do
  export OLTP_CASE_NAME=$oltp_case_name_iter
  echo ""
  echo ""
  echo ""
  echo "/------------------------------------------------------------\\"
  printf "  Run Test: %-20s  |\n" $OLTP_CASE_NAME
  echo "\\------------------------------------------------------------/"
  run
done


SYSBENCH_RESULT_FILE_NAME=$BASEPATH/sysbench_result.out
SYSBENCH_RESULT_SHOW_FILE_NAME=$BASEPATH/sysbench_result_show.out

sed 's/+         read/> > > +         read/g' $SYSBENCH_RESULT_SHOW_FILE_NAME -i
sed 's/+         write/> > > +         write/g' $SYSBENCH_RESULT_SHOW_FILE_NAME -i
sed 's/+         other/> > > +         other/g' $SYSBENCH_RESULT_SHOW_FILE_NAME -i
sed 's/+         total/> > > +         total/g' $SYSBENCH_RESULT_SHOW_FILE_NAME -i

cd $SYSBENCH_HISTORY_DIR
mkdir $CUR_BACKUP_DIR
cp $SYSBENCH_RESULT_SHOW_FILE_NAME $SYSBENCH_HISTORY_DIR/$CUR_BACKUP_DIR
