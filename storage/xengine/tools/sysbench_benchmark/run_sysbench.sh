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
echo $LD_LIBRARY_PATH
# check server can be connected
$INSTALL_PATH/bin/mysql -S $DATA_PATH/mysql1.sock --port=$SVR_PORT -uroot mysql -e "SELECT \"CONNECTED\"" > /dev/null
echo check_connect command: $INSTALL_PATH/bin/mysql -s $DATA_PATH/mysql1.sock --port=$SVR_PORT -uroot mysql
check_result "Error, can not connect to server"

# record cpu/io metric
cd $BASEPATH
sh record_cpu_io.sh
check_result "Error, record cpu, io metric failed"

# run sysbench
cd $SYSBENCH_PATH

SYSBENCH_RESULT_FILE_NAME=$BASEPATH/sysbench_result.out
SYSBENCH_RESULT_SHOW_FILE_NAME=$BASEPATH/sysbench_result_show.out

#cat $BASEPATH/create_sbtest.sql
$INSTALL_PATH/bin/mysql -S $DATA_PATH/mysql1.sock --port=$SVR_PORT -uroot mysql -e "source $BASEPATH/create_sbtest.sql;"
echo create_sbtest command: $INSTALL_PATH/bin/mysql -S $DATA_PATH/mysql1.sock --port=$SVR_PORT -uroot mysql -e "source $BASEPATH/create_sbtest.sql;"
check_result "Error, create sbtest user error"

export MYSQL_PWD=sbtest # mysql password
echo "try to reset sbtest database"
$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "DROP DATABASE IF EXISTS sbtest;"
echo "try to re-create database sbtest"
$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "CREATE DATABASE IF NOT EXISTS sbtest;"
#$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "SHOW DATABASES;"

echo "try to create test tables"
for table_id in `seq 1 $SYSBENCH_TABLE_NUM`
do
  $INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e \
    "USE sbtest;\
     CREATE TABLE IF NOT EXISTS sbtest$table_id (id BIGINT NOT NULL AUTO_INCREMENT,
                                                 k INT(10) UNSIGNED NOT NULL DEFAULT '0',
                                                 c CHAR(120) COLLATE UTF8MB4_BIN NOT NULL DEFAULT '',
                                                 pad CHAR(60) NOT NULL DEFAULT '' COLLATE UTF8MB4_BIN,
                                                 PRIMARY KEY (id) COMMENT 'cf_pri_$table_id',
                                                 KEY k_1(k) COMMENT 'cf_key_$table_id',
                                                 KEY k_2(pad))
                                                 ENGINE=$SYSBENCH_ENGINE;"
done

#$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "USE sbtest; SHOW TABLES"
echo "" > $RUNNING_CONFIG_FILE
$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "SHOW VARIABLES LIKE 'log_bin';" >> $RUNNING_CONFIG_FILE
$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "SHOW VARIABLES LIKE 'sync_binlog';" >> $RUNNING_CONFIG_FILE
$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "SHOW VARIABLES LIKE 'xengine_flush_log_at_trx_commit';" >> $RUNNING_CONFIG_FILE
$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "SHOW VARIABLES LIKE 'general_log';" >> $RUNNING_CONFIG_FILE
$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "SHOW VARIABLES LIKE 'order_commit';" >> $RUNNING_CONFIG_FILE


# Configure your server HERE
# Configure your server HERE
# Configure your server HERE
$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "SET GLOBAL slow_query_log=ON;" >> $RUNNING_CONFIG_FILE
$INSTALL_PATH/bin/mysql -h $SVR_IP -P $SVR_PORT -usbtest -e "SET GLOBAL long_query_time=0.01;" >> $RUNNING_CONFIG_FILE

cat $RUNNING_CONFIG_FILE

SYSBENCH_TEST_START_TIME=`date`

cd $SYSBENCH_BIN_PATH/lua/
echo sysbench result file name: $SYSBENCH_RESULT_FILE_NAME
echo sysbench path = $SYSBENCH_BIN_PATH/sysbench 

echo "ensure pipe $PIPE_NAME"
if [[ ! -p $PIPE_NAME ]]; then
  mkfifo $PIPE_NAME
fi

if [[ $WITH_SQLCHART ]]; then
  echo "put to $PIPE_NAME to nofity sqlchart sample"
  echo "start run sysbench" > $PIPE_NAME
fi

echo "Start to Run Sysbench........"

#$SYSBENCH_BIN_PATH/sysbench oltp_insert.lua --time=$SYSBENCH_MAX_TIME --mysql-host=$SVR_IP --mysql-port=$SVR_PORT --auto-inc=0 --thread-init-timeout=1000 --mysql-db=sbtest --mysql-user=sbtest --mysql-password=sbtest --table-size=$SYSBENCH_TABLE_SIZE --db-ps-mode=auto --tables=$SYSBENCH_TABLE_NUM --report-interval=1 --threads=$SYSBENCH_THREAD_NUM --prepare=true run 2>&1 > $SYSBENCH_RESULT_FILE_NAME
if [[ $OLTP_CASE_NAME == oltp_insert.lua ]]; then
  $SYSBENCH_BIN_PATH/sysbench $OLTP_CASE_NAME --time=$SYSBENCH_MAX_TIME --mysql-host=$SVR_IP --mysql-port=$SVR_PORT --auto-inc=0 --thread-init-timeout=1000 --mysql-db=sbtest --mysql-user=sbtest --mysql-password=sbtest --table-size=$SYSBENCH_TABLE_SIZE --db-ps-mode=auto --tables=$SYSBENCH_TABLE_NUM --report-interval=1 --threads=$SYSBENCH_THREAD_NUM run 2>&1 > $SYSBENCH_RESULT_FILE_NAME
elif [[ $OLTP_CASE_NAME == oltp_write_only.lua ]]; then
  $SYSBENCH_BIN_PATH/sysbench $OLTP_CASE_NAME --time=$SYSBENCH_MAX_TIME --mysql-host=$SVR_IP --mysql-port=$SVR_PORT --auto-inc=0 --thread-init-timeout=1000 --mysql-db=sbtest --mysql-user=sbtest --mysql-password=sbtest --table-size=$SYSBENCH_TABLE_SIZE --db-ps-mode=auto --tables=$SYSBENCH_TABLE_NUM --report-interval=1 --threads=$SYSBENCH_THREAD_NUM --mysql-ignore-errors=1062 run 2>&1 > $SYSBENCH_RESULT_FILE_NAME
else
  echo "start sysbench prepare"
  $SYSBENCH_BIN_PATH/sysbench $OLTP_CASE_NAME --time=$SYSBENCH_MAX_TIME --mysql-host=$SVR_IP --mysql-port=$SVR_PORT --auto-inc=0 --thread-init-timeout=1000 --mysql-db=sbtest --mysql-user=sbtest --mysql-password=sbtest --table-size=$SYSBENCH_TABLE_SIZE --db-ps-mode=auto --tables=$SYSBENCH_TABLE_NUM --report-interval=1 --threads=$SYSBENCH_THREAD_NUM prepare 2>&1 > $SYSBENCH_RESULT_FILE_NAME
  echo "start sysbench run"
  $SYSBENCH_BIN_PATH/sysbench $OLTP_CASE_NAME --time=$SYSBENCH_MAX_TIME --mysql-host=$SVR_IP --mysql-port=$SVR_PORT --auto-inc=0 --thread-init-timeout=1000 --mysql-db=sbtest --mysql-user=sbtest --mysql-password=sbtest --table-size=$SYSBENCH_TABLE_SIZE --db-ps-mode=auto --tables=$SYSBENCH_TABLE_NUM --report-interval=1 --threads=$SYSBENCH_THREAD_NUM run 2>&1 > $SYSBENCH_RESULT_FILE_NAME
fi

cat $SYSBENCH_RESULT_FILE_NAME

run_sysbench_ret=$?

if [ $run_sysbench_ret -ne 0 ]; then
  echo "run sysbench error, shell run ret=$run_sysbench_ret" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
  exit 1
fi


SYSBENCH_TEST_END_TIME=`date`

# report test information for bianque
process=$(ps -elf | grep mysqld | grep "$DATA_PATH/mysql1.sock")
echo "Test information For bianque"
echo "> ip: $SVR_IP"
echo "> mysqld process id: $MYSQLD_PROCESS_ID"
echo "> start time: $SYSBENCH_TEST_START_TIME"
echo "> end   time: $SYSBENCH_TEST_END_TIME"

#echo "" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
#echo "" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
#echo "" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
#echo "** **" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
#echo "## $OLTP_CASE_NAME   $SYSBENCH_ENGINE" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
#echo "[duration_time: $SYSBENCH_MAX_TIME sec]" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
#echo "" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
#grep "SQL statistics:" $SYSBENCH_RESULT_FILE_NAME >> $SYSBENCH_RESULT_SHOW_FILE_NAME
#grep "SQL statistics:" $SYSBENCH_RESULT_FILE_NAME -A 9 | tail -n 8 | sed 's/^/> + /g' >> $SYSBENCH_RESULT_SHOW_FILE_NAME
#grep "Latency (ms):" $SYSBENCH_RESULT_FILE_NAME >> $SYSBENCH_RESULT_SHOW_FILE_NAME
#grep "Latency (ms):" $SYSBENCH_RESULT_FILE_NAME -A 6 | tail -n 5 | sed 's/^/> + /g' >> $SYSBENCH_RESULT_SHOW_FILE_NAME

echo "" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
echo "" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
echo "" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
echo "** **" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
echo "## $OLTP_CASE_NAME, $SYSBENCH_ENGINE, $SYSBENCH_THREAD_NUM threads" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
echo "" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
echo "[duration_time: $SYSBENCH_MAX_TIME sec]" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
echo "" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
grep "SQL statistics:" $SYSBENCH_RESULT_FILE_NAME >> $SYSBENCH_RESULT_SHOW_FILE_NAME
grep "SQL statistics:" $SYSBENCH_RESULT_FILE_NAME -A 9 | grep -v "SQL statistics:" | sed 's/^/> + /g' >> $SYSBENCH_RESULT_SHOW_FILE_NAME
echo "" >> $SYSBENCH_RESULT_SHOW_FILE_NAME
grep "Latency (ms):" $SYSBENCH_RESULT_FILE_NAME >> $SYSBENCH_RESULT_SHOW_FILE_NAME
grep "Latency (ms):" $SYSBENCH_RESULT_FILE_NAME -A 6 | grep -v "Latency" | sed 's/^/> + /g' >> $SYSBENCH_RESULT_SHOW_FILE_NAME


