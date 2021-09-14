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

WAIT_READY_TIMEOUT=1000
CURRENT_CHECK=0

export LD_LIBRARY_PATH=/usr/local/gcc-5.3.0/lib64/:$INSTALL_PATH/lib:$LD_LIBRARY_PATH

while [ $CURRENT_CHECK -le $WAIT_READY_TIMEOUT ]
do
  CURRENT_CHECK=$(($CURRENT_CHECK + 1))
  $INSTALL_PATH/bin/mysql -S $DATA_PATH/mysql1.sock --port=$SVR_PORT -uroot mysql -e "select \"connected\""

  if [ $? -eq 0 ]; then
    echo "Server is ready now, WAIT_READY_TIMEOUT=$WAIT_READY_TIMEOUT, CURRENT_CHECK=$CURRENT_CHECK"
    #$INSTALL_PATH/bin/mysql -S $DATA_PATH/mysql1.sock --port=$SVR_PORT -uroot mysql -e "show tables;"
    exit 0
  fi

  process=$(ps -elf | grep mysqld | grep "$DATA_PATH/mysql1.sock" | wc -l)
  if [ $process -ne 1 ]; then
    echo "process=$process"
    echo "Error, wait server start error"
    exit 1
  fi

  echo "Continue to wait server ready, WAIT_READY_TIMEOUT=$WAIT_READY_TIMEOUT, CURRENT_CHECK=$CURRENT_CHECK"
  sleep 1
done

echo "Error, wait server ready timeout WAIT_READY_TIMEOUT=$WAIT_READY_TIMEOUT, CURRENT_CHECK=$CURRENT_CHECK"
exit 1

