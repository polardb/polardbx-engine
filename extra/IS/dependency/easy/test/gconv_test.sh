#!/bin/bash

# Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.
# 
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License, version 2.0, as published by the
# Free Software Foundation.
# 
# This program is also distributed with certain software (including but not
# limited to OpenSSL) that is licensed under separate terms, as designated in a
# particular file or component or in included license documentation. The authors
# of MySQL hereby grant you an additional permission to link the program and
# your derivative works with the separately licensed software that they have
# included with MySQL.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
# for more details.
# 
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

ROOT_DIR=/home/duolong/run/cai/htdocs/gconv
if [ ! -z "$1" ]; then
    ROOT_DIR="$1"
fi


CUR_PATH=`pwd`
cd `dirname $0`/..
BASE_HOME=`pwd`
echo $BASE_HOME

cd $CUR_PATH 
make -s check
lcov -d $BASE_HOME/src -t 'gcov' -o "$ROOT_DIR/src.info" -c
lcov -d $BASE_HOME/test -t 'gcov' -o "$ROOT_DIR/test.info" -c
lcov -a $ROOT_DIR/test.info -a $ROOT_DIR/src.info -o $ROOT_DIR/total.inf
genhtml -o $ROOT_DIR/result $ROOT_DIR/total.inf 

