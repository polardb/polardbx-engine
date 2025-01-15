#!/bin/sh

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


echo "cmd arg: $1 $2 $3 $4"
dos2unix $1/script/*.sh
version=1.0.0
prefix=/home/admin
RELEASE_DATE=`cat ../RELEASE_DATE`
RELEASE_TIME=$4

if [ ${#RELEASE_TIME} -gt 12 ]; then
    RELEASE_TIME=`date -d "${RELEASE_TIME:0:8} ${RELEASE_TIME:8:2}:${RELEASE_TIME:10:2}:${RELEASE_TIME:12:2}" +%s|cut -c -8`
fi
release_date_time="$RELEASE_DATE"_"$RELEASE_TIME"
echo "rpm bulid release_date_time" $release_date_time

create=$(which rpm_create)
if [ "x$create" == "x" ] || [ ! -f $create ] ; then
    create="./rpm_create"
fi
cmd="$create $2.spec -v $version -r $release_date_time -p $prefix"
echo $cmd
eval $cmd
