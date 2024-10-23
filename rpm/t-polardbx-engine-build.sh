#!/bin/sh

dos2unix $1/script/*.sh
version=1.0.0
prefix=/home/admin
RELEASE=$4
if [ ${#RELEASE} -gt 12 ]; then
    RELEASE=`date -d "${RELEASE:0:8} ${RELEASE:8:2}:${RELEASE:10:2}:${RELEASE:12:2}" +%s|cut -c -8`
fi
DATE_RELEASE="`date +%Y%m%d`_$RELEASE"
export RELEASE
export DATE_RELEASE
echo "rpm bulid RELEASE" $RELEASE
echo "rpm bulid DATE_RELEASE" $DATE_RELEASE
create=$(which rpm_create)
if [ "x$create" == "x" ] || [ ! -f $create ] ; then
    create="./rpm_create"
fi
cmd="$create $2.spec -v $version -r "$DATE_RELEASE" -p $prefix"
echo $cmd
eval $cmd
