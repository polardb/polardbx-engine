#!/bin/sh

# Copyright (c) 2003, 2014, Oracle and/or its affiliates. All rights reserved.
# Use is subject to license terms
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

cores=`find result -name 'core*'`
if [ "$cores" ]
then
    for i in "$cores"
    do
	atrt-backtrace.sh $i
    done
fi

STATEFILE=atrt-analyze-result.state
OLDOK=`cat "${STATEFILE}" 2>/dev/null`
LOGFILES=`find result/ -name log.out`
awk '
BEGIN                  { FAILED = OK = 0 }
/NDBT_ProgramExit: OK/ { OK++ ; next }
/NDBT_ProgramExit: /   { FAILED++ }
END                    { if (FAILED || !(OK > int(OLDOK)))
                           { OK = 0 }
                         print OK
                         if (OK) { exit(0) }
                         else    { exit(1) }
                       }
' OLDOK="${OLDOK}" ${LOGFILES} > "${STATEFILE}"
