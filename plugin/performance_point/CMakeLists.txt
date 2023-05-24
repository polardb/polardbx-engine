# Copyright (c) 2006, 2018, Alibaba and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

SET(PERFORMANCE_POINT_SOURCES
    plugin_pps.h
    pps.h
    pps_stat.h
    pps.cc
    pps_stat.cc
    pps_server.cc
    pps_table_iostat.cc)

#Since pfs_connect_attr_t linked all static plugin, and PPS plugin need ppi.cc
#so add the library ppi dependency

MYSQL_ADD_PLUGIN(performance_point ${PERFORMANCE_POINT_SOURCES}
                 MANDATORY STATIC_ONLY
                 MODULE_OUTPUT_NAME "performance_point" LINK_LIBRARIES ppi)
