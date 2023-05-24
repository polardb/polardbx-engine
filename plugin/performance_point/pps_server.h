/* Copyright (c) 2000, 2018, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef PP_PPS_SERVER_H_INCLUDED
#define PP_PPS_SERVER_H_INCLUDED

#include "my_inttypes.h"

/** The performance point variables */
extern bool opt_performance_point_lock_rwlock_enabled;

/* Whether the performance point take effect */
extern bool opt_performance_point_enabled;

extern bool opt_performance_point_dbug_enabled;

/* The max array size of IO statisitcs */
extern ulong performance_point_iostat_volume_size;

/* The time interval of aggregating IO statistics */
extern ulong performance_point_iostat_interval;

/* pre_init_xxx should be called before performance_point plugin init */
extern void pre_init_performance_point();

extern void destroy_performance_point();

#endif
