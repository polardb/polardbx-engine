/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_CCL_CCL_BUCKET_INCLUDED
#define SQL_CCL_CCL_BUCKET_INCLUDED

#include "my_inttypes.h"

/* CCL queue bucket count predefined MAX and DEFAULT value */
#define CCL_QUEUE_BUCKET_COUNT_MAX 64
#define CCL_QUEUE_BUCKET_COUNT_DEFAULT 4

/* CCL queue bucket size predefined MAX and DEFAULT value */
#define CCL_QUEUE_BUCKET_SIZE_MAX 4096
#define CCL_QUEUE_BUCKET_SIZE_DEFAULT 64

namespace im {

extern ulonglong ccl_queue_bucket_size;

extern ulonglong ccl_queue_bucket_count;

/**
  Init the queue buckets, it will clear all the buckets and insert again.
*/
extern void ccl_queue_buckets_init(ulonglong bucket_count,
                                   ulonglong bucket_size);

} /* namespace im */

#endif
