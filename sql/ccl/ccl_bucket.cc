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

#include "sql/ccl/ccl_bucket.h"
#include "sql/ccl/ccl.h"

namespace im {

/* Ccl queue bucket size */
ulonglong ccl_queue_bucket_size = CCL_QUEUE_BUCKET_SIZE_DEFAULT;

/* Ccl queue bucket count */
ulonglong ccl_queue_bucket_count = CCL_QUEUE_BUCKET_COUNT_DEFAULT;

/**
  Init the queue buckets, it will clear all the buckets and insert again.
*/
void ccl_queue_buckets_init(ulonglong bucket_count, ulonglong bucket_size) {
  DBUG_ENTER("init_ccl_queue_buckets");
  assert(System_ccl::instance());

  System_ccl::instance()->get_queue_buckets()->init_queue_buckets(
      bucket_count, bucket_size, Ccl_error_level::CCL_WARNING);

  DBUG_VOID_RETURN;
}

} /* namespace im */
