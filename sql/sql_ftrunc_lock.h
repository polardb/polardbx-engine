/* Copyright (c) 2024, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_FTRUNC_LOCK_INCLUDED
#define SQL_FTRUNC_LOCK_INCLUDED

#include <sys/types.h>

#include "my_systime.h"  // Timeout_type, TIMEOUT_INF
#include "sql/mdl.h"     // enum_mdl_type

class THD;

/**
  Acquire Ftrunc Lock.

  @param[in] thd                Current thread context
  @param[in] lock_wait_timeout  How many seconds to wait before timeout.
  @param[in] mdl_type           MDL lock type (MDL_EXCLUSIVE or MDL_SHARED)

  @return Operation status.
    @retval false Success
    @retval true  Failure
*/

bool acquire_ftrunc_lock(THD *thd, ulong lock_wait_timeout,
                         enum_mdl_type mdl_type);

/**
  Release Ftrunc Lock if it was acquired.

  @param[in] thd         Current thread context
*/

void release_ftrunc_lock(THD *thd);

#endif /* SQL_FTRUNC_LOCK_INCLUDED */

