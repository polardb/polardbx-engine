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

#include "sql_ftrunc_lock.h"

#include <cassert>

#include "sql/mdl.h"
#include "sql_class.h"

/**
  MDL_release_locks_visitor subclass to release MDL for FTRUNC_LOCK.
*/

class Release_all_ftrunc_locks : public MDL_release_locks_visitor {
 public:
  bool release(MDL_ticket *ticket) override {
    return ticket->get_key()->mdl_namespace() == MDL_key::FTRUNC_LOCK;
  }
};

bool acquire_ftrunc_lock(THD *thd, ulong lock_wait_timeout,
                         enum_mdl_type mdl_type) {
  assert(mdl_type == MDL_EXCLUSIVE || mdl_type == MDL_SHARED);

  MDL_request mdl_request;
  enum_mdl_duration duration = MDL_EXPLICIT;

  MDL_REQUEST_INIT(&mdl_request, MDL_key::FTRUNC_LOCK, "", "", mdl_type,
                   duration);
  return thd->mdl_context.acquire_lock(&mdl_request, lock_wait_timeout);
}

void release_ftrunc_lock(THD *thd) {
  Release_all_ftrunc_locks lock_visitor;
  thd->mdl_context.release_locks(&lock_visitor);
}
