/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file sql/xa/komodo0xa.cc
 Komodo xa transaction.

 Created 2025-05-28 by komodo
 *******************************************************/
#include <new>

#include "sql/xa/komodo0xa.h"

namespace komodo {

/** Singleton xa manager instance. */
XA_manager *XA_manager::xa_mgr = nullptr;

/** New instance when tansaction cache inited.
 *
 * @retval	false	success;*/
bool XA_manager::create_instance() {
  assert(xa_mgr == nullptr);
  xa_mgr = new (std::nothrow) XA_manager();
  return xa_mgr == nullptr;
}

/** Destroy instance */
void XA_manager::destroy_instance() {
  if (xa_mgr) {
    delete xa_mgr;
    xa_mgr = nullptr;
  }
}

/** Return thread running num that are executing xa command. */
ulonglong xa_thread_running() {
  return XA_manager::instance()->thread_running();
}

}  // namespace komodo
