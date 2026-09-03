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

/** @file sql/xa/komodo0xa.h
 Komodo xa transaction.

 Created 2025-05-28 by komodo
 *******************************************************/

#ifndef komodo0xa_h
#define komodo0xa_h

#include <assert.h>
#include <stddef.h>
#include <sys/types.h>
#include <atomic>

#include "my_inttypes.h"
#include "sql/raii/sentry.h"

namespace komodo {

/** XA transaction manager */
class XA_manager {
 public:
  /** New instance when transaction cache inited.
   *
   * @retval	false	success;*/
  static bool create_instance();

  /** Destroy instance */
  static void destroy_instance();

  // Getter instance
  static XA_manager *instance() {
    assert(xa_mgr);
    return xa_mgr;
  }

  /** Increase thread running for xa command */
  void inc_thread_running() { m_atomic_thread_running++; }
  /** Decrease thread running for xa command */
  void dec_thread_running() { m_atomic_thread_running--; }
  // Get thread running which is executing xa command.
  ulonglong thread_running() const {
    return m_atomic_thread_running.load(std::memory_order_relaxed);
  }

 private:
  XA_manager(const XA_manager &) = delete;
  XA_manager(const XA_manager &&) = delete;
  XA_manager &operator=(const XA_manager &) = delete;

 private:
  explicit XA_manager() : m_atomic_thread_running(0) {}

  virtual ~XA_manager() { assert(m_atomic_thread_running == 0); }

  /** Singleton xa manager instance. */
  static XA_manager *xa_mgr;

 private:
  std::atomic<ulonglong> m_atomic_thread_running;
};

/** Guard thread running for xa command. */
class XA_running_guard : public raii::Sentry<> {
 public:
  XA_running_guard()
      : Sentry{[]() -> void { XA_manager::instance()->dec_thread_running(); }} {
    /** inc running */
    XA_manager::instance()->inc_thread_running();
  }

  virtual ~XA_running_guard() override = default;
};

/** Return thread running counter that executing xa command. */
extern ulonglong xa_thread_running();

}  // namespace komodo

#endif
