/* Copyright (c) 2008, 2023, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef XA_TRX_INCUDED
#define XA_TRX_INCUDED

#include "sql/lizard_binlog.h"
#include "sql/xa.h"

#include "lizard_iface.h"

class THD;
struct LEX;

namespace lizard {
namespace xa {

/**
  Apply for a readwrite transaction specially for external xa through allocating
  transaction slot from transaction slot storage engine.

    1. start trx in transaction slot storage engine.[ttse]
    2. register ttse as a participants
    3. alloc transaction slot in ttse
    4. register binlog as another participants if need

  @param[in]	Thread handler
  @param[in]	XID
  @param[out]	transaction slot address
*/
extern bool apply_trx_for_xa(THD *thd, const XID *xid, my_slot_ptr_t *slot_ptr);

class Simple_timer {
 public:
  Simple_timer() : m_ts(0) {}

  void update() { m_ts = std::time(0); }

  uint64_t since_last_update() const {
    std::time_t now = std::time(0);
    return now - m_ts;
  }

 private:
  std::time_t m_ts;
};

class Lazy_printer {
 public:
  Lazy_printer(const uint32_t interval_secs)
      : m_internal_secs(interval_secs), m_timer(), m_first(true) {}

  bool print(const char *msg);
  void reset();

 private:
  /** log printing interval */
  const uint32_t m_internal_secs;

  /** Timer */
  Simple_timer m_timer;

  /** First time to print, no take interval into consideration. */
  bool m_first;
};

extern bool opt_no_heartbeat_freeze;
extern bool no_heartbeat_freeze;
extern uint64_t opt_no_heartbeat_freeze_timeout;

class Heartbeat_freezer {
 public:
  Heartbeat_freezer() : m_is_freeze(false) {}

  bool is_freeze() { return m_is_freeze; }

  void heartbeat() {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_timer.update();
    m_is_freeze = false;
  }

  bool determine_freeze() {
    uint64_t diff_time;
    bool block;
    constexpr uint64_t PRINTER_INTERVAL_SECONDS = 180;
    static Lazy_printer printer(PRINTER_INTERVAL_SECONDS);

    std::lock_guard<std::mutex> guard(m_mutex);

    if (!opt_no_heartbeat_freeze) {
      block = false;
      goto exit_func;
    }

    diff_time = m_timer.since_last_update();

    block = (diff_time > opt_no_heartbeat_freeze_timeout);

  exit_func:
    if (block) {
      printer.print(
          "The purge sys is blocked because no heartbeat has been received "
          "for a long time. If you want to advance the purge sys, please call "
          "dbms_xa.send_heartbeat().");

      m_is_freeze = true;
    } else {
      m_is_freeze = false;
      printer.reset();
    }

    return block;
  }

 private:
  /** Timer for check timeout. */
  Simple_timer m_timer;

  /* No need to use std::atomic because no need to read the newest value
  immediately. */
  bool m_is_freeze;

  /* Mutex modification of m_is_freeze. */
  std::mutex m_mutex;
};

extern bool cn_heartbeat_timeout_freeze_updating(LEX *const lex);

extern bool cn_heartbeat_timeout_freeze_applying_event(THD *);

extern void hb_freezer_heartbeat();

extern bool hb_freezer_is_freeze();

}  // namespace xa
}  // namespace lizard

#endif  // XA_TRX_INCUDED
