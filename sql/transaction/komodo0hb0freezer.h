/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/transaction/komodo0hb0freezer.h


  Created 2023-06-20 by zanye
 *******************************************************/

#ifndef komodo0hb0freezer_h
#define komodo0hb0freezer_h

#include <stdint.h> // uint32_t...
#include <ctime> // std::time_t...
#include <mutex>

#include "sql/ut/komodo0lazy0exec.h"

class THD;
class LEX;

namespace komodo {

extern bool opt_no_heartbeat_freeze;

extern bool no_heartbeat_freeze;

extern uint64_t opt_no_heartbeat_freeze_timeout;

class Heartbeat_freezer {
 public:
  static constexpr uint64_t PRINTER_INTERVAL_SECONDS = 180;

  Heartbeat_freezer() : m_is_freeze(false) {}

  bool is_freeze() { return m_is_freeze; }

  void heartbeat();

  bool determine_freeze();

 private:
  /** Timer for check timeout. */
  Simple_timer m_timer;

  /* No need to use std::atomic because no need to read the newest value
  immediately. */
  bool m_is_freeze;

  /* Mutex modification of m_is_freeze, m_timer. */
  std::mutex m_mutex;
};

/** Check if should be blocked all update DML becase the heartbeat timeout. */
extern bool cn_heartbeat_timeout_freeze_updating(LEX *const lex);

/** Check if should be blocked all slave apply becase the heartbeat timeout. */
extern bool cn_heartbeat_timeout_freeze_applying_event(THD *);

/** Generte a heartbeat. */
extern void hb_freezer_heartbeat();

/** Determine if frozen all update, purge and apply. */
extern bool hb_freezer_determine_freeze();
}  // namespace komodo

#endif
