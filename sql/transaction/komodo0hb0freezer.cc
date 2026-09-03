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

#include "mysql/components/services/log_builtins.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "sql/mysqld.h"

#include "sql/ut/komodo0lazy0exec.h"
#include "sql/transaction/komodo0hb0freezer.h"

namespace komodo {

bool opt_no_heartbeat_freeze;

bool no_heartbeat_freeze;

uint64_t opt_no_heartbeat_freeze_timeout;

static Heartbeat_freezer hb_freezer;

/*************************************************
 *                Heartbeat Freezer              *
 *************************************************/
void Heartbeat_freezer::heartbeat() {
  std::lock_guard<std::mutex> guard(m_mutex);
  m_timer.update();
  m_is_freeze = false;
}

bool Heartbeat_freezer::determine_freeze() {
  bool block;
  static Lazy_executor printer(std::chrono::seconds{PRINTER_INTERVAL_SECONDS});

  std::lock_guard<std::mutex> guard(m_mutex);

  if (!opt_no_heartbeat_freeze) {
    block = false;
    goto exit_func;
  }

  block = (m_timer.since_last_update() >
           std::chrono::seconds(opt_no_heartbeat_freeze_timeout));

exit_func:
  if (block) {
    printer.try_run([]() {
      LogErr(INFORMATION_LEVEL, ER_LIZARD,
             "The purge sys is blocked because no heartbeat has been received "
             "for a long time. If you want to advance the purge sys, please "
             "call dbms_xa.send_heartbeat().");
    });

    m_is_freeze = true;
  } else {
    m_is_freeze = false;
    printer.reset();
  }

  return block;
}

void hb_freezer_heartbeat() { hb_freezer.heartbeat(); }

bool hb_freezer_determine_freeze() { return hb_freezer.determine_freeze(); }

bool hb_freezer_is_freeze() {
  return opt_no_heartbeat_freeze && hb_freezer.is_freeze();
}

bool cn_heartbeat_timeout_freeze_updating(LEX *const lex) {
  DBUG_EXECUTE_IF("hb_timeout_do_not_freeze_operation", { return false; });
  switch (lex->sql_command) {
    case SQLCOM_ADMIN_PROC:
      break;

    default:
      if ((sql_command_flags[lex->sql_command] & CF_CHANGES_DATA) &&
          hb_freezer_is_freeze() && likely(mysqld_server_started)) {
        my_error(ER_XA_PROC_HEARTBEAT_FREEZE, MYF(0));
        return true;
      }
  }

  return false;
}

bool cn_heartbeat_timeout_freeze_applying_event(THD *thd) {
  static Lazy_executor printer(
      std::chrono::seconds{Heartbeat_freezer::PRINTER_INTERVAL_SECONDS});

  if (hb_freezer_is_freeze()) {
    THD_STAGE_INFO(thd, stage_wait_for_cn_heartbeat);

    printer.try_run([]() {
      LogErr(INFORMATION_LEVEL, ER_LIZARD,
             "Applying event is blocked because no heartbeat has been received "
             "for a long time. If you want to advance it, please call "
             "dbms_xa.send_heartbeat() (or set global "
             "innodb_cn_no_heartbeat_freeze = 0).");
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    return true;
  } else {
    printer.reset();
    return false;
  }
}

}  // namespace komodo
