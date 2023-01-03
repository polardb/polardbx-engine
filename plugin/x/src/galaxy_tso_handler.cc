/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


#include "galaxy_tso_handler.h"
#include "plugin/x/src/xpl_error.h"
#include "plugin/x/src/xpl_log.h"
#include "sql/timestamp_service.h"

#include <sys/time.h>

namespace gx {

ngs::Error_code Get_tso_handler::execute(const Mysqlx::GetTSO &msg) {
  uint64_t ts = 0;

  std::string owner = msg.leader_name();
  int32_t batch = msg.batch_count();

  log_debug("tso required leader_name is %s, batch is %d", owner.c_str(),
            batch);

  int err_no = 0;
  if ((err_no = get_tso(ts, batch))) {
    log_error(ER_XPLUGIN_ERROR_MSG, "tso get failed");
  } else if (send_tso(ts, err_no)) {
    log_error(ER_XPLUGIN_ERROR_MSG, "tso send failed");
  }

  return err_no ? ngs::Error(ER_X_GALAXY_TSO_FAILED, "tso acquire failed")
                : ngs::Success();
}

int Get_tso_handler::get_tso(uint64_t &ts, int32_t batch) {
  THD *thd = m_session->get_thd();
  int32_t ret = 0;

  TimestampService ts_service(thd, SYS_GTS_DB, SYS_GTS_TABLE);

  /* Open the base table */
  if (ts_service.init()) {
    ret = 1;
  } else {
    /* Get one timestamp value from base table */
    if (ts_service.get_timestamp(ts, batch)) {
      ret = 1;
    }

    ts_service.deinit();
  }

  return ret;
}

int Get_tso_handler::send_tso(uint64_t ts, int32_t err_no) {
  auto &proto = m_session->proto();
  proto.send_tso(ts, err_no);

  return 0;
}

}  // namespace gx
