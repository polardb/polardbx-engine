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


#ifndef PLUGIN_X_SRC_GALAXY_TSO_HANDLER_H
#define PLUGIN_X_SRC_GALAXY_TSO_HANDLER_H

#include "plugin/x/generated/protobuf/galaxyx.pb.h"
#include "plugin/x/ngs/include/ngs/error_code.h"
#include "plugin/x/ngs/include/ngs/interface/session_interface.h"
#include "plugin/x/ngs/include/ngs/interface/sql_session_interface.h"
#include "plugin/x/ngs/include/ngs/protocol_fwd.h"

// The name of default sequence table used for TSO service
#define SYS_GTS_DB      "mysql"
#define SYS_GTS_TABLE   "gts_base"

// Error code which will be sent to client
#define GTS_PROTOCOL_SUCCESS 0
#define GTS_PROTOCOL_INIT_FAILED 1
#define GTS_PROTOCOL_SERVICE_ERROR 2

namespace ngs {
class Session_interface;
}

namespace gx {

class Get_tso_handler {
 public:
  explicit Get_tso_handler(ngs::Session_interface *session)
      : m_session(session) {}

  ngs::Error_code execute(const Mysqlx::GetTSO &msg);

  int get_tso(uint64_t &ts, int32_t batch);
  int send_tso(uint64_t ts, int32_t err_no);

 private:
  ngs::Session_interface *m_session;
};

}  // namespace gx

#endif // PLUGIN_POLARX_SRC_EXEC_GET_TSO_HANDLER_H_
