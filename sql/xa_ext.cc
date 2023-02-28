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

#include "storage/innobase/include/lizard0xa0iface.h"

#include "sql/binlog_ext.h"

namespace lizard {
namespace xa {
bool replay_trx_slot_alloc_on_slave(THD *thd) {
  if (thd->slave_thread) {
    if (lizard::xa::start_and_register_rw_trx_for_xa(thd) ||
        lizard::xa::trx_slot_assign_for_xa(thd, nullptr)) {
      /** TODO: fix it <01-03-23, zanye.zjy> */
      my_error(ER_XA_PROC_REPLAY_TRX_SLOT_ALLOC_ERROR, MYF(0));
      return true;
    } else if ((thd->variables.option_bits & OPTION_BIN_LOG) &&
               lizard::xa::binlog_start_trans(thd)) {
      my_error(ER_XA_PROC_REPLAY_REGISTER_BINLOG_ERROR, MYF(0));
      return true;
    }
  }

  return false;
}
}  // namespace xa
}  // namespace lizard
