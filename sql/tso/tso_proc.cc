/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.

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

#include "tso_proc.h"
#include "my_inttypes.h"
#include "sql/ha_sequence.h"
#include "sql/log.h"
#include "sql/protocol.h"
#include "sql/timestamp_service.h"

namespace im {

/**
  Timestamp service native procedure schema: dbms_tso
*/
const LEX_CSTRING TSO_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_tso")};

/**
  Get the instance of native procedure.

  @retval       Handler to the proc instance
*/
Proc *Proc_get_timestamp::instance() {
  static Proc_get_timestamp *proc = new Proc_get_timestamp(key_memory_package);
  return proc;
}

Sql_cmd *Proc_get_timestamp::evoke_cmd(THD *thd,
                                       mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Cmd_get_timestamp(thd, list, this);
}

/**
  Execute the native procedure.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Cmd_get_timestamp::pc_execute(THD *thd) {
  bool ret = false;

  assert(m_proc->get_result_type() == Proc::Result_type::RESULT_SET);

  /* Get db name and table name */
  auto it = m_list->begin();
  Item_string *db_name_item = dynamic_cast<Item_string *>(*(it++));
  String *db_name = db_name_item->val_str(nullptr);

  Item_string *table_name_item = dynamic_cast<Item_string *>(*(it++));
  String *table_name = table_name_item->val_str(nullptr);

  /* Get number of timestamp value requested */
  Item_int *batch_size_item = dynamic_cast<Item_int *>(*(it++));
  uint64 batch_size = batch_size_item->val_uint();

  /* Initialize timestamp service and get the timestamp value */
  TimestampService ts_service(thd, db_name->ptr(), table_name->ptr());

  /* Open the base table */
  if (ts_service.init()) {
    ret = true;
  } else {
    /* Get batch_size timestamp values from base table */
    ret = ts_service.get_timestamp(m_timestamp, batch_size);

    ts_service.deinit();
  }

  return ret;
}

/**
  Get the TIMESTAMP value and send it to client.

  @param[in]    thd           Thread context
  @param[in]    error         Error occurred previously

  @retval       None
*/
void Cmd_get_timestamp::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();

  /* No need to proceed if error occurred */
  if (error) {
    assert(thd->is_error());
    return;
  }

  if (m_proc->send_result_metadata(thd)) return;

  protocol->start_row();
  protocol->store((longlong)m_timestamp);

  if (protocol->end_row()) return;

  my_eof(thd);
}

}  // namespace im
