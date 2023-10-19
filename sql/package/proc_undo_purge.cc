/* Copyright (c) 2018, 2023, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/auth/auth_acls.h"
#include "sql/auth/sql_security_ctx.h"

#include "sql/package/package_parse.h"
#include "sql/package/proc.h"
#include "sql/package/proc_undo_purge.h"

#include "sql/mysqld.h"
#include "sql/protocol.h"

namespace im {

const LEX_CSTRING PROC_UNDO_SCHEMA = {C_STRING_WITH_LEN("dbms_undo")};

Proc *Proc_purge_status::instance() {
  static Proc_purge_status *proc = new Proc_purge_status(key_memory_package);

  return proc;
}

Sql_cmd *Proc_purge_status::evoke_cmd(THD *thd,
                                      mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_purge_status(thd, list, this);
}

bool Sql_cmd_purge_status::pc_execute(THD *) {
  bool error = false;
  return error;
}

void Sql_cmd_purge_status::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();

  if (error) {
    assert(thd->is_error());
    return;
  }

  Undo_purge_show_result result;
  innodb_hton->ext.get_undo_purge_status(&result);

  if (m_proc->send_result_metadata(thd)) return;

  protocol->start_row();
  protocol->store_longlong(result.used_size, true);
  protocol->store_longlong(result.file_size, true);
  protocol->store_longlong(result.retained_time, true);
  protocol->store_longlong(result.retention_time, true);
  protocol->store_longlong(result.reserved_size, true);
  protocol->store_longlong(result.retention_size_limit, true);
  protocol->store(&result.blocked_cause);
  if (result.blocked_utc != 0) {
    String utc_str;
    utc_to_str(result.blocked_utc, &utc_str);
    protocol->store(&utc_str);
  } else {
    protocol->store_null();
  }

  if (protocol->end_row()) return;

  my_eof(thd);
}

/**
  A helper function to convert a timestamp (microseconds since epoch) to a
  string of the form YYYY-MM-DD HH:MM:SS.UUUUUU.
*/
size_t Sql_cmd_purge_status::utc_to_str(ulonglong timestamp, String *s) {
  char buf[256];
  time_t seconds = (time_t)(timestamp / 1000000);
  int useconds = (int)(timestamp % 1000000);
  struct tm time_struct;
  localtime_r(&seconds, &time_struct);
  size_t length = strftime(buf, 255, "%F %T", &time_struct);
  length += sprintf(buf + length, ".%06d", useconds);
  length += strftime(buf + length, 255, " %Z", &time_struct);

  s->copy(buf, strlen(buf), s->charset());
  return length;
}
} /* namespace im */
