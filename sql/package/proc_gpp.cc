/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/package/proc_gpp.cc
 Native procedure for retrieving gpp related statistics.

 Created 2024-04-10 by Ting Yuan
 *******************************************************/

#include "sql/package/proc_gpp.h"
#include "sql/mysqld.h"

namespace im {

const LEX_CSTRING PROC_STAT_SCHEMA = {C_STRING_WITH_LEN("dbms_stat")};

Proc *Proc_index_stat_flush_gpp::instance() {
  static Proc_index_stat_flush_gpp *proc =
      new Proc_index_stat_flush_gpp(key_memory_package);

  return proc;
}

Sql_cmd *Proc_index_stat_flush_gpp::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_index_stat_flush_gpp(thd, list, this);
}

bool Sql_cmd_index_stat_flush_gpp::pc_execute(THD *) {
  innodb_hton->ext.flush_gpp_stat();
  return false;
}

} /* namespace im */
