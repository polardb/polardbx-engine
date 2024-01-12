/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file sql/lizard0recovery.h

  Transaction coordinator log recovery

  Created 2023-06-14 by Jianwei.zhao
 *******************************************************/

#ifndef SQL_BINLOG_LIZARD0RECOVERY_H
#define SQL_BINLOG_LIZARD0RECOVERY_H

#include "sql/binlog/binlog_xa_specification.h"
#include "sql/mem_root_allocator.h"
#include "sql/xa.h"

#include "sql/lizard0handler.h"

namespace binlog {

constexpr uint32_t XA_SPEC_RECOVERY_SERVER_VERSION_REQUIRED = 80032;

/** Binlog XA specification recovery */
class XA_spec_recovery {
 public:
  XA_spec_recovery()
      : m_mem_root(key_memory_binlog_recover_exec,
                   static_cast<size_t>(my_getpagesize())),
        m_spec_list(&m_mem_root) {}

  void add(const my_xid xid, const Binlog_xa_specification &spec);
  void add(const XID &xid, const Binlog_xa_specification &spec);

  XA_spec_list *xa_spec_list() { return &m_spec_list; }

  void clear() { m_spec_list.clear(); }

 private:
  MEM_ROOT m_mem_root;
  XA_spec_list m_spec_list;
};

void log_gtid_set(Gtid_set *gtid_set, bool need_lock, const char *title);

}  // namespace binlog
#endif
