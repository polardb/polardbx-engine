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

/** @file sql/lizard_binlog.h

  Binlog behaviours related to lizard system.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/

#ifndef LIZARD_BINLOG_H
#define LIZARD_BINLOG_H

class THD;
class Binlog_event_writer;

extern bool opt_gcn_write_event;

class Gcn_manager {
 public:
  Gcn_manager() {}

  virtual ~Gcn_manager() {}

  bool assign_gcn_to_flush_group(THD *first_seen);

  bool write_gcn(THD *thd, Binlog_event_writer *writer);

 private:
  Gcn_manager(const Gcn_manager &) = default;
  Gcn_manager &operator=(const Gcn_manager &) = default;
  Gcn_manager(Gcn_manager &&) = default;
};

namespace lizard {
namespace xa {
/**
  Like binlog_start_trans_and_stmt. The difference is:

  binlog_start_trans_and_stmt is not sure whether to open a statement or a
  transaction, while binlog_start_trans is to open a transaction
  deterministically.
*/
int binlog_start_trans(THD *thd);
}  // namespace xa
}  // namespace lizard

#endif
