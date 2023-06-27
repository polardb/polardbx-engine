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
#include "sql/binlog.h"
#include "sql/mysqld.h" //innodb_hton

#include "lizard_iface.h"
#include "sql/gcn_log_event.h"
#include "sql/lizard_binlog.h"
#include "sql/lizard0handler.h" // load_gcn...

bool Gcn_manager::assign_gcn_to_flush_group(THD *first_seen) {
  bool err = false;

  for (THD *head = first_seen; head; head = head->next_to_commit) {
    if (head->owned_commit_gcn.is_empty()) {
      head->owned_commit_gcn.set(innodb_hton->ext.load_gcn(),
                                 MYSQL_CSR_AUTOMATIC);
    }
  }
  return err;
}

