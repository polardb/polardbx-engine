/* Copyright (c) 2008, 2018, Alibaba and/or its affiliates. All rights reserved.

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
#ifndef XA_EXT_INCUDED
#define XA_EXT_INCUDED

namespace lizard {
namespace xa {
/**
  1. start trx in innodb
  2. register innodb as a participants
  3. alloc transaction slot in innodb
  4. register binlog as another participants if need
*/
bool replay_trx_slot_alloc_on_slave(THD *thd);
}
}  // namespace lizard

struct LEX;
class THD;
namespace im {
bool cn_heartbeat_timeout_freeze_updating(LEX *const lex);
bool cn_heartbeat_timeout_freeze_applying_event(THD *);
}

#endif // XA_EXT_INCUDED
