/* Copyright (c) 2000, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_LEX_EXT_H_INCLUDED
#define SQL_LEX_EXT_H_INCLUDED

#include "lex_string.h"
#include "my_inttypes.h"

class THD;
struct Parse_context;
class TABLE_LIST;
class Item;

namespace im {

class Snapshot_info_t;

struct Table_snapshot {
  Item *ts;
  Item *scn;
  Item *gcn;

  bool is_set() const { return (ts || scn || gcn); }
  bool valid() const {
    return ((ts == nullptr ? 0 : 1) + (scn == nullptr ? 0 : 1) +
                (gcn == nullptr ? 0 : 1) <=
            1);
  }

  bool itemize(Parse_context *pc, TABLE_LIST *owner);
  bool fix_fields(THD *thd);
  bool evaluate(Snapshot_info_t *snapshot);

  static bool evaluate_timestamp(Item *ts, uint64_t *ts_out);
  static bool evaluate_scn(Item *scn, uint64_t *scn_out);

  static bool evaluate_gcn(Item *gcn, uint64_t *gcn_out);
};

struct Table_snapshot_and_alias {
  LEX_CSTRING alias;
  Table_snapshot snapshot;
};

} // namespace im

#endif

