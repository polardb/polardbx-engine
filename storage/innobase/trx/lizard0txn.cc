/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0txn.cc
  Lizard transaction tablespace implementation.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/

#include "trx0purge.h"

#include "lizard0dict.h"
#include "lizard0txn.h"

namespace lizard {

/** Global singlton txn tablespaces */
TXN_tablespaces txn_spaces;

/** Currently, hardcode four undo tablespaces as lizard transaction tbs. */
static_assert(dict_lizard::s_max_txn_space_id -
                      dict_lizard::s_min_txn_space_id + 1 ==
                  FSP_IMPLICIT_TXN_TABLESPACES,
              "Lizard implicit transaction needs four undo tablespace!");

/** Judge whether it's lizard transaction tablespace by num
@param[in]      num         >=1 and <= 127
@return         true        yes */
bool fsp_is_txn_tablespace_by_num(space_id_t num) {
  space_id_t space_num;
  ut_ad(num <= FSP_MAX_UNDO_TABLESPACES);

  for (space_id_t id = dict_lizard::s_max_txn_space_id;
       id >= dict_lizard::s_min_txn_space_id; id--) {
    space_num = undo::id2num(id);
    if (num == space_num) return true;
  }

  return false;
}

/** Judge whether it's lizard transaction tablespace by space id
@param[in]      num
@return         true        yes */
bool fsp_is_txn_tablespace_by_id(space_id_t id) {
  if (id >= dict_lizard::s_min_txn_space_id &&
      id <= dict_lizard::s_max_txn_space_id)
    return true;

  return false;
}

/** Make sure txn tablespace position in undo::tablespaces */
int fsp_txn_tablespace_pos_by_id(space_id_t id) {
  if (id >= dict_lizard::s_min_txn_space_id &&
      id <= dict_lizard::s_max_txn_space_id)
    return dict_lizard::s_max_txn_space_id - id;

  return -1;
}

/** validate the first undo tablespace is always transaction tablespace */
bool txn_always_first_undo_tablespace(undo::Tablespaces *spaces) {
  ut_ad(spaces->size() >= FSP_IMPLICIT_UNDO_TABLESPACES);

  for (uint i = 0; i < FSP_IMPLICIT_TXN_TABLESPACES; i++) {
    /** Array position must be the same */
    if (spaces->at(i) != txn_spaces.at(i)) return false;

    /** Space id must be from max_txn_space_id to min_txn_space_id */
    if ((dict_lizard::s_max_txn_space_id - spaces->at(i)->id()) != i)
      return false;
  }

  return true;
}

}  // namespace lizard

/**
  Mark the undo tablespace as lizard transaction tablespace
  @param[in]    undo space
*/
void undo::Tablespaces::mark_txn() {
  undo::Tablespace *undo_space;
  int pos;

  for (auto it = m_spaces.begin(); it != m_spaces.end(); it++) {
    undo_space = *it;

    if (lizard::fsp_is_txn_tablespace_by_id(undo_space->id()) &&
        !undo_space->is_txn()) {
      /** Retrieve the fixed position */
      pos = lizard::fsp_txn_tablespace_pos_by_id(undo_space->id());
      ut_ad(pos >= 0);

      /** Insert into the fixed position */
      undo_space->set_txn();
      lizard::txn_spaces.insert(pos > (int)lizard::txn_spaces.size()
                                    ? lizard::txn_spaces.end()
                                    : lizard::txn_spaces.begin() + pos,
                                undo_space);
    }
  }
}
