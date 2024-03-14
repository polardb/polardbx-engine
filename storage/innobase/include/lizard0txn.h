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

/** @file include/lizard0txn.h
  Lizard transaction tablespace implementation.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn_h
#define lizard0txn_h

#include "lizard0txn0types.h"
#include "ut0new.h"

namespace undo {
class Tablespace;
class Tablespaces;
}  // namespace undo

namespace lizard {

/** Type of txn tablespace container */
using TXN_tablespaces =
    std::vector<undo::Tablespace *, ut::allocator<undo::Tablespace *>>;

/** Global singlton txn tablespaces */
extern TXN_tablespaces txn_spaces;

/** Judge whether it's lizard transaction tablespace by num
@param[in]      num         >=1 and <= 127
@return         true        yes */
bool fsp_is_txn_tablespace_by_num(space_id_t num);

/** Judge whether it's lizard transaction tablespace by space id
@param[in]      num         space id
@return         true        yes */
bool fsp_is_txn_tablespace_by_id(space_id_t id);

/** Make sure txn tablespace position in undo::tablespaces */
int fsp_txn_tablespace_pos_by_id(space_id_t id);

/**
  Mark the undo tablespace as lizard transaction tablespace
  @param[in]    undo spaces
*/
void mark_txn_tablespace(undo::Tablespaces *spaces);

/** validate the first undo tablespaces are always transaction tablespace */
bool txn_always_first_undo_tablespace(undo::Tablespaces *spaces);

}  // namespace lizard

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

#define lizard_verify_txn_tablespace_by_id(id, is)       \
  do {                                                   \
    ut_a(lizard::fsp_is_txn_tablespace_by_id(id) == is); \
  } while (0)

#else

#define lizard_verify_txn_tablespace_by_id(id, is)

#endif

#endif  // lizard0txn_h
