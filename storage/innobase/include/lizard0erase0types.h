/*****************************************************************************

Copyright (c) 1996, 2024, Oracle and/or its affiliates.

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

/** @file include/lizard0erase0types.h
 Erase old versions for semi-purge

 Created 13/05/2024 jiyang.zhang
 *******************************************************/

#ifndef lizard0erase0types_h
#define lizard0erase0types_h

#include "lizard0undo0types.h"

struct txn_cursor_t {
  /** trx_id that is used to check if the TXN is valid. */
  trx_id_t trx_id;

  /** TXN address */
  slot_addr_t txn_addr;

  txn_cursor_t() : trx_id(0), txn_addr() {}
};

namespace lizard {

/**
  The element of minimum heap for the erase sys.
*/
class UpdateUndoRseg {
 public:
  explicit UpdateUndoRseg(scn_t scn, trx_rseg_t *rseg)
      : m_scn(scn), m_rseg(rseg) {
    ut_ad(!m_rseg || !m_rseg->is_txn);
  }

  /** Default constructor */
  UpdateUndoRseg() : UpdateUndoRseg(0, nullptr) {}

  void set_scn(scn_t scn) { m_scn = scn; }

  scn_t get_scn() const { return m_scn; }

  void set_rseg(trx_rseg_t *rseg) { m_rseg = rseg; }

  trx_rseg_t *get_rseg() const { return m_rseg; }

  /** Compare two UpdateUndoRseg based on scn.
  @param lhs first element to compare
  @param rhs second element to compare
  @return true if elem1 > elem2 else false.*/
  bool operator()(const UpdateUndoRseg &lhs, const UpdateUndoRseg &rhs) {
    return (lhs.m_scn > rhs.m_scn);
  }

 private:
  scn_t m_scn;

  /** Rollback segments of update undo. */
  trx_rseg_t *m_rseg;
};

/**
  Use priority_queue as the minimum heap structure
  which is order by scn number */
typedef std::priority_queue<
    UpdateUndoRseg, std::vector<UpdateUndoRseg, ut::allocator<UpdateUndoRseg>>,
    UpdateUndoRseg>
    erase_heap_t;

}  // namespace lizard

#endif
