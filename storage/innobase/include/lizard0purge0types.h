/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0purge.h
 Lizard transaction purge system implementation.

 Created 2020-03-27 by zanye.zjy
 *******************************************************/

#ifndef lizard0purge0types_h
#define lizard0purge0types_h

#include "lizard0undo0types.h"

namespace lizard {

/**
  The element of minimum heap for the purge.
*/
class TxnUndoRsegs {
 public:
  explicit TxnUndoRsegs(scn_t scn) : m_scn(scn) {
    for (auto &rseg : m_rsegs) {
      rseg = nullptr;
    }
  }

  /** Default constructor */
  TxnUndoRsegs() : TxnUndoRsegs(0) {}

  void set_scn(scn_t scn) { m_scn = scn; }

  scn_t get_scn() const { return m_scn; }

  /** Add rollback segment.
  @param rseg rollback segment to add. */
  void insert(trx_rseg_t *rseg) {
    for (size_t i = 0; i < m_rsegs_n; ++i) {
      if (m_rsegs[i] == rseg) {
        return;
      }
    }
    // ut_a(m_rsegs_n < 2);
    /* Lizard: one more txn rseg. */
    ut_a(m_rsegs_n < 2 + 1);
    m_rsegs[m_rsegs_n++] = rseg;
  }

  Rsegs_array<3>::iterator arrange_txn_first() {
    ut_ad(m_rsegs.size() > 0);

    auto iter = begin();
    while (iter != end()) {
      if ((*iter)->is_txn) {
        if (iter != begin()) {
          /* Move txn rseg to position 0 */
          std::swap(*iter, m_rsegs.front());
        }
        break;
      }
      ++iter;
    }

    /* If no txn rseg, then only one temp rseg */
    ut_ad(iter != end() || size() == 1);

    return begin();
  }

  /** Number of registered rsegs.
  @return size of rseg list. */
  size_t size() const { return (m_rsegs_n); }

  /**
  @return an iterator to the first element */
  typename Rsegs_array<3>::iterator begin() { return m_rsegs.begin(); }

  /**
  @return an iterator to the end */
  typename Rsegs_array<3>::iterator end() {
    return m_rsegs.begin() + m_rsegs_n;
  }

  /** Append rollback segments from referred instance to current
  instance. */
  void insert(const TxnUndoRsegs &append_from) {
    ut_ad(get_scn() == append_from.get_scn());
    for (size_t i = 0; i < append_from.m_rsegs_n; ++i) {
      insert(append_from.m_rsegs[i]);
    }
  }

  /** Compare two TxnUndoRsegs based on scn.
  @param lhs first element to compare
  @param rhs second element to compare
  @return true if elem1 > elem2 else false.*/
  bool operator()(const TxnUndoRsegs &lhs, const TxnUndoRsegs &rhs) {
    return (lhs.m_scn > rhs.m_scn);
  }

  /** Compiler defined copy-constructor/assignment operator
  should be fine given that there is no reference to a memory
  object outside scope of class object.*/

 private:
  scn_t m_scn;

  size_t m_rsegs_n{};

  /** Rollback segments of a transaction, scheduled for purge. */
  // Rsegs_array<2> m_rsegs;
  /* Lizard: one more txn rseg. */
  Rsegs_array<3> m_rsegs;
};

/**
  Use priority_queue as the minimum heap structure
  which is order by scn number */
typedef std::priority_queue<
    TxnUndoRsegs, std::vector<TxnUndoRsegs, ut::allocator<TxnUndoRsegs>>,
    TxnUndoRsegs>
    purge_heap_t;

} /* namespace lizard */

#endif
