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

/** @file include/lizard0trx0purge.h
 Lizard transaction purge system implementation.

 Created 2020-03-27 by zanye.zjy
 *******************************************************/

#ifndef lizard0trx0purge0types_h
#define lizard0trx0purge0types_h


namespace lizard {

/**
  The element of minimum heap for the purge.
*/
class purge_rsegs_t {
 public:
  explicit purge_rsegs_t(scn_t scn) : m_scn(scn), m_rsegs_n(0), m_rsegs() {
    for (auto &rseg : m_rsegs) {
      rseg = nullptr;
    }
  }

  /** Default constructor */
  purge_rsegs_t() : purge_rsegs_t(0) {}

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
    ut_a(m_rsegs_n < 2);
    m_rsegs[m_rsegs_n++] = rseg;
  }

  /** Number of registered rsegs.
  @return size of rseg list. */
  size_t size() const { return (m_rsegs_n); }

  /**
  @return an iterator to the first element */
  typename Rsegs_array<2>::iterator begin() { return m_rsegs.begin(); }

  /**
  @return an iterator to the end */
  typename Rsegs_array<2>::iterator end() {
    return m_rsegs.begin() + m_rsegs_n;
  }

  /** Append rollback segments from referred instance to current
  instance. */
  void insert(const purge_rsegs_t &append_from) {
    ut_ad(get_scn() == append_from.get_scn());
    for (size_t i = 0; i < append_from.m_rsegs_n; ++i) {
      insert(append_from.m_rsegs[i]);
    }
  }

  /** Compare two purge_rsegs_t based on scn.
  @param lhs first element to compare
  @param rhs second element to compare
  @return true if elem1 > elem2 else false.*/
  bool operator()(const purge_rsegs_t &lhs, const purge_rsegs_t &rhs) {
    return (lhs.m_scn > rhs.m_scn);
  }

  /** Compiler defined copy-constructor/assignment operator
  should be fine given that there is no reference to a memory
  object outside scope of class object.*/

  void store_null() {
    m_scn = SCN_NULL;
    m_rsegs_n = 0;
  }

  bool is_null() const { return m_scn == SCN_NULL && m_rsegs_n == 0; }

 private:
  scn_t m_scn;

  size_t m_rsegs_n{};

  /** Rollback segments of a transaction, scheduled for purge. */
  Rsegs_array<2> m_rsegs;
};

/**
  Use priority_queue as the minimum heap structure
  which is order by scn number */
typedef std::priority_queue<
    purge_rsegs_t, std::vector<purge_rsegs_t, ut::allocator<purge_rsegs_t>>,
    purge_rsegs_t>
    purge_heap_t;

} /* namespace lizard */

#endif
