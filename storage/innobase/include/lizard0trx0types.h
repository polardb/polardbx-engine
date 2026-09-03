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

/** @file include/lizard0trx0types.h
  Lizard transaction structure.

 Created 2020-08-27 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0trx0types_h
#define lizard0trx0types_h

#include "lizard0trx0purge0types.h"

#include "trx0types.h"

namespace lizard {

/** Rsegs which are put into history list when commit. */
class commit_rsegs_t {
 public:
  commit_rsegs_t() : m_scn(SCN_NULL), m_stored(false), m_rsegs(SCN_NULL) {}

  virtual ~commit_rsegs_t() {}

  void store_null() {
    m_scn = SCN_NULL;
    m_stored = false;
    m_rsegs.store_null();
  }

  bool is_null() const { return m_rsegs.is_null(); }

  void store(trx_rseg_t *rseg) {
    ut_ad(!rseg->is_txn);

    m_rsegs.insert(rseg);
    m_stored = true;
  }

  /** submit purge rseg when commit. */
  void submit(scn_t scn);

 private:
  /** Commit scn. */
  scn_t m_scn;
  /** Has purge rseg */
  bool m_stored;
  /** For purge system. */
  purge_rsegs_t m_rsegs;
};

/** Rsegs whose mutexes are held by a transaction.
 *
 * Acquire the mutexes of all the rsegs modified by the transaction in a
 * fixed order in advance, so the following undo modifications can be
 * done without any further mutex ordering concern. Which rsegs are
 * actually locked is exposed by the *_rseg_updated() accessors.
 *
 * NOTE: it's not a RAII guard, the caller must release_mutex()
 * explicitly, which is asserted by the destructor. */
struct locked_rsegs_t {
  locked_rsegs_t(trx_t *trx);
  ~locked_rsegs_t();

  /** Release all the acquired rseg mutexes. */
  void release_mutex();

  /** @retval	true if the txn rseg is modified and locked */
  bool txn_rseg_updated() const { return m_txn_rseg_locked; }
  /** @retval	true if the redo rseg is modified and locked */
  bool redo_rseg_updated() const { return m_redo_rseg_locked; }
  /** @retval	true if the temp rseg is modified and locked */
  bool temp_rseg_updated() const { return m_temp_rseg_locked; }

 private:
  /** The transaction which owns the rsegs. */
  trx_t *m_trx;
  /** Whether the txn rseg mutex is held. */
  bool m_txn_rseg_locked;
  /** Whether the redo rseg mutex is held. */
  bool m_redo_rseg_locked;
  /** Whether the temp rseg mutex is held. */
  bool m_temp_rseg_locked;
};

}  // namespace lizard
#endif
