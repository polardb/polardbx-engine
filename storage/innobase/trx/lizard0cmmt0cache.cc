/*****************************************************************************
Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyEngine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyEngine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/** @file include/lizard0cmmt0cache.h
  Lizard transaction commit number cache

 Created 2021-11-05 by Jianwei.zhao
 *******************************************************/

#include "trx0trx.h"

#include "lizard0cmmt0cache.h"
#include "lizard0undo.h"

namespace lizard {

/** Search from tcn cache and overwrite the txn rec include
 * [UBA flag, scn, gcn]
 *
 * @param[in/out]   txn rec
 * @param[in/out]   txn state
 *
 * @retval          cache hit or not
 * */
template <typename Cache_type, typename Stat_type>
bool CCache<Cache_type, Stat_type>::find(txn_rec_t *txn_rec,
                                         txn_status_t *txn_status) {
  ut_ad(txn_rec && txn_status);

  /* Disable tcn cache for stress test of panda index.*/
  if (lizard::inject_stress_test_for_panda) {
    return false;
  }

  if (m_cache) {
    tcn_t tcn = m_cache->search(txn_rec->trx_id);
    if (tcn.trx_id == txn_rec->trx_id) {
      undo_addr_t::set_commit(&txn_rec->undo_ptr, tcn.csr, tcn.is_slave);
      txn_rec->scn = tcn.scn;
      txn_rec->gcn = tcn.gcn;
      *txn_status = tcn.status;

      ut_ad(txn_rec->is_whole_committed());

      /** Hit counter increment */
      m_n_hit.inc();
      return true;
    }
    /** Miss counter increment */
    m_n_miss.inc();
  }
  return false;
}

/** Cache Commit Number after txn_rec was looked up, that maybe isn't
 * real original SCN/GCN of the txn_rec->trx_id transaction, but we still
 * and only use it.
 *
 * @param[in]	txn rec info that looked up */
template <typename Cache_type, typename Stat_type>
void CCache<Cache_type, Stat_type>::insert(const txn_rec_t &txn_rec,
                                           const txn_status_t &status) {
  ut_a(txn_rec.is_whole_committed());
  ut_a(txn_rec.trx_id != 0);

  if (m_cache) {
    tcn_t value(txn_rec, status);
    m_cache->insert(value);

    /** Write counter increment */
    m_n_write.inc();
  }
}

/** Cache Commit Number after trx commit, the SCN/GCN is real commit number
 * of that transaction.
 *
 * @param[in]	Committed trx
 * @param[in]	Allocated commit number
 * */
template <typename Cache_type, typename Stat_type>
void CCache<Cache_type, Stat_type>::insert(const trx_t *trx, bool serialised) {
  if (m_cache) {
    if (trx->id > 0 && serialised) {
      ut_ad(trx->txn_desc.is_whole_committed());

      tcn_t value(trx->id, trx->txn_desc.cmmt, trx->txn_desc.undo_ptr,
                  txn_status_t::COMMITTED);
      m_cache->insert(value);

      /** Write counter increment */
      m_n_write.inc();
    }
  }
}

template bool Plain_ccache::find(txn_rec_t *, txn_status_t *);
template void Plain_ccache::insert(txn_rec_t const &, txn_status_t const &);
template void Plain_ccache::insert(trx_t const *, bool);
template bool Atomic_ccache::find(txn_rec_t *, txn_status_t *);
template void Atomic_ccache::insert(txn_rec_t const &, txn_status_t const &);
template void Atomic_ccache::insert(trx_t const *, bool);

}  // namespace lizard
