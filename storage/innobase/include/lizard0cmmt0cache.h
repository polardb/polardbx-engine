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

#ifndef lizard0cmmt0cache_h
#define lizard0cmmt0cache_h

#include "hash0hash.h"

#include "lizard0cmmt0cache0types.h"
#include "lizard0mon.h"
#include "lizard0cmmt0types.h"
#include "lizard0txn0rec.h"
#include "lizard0txn0slot0types.h"
#include "lizard0txn0slot0uba0types.h"

#include "rem0types.h"
#include "trx0types.h"

namespace lizard {

struct tcn_t {
 public:
  /** Transaction system commit number that has committed. */
  scn_t scn;
  /** Transaction global commit number that has committed. */
  gcn_t gcn;
  /** Transaction id that has committed. */
  unsigned long trx_id : 48;
  /** Commit number source for gcn */
  unsigned long csr : 1;
  /** Check if share commit number. */
  unsigned long is_slave : 1;
  /** Transaction Slot state */
  txn_status_t status;

  /** Constructor */
  explicit tcn_t() {
    trx_id = 0;
    scn = SCN_NULL;
    gcn = GCN_NULL;
    csr = CSR_AUTOMATIC;
    is_slave = 0;
    status = txn_status_t::ACTIVE;
  }
  /** Constructor
   *
   * @param[in]	txn rec
   * @param[in]	status_arg txn status */
  explicit tcn_t(const txn_rec_t &txn_rec, const txn_status_t &status_arg) {
    trx_id = txn_rec.trx_id;
    scn = txn_rec.scn;
    gcn = txn_rec.gcn;
    csr = undo_addr_t::decode_csr(txn_rec.undo_ptr);
    is_slave = undo_addr_t::decode_slave(txn_rec.undo_ptr);
    status = status_arg;
  }
  /** Constructor
   *
   * @param[in]	id		transaction id
   * @param[in]	cmmt	commit mark
   * @param[in]	undo_ptr	undo pointer
   * @param[in]	status_arg txn status */
  explicit tcn_t(const trx_id_t id, const commit_mark_t &cmmt,
                 const undo_ptr_t &undo_ptr, const txn_status_t &status_arg) {
    trx_id = id;
    scn = cmmt.scn;
    gcn = cmmt.gcn;
    csr = undo_addr_t::decode_csr(undo_ptr);
    is_slave = undo_addr_t::decode_slave(undo_ptr);
    status = status_arg;

    ut_ad(csr == cmmt.csr && status != txn_status_t::ACTIVE);
  }

  trx_id_t key() const { return trx_id; }
};

/** Check tcn_t size. */
static_assert(sizeof(tcn_t) == 24, "tcn_t sizeof must be 24 bytes.");

/** Transaction commit number cache. */
template <typename Cache_type, typename Stat_type>
class CCache {
 private:
  using ulint_ctr_1_t = ib_counter_t<ulint, 1, single_indexer_t>;

 public:
  /** Constructor
   *
   * @param[in]	n_entry		Cache entry number */
  CCache(ulint n_entry)
      : m_n_entry(0), m_cache(nullptr), m_n_hit(), m_n_miss(), m_n_write() {
    if (n_entry != 0) {
      m_n_entry = n_entry;
      m_cache = ut::new_withkey<Cache_type>(
          ut::make_psi_memory_key(tcn_mem_key), m_n_entry, tcn_mem_key);
    }
  }
  /** Destructor */
  virtual ~CCache() {
    if (m_cache) {
      ut::delete_(m_cache);
      m_cache = nullptr;
      m_n_entry = 0;
    }
  }
  /** Get cache memory size.
   *
   * @retval		cache memory size */
  ulint size() const { return m_n_entry * sizeof(tcn_t); }
  /** Get cache entry number.
   *
   * @retval		cache entry number */
  ulint n_entry() const { return m_n_entry; }

  /** Search from tcn cache and overwrite the txn rec include
   * [UBA flag, scn, gcn]
   *
   * @param[in/out]   txn rec
   * @param[in/out]   txn lookup status
   *
   * @retval          cache hit or not
   * */
  bool find(txn_rec_t *txn_rec, txn_status_t *txn_status);

  /** Cache Commit Number if txn_rec was looked up.
   *
   * @param[in]	txn rec info that looked up
   * @param[in]	txn status */
  void insert(const txn_rec_t &txn_rec, const txn_status_t &status);
  /** Cache Commit Number after trx commit.
   *
   * @param[in]	Committed trx
   * @param[in]	Allocated commit number
   */
  void insert(const trx_t *trx, bool serialised);
  /** Get cache hit counter.
   *
   * @retval		counter value */
  ulint hit_counter() const { return m_n_hit; }
  /** Get cache miss counter.
   *
   * @retval		counter value */
  ulint miss_counter() const { return m_n_miss; }
  /** Get cache write counter.
   *
   * @retval		counter value */
  ulint write_counter() const { return m_n_write; }

 private:
  /** How many tcn_t entries. */
  ulint m_n_entry;
  /** Cache implemention*/
  Cache_iface<trx_id_t, tcn_t> *m_cache;
  /** Hit counter when search */
  Stat_type m_n_hit;
  /** Miss counter when search */
  Stat_type m_n_miss;
  /** write counter when commit or replacement. */
  Stat_type m_n_write;
};

/**Stat type include 64 ulint slot.*/
using ulint_ctr_64_t = ib_counter_t<ulint, 64>;
/** Stat type ulint */
using ulint_ctr_1_t = ib_counter_t<ulint, 1, single_indexer_t>;

/** Commit cache that use atomic array */
using Atomic_ccache = CCache<Atomic_array<trx_id_t, tcn_t>, ulint_ctr_64_t>;

/** Commit cache that use normal array without thread safe. */
using Plain_ccache = CCache<Plain_array<trx_id_t, tcn_t>, ulint_ctr_1_t>;

}  // namespace lizard

#endif
