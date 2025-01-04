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

/** @file include/lizard0cleanout.h
 Lizard cleanout operation

 Created 2020-04-15 by Jianwei.zhao
 *******************************************************/

#include "btr0pcur.h"
#include "fil0fil.h"
#include "lock0lock.h"
#include "que0que.h"
#include "row0row.h"
#include "sync0types.h"
#include "trx0rseg.h"
#include "trx0types.h"
#include "trx0undo.h"
#include "row0mysql.h"

#include "lizard0cleanout.h"
#include "lizard0dbg.h"
#include "lizard0dict.h"
#include "lizard0mon.h"
#include "lizard0row.h"
#include "lizard0txn.h"
#include "lizard0undo.h"
#include "lizard0ut.h"
#include "lizard0btr0cur.h"
#include "lizard0row0gpp.h"

namespace lizard {

/*----------------------------------------------------------------*/
/* Lizard cleanout structure and function. */
/*----------------------------------------------------------------*/

/** Whether to write redo log when cleanout */
bool opt_cleanout_write_redo = false;

/** Whether disable the delayed cleanout when read */
bool opt_cleanout_disable = false;

/** Whether disable the gpp cleanout when read */
bool opt_gpp_cleanout_disable = false;

/** Whether disable the ddl cleanout when ddl */
bool opt_ddl_cleanout_disable = false;

// /** Commit cleanout profiles */
ulint srv_commit_cleanout_max_rows = Commit_cleanout::STATIC_CURSORS;
/*----------------------------------------------------------------*/
/* Lizard cleanout by cursor. */
/*----------------------------------------------------------------*/

Cursor::Cursor(const Cursor &other)
    : m_old_stored(other.m_old_stored),
      m_old_rec(other.m_old_rec),
      m_block(other.m_block),
      m_index(other.m_index),
      m_modify_clock(other.m_modify_clock),
      m_block_when_stored(other.m_block_when_stored) {}

Cursor &Cursor::operator=(const Cursor &other) {
  if (this != &other) {
    m_old_stored = other.m_old_stored;
    m_old_rec = other.m_old_rec;
    m_block = other.m_block;
    m_index = other.m_index;
    m_modify_clock = other.m_modify_clock;
    m_block_when_stored = other.m_block_when_stored;
  }
  return *this;
}

/** Store the record position and related commit number.
 *
 * @param[in]		cursor
 * @param[in]		trx id
 * @param[in]		commit number
 *
 * @retval		true	successful */
bool Cursor::store_position(btr_pcur_t *pcur) {
  ut_ad(pcur);
  return store_position(pcur->get_btr_cur()->index, pcur->get_block(),
                        page_cur_get_rec(pcur->get_page_cur()));
}

/** Store the record position which is still active, and will cleanout after
 * commit.
 *
 * @param[in]		cursor
 * @param[in]		trx id
 * @param[in]		commit number
 *
 * @retval		true	successful */
bool Cursor::store_position(dict_index_t *index, buf_block_t *block,
                            rec_t *rec) {
  ut_ad(index && block && rec);

  m_index = index;
  m_block = block;
  m_old_rec = rec;

#ifdef UNIV_DEBUG
  auto page = page_align(m_old_rec);
  ut_ad(!page_is_empty(page) && page_is_leaf(page));
  ut_ad(!m_block->page.file_page_was_freed);
  ut_ad(!index->table->is_temporary());
#endif

  /* Function try to check if block is S/X latch. */
  m_modify_clock = m_block->get_modify_clock(
      IF_DEBUG(fsp_is_system_temporary(m_block->page.id.space())));

  m_block_when_stored.store(m_block);

  m_old_stored = true;

  return true;
}

bool Cursor::restore_position(mtr_t *mtr, ut::Location location) {
  ut_ad(m_old_stored == true);
  ut_ad(m_old_rec != nullptr);

  /** Cleanout will modify leaf page */
  ulint latch_mode = BTR_MODIFY_LEAF;
  Page_fetch fetch_mode = Page_fetch::SCAN;

  /* Try optimistic restoration. */
  if (m_block_when_stored.run_with_hint([&](buf_block_t *hint) {
        return hint != nullptr &&
               buf_page_optimistic_get(latch_mode, hint, m_modify_clock,
                                       fetch_mode, location.filename,
                                       location.line, mtr);
      })) {
    return true;
  }

  lizard_stats.cleanout_cursor_restore_failed.inc();

  return false;
}

ulint CCursor::cleanout() {
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;

  txn_rec_t old_txn_rec;
  ulint cleaned = 0;
  ut_ad(m_stored && m_txn_rec.is_committed());

  mtr_t mtr;
  mtr.start();
  if (!opt_cleanout_write_redo) mtr.set_log_mode(MTR_LOG_NO_REDO);
  
  if (!restore_position(&mtr, UT_LOCATION_HERE)) goto mtr_end;

  /** Only user record position was stored. */
  ut_a(page_rec_is_user_rec(m_old_rec));

  rec_offs_init(offsets_);
  offsets = rec_get_offsets(m_old_rec, m_index, offsets,
                            m_index->n_uniq + 2 + DATA_N_LIZARD_COLS,
                            UT_LOCATION_HERE, &heap);
  row_get_txn_rec(m_old_rec, m_index, offsets, &old_txn_rec);

  if (old_txn_rec.trx_id == m_txn_rec.trx_id) {
    ut_ad(m_txn_rec.slot() == old_txn_rec.slot());

    /** If trx state is active ,try to cleanout */
    if (old_txn_rec.is_active()) {
      /** Modify the scn and undo ptr */
      row_upd_rec_lizard_fields_in_cleanout(m_old_rec,
                                            buf_block_get_page_zip(m_block),
                                            m_index, offsets, &m_txn_rec);

      /** Write the redo log */
      btr_cur_upd_lizard_fields_clust_rec_log(m_old_rec, m_index, &m_txn_rec,
                                              &mtr);

      cleaned++;
    }
  }

mtr_end:
  mtr.commit();

  if (heap) mem_heap_free(heap);
  return cleaned;
}

bool CCursor::is_on_same_page(btr_pcur_t *pcur) const {
  ut_ad(m_old_stored);
  ut_ad(m_old_rec != nullptr);

  auto block = pcur->get_block();
  auto btr_cur = pcur->get_btr_cur();
  if (block != m_block || btr_cur->index != m_index) return false;

  auto modify_clock = block->get_modify_clock(
      IF_DEBUG(fsp_is_system_temporary(block->page.id.space())));

  return modify_clock == m_modify_clock;
}

ulint SCursor ::cleanout() {
  ulint cleaned = 0;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  ulint gpp_no_offset;
  if (!m_stored) return cleaned;

  mtr_t mtr;
  mtr.start();

  if (!opt_cleanout_write_redo) mtr.set_log_mode(MTR_LOG_NO_REDO);

  if (!restore_position(&mtr, UT_LOCATION_HERE)) goto mtr_end;

  rec_offs_init(offsets_);
  offsets = rec_get_offsets(m_old_rec, m_index, offsets, ULINT_UNDEFINED,
                            UT_LOCATION_HERE, &heap);
  gpp_no_offset = row_get_gpp_no_offset(m_index, offsets);
  if (gpp_no_offset != m_gpp_no_offset) {
    lizard_error(ER_LIZARD)
        << "gpp_no_offset is not match in GPP cleanout, stored offset is "
        << m_gpp_no_offset << " but actual offset is " << gpp_no_offset;
    ut_ad(0);
    goto mtr_end;
  }

  if (page_rec_is_user_rec(m_old_rec)) {
    /** Backfill gpp_no */
    row_upd_rec_gpp_no_in_cleanout(m_old_rec, buf_block_get_page_zip(m_block),
                                   m_index, gpp_no_offset, m_gpp_no);

    /** Write the redo log */
    btr_cur_upd_gpp_no_sec_rec_log(m_old_rec, m_index, gpp_no_offset, m_gpp_no,
                                   &mtr);

    cleaned++;
  }

mtr_end:
  mtr.commit();
  if (heap) mem_heap_free(heap);
  return cleaned;
}

cleanout_ctx_t::cleanout_ctx_t(btr_pcur_t *pcur)
    : m_pcur(pcur), m_cleanout(nullptr) {
  if (m_pcur) m_cleanout = m_pcur->m_cleanout;
}

ulint DDL_cleanout::do_cleanout() {
  mem_heap_t *heap = nullptr;
  buf_block_t *block = nullptr;
  dict_index_t *index = nullptr;
  ulint cleaned = 0;

  if (!m_cursor.stored()) return cleaned;

  mtr_t mtr;
  mtr.start();
  if (!opt_cleanout_write_redo) mtr.set_log_mode(MTR_LOG_NO_REDO);

  if (!m_cursor.restore_position(&mtr, UT_LOCATION_HERE)) goto mtr_end;

  block = m_cursor.get_block();
  index = m_cursor.get_index();

  for (uint i = 0; i < m_rec_nums; i++) {
    rec_t *old_rec = m_old_recs[i];
    txn_rec_t txn_rec = m_txn_recs[i];

    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    ulint *offsets = offsets_;
    rec_offs_init(offsets_);

    offsets = rec_get_offsets(old_rec, index, offsets,
                              index->n_uniq + 2 + DATA_N_LIZARD_COLS,
                              UT_LOCATION_HERE, &heap);

    txn_rec_t old_txn_rec;
    row_get_txn_rec(old_rec, index, offsets, &old_txn_rec);

    if (old_txn_rec.trx_id == txn_rec.trx_id) {
      ut_ad(txn_rec.slot() == old_txn_rec.slot());

      /** If trx state is active ,try to cleanout */
      if (old_txn_rec.is_active()) {
        /** Modify the scn and undo ptr */
        row_upd_rec_lizard_fields_in_cleanout(
            old_rec, buf_block_get_page_zip(block), index, offsets, &txn_rec);

        /** Write the redo log */
        btr_cur_upd_lizard_fields_clust_rec_log(old_rec, index, &txn_rec, &mtr);

        cleaned++;
      }
    }

    if (heap) mem_heap_empty(heap);
  }

mtr_end:
  mtr.commit();
  if (heap) mem_heap_free(heap);

  return cleaned;
}

/**
  Collect cursor which need to cleanout when scan

  @param[in]        trx_id
  @param[in]        txn_rec         txn description and state
  @param[in]        rec             current rec
  @param[in]        index           cluster index
  @parma[in]        offsets         rec_get_offsets(rec, index)
  @param[in/out]    pcur            cursor
*/
void Scan_cleanout::collect(const trx_id_t trx_id, const txn_rec_t &txn_rec,
                            const rec_t *rec, const dict_index_t *index,
                            const ulint *offsets, btr_pcur_t *pcur) {
  if (opt_cleanout_disable) return;

  ut_ad(index->is_clustered());
  ut_ad(index == pcur->get_btr_cur()->index);
  ut_ad(rec == pcur->get_rec());
  ut_ad(page_get_page_no(pcur->get_page()) ==
        page_get_page_no(page_align(rec)));

  acquire_for_lizard(pcur, txn_rec);
}

/** Collect record */
void DDL_cleanout::collect(const trx_id_t trx_id, const txn_rec_t &txn_rec,
                           const rec_t *rec, const dict_index_t *index,
                           const ulint *offsets, btr_pcur_t *pcur) {
  assert_row_lizard_valid(rec, index, offsets);
  ut_ad(index->is_clustered());

  store(rec, txn_rec, pcur);

  ut_ad(page_get_page_no(page_align(m_cursor.get_old_rec())) ==
        page_get_page_no(page_align(rec)));
}

/**
  Collect rows updated in current transaction.

  @param[in]        thr             current session
  @param[in]        cursor          btr cursor
  @param[in]        rec             current rec
  @param[in]        flags           mode flags for btr_cur operations
*/
void commit_cleanout_collect(que_thr_t *thr, btr_cur_t *cursor, rec_t *rec,
                             ulint flags) {
  /** Skip the collection if the transaction does not require undo logging or if
   * system fields should be retained. */
  if ((flags & BTR_KEEP_SYS_FLAG) || (flags & BTR_NO_UNDO_LOG_FLAG)) {
    return;
  }

  // In dict_persist_to_dd_table_buffer, no thr allocated,
  // Now we skip those background tasks.
  if (thr == nullptr) {
    return;
  }

  trx_t *trx = thr_get_trx(thr);
  ut_a(trx);

  // Skip purge trx, or temp table.
  if (trx->id == 0) {
    ut_ad(strlen(trx->op_info) == 0 || strcmp(trx->op_info, "purge trx") == 0);
    return;
  }

  auto block = btr_cur_get_block(cursor);
  auto page = buf_block_get_frame(block);
  auto leaf = page_is_leaf(page);
  auto index = cursor->index;

  if (leaf && index->is_clustered() && !index->table->is_temporary() &&
      !dict_index_is_ibuf(index)) {
    ut_ad(rec != nullptr);
    ut_ad(rec == btr_cur_get_rec(cursor) /* update */ ||
          rec == page_rec_get_next(btr_cur_get_rec(cursor)) /* insert */);
    ut_ad(page_rec_is_user_rec(rec));
    ut_ad(trx->cleanout != nullptr);

    /** Ensure that the commit cleanout operation is under the protection of the
     * transaction table locks, unless the table is permanent in dict sys. */
    ut_ad(dict_sys->is_permanent_table(index->table) ||
          lock_table_has_locks(index->table));

    trx->cleanout->push_cursor(index, block, rec);
  }
}

/**
  After search row complete, do the cleanout.

  @param[in]      prebuilt

  @retval         count       cleaned records count
*/
void cleanout_after_read(row_prebuilt_t *prebuilt) {
  ut_ad(prebuilt);
  btr_pcur_t *pcur;

  /** cursor maybe fixed on prebuilt->pcur or prebuilt->clust_pcur */

  /** Find the collected and need cleanout pages or cursors */
  pcur = prebuilt->pcur;
  if (pcur && pcur->m_cleanout) {
    pcur->m_cleanout->execute();
  }

  pcur = prebuilt->clust_pcur;
  if (pcur && pcur->m_cleanout) {
    pcur->m_cleanout->execute();
  }
}

/**
  Cleanout rows at transaction commit.

*/
void cleanout_after_commit(trx_t *trx, bool serialised) {
  ut_ad(trx != nullptr);
  ut_ad(trx->cleanout != nullptr);

  /** Skip cleanout as the transaction is a full rollback or non-modification*/
  if (!serialised || trx->is_rollback) {
    trx->cleanout->clear();
    return;
  }
  ut_ad(trx->txn_desc.is_whole_committed());

  txn_rec_t txn_rec{trx->id, trx->txn_desc.cmmt.scn, trx->txn_desc.undo_ptr,
                    trx->txn_desc.cmmt.gcn};

  trx->cleanout->set_commit(txn_rec);
  trx->cleanout->execute();
}

}  // namespace lizard
