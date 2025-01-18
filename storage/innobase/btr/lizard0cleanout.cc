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
#include "lizard0row0clover.h"
#include "lizard0row0bamboo.h"
#include "lizard0btr0cur0clover.h"
#include "lizard0btr0cur0gpp.h"
#include "lizard0btr0cur0bamboo.h"

namespace lizard {

/*----------------------------------------------------------------*/
/* Lizard cleanout structure and function. */
/*----------------------------------------------------------------*/

/** Whether to write redo log when cleanout */
bool opt_cleanout_write_redo = false;

/** Whether disable the delayed cleanout when read */
bool opt_txn_cleanout_disable = false;

/** Whether disable the gpp cleanout when read */
bool opt_gpp_cleanout_disable = false;

/** Whether disable the ddl cleanout when ddl */
bool opt_ddl_cleanout_disable = false;

// /** Commit cleanout profiles */
ulint srv_commit_cleanout_max_rows = Commit_cleanout::STATIC_CURSORS;

/** Make page dirty if cleaned records are more than threshold and page was
 * still clean */
ulint srv_cleanout_dirty_threshold = 5;
/*----------------------------------------------------------------*/
/* Lizard cleanout by cursor. */
/*----------------------------------------------------------------*/

Cursor::Cursor(const Cursor &other)
    : m_old_stored(other.m_old_stored),
      m_old_rec(other.m_old_rec),
      m_block(other.m_block),
      m_index(other.m_index),
      m_modify_clock(other.m_modify_clock),
      m_block_when_stored(other.m_block_when_stored),
      m_log_mode(other.m_log_mode) {}

Cursor &Cursor::operator=(const Cursor &other) {
  if (this != &other) {
    m_old_stored = other.m_old_stored;
    m_old_rec = other.m_old_rec;
    m_block = other.m_block;
    m_index = other.m_index;
    m_modify_clock = other.m_modify_clock;
    m_block_when_stored = other.m_block_when_stored;
    m_log_mode = other.m_log_mode;
  }
  return *this;
}

/** Store the record position.
 *
 * @param[in]		cursor
 *
 * @retval		true	successful */
bool Cursor::store_position(const btr_cur_t *bcur, mtr_log_t log_mode) {
  ut_ad(bcur);
  m_index = bcur->index;
  m_block = btr_cur_get_block(bcur);
  m_old_rec = btr_cur_get_rec(bcur);

#ifdef UNIV_DEBUG
  auto page = page_align(m_old_rec);
  ut_ad(!page_is_empty(page) && page_is_leaf(page));
  ut_ad(!m_block->page.file_page_was_freed);
  ut_ad(!m_index->table->is_temporary());
#endif

  /* Function try to check if block is S/X latch. */
  m_modify_clock = m_block->get_modify_clock(
      IF_DEBUG(fsp_is_system_temporary(m_block->page.id.space())));

  m_block_when_stored.store(m_block);

  m_log_mode = log_mode;

  m_old_stored = true;

  return true;
}

/** Choose log mode according to setting and page state. */
void Cursor::set_log_mode(mtr_t *mtr) {
  buf_page_t *page = &m_block->page;
  ut_ad(mtr_memo_contains_flagged(mtr, m_block, MTR_MEMO_PAGE_X_FIX));

  if (page->get_cleanouts() >= srv_cleanout_dirty_threshold &&
      !page->is_dirty() && m_log_mode == MTR_LOG_NONE) {
    m_log_mode = MTR_LOG_NO_REDO;
  }

  mtr->set_log_mode(m_log_mode);
}

/** inc page cleanouts */
void Cursor::inc_cleanouts(mtr_t *mtr) {
  buf_page_t *page = &m_block->page;
  ut_ad(mtr_memo_contains_flagged(mtr, m_block, MTR_MEMO_PAGE_X_FIX));

  page->inc_cleanouts();
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
    set_log_mode(mtr);
    return true;
  }

  cleanout_cursor_restore_fail_stat();

  return false;
}

/** Cleanout txn field in record, include Clover or Bamboo layout
 *
 * @retval	how many records were cleanouted. */
ulint TCursor::cleanout() {
  ut_ad(m_index->is_clustered() || m_index->is_panda());
  /** We have ignored temporary table when collected. */
  ut_ad(!m_index->table->skip_alter_undo);
  ut_ad(!m_index->table->is_temporary());
  ut_ad(m_stored && m_txn_rec.is_committed());

  ut_ad(txn_layout_is_arranged(m_layout));

  ulint cleaned = 0;
  switch (m_layout) {
    case TL_CLOVER:
      cleaned = cleanout_clover_rec();
      break;
    case TL_BAMBOO:
      cleaned = cleanout_bamboo_rec();
      break;
    default:
      ut_error;
  }
  return cleaned;
}

/** Cleanout Clover layout txn record.
 *
 * @retval	How many record was cleanouted.*/
ulint TCursor::cleanout_clover_rec() {
  ut_ad(m_index->is_clustered());
  ut_ad(m_layout == TL_CLOVER);

  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;

  txn_rec_t old_txn_rec;
  ulint cleaned = 0;

  mtr_t mtr;
  mtr.start();

  if (!restore_position(&mtr, UT_LOCATION_HERE)) goto mtr_end;

  /** Only user record position was stored. */
  ut_a(page_rec_is_user_rec(m_old_rec));

  rec_offs_init(offsets_);
  offsets = rec_get_offsets(m_old_rec, m_index, offsets,
                            m_index->n_uniq + 2 + DATA_N_LIZARD_COLS,
                            UT_LOCATION_HERE, &heap);
  row_get_txn_rec(m_old_rec, m_index, offsets, m_layout, &old_txn_rec);

  if (old_txn_rec.trx_id == m_txn_rec.trx_id) {
    ut_ad(m_txn_rec.slot() == old_txn_rec.slot());

    /** If trx state is active ,try to cleanout */
    if (old_txn_rec.is_active()) {
      /** Modify the scn/undo ptr/gcn */
      row_upd_rec_clover_fields_in_cleanout(m_old_rec,
                                            buf_block_get_page_zip(m_block),
                                            m_index, offsets, &m_txn_rec);
      /** Write redo log */
      btr_cur_upd_clover_fields_clust_rec_log(m_old_rec, m_index, &m_txn_rec,
                                              &mtr);

      inc_cleanouts(&mtr);
      cleaned++;
    }
  }

mtr_end:
  mtr.commit();

  if (heap) mem_heap_free(heap);
  return cleaned;
}

bool TCursor::is_on_same_page(const btr_cur_t *bcur) const {
  ut_ad(m_old_stored);
  ut_ad(m_old_rec != nullptr);
  ut_ad(m_index->is_clustered());
  ut_ad(m_layout == TL_CLOVER);

  auto block = btr_cur_get_block(bcur);
  if (block != m_block || bcur->index != m_index) return false;

  auto modify_clock = block->get_modify_clock(
      IF_DEBUG(fsp_is_system_temporary(block->page.id.space())));

  return modify_clock == m_modify_clock;
}

/** Cleanout Bamboo layout txn record.
 *
 * @retval	How many record was cleanouted.*/
ulint TCursor::cleanout_bamboo_rec() {
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;

  txn_rec_t old_txn_rec;
  ulint cleaned = 0;

  ut_ad(dict_index_is_panda(m_index));
  ut_ad(m_layout == TL_BAMBOO);

  mtr_t mtr;
  mtr.start();

  if (!restore_position(&mtr, UT_LOCATION_HERE)) goto mtr_end;

  /** Only user record position was stored. */
  ut_a(page_rec_is_user_rec(m_old_rec));

  rec_offs_init(offsets_);
  offsets = rec_get_offsets(m_old_rec, m_index, offsets, ULINT_UNDEFINED,
                            UT_LOCATION_HERE, &heap);
  row_get_txn_rec(m_old_rec, m_index, offsets, m_layout, &old_txn_rec);

  if (old_txn_rec.trx_id == m_txn_rec.trx_id) {
    ut_ad(m_txn_rec.slot() == old_txn_rec.slot());

    /** If trx state is active ,try to cleanout */
    if (old_txn_rec.is_active()) {
      /** Modify the scn and undo ptr */
      row_upd_rec_bamboo_fields_in_cleanout(m_old_rec,
                                            buf_block_get_page_zip(m_block),
                                            m_index, offsets, &m_txn_rec);

      /** Write redo log */
      btr_cur_upd_bamboo_fields_sec_rec_log(m_old_rec, m_index, &m_txn_rec,
                                            &mtr);

      inc_cleanouts(&mtr);
      cleaned++;
    }
  }

mtr_end:
  mtr.commit();

  DBUG_EXECUTE_IF("crash_after_panda_cleanout", sleep(2); DBUG_SUICIDE(););

  if (heap) mem_heap_free(heap);
  return cleaned;
}

ulint GCursor ::cleanout() {
  ulint cleaned = 0;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  ulint gpp_no_offset;
  if (!m_stored) return cleaned;

  mtr_t mtr;
  mtr.start();

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

    inc_cleanouts(&mtr);
    cleaned++;
  }

mtr_end:
  mtr.commit();
  if (heap) mem_heap_free(heap);
  return cleaned;
}

/** Collect txn record for cleanout.
 *
 * @param[in]		btree cursor
 * @param[in]		commit info
 * @param[in]		rec layout
 *
 * @retval		txn cursor for cleanout.
 * */
TCursor *Scan_cleanout::collect_txn(const btr_cur_t *bcur,
                                    const txn_rec_t &txn_rec,
                                    const txn_layout_t &layout,
                                    mtr_log_t log_mode) {
  const dict_index_t *index = nullptr;
  TCursor *cursor = nullptr;

  if (opt_txn_cleanout_disable) return cursor;

  index = bcur->index;
  if (!index->table->is_temporary()) {
    switch (layout) {
      case TL_NONE:
        break;
      case TL_CLOVER:
      case TL_BAMBOO:
        cursor = request_txn(bcur, txn_rec, layout, log_mode);
        break;
    }
  }
  return cursor;
}

/** Collect gpp record for cleanout.
 *
 * @param[in]		btree cursor
 * @param[in]		gpp no offset within record
 *
 * @retval		gpp cursor for cleanout.
 * */
GCursor *Scan_cleanout::collect_gpp(const btr_cur_t *btr,
                                    const ulint gpp_no_offset,
                                    mtr_log_t log_mode) {
  GCursor *cursor = nullptr;

  if (opt_gpp_cleanout_disable) return cursor;

  cursor = request_gpp(btr, gpp_no_offset, log_mode);

  return cursor;
}

/** Collect txn record for cleanout.
 *
 * @param[in]		btree cursor
 * @param[in]		rec layout
 *
 * @retval		txn cursor for cleanout.
 * */
TCursor *Commit_cleanout::collect_txn(const btr_cur_t *bcur,
                                      const txn_rec_t &txn_rec,
                                      const txn_layout_t &layout,
                                      mtr_log_t log_mode) {
  TCursor *cursor = nullptr;
  /**transaction is still active when collect. */
  ut_ad(txn_rec.is_active());

  cursor = request_txn(bcur, layout, log_mode);

  return cursor;
}

  /** Collect txn record for cleanout.
   *
   * @param[in]		btree cursor
   * @param[in]		commit info
   * @param[in]		rec layout
   *
   * @retval		txn cursor for cleanout.
   * */
TCursor *DDL_cleanout::collect_txn(const btr_cur_t *bcur,
                                   const txn_rec_t &txn_rec,
                                   const txn_layout_t &layout,
                                   mtr_log_t log_mode) {
  if (opt_ddl_cleanout_disable) return nullptr;

  /** Store the first record of new page. */
  if (!m_cursor.stored()) {
    ut_ad(m_rec_nums == 0);
    m_cursor.store(bcur, layout, log_mode);
  }

  /** If the record is on the different page, return. */
  if (!m_cursor.is_on_same_page(bcur)) {
    clear();
    return nullptr;
  }

  if (m_rec_nums >= MAX_CURSORS) return nullptr;

  m_old_recs[m_rec_nums] = const_cast<rec_t *>(btr_cur_get_rec(bcur));
  m_txn_recs[m_rec_nums] = txn_rec;
  m_rec_nums++;

  ut_ad(page_get_page_no(page_align(m_cursor.get_old_rec())) ==
        page_get_page_no(page_align(btr_cur_get_rec(bcur))));

  /**Attention: always first rec cursor on page. */
  return &m_cursor;
}

void DDL_cleanout::execute() {
  mem_heap_t *heap = nullptr;
  buf_block_t *block = nullptr;
  dict_index_t *index = nullptr;
  ulint cleaned = 0;

  if (!m_cursor.stored()) return;

  ut_ad(m_cursor.get_layout() == TL_CLOVER);

  mtr_t mtr;
  mtr.start();

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

    txn_rec_t old_txn_rec(old_rec, index, offsets, m_cursor.get_layout());

    if (old_txn_rec.trx_id == txn_rec.trx_id) {
      ut_ad(txn_rec.slot() == old_txn_rec.slot());

      /** If trx state is active ,try to cleanout */
      if (old_txn_rec.is_active()) {
        /** Modify the scn and undo ptr */
        row_upd_rec_clover_fields_in_cleanout(
            old_rec, buf_block_get_page_zip(block), index, offsets, &txn_rec);

        /** Write the redo log */
        btr_cur_upd_clover_fields_clust_rec_log(old_rec, index, &txn_rec, &mtr);

        m_cursor.inc_cleanouts(&mtr);
        cleaned++;
      }
    }

    if (heap) mem_heap_empty(heap);
  }

mtr_end:
  mtr.commit();
  if (heap) mem_heap_free(heap);

  ddl_cleanout_clean_stat(cleaned);

  clear();
}

/**
  Collect rows updated in current transaction.

  @param[in]        thr             current session
  @param[in]        cursor          btr cursor
  @param[in]        rec             current rec
  @param[in]        flags           mode flags for btr_cur operations
*/
void commit_cleanout_collect(que_thr_t *thr, btr_cur_t *cursor, rec_t *rec,
                             ulint flags, const txn_layout_t &layout) {
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

  if (leaf && !index->table->is_temporary() && !dict_index_is_ibuf(index)) {
    switch (layout) {
      case TL_NONE:
        break;
      case TL_CLOVER:
      case TL_BAMBOO:
        ut_ad(rec != nullptr);
        ut_ad(rec == btr_cur_get_rec(cursor) /* update */ ||
              rec == page_rec_get_next(btr_cur_get_rec(cursor)) /* insert */);
        ut_ad(page_rec_is_user_rec(rec));
        ut_ad(trx->cleanout != nullptr);

        /** Ensure that the commit cleanout operation is under the protection of
         * the transaction table locks, unless the table is permanent in dict
         * sys.
         */
        ut_ad(dict_sys->is_permanent_table(index->table) ||
              lock_table_has_locks(index->table));

        btr_cur_t dup_cursor;
        btr_cur_position(index, rec, block, &dup_cursor);

        cleanout_ctx_t cctx(&dup_cursor, trx->cleanout);

        txn_rec_t txn_rec{trx->id, trx->txn_desc.cmmt.scn,
                          trx->txn_desc.undo_ptr, trx->txn_desc.cmmt.gcn};
        ut_ad(txn_rec.is_active());

        cctx(MTR_LOG_NONE).collect_txn(txn_rec, layout);
        break;
    }
  }
  return;
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

  trx->cleanout->commit(txn_rec);
  trx->cleanout->execute();
}

/** Constructor */
cleanout_ctx_t::cleanout_ctx_t(btr_pcur_t *pcur)
    : m_bcur(nullptr),
      m_cleanout(nullptr),
      m_tcursor(nullptr),
      m_gcursor(nullptr),
      m_log_mode(MTR_LOG_ALL),
      m_setting(false) {
  if (pcur) {
    m_bcur = pcur->get_btr_cur();
    m_cleanout = pcur->m_cleanout;
  }
}

/** Constructor */
cleanout_ctx_t::cleanout_ctx_t(btr_pcur_t *pcur, Cleanout *cleanout)
    : m_bcur(nullptr),
      m_cleanout(cleanout),
      m_tcursor(nullptr),
      m_gcursor(nullptr),
      m_log_mode(MTR_LOG_ALL),
      m_setting(false) {
  if (pcur) {
    m_bcur = pcur->get_btr_cur();
  }
}

/** Constructor */
cleanout_ctx_t::cleanout_ctx_t(btr_cur_t *bcur, Cleanout *cleanout)
    : m_bcur(bcur),
      m_cleanout(cleanout),
      m_tcursor(nullptr),
      m_gcursor(nullptr),
      m_log_mode(MTR_LOG_ALL),
      m_setting(false) {}

}  // namespace lizard
