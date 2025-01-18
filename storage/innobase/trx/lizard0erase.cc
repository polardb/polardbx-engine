/*****************************************************************************

Copyright (c) 1996, 2024, Oracle and/or its affiliates. Copyright (c) 2023, 2024, Alibaba and/or its affiliates.

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

/** @file trx/lizard0erase.cc
 Erase old versions for semi-purge

 Created 13/05/2024 jiyang.zhang
 *******************************************************/

#include "que0que.h"
#include "row0purge.h"
#include "trx0rec.h"
#include "trx0rseg.h"
#include "row0upd.h"

#include "lizard0undo.h"
#include "lizard0erase.h"
#include "lizard0undo0types.h"
#include "lizard0undo0retent.h"

/** A sentinel undo record used as a return value when we have a whole
undo log which can be skipped by purge */
extern trx_undo_rec_t trx_purge_ignore_rec;

/** lizard: a sentinel undo record to identify the BLOCKED record */
extern trx_undo_rec_t trx_purge_blocked_rec;

extern que_t *trx_purge_graph_build(trx_t *trx, ulint n_purge_threads,
                                    lizard::e_2pp_phase phase);
trx_undo_rec_t *trx_undo_get_next_rec_from_next_page(
    space_id_t space, const page_size_t &page_size, const page_t *undo_page,
    page_no_t page_no, ulint offset, ulint mode, mtr_t *mtr);


namespace lizard {

/** The global data structure coordinating a purge */
trx_erase_t *erase_sys = nullptr;

void trx_erase_sys_mem_create() {
  erase_sys = static_cast<trx_erase_t *>(
      ut::zalloc_withkey(UT_NEW_THIS_FILE_PSI_KEY, sizeof(*erase_sys)));

  new (&erase_sys->iter) purge_iter_t;
  new (&erase_sys->limit) purge_iter_t;
  /* new (&erase_sys->thds) ut::unordered_set<THD *>; */
#ifdef UNIV_DEBUG
  new (&erase_sys->done) purge_iter_t;
#endif /* UNIV_DEBUG */

  mutex_create(LATCH_ID_ERASE_SYS_PQ, &erase_sys->pq_mutex);

  erase_sys->heap = mem_heap_create(8 * 1024, UT_LOCATION_HERE);
}

void trx_erase_sys_close() {
  /** erase_sys->query come from purge_sys->query. */
  /* que_graph_free(erase_sys->query); */
  erase_sys->query = nullptr;

  mem_heap_free(erase_sys->heap);

  erase_sys->heap = nullptr;

  call_destructor(&erase_sys->erased_gcn);

  mutex_free(&erase_sys->pq_mutex);

  if (erase_sys->erase_heap != nullptr) {
    ut::delete_(erase_sys->erase_heap);
    erase_sys->erase_heap = nullptr;
  }

  ut::free(erase_sys);

  erase_sys = nullptr;
}

static void trx_erase_set_erased_scn(scn_t txn_scn) {
  erase_sys->erased_scn.store(txn_scn);
}

#ifdef UNIV_DEBUG
static trx_rseg_t *trx_erase_find_oldest_log(fil_addr_t &log_addr,
                                             commit_mark_t &cmmt);
#endif /* UNIV_DEBUG */

/**
  Load erased_scn

  @retval              a valid scn if found, or purge_sys->purged_scn.
*/
static scn_t trx_erase_reload_erased_scn() {
  /** If undo log scan is forbidden, erase_sys->erased_scn can't get a valid
  value */
  if (srv_force_recovery >= SRV_FORCE_NO_UNDO_LOG_SCAN) {
    return ERASED_SCN_INVALID;
  }

  scn_t min_erase_scn = SCN_NULL;

  ut_ad(purge_sys);
  ut_ad(erase_sys);

  if (!erase_sys->erase_heap->empty()) {
    min_erase_scn = erase_sys->erase_heap->top().get_scn();
#ifdef UNIV_DEBUG
    fil_addr_t addr;
    commit_mark_t cmmt;
    ut_ad(trx_erase_find_oldest_log(addr, cmmt) != nullptr);
    ut_ad(cmmt.scn == min_erase_scn);
#endif /* UNIV_DEBUG */
  }

  /**
   * There may still be undo logs with smaller SCNs in the purge sys that need
   * to be erased. Identify the minimum SCN from both the purge heap and the
   * erase heap to determine the erased_scn. */
  min_erase_scn =
      std::min(purge_sys->truncating_list_scn.get_min(), min_erase_scn);

  ut_ad(min_erase_scn != SCN_NULL);

  return min_erase_scn;
}

void trx_erase_sys_initialize(uint32_t n_erase_threads,
                              lizard::erase_heap_t *erase_heap) {
  ut_a(n_erase_threads > 0);
  ut_a(erase_heap);

  /* Take ownership of erase_heap, we are responsible for freeing it. */
  erase_sys->erase_heap = erase_heap;

  ut_a(purge_sys->query != nullptr);
  erase_sys->query = purge_sys->query;

  trx_erase_set_erased_scn(lizard::trx_erase_reload_erased_scn());

  new (&erase_sys->erased_gcn) lizard::Erased_gcn;

  erase_sys->erased_gcn.init();

  erase_sys->clone_oldest_vision(purge_sys->truncating_list_scn.get_min());

  ut_a(erase_sys->rseg == nullptr);
  ut_a(erase_sys->next_stored == false);
  ut_a(erase_sys->offset == 0);
}

static void trx_erase_start_sp() {
  que_thr_t *thr = nullptr;
  for (thr = UT_LIST_GET_FIRST(erase_sys->query->thrs); thr != nullptr;
       thr = UT_LIST_GET_NEXT(thrs, thr)) {
    purge_node_t *node = static_cast<purge_node_t *>(thr->child);
    node->start_sp_erase();
  }
}

/** Calculates the file address of an undo log header when we have the file
 address of its semi-purge list node.
 @return file address of the log */
static inline fil_addr_t trx_erase_get_log_from_sp(
    fil_addr_t node_addr) /*!< in: file address of the semi-purge
                          list node of the log */
{
  node_addr.boffset -= TRX_UNDO_HISTORY_NODE;

  return (node_addr);
}

/** Remove an undo log header from the semi-purge list.
@param[in,out]  rseg_hdr        Rollback segment header
@param[in]      log_hdr         Undo log header
@param[in,out]  mtr             Mini-transaction. */
static inline void trx_erase_remove_log_hdr_sp(trx_rsegf_t *rseg_hdr,
                                               trx_ulogf_t *log_hdr,
                                               mtr_t *mtr) {
  flst_remove(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST,
              log_hdr + TRX_UNDO_HISTORY_NODE, mtr);
}

/** Gets the first record in an undo log.
@param[out]     modifier_trx_id The modifier trx identifier.
@param[in]      space           Undo log header space
@param[in]      page_size       Page size
@param[in]      hdr_page_no     Undo log header page number
@param[in]      hdr_offset      Undo log header offset on page
@param[in,out]  mtr             Mini-transaction
@return undo log record, the page latched, NULL if none */
static trx_undo_rec_t *trx_erase_undo_get_first_rec(
    trx_id_t *modifier_trx_id, bool *del_marks, slot_addr_t *slot_addr,
    bool *is_2pp_log, space_id_t space, const page_size_t &page_size,
    page_no_t hdr_page_no, ulint hdr_offset, mtr_t *mtr) {
  page_t *undo_page;
  trx_undo_rec_t *rec;
  trx_ulogf_t *undo_header;

  const page_id_t page_id(space, hdr_page_no);

  undo_page = trx_undo_page_get_s_latched(page_id, page_size, mtr);

  undo_header = undo_page + hdr_offset;

  *modifier_trx_id = mach_read_from_8(undo_header + TRX_UNDO_TRX_ID);

  *del_marks =
      mtr_read_ulint(undo_header + TRX_UNDO_DEL_MARKS, MLOG_2BYTES, mtr);

  *is_2pp_log = trx_undo_log_is_2pp(undo_header, mtr);

  *slot_addr = trx_undo_hdr_read_slot(undo_header, mtr);

  rec = trx_undo_page_get_first_rec(undo_page, hdr_page_no, hdr_offset);

  if (rec) {
    return (rec);
  }

  return (trx_undo_get_next_rec_from_next_page(
      space, page_size, undo_page, hdr_page_no, hdr_offset, RW_S_LATCH, mtr));
}

/** Position the erase sys "iterator" on the undo record to use for erasing.
@param[in]      page_size       page size */
static void trx_erase_read_undo_rec(const page_size_t &page_size) {
  ulint offset;
  page_no_t page_no;
  uint64_t undo_no;
  space_id_t undo_rseg_space;
  trx_id_t modifier_trx_id;
  mtr_t mtr;
  trx_undo_rec_t *undo_rec = nullptr;
  bool del_marks = false;
  slot_addr_t txn_addr;
  txn_cursor_t txn_cursor;
  bool is_2pp_log;

  ut_a(erase_sys->hdr_offset != 0);
  ut_a(erase_sys->hdr_page_no != FIL_NULL);
  ut_a(erase_sys->rseg != nullptr);

  page_no = erase_sys->hdr_page_no;

  mtr_start(&mtr);

  /** NOTES: The following may lock two pages, but the mutex of rseg is not
   * taken. Because this update undo must have been purged, that is, it will no
   * longer be used by other threads to take multiple pages at the same time. */
  undo_rec = trx_erase_undo_get_first_rec(
      &modifier_trx_id, &del_marks, &txn_addr, &is_2pp_log,
      erase_sys->rseg->space_id, page_size, erase_sys->hdr_page_no,
      erase_sys->hdr_offset, &mtr);

  /** Store txn cursor. */
  txn_cursor.trx_id = modifier_trx_id;
  txn_cursor.txn_addr = txn_addr;

  ut_ad(erase_sys->rseg->last_erase_del_marks == del_marks);

  if (del_marks && is_2pp_log) {
    if (undo_rec != nullptr) {
      offset = page_offset(undo_rec);
      undo_no = trx_undo_rec_get_undo_no(undo_rec);
      undo_rseg_space = erase_sys->rseg->space_id;
      page_no = page_get_page_no(page_align(undo_rec));
    } else {
      offset = 0;
      undo_no = 0;
      undo_rseg_space = SPACE_UNKNOWN;
    }

  } else {
    offset = 0;
    undo_no = 0;
    undo_rseg_space = SPACE_UNKNOWN;
    modifier_trx_id = 0;
  }

  mtr_commit(&mtr);

  erase_sys->offset = offset;
  erase_sys->page_no = page_no;
  erase_sys->txn_cursor = txn_cursor;
  erase_sys->iter.undo_no = undo_no;
  erase_sys->iter.modifier_trx_id = modifier_trx_id;
  erase_sys->iter.undo_rseg_space = undo_rseg_space;

  erase_sys->next_stored = true;
}

/**
  Get last (oldest) log header from semi purge list.
  @params[in]   rseg            update undo rollback segment
  @params[out]  log header address
  @params[out]	rollback segment statistics

  @retval	commit mark of log header
*/
commit_mark_t trx_erase_get_last_log(trx_rseg_t *rseg, fil_addr_t &addr,
                                     rseg_stat_t *stat) {
  mtr_t mtr;
  trx_rsegf_t *rseg_hdr;
  trx_ulogf_t *log_hdr;
  page_t *undo_page;
  commit_mark_t cmmt;

  ut_a(!fsp_is_system_temporary(rseg->space_id));
  ut_a(!rseg->is_txn);

  mtr_start(&mtr);
  rseg->latch();

  rseg_hdr =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

  if (stat) {
    stat->rseg_pages = rseg->get_curr_size();
    stat->secondary_pages = mtr_read_ulint(
        rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST_SIZE, MLOG_4BYTES, &mtr);
    stat->secondary_length = flst_get_len(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST);
  }

  addr = trx_erase_get_log_from_sp(
      flst_get_last(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST, &mtr));

  if (addr.page == FIL_NULL) {
    rseg->unlatch();
    mtr_commit(&mtr);
    return cmmt;
  }

  undo_page = trx_undo_page_get_s_latched(page_id_t(rseg->space_id, addr.page),
                                          rseg->page_size, &mtr);

  log_hdr = undo_page + addr.boffset;

  ut_a(trx_useg_is_2pp(undo_page, rseg->page_size, &mtr));

  cmmt = lizard::trx_undo_hdr_read_cmmt(log_hdr, &mtr);

  rseg->unlatch();
  mtr_commit(&mtr);
  return cmmt;
}

#ifdef UNIV_DEBUG
/** Find oldest log header in sp lists by polling.
 *
 * @param[out]		rseg
 * @param[out]		log header address
 * @param[out]		commit mark info
 *
 * @retval	roll segment
 * */
static trx_rseg_t *trx_erase_find_oldest_log(fil_addr_t &log_addr,
                                             commit_mark_t &cmmt) {
  trx_rseg_t *rseg = nullptr;
  cmmt.reset();

  mutex_enter(&undo::ddl_mutex);
  undo::spaces->s_lock();
  for (auto undo_space : undo::spaces->m_spaces) {
    if (undo_space->is_txn()) {
      continue;
    }

    undo_space->rsegs()->s_lock();
    for (auto last_rseg : *undo_space->rsegs()) {
      fil_addr_t last_addr;
      commit_mark_t last_cmmt = trx_erase_get_last_log(last_rseg, last_addr);
      if (!last_addr.is_null() && last_cmmt.scn < cmmt.scn) {
        log_addr = last_addr;
        cmmt = last_cmmt;
        rseg = last_rseg;
      }
    }
    undo_space->rsegs()->s_unlock();
  }
  undo::spaces->s_unlock();

  mutex_exit(&undo::ddl_mutex);
  return rseg;
}
#endif /* UNIV_DEBUG */

/** Find next log header in sp lists. */
static void trx_erase_sys_set_next() {
  mutex_enter(&erase_sys->pq_mutex);
  lizard_erased_scn_validation();

  scn_t top_scn = 0;
  trx_rseg_t *top_rseg = nullptr;

  if (!erase_sys->erase_heap->empty()) {
    const UpdateUndoRseg top = erase_sys->erase_heap->top();
    top_rseg = top.get_rseg();
    top_scn = top.get_scn();

    /**
     * To ensure that erase operations are executed in the correct SCN order,
     * an undo log can only be erased if there are no remaining undo logs
     * with smaller SCNs in the purge system that need to be migrated to
     * the semi-purge list.
     */
    if (top_scn >= erase_sys->oldest_vision.load()) {
      mutex_exit(&erase_sys->pq_mutex);

      erase_sys->rseg = nullptr;
      return;
    }

    erase_sys->erase_heap->pop();

    mutex_exit(&erase_sys->pq_mutex);
  } else {
    mutex_exit(&erase_sys->pq_mutex);

    erase_sys->rseg = nullptr;
    return;
  }

  ut_a(top_rseg != nullptr);
  erase_sys->rseg = top_rseg;

  erase_sys->rseg->latch();

  ut_a(erase_sys->rseg->last_erase_page_no != FIL_NULL);
  ut_ad(erase_sys->rseg->last_erase_ommt.scn == top_scn);
  erase_sys->iter.ommt = erase_sys->rseg->last_erase_ommt;
  erase_sys->hdr_offset = erase_sys->rseg->last_erase_offset;
  erase_sys->hdr_page_no = erase_sys->rseg->last_erase_page_no;

  erase_sys->rseg->unlatch();
}

/** Chooses the next undo log to erase and updates the info in erase_sys. This
 function is used to initialize erase_sys when the next record to purge is not
 known, and also to update the erase system info on the next record when erase
 has handled the whole undo log for a transaction.
 purge has handled the whole undo log for a transaction. */
static void trx_erase_choose_next_log() {
  ut_ad(erase_sys->next_stored == false);

  trx_erase_sys_set_next();

  if (erase_sys->rseg != nullptr) {
    trx_erase_read_undo_rec(erase_sys->rseg->page_size);
  }
}

static void trx_erase_rseg_get_next_sp_log(
    trx_rseg_t *rseg,       /*!< in: rollback segment */
    ulint *n_pages_handled) /*!< in/out: number of UNDO pages
                            handled */
{
  mtr_t mtr;
  commit_mark_t cmmt;

  rseg->latch();

  ut_a(rseg->last_erase_page_no != FIL_NULL);

  erase_sys->iter.undo_no = 0;
  erase_sys->iter.undo_rseg_space = SPACE_UNKNOWN;
  erase_sys->next_stored = false;
  erase_sys->iter.ommt = rseg->last_erase_ommt;

  mtr_start(&mtr);

  auto undo_page = trx_undo_page_get_s_latched(
      page_id_t(rseg->space_id, rseg->last_erase_page_no), rseg->page_size,
      &mtr);

  auto log_hdr = undo_page + rseg->last_erase_offset;

  /* Increase the purge page count by one for every handled log */

  (*n_pages_handled)++;

  auto prev_log_addr = trx_erase_get_log_from_sp(
      flst_get_prev_addr(log_hdr + TRX_UNDO_HISTORY_NODE, &mtr));

  if (prev_log_addr.page == FIL_NULL) {
    /* No logs left in the sp list */

    rseg->last_erase_page_no = FIL_NULL;

    mtr_commit(&mtr);
    rseg->unlatch();
    return;
  }

  mtr_commit(&mtr);
  rseg->unlatch();

  /* Read the trx number and del marks from the previous log header */
  mtr_start(&mtr);

  log_hdr =
      trx_undo_page_get_s_latched(page_id_t(rseg->space_id, prev_log_addr.page),
                                  rseg->page_size, &mtr) +
      prev_log_addr.boffset;

  auto del_marks = mach_read_from_2(log_hdr + TRX_UNDO_DEL_MARKS);

  cmmt = lizard::trx_undo_hdr_read_cmmt(log_hdr, &mtr);

  mtr_commit(&mtr);

  rseg->latch();

  rseg->last_erase_page_no = prev_log_addr.page;
  rseg->last_erase_offset = prev_log_addr.boffset;
  rseg->last_erase_del_marks = del_marks;
  rseg->last_erase_ommt = cmmt;

  lizard::UpdateUndoRseg elem(cmmt.scn, rseg);

  mutex_enter(&erase_sys->pq_mutex);

  erase_sys->erase_heap->push(std::move(elem));

  lizard_erased_scn_validation();

  mutex_exit(&erase_sys->pq_mutex);

  rseg->unlatch();
}

/** Gets the next record to erase and updates the info in the purge system.
 @return copy of an undo log record or pointer to the dummy undo log record */
static trx_undo_rec_t *trx_erase_get_next_rec(
    ulint *n_pages_handled, /*!< in/out: number of UNDO pages
                            handled */
    mem_heap_t *heap)       /*!< in: memory heap where copied */
{
  trx_undo_rec_t *rec;
  trx_undo_rec_t *rec_copy;
  trx_undo_rec_t *rec2;
  page_t *undo_page;
  page_t *page;
  ulint offset;
  page_no_t page_no;
  space_id_t space;
  mtr_t mtr;

  ut_ad(erase_sys->next_stored);

  space = erase_sys->rseg->space_id;
  page_no = erase_sys->page_no;
  offset = erase_sys->offset;

  const page_size_t page_size(erase_sys->rseg->page_size);

  if (offset == 0) {
    /* It is the dummy undo log record, which means that there is no need to
    purge this undo log */

    trx_erase_rseg_get_next_sp_log(erase_sys->rseg, n_pages_handled);

    trx_erase_choose_next_log();

    return (&trx_purge_ignore_rec);
  }

  mtr_start(&mtr);

  undo_page =
      trx_undo_page_get_s_latched(page_id_t(space, page_no), page_size, &mtr);

  rec = undo_page + offset;

  rec2 = rec;

  for (;;) {
    ulint type;
    trx_undo_rec_t *next_rec;
    ulint cmpl_info;

    /* Try first to find the next record which requires a purge operation from
    the same page of the same undo log */

    next_rec = trx_undo_page_get_next_rec(rec2, erase_sys->hdr_page_no,
                                          erase_sys->hdr_offset);

    if (next_rec == nullptr) {
      rec2 = trx_undo_get_next_rec(rec2, erase_sys->hdr_page_no,
                                   erase_sys->hdr_offset, &mtr);
      break;
    }

    rec2 = next_rec;

    type = trx_undo_rec_get_type(rec2);

    if (type == TRX_UNDO_DEL_MARK_REC) {
      break;
    }

    cmpl_info = trx_undo_rec_get_cmpl_info(rec2);

    if (trx_undo_rec_get_extern_storage(rec2)) {
      break;
    }

    if ((type == TRX_UNDO_UPD_EXIST_REC) &&
        !(cmpl_info & UPD_NODE_NO_ORD_CHANGE)) {
      break;
    }
  }

  if (rec2 == nullptr) {
    mtr_commit(&mtr);

    trx_erase_rseg_get_next_sp_log(erase_sys->rseg, n_pages_handled);

    trx_erase_choose_next_log();

    mtr_start(&mtr);

    undo_page =
        trx_undo_page_get_s_latched(page_id_t(space, page_no), page_size, &mtr);

  } else {
    page = page_align(rec2);

    erase_sys->offset = rec2 - page;
    erase_sys->page_no = page_get_page_no(page);
    erase_sys->iter.undo_no = trx_undo_rec_get_undo_no(rec2);
    erase_sys->iter.undo_rseg_space = space;

    if (undo_page != page) {
      /* We advance to a new page of the undo log: */
      (*n_pages_handled)++;
    }
  }

  rec_copy = trx_undo_rec_copy(undo_page, static_cast<uint32_t>(offset), heap);

  mtr_commit(&mtr);

  return (rec_copy);
}

/** Fetches the next undo log record from the sp list to erase. It must
 be released with the corresponding release function.
 @return copy of an undo log record or pointer to trx_purge_ignore_rec,
 if the whole undo log can skipped in purge; NULL if none left */
trx_undo_rec_t *trx_erase_fetch_next_rec(
    trx_id_t *modifier_trx_id,
    /*!< out: modifier trx id. this is the
    trx that created the undo record. */
    roll_ptr_t *roll_ptr,   /*!< out: roll pointer to undo record */
    ulint *n_pages_handled, /*!< in/out: number of UNDO log pages
                            handled */
    mem_heap_t *heap)       /*!< in: memory heap where copied */
{
  /** 1. Try to choose next sp log, erase_sys->rseg will point at the RSEG if
  having. */
  if (!erase_sys->next_stored) {
    trx_erase_choose_next_log();

    if (!erase_sys->next_stored) {
      DBUG_PRINT("ib_semi_purge", ("no logs left in the semi purge list"));
      return nullptr;
    }
  }

  ut_a(erase_sys->rseg != nullptr);
  ut_ad(erase_sys->next_stored);

  /** 2. Check if the log header can be erased. */
  if (txn_retention_satisfied(erase_sys->iter.ommt.us)) {
    erase_sys->push_erased(erase_sys->iter.ommt);
    /** Call after **trx_erase_read_undo_rec** */
    txn_undo_set_state_at_erase(erase_sys->txn_cursor, erase_sys->iter.ommt.scn,
                                erase_sys->rseg->page_size);
  } else {
    return &trx_purge_blocked_rec;
  }

  *roll_ptr = trx_undo_build_roll_ptr(false, erase_sys->rseg->space_id,
                                      erase_sys->page_no, erase_sys->offset);

  *modifier_trx_id = erase_sys->iter.modifier_trx_id;

  /** 3. Fetch next undo log record. */

  /* The following call will advance the stored values of the
  erase iterator. */

  return (trx_erase_get_next_rec(n_pages_handled, heap));
}

/** Frees a rollback segment which is in the semi-purge list.
Removes the rseg hdr from the semi-purge list.
@param[in,out]  rseg            rollback segment
@param[in]      hdr_addr        file address of log_hdr */
static void trx_erase_free_segment_sp(trx_rseg_t *rseg, fil_addr_t hdr_addr) {
  mtr_t mtr;
  trx_rsegf_t *rseg_hdr;
  trx_ulogf_t *log_hdr;
  trx_usegf_t *seg_hdr;
  ulint seg_size;
  ulint sp_size;
  bool marked = false;

  for (;;) {
    page_t *undo_page;

    mtr_start(&mtr);

    rseg->latch();

    rseg_hdr =
        trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

    undo_page = trx_undo_page_get(page_id_t(rseg->space_id, hdr_addr.page),
                                  rseg->page_size, &mtr);

    seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
    log_hdr = undo_page + hdr_addr.boffset;

    /* Mark the last undo log totally purged, so that if the
    system crashes, the tail of the undo log will not get accessed
    again. The list of pages in the undo log tail gets inconsistent
    during the freeing of the segment, and therefore purge should
    not try to access them again. */

    if (!marked) {
      marked = true;
      mlog_write_ulint(log_hdr + TRX_UNDO_DEL_MARKS, false, MLOG_2BYTES, &mtr);
    }

    if (fseg_free_step_not_header(seg_hdr + TRX_UNDO_FSEG_HEADER, false,
                                  &mtr)) {
      break;
    }

    rseg->unlatch();

    mtr_commit(&mtr);
  }

  /** Might not the final log in the undo log segment. */
  /* ut_ad(mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG) == 0); */

  /* The page list may now be inconsistent, but the length field
  stored in the list base node tells us how big it was before we
  started the freeing. */

  seg_size = flst_get_len(seg_hdr + TRX_UNDO_PAGE_LIST);

  /* We may free the undo log segment header page; it must be freed
  within the same mtr as the undo log header is removed from the
  history list: otherwise, in case of a database crash, the segment
  could become inaccessible garbage in the file space. */

  lizard::trx_erase_remove_log_hdr_sp(rseg_hdr, log_hdr, &mtr);

  do {
    /* Here we assume that a file segment with just the header
    page can be freed in a few steps, so that the buffer pool
    is not flooded with bufferfixed pages: see the note in
    fsp0fsp.cc. */

  } while (!fseg_free_step(seg_hdr + TRX_UNDO_FSEG_HEADER, false, &mtr));

  sp_size = mtr_read_ulint(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST_SIZE,
                           MLOG_4BYTES, &mtr);
  ut_ad(sp_size >= seg_size);
  mlog_write_ulint(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST_SIZE, sp_size - seg_size,
                   MLOG_4BYTES, &mtr);

  rseg->decr_curr_size(seg_size);
  rseg->unlatch();
  mtr_commit(&mtr);
}

/** Removes unnecessary history data from a rollback segment. */
static void trx_erase_truncate_rseg_sp(
    trx_rseg_t *rseg,          /*!< in: rollback segment */
    const purge_iter_t *limit) /*!< in: truncate offset */
{
  fil_addr_t hdr_addr;
  fil_addr_t prev_hdr_addr;
  trx_rsegf_t *rseg_hdr;
  page_t *undo_page;
  trx_ulogf_t *log_hdr;
  trx_usegf_t *seg_hdr;
  mtr_t mtr;
  scn_t undo_trx_scn;
  trx_upagef_t *page_hdr;
  ulint type;
  bool last_log;

  mtr_start(&mtr);

  rseg->latch();

  rseg_hdr =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

  hdr_addr = trx_erase_get_log_from_sp(
      flst_get_last(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST, &mtr));
loop:
  if (hdr_addr.page == FIL_NULL) {
    rseg->unlatch();
    mtr_commit(&mtr);

    return;
  }

  undo_page = trx_undo_page_get(page_id_t(rseg->space_id, hdr_addr.page),
                                rseg->page_size, &mtr);

  log_hdr = undo_page + hdr_addr.boffset;

  undo_trx_scn = lizard::trx_undo_hdr_read_cmmt(log_hdr, &mtr).scn;

  if (undo_trx_scn >= limit->ommt.scn) {

    if (undo_trx_scn == limit->ommt.scn &&
        rseg->space_id == limit->undo_rseg_space) {
      trx_undo_truncate_start(rseg, hdr_addr.page, hdr_addr.boffset,
                              limit->undo_no, false);
    }

    rseg->unlatch();
    mtr_commit(&mtr);

    return;
  }

  prev_hdr_addr = trx_erase_get_log_from_sp(
      flst_get_prev_addr(log_hdr + TRX_UNDO_HISTORY_NODE, &mtr));

  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  type = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_TYPE);

  ut_a(type == TRX_UNDO_UPDATE);
  ut_ad(lizard::trx_useg_is_2pp(undo_page, rseg->page_size, &mtr));

  last_log = mach_read_from_2(seg_hdr + TRX_UNDO_STATE) == TRX_UNDO_TO_PURGE &&
             mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG) == 0;

  if (last_log) {
    rseg->unlatch();
    mtr_commit(&mtr);

    trx_erase_free_segment_sp(rseg, hdr_addr);
  } else {
    /* Remove the log hdr from the semi-purge history. */
    trx_erase_remove_log_hdr_sp(rseg_hdr, log_hdr, &mtr);

    rseg->unlatch();
    mtr_commit(&mtr);
  }

  mtr_start(&mtr);

  rseg->latch();

  rseg_hdr =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

  hdr_addr = prev_hdr_addr;

  goto loop;
}

/** Removes unnecessary history data from rollback segments.
NOTE that when this function is called, the caller must not
have any latches on undo log pages!
@param[in]  limit  Truncate limit
@param[in]  view   Purge view */
static void trx_erase_truncate_history(purge_iter_t *limit) {
  MONITOR_INC_VALUE(MONITOR_PURGE_TRUNCATE_HISTORY_COUNT, 1);

  auto counter_time_truncate_history = std::chrono::steady_clock::now();

  ut_ad(limit->ommt.scn <= purge_sys->vision.snapshot_scn());

  /* Purge rollback segments in all undo tablespaces.  This may take
  some time and we do not want an undo DDL to attempt an x_lock during
  this time.  If it did, all other transactions seeking a short s_lock()
  would line up behind it.  So get the ddl_mutex before this s_lock(). */
  mutex_enter(&undo::ddl_mutex);
  undo::spaces->s_lock();
  for (auto undo_space : undo::spaces->m_spaces) {

    if (undo_space->is_txn()) {
      continue;
    }

    /* Purge rollback segments in this undo tablespace. */
    undo_space->rsegs()->s_lock();

    for (auto rseg : *undo_space->rsegs()) {
      trx_erase_truncate_rseg_sp(rseg, limit);
    }
    undo_space->rsegs()->s_unlock();
  }

  undo::spaces->s_unlock();
  mutex_exit(&undo::ddl_mutex);

  /** Should be always empty. */
  ut_a(trx_sys->rsegs.is_empty());

  MONITOR_INC_TIME(MONITOR_PURGE_TRUNCATE_HISTORY_MICROSECOND,
                   counter_time_truncate_history);
}

/** Remove old historical changes from the rollback segments. */
static void trx_erase_truncate(void) {
  ut_ad(trx_erase_check_limit());

  if (erase_sys->limit.ommt.scn == 0) {
    trx_erase_truncate_history(&erase_sys->iter);
  } else {
    trx_erase_truncate_history(&erase_sys->limit);
  }

  /* Attempt to truncate an undo tablespace. */
  /* trx_purge_truncate_undo_spaces(); */
}

static void trx_erase_wait_for_workers_to_complete() {
  ulint i = 0;
  ulint n_submitted = erase_sys->n_submitted;

  /* Ensure that the work queue empties out. */
  while (erase_sys->n_completed.load() != n_submitted) {
    if (++i < 10) {
      std::this_thread::yield();
    } else {
      if (srv_get_task_queue_length() > 0) {
        srv_release_threads(SRV_WORKER, 1);
      }

      std::this_thread::sleep_for(std::chrono::microseconds(20));
      i = 0;
    }
  }

  /* None of the worker threads should be doing any work. */
  ut_a(erase_sys->n_submitted == erase_sys->n_completed);

  /* There should be no outstanding tasks as long
  as the worker threads are active. */
  ut_a(srv_get_task_queue_length() == 0);
}

ulint trx_erase(ulint n_purge_threads, /*!< in: number of purge tasks
                                       to submit to the work queue */
                ulint batch_size,      /*!< in: the maximum number of records
                                       to purge in one batch */
                bool truncate)         /*!< in: truncate history if true */
{
  que_thr_t *thr = nullptr;
  ulint n_pages_handled;

  /* The number of tasks submitted should be completed. */
  ut_a(erase_sys->n_submitted == erase_sys->n_completed);

  ut_ad(erase_sys->erased_scn.load() <=
        purge_sys->truncating_list_scn.get_min());
  erase_sys->clone_oldest_vision(purge_sys->truncating_list_scn.get_min());

  trx_erase_start_sp();

  /* Fetch the UNDO recs that need to be erased. */
  n_pages_handled = trx_erase_attach_undo_recs(n_purge_threads, batch_size);

  /* Do we do an asynchronous purge or not ? */
  if (n_purge_threads > 1) {
    /* Submit the tasks to the work queue. */
    for (ulint i = 0; i < n_purge_threads - 1; ++i) {
      thr = que_fork_scheduler_round_robin(erase_sys->query, thr);

      ut_a(thr != nullptr);

      srv_que_task_enqueue_low(thr);
    }

    thr = que_fork_scheduler_round_robin(erase_sys->query, thr);
    ut_a(thr != nullptr);

    erase_sys->n_submitted += n_purge_threads - 1;

    goto run_synchronously;

    /* Do it synchronously. */
  } else {
    thr = que_fork_scheduler_round_robin(erase_sys->query, nullptr);
    ut_ad(thr);

  run_synchronously:
    ++erase_sys->n_submitted;

    que_run_threads(thr);

    erase_sys->n_completed.fetch_add(1);

    if (n_purge_threads > 1) {
      trx_erase_wait_for_workers_to_complete();
    }
  }

  ut_a(erase_sys->n_submitted == erase_sys->n_completed);

#ifdef UNIV_DEBUG
  if (erase_sys->limit.ommt.scn == 0) {
    erase_sys->done = erase_sys->iter;
  } else {
    erase_sys->done = erase_sys->limit;
  }
#endif /* UNIV_DEBUG */

  for (thr = UT_LIST_GET_FIRST(erase_sys->query->thrs); thr != nullptr;
       thr = UT_LIST_GET_NEXT(thrs, thr)) {
    purge_node_t *node = static_cast<purge_node_t *>(thr->child);
    node->free_lob_pages();
  }

  DBUG_EXECUTE_IF(
      "crash_during_erase", if (n_pages_handled >= 3) { DBUG_SUICIDE(); });

  ut_a(!srv_upgrade_old_undo_found);
  /** trx_erase_truncate will always truncate current sp log, so the current
  sp log must be all erased, which is specified by next_sp_log == true */
  if (truncate) {
    trx_erase_truncate();
  }

  /** TODO: might increment.. <14-05-24, zanye.zjy> */
  /* MONITOR_INC_VALUE(MONITOR_PURGE_INVOKED, 1); */
  /* MONITOR_INC_VALUE(MONITOR_PURGE_N_PAGE_HANDLED, n_pages_handled); */

  return (n_pages_handled);
}

/**
  precheck if txn of the row is erased, without really reading txn

  @param[in]    txn_rec     the current row to be checked

  @retval       bool        true if the corresponding txn has been purged
*/
bool txn_rec_is_erased_by_precheck(const txn_rec_t *txn_rec) {
  if (txn_rec->is_committed()) {
    return (txn_rec->scn <= erase_sys->erased_scn);
  }
  return false;
}

void trx_erase_t::push_erased(const commit_order_t &ommt) {
  ut_a(!ommt.is_null());
  erased_scn.store(ommt.scn);
  erased_gcn.flush(ommt.gcn);
}

void trx_erase_t::clone_oldest_vision(scn_t scn) {
  oldest_vision.store(scn);
}

void trx_rseg_init_erase_heap(trx_rseg_t *rseg, trx_rsegf_t *rseg_hdr,
                              lizard::erase_heap_t *erase_heap, ulint sp_len,
                              bool parallel, mtr_t *mtr) {
  ut_ad(!rseg->is_txn);

  if (sp_len == 0) {
    rseg->last_erase_page_no = FIL_NULL;
    ut_a(rseg->last_erase_ommt.is_null());
    return;
  }

  auto node_addr = trx_erase_get_log_from_sp(
      flst_get_last(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST, mtr));

  rseg->last_erase_page_no = node_addr.page;
  rseg->last_erase_offset = node_addr.boffset;

  auto undo_log_hdr =
      trx_undo_page_get(page_id_t(rseg->space_id, node_addr.page),
                        rseg->page_size, mtr) +
      node_addr.boffset;

  /** Lizard: Retrieve the lowest SCN from semi-purge list. */
  commit_mark_t cmmt = lizard::trx_undo_hdr_read_cmmt(undo_log_hdr, mtr);
  assert_commit_mark_allocated(cmmt);
  rseg->last_erase_ommt = cmmt;

  rseg->last_erase_del_marks =
      mtr_read_ulint(undo_log_hdr + TRX_UNDO_DEL_MARKS, MLOG_2BYTES, mtr);

  lizard::UpdateUndoRseg elem(rseg->last_erase_ommt.scn, rseg);

  if (rseg->last_erase_page_no != FIL_NULL) {
    ut_ad(srv_is_being_started);

    ut_ad(rseg->space_id == TRX_SYS_SPACE ||
          (srv_is_upgrade_mode != undo::is_reserved(rseg->space_id)));

    if (parallel) {
      mutex_enter(&erase_sys->pq_mutex);
      erase_heap->push(std::move(elem));
      mutex_exit(&erase_sys->pq_mutex);
    } else {
      erase_heap->push(std::move(elem));
    }
  }
}

/**
 * Add the rseg into the erase heap.
 *
 * @param rseg        Rollback segment
 * @param scn         Scn of the undo log added to the semi-purge list
 */
void trx_add_rsegs_for_erase(trx_rseg_t *rseg, const fil_addr_t &hdr_addr,
                             bool del_mark, const commit_mark_t &cmmt) {
  ut_ad(cmmt.scn != SCN_NULL);
  ut_ad(rseg && !rseg->is_txn);
  ut_ad(mutex_own(&rseg->mutex));

  if (rseg->last_erase_page_no == FIL_NULL) {
    /** Assign last_erase_*. */
    rseg->last_erase_page_no = hdr_addr.page;
    rseg->last_erase_offset = hdr_addr.boffset;
    rseg->last_erase_del_marks = del_mark;
    rseg->last_erase_ommt = cmmt;

    /** Push erase heap. */
    UpdateUndoRseg elem(cmmt.scn, rseg);
    mutex_enter(&erase_sys->pq_mutex);

    DBUG_EXECUTE_IF(
        "crash_during_purge_truncate",
        if (!lizard::erase_sys->erase_heap->empty() &&
            lizard::erase_sys->erase_heap->top().get_scn() > elem.get_scn()) {
          DBUG_SUICIDE();
        });

    erase_sys->erase_heap->push(std::move(elem));
    lizard_erased_scn_validation();
    mutex_exit(&erase_sys->pq_mutex);
  }
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/**
 * Validate all transactions whose SCN > erased_scn is always unerased.
 * @return         true      sucessful validation
 */
bool erased_scn_validation() {
  bool ret = false;
  scn_t top_scn;

  /** If undo log scan is forbidden, erase_sys->erased_scn can't get a valid
  value */
  if (srv_force_recovery >= SRV_FORCE_NO_UNDO_LOG_SCAN) {
    return true;
  }

  /* erase sys not init yet */
  if (!erase_sys) return true;

  ut_a(mutex_own(&erase_sys->pq_mutex));

  ut_a(erase_sys->erased_scn.load() != ERASED_SCN_INVALID);

  if (!erase_sys->erase_heap->empty()) {
    top_scn = erase_sys->erase_heap->top().get_scn();
    ret = (erase_sys->erased_scn.load() <= top_scn);
  } else {
    ret = (erase_sys->erased_scn.load() <= purge_sys->purged_scn.load());
  }
  ut_ad(ret);

  return ret;
}
#endif /* UNIV_DEBUG || defined LIZARD_DEBUG */

}  // namespace lizard
