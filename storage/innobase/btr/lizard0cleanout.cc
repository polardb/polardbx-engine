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

/** @file include/lizard0cleanout.h
 Lizard cleanout operation

 Created 2020-04-15 by Jianwei.zhao
 *******************************************************/

#include "btr0pcur.h"
#include "fil0fil.h"
#include "sync0types.h"
#include "trx0rseg.h"
#include "trx0types.h"
#include "trx0undo.h"

#include "lizard0cleanout.h"
#include "lizard0dbg.h"
#include "lizard0dict.h"
#include "lizard0mon.h"
#include "lizard0row.h"
#include "lizard0txn.h"
#include "lizard0undo.h"
#include "lizard0ut.h"

namespace std {

/** Reuse the fold hash of buf page*/
size_t hash<Undo_hdr_key>::operator()(const Undo_hdr_key &p) const {
  return (p.first << 20) + p.first + p.second;
}

}  // namespace std

/** Compare */
bool Undo_hdr_equal::operator()(const Undo_hdr_key &lhr,
                                const Undo_hdr_key &rhs) const {
  return lhr.first == rhs.first && lhr.second == rhs.second;
}

#ifdef UNIV_PFS_MUTEX
/* lizard undo hdr hash mutex PFS key */
mysql_pfs_key_t undo_hdr_hash_mutex_key;
#endif

namespace lizard {

/**
   Lizard Cleanout

   1) Safe Cleanout

     -- When setting on safe mode, innodb will maintain the allocated txn undo
        log segment hash table, then before cleanout, it will search the hash
        table by [space_id + page_no] which is interpreted through UBA, if hit,
        then will continue, other than give up and assign a fake SCN number.
*/

/** Whether to write redo log when cleanout */
bool opt_cleanout_write_redo = false;

/** Whether do the safe cleanout */
bool opt_cleanout_safe_mode = false;

/** Global txn undo logs container */
Partition<Undo_logs, Undo_hdr, TXN_UNDO_HASH_PARTITIONS> *txn_undo_logs =
    nullptr;

void txn_undo_hash_init() {
  ut_ad(txn_undo_logs == nullptr);
  txn_undo_logs =
      new Partition<Undo_logs, Undo_hdr, TXN_UNDO_HASH_PARTITIONS>();
}

void txn_undo_hash_close() {
  if (txn_undo_logs != nullptr) {
    delete txn_undo_logs;
    txn_undo_logs = nullptr;
  }
}

static void txn_undo_hdr_hash_insert(Undo_hdr hdr) {
  bool result = txn_undo_logs->insert(hdr);
  if (result) lizard_stats.txn_undo_log_hash_element.inc();
}
/**
  Put txn undo into hash table.

  @param[in]      undo      txn undo memory structure.
*/
void txn_undo_hash_insert(trx_undo_t *undo) {
  ut_ad(undo->type == TRX_UNDO_TXN);

  if (opt_cleanout_safe_mode == false) return;

  Undo_hdr hdr = {undo->space, undo->hdr_page_no};
  txn_undo_hdr_hash_insert(hdr);
}
/**
  Put all the undo log segment into hash table include active undo,
  cached undo, history list, free list.

  @param[in]      space_id      rollback segment space
  @param[in]      rseg_hdr      rollback segment header page
  @param[in]      rseg          rollback segment memory object
  @param[in]      mtr           mtr that hold the rseg hdr page
*/
void trx_rseg_init_undo_hdr_hash(space_id_t space_id, trx_rsegf_t *rseg_hdr,
                                 trx_rseg_t *rseg, mtr_t *mtr) {
  trx_undo_t *undo;

  if (opt_cleanout_safe_mode == false || !fsp_is_txn_tablespace_by_id(space_id))
    return;

  /** Loop the txn undo log list */
  lizard_ut_ad(UT_LIST_GET_LEN(rseg->insert_undo_list) == 0);
  lizard_ut_ad(UT_LIST_GET_LEN(rseg->update_undo_list) == 0);

  for (undo = UT_LIST_GET_FIRST(rseg->txn_undo_list); undo != NULL;
       undo = UT_LIST_GET_NEXT(undo_list, undo)) {
    Undo_hdr hdr = {undo->space, undo->hdr_page_no};
    txn_undo_hdr_hash_insert(hdr);
  }

  for (undo = UT_LIST_GET_FIRST(rseg->txn_undo_cached); undo != NULL;
       undo = UT_LIST_GET_NEXT(undo_list, undo)) {
    Undo_hdr hdr = {undo->space, undo->hdr_page_no};
    txn_undo_hdr_hash_insert(hdr);
  }
  /** loop the rseg history list */
  mtr_t temp_mtr;
  page_t *undo_page;

  fil_addr_t node_addr;
  node_addr = flst_get_first(rseg_hdr + TRX_RSEG_HISTORY, mtr);

  while (node_addr.page != FIL_NULL) {
    Undo_hdr hdr = {space_id, node_addr.page};
    txn_undo_hdr_hash_insert(hdr);

    temp_mtr.start();
    undo_page = trx_undo_page_get_s_latched(page_id_t(space_id, node_addr.page),
                                            rseg->page_size, &temp_mtr);

    node_addr = flst_get_next_addr(undo_page + node_addr.boffset, &temp_mtr);

    temp_mtr.commit();
  }

  /** loop the rseg free list */
  node_addr = flst_get_first(rseg_hdr + TXN_RSEG_FREE_LIST, mtr);

  while (node_addr.page != FIL_NULL) {
    Undo_hdr hdr = {space_id, node_addr.page};
    txn_undo_hdr_hash_insert(hdr);

    temp_mtr.start();
    undo_page = trx_undo_page_get_s_latched(page_id_t(space_id, node_addr.page),
                                            rseg->page_size, &temp_mtr);

    node_addr = flst_get_next_addr(undo_page + node_addr.boffset, &temp_mtr);

    temp_mtr.commit();
  }
}

Undo_logs::Undo_logs() { mutex_create(LATCH_ID_UNDO_HDR_HASH, &m_mutex); }

Undo_logs::~Undo_logs() { mutex_free(&m_mutex); }

bool Undo_logs::insert(Undo_hdr hdr) {
  Undo_hdr_key key(hdr.space_id, hdr.page_no);
  mutex_enter(&m_mutex);
  auto it = m_hash.insert(std::pair<Undo_hdr_key, bool>(key, true));
  mutex_exit(&m_mutex);

  return it.second;
}

bool Undo_logs::exist(Undo_hdr hdr) {
  bool exist;
  Undo_hdr_key key(hdr.space_id, hdr.page_no);

  mutex_enter(&m_mutex);
  auto it = m_hash.find(key);
  if (it == m_hash.end()) {
    exist = false;
    lizard_stats.txn_undo_log_hash_miss.inc();
  } else {
    exist = true;
    lizard_stats.txn_undo_log_hash_hit.inc();
  }
  mutex_exit(&m_mutex);

  return exist;
}

/**
  Write redo log when updating scn and uba fileds in physical records.
  @param[in]      rec        physical record
  @param[in]      index      dict that interprets the row record
  @param[in]      txn_rec    txn info from the record
  @param[in]      mtr        mtr
*/
void btr_cur_upd_lizard_fields_clust_rec_log(const rec_t *rec,
                                             const dict_index_t *index,
                                             const txn_rec_t *txn_rec,
                                             mtr_t *mtr) {
  byte *log_ptr = nullptr;

  ut_ad(!!page_rec_is_comp(rec) == dict_table_is_comp(index->table));
  ut_ad(index->is_clustered());
  ut_ad(mtr);

  if (!mlog_open_and_write_index(mtr, rec, index, MLOG_REC_CLUST_LIZARD_UPDATE,
                                 1 + 1 + DATA_SCN_ID_LEN + DATA_UNDO_PTR_LEN +
                                     DATA_GCN_ID_LEN + 14 + 2,
                                 log_ptr)) {
    return;
  }

  log_ptr = row_upd_write_lizard_vals_to_log(index, txn_rec, log_ptr, mtr);

  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

/**
  Parse the txn info from redo log record, and apply it if necessary.
  @param[in]      ptr        buffer
  @param[in]      end        buffer end
  @param[in]      page       page (NULL if it's just get the length)
  @param[in]      page_zip   compressed page, or NULL
  @param[in]      index      index corresponding to page

  @return         return the end of log record or NULL
*/
byte *btr_cur_parse_lizard_fields_upd_clust_rec(byte *ptr, byte *end_ptr,
                                                page_t *page,
                                                page_zip_des_t *page_zip,
                                                const dict_index_t *index) {
  ulint pos;
  scn_t scn;
  undo_ptr_t undo_ptr;
  gcn_t gcn;
  ulint rec_offset;
  rec_t *rec;

  ut_ad(!page || !!page_is_comp(page) == dict_table_is_comp(index->table));

  ptr = row_upd_parse_lizard_vals(ptr, end_ptr, &pos, &scn, &undo_ptr, &gcn);

  if (ptr == nullptr) {
    return nullptr;
  }

  /** 2 bytes offset */
  if (end_ptr < ptr + 2) {
    return (nullptr);
  }

  rec_offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(rec_offset <= UNIV_PAGE_SIZE);

  if (page) {
    mem_heap_t *heap = nullptr;
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    ulint *offsets = nullptr;
    rec = page + rec_offset;

    rec_offs_init(offsets_);

    offsets = rec_get_offsets(rec, index, nullptr, ULINT_UNDEFINED,
                              UT_LOCATION_HERE, &heap);

    if (page_is_comp(page) || rec_offs_comp(offsets)) {
      assert_lizard_dict_index_check_no_check_table(index);
    } else {
      /** If it's non-compact, the info of index will not be written in redo
      log, but it can be self-explanatory because there is a offsets array in
      physical record. See the function **rec_get_offsets** */
      lizard_ut_ad(index && index->table && !index->table->col_names);
      lizard_ut_ad(!rec_offs_comp(offsets));
    }

    row_upd_rec_lizard_fields_in_recovery(rec, page_zip, index, pos, offsets,
                                          scn, undo_ptr, gcn);

    if (UNIV_LIKELY_NULL(heap)) mem_heap_free(heap);
  }

  return ptr;
}

/*----------------------------------------------------------------*/
/* Lizard cleanout structure and function. */
/*----------------------------------------------------------------*/

/** Whether disable the delayed cleanout when read */
bool opt_cleanout_disable = false;

/** Commit cleanout profiles */
ulint commit_cleanout_max_rows = COMMIT_CLEANOUT_DEFAULT_ROWS;

/** Lizard max scan record count once cleanout one page.*/
ulint cleanout_max_scans_on_page = 0;

/** Lizard max clean record count once cleanout one page.*/
ulint cleanout_max_cleans_on_page = 1;

/** Lizard cleanout mode, default(cursor) */
ulong cleanout_mode = CLEANOUT_BY_CURSOR;

Page::Page(const page_id_t &page_id, const dict_index_t *index)
    : m_page_id(page_id) {
  ut_ad(index->is_clustered());
  ut_ad(!index->table->is_intrinsic());
  m_index = index;
}

Page &Page::operator=(const Page &page) {
  if (this != &page) {
    m_page_id = page.m_page_id;
    m_index = page.m_index;
  }
  return *this;
}

Page::Page(const Page &page) : m_page_id(page.m_page_id) {
  if (this != &page) {
    m_index = page.m_index;
  }
}

/**
  Add the committed trx into hash map.
  @param[in]    trx_id
  @param[in]    trx_commit

  @retval       true        Add success
  @retval       false       Add failure
*/
bool Cleanout_pages::push_trx(trx_id_t trx_id, txn_commit_t txn_commit) {
  auto it =
      m_txns.insert(std::pair<trx_id_t, txn_commit_t>(trx_id, txn_commit));

  if (it.second) m_txn_num++;
  return it.second;
}

/**
  Put the page that needed to cleanout into vector.

  @param[in]      page_id
  @param[in]      index
*/
void Cleanout_pages::push_page(const page_id_t &page_id,
                               const dict_index_t *index) {
  Page page(page_id, index);
  if (m_page_num == 0 || (m_page_num > 0 && !(page == m_last_page))) {
    m_pages.push_back(page);
    m_page_num++;
    m_last_page = page;
  }
}

bool Cleanout_pages::is_empty() {
  if (m_page_num == 0 && m_txn_num == 0) return true;

  return false;
}

void Cleanout_pages::init() {
  if (m_page_num > 0) {
    m_pages.clear();
    m_page_num = 0;
  }
  if (m_txn_num > 0) {
    m_txns.clear();
    m_txn_num = 0;
  }
}

Cleanout_pages::~Cleanout_pages() {
  m_pages.clear();
  m_txns.clear();
  m_page_num = 0;
  m_txn_num = 0;
}

/*----------------------------------------------------------------*/
/* Lizard cleanout by cursor. */
/*----------------------------------------------------------------*/

Cursor::Cursor(const Cursor &cursor) : m_page_id(cursor.page_id()) {
  ut_ad(cursor.stored());

  if (this != &cursor) {
    m_old_stored = cursor.m_old_stored;
    m_old_rec = cursor.m_old_rec;
    m_block = cursor.m_block;
    m_index = cursor.m_index;
    m_modify_clock = cursor.m_modify_clock;
    m_block_when_stored = cursor.m_block_when_stored;
    m_page_id.reset(cursor.m_page_id.space(), cursor.m_page_id.page_no());
    m_is_used_by_tcn = cursor.m_is_used_by_tcn;
    m_txns = cursor.m_txns;
  }
}

Cursor &Cursor::operator=(const Cursor &cursor) {
  if (this != &cursor) {
    m_old_stored = cursor.m_old_stored;
    m_old_rec = cursor.m_old_rec;
    m_block = cursor.m_block;
    m_index = cursor.m_index;
    m_modify_clock = cursor.m_modify_clock;
    m_block_when_stored = cursor.m_block_when_stored;
    m_page_id.reset(cursor.m_page_id.space(), cursor.m_page_id.page_no());
    m_is_used_by_tcn = cursor.m_is_used_by_tcn;
    m_txns = cursor.m_txns;
  }
  return *this;
}

bool Cursor::store_position(btr_pcur_t *pcur) {
  ut_ad(pcur);
  m_block = pcur->get_block();
  m_index = pcur->get_btr_cur()->index;
  m_old_rec = page_cur_get_rec(pcur->get_page_cur());
  m_page_id.reset(m_block->page.id.space(), m_block->page.id.page_no());

  ut_d(auto page = page_align(m_old_rec));

  ut_ad(!page_is_empty(page) && page_is_leaf(page));

  /* Function try to check if block is S/X latch. */
  m_modify_clock = m_block->get_modify_clock(
      IF_DEBUG(fsp_is_system_temporary(m_block->page.id.space())));

  m_block_when_stored.store(m_block);

  m_old_stored = true;
  return true;
}

bool Cursor::store_position(dict_index_t *index, buf_block_t *block,
                            rec_t *rec) {
  ut_ad(index && block && rec);

  m_index = index;
  m_block = block;
  m_old_rec = rec;
  m_page_id.reset(m_block->page.id.space(), m_block->page.id.page_no());

#ifdef UNIV_DEBUG
  auto page = page_align(m_old_rec);
  ut_ad(!page_is_empty(page) && page_is_leaf(page));
  ut_ad(!m_block->page.file_page_was_freed);
  ut_ad(index->is_clustered());
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

/**
  Add the committed trx into hash map.
  @param[in]    trx_id
  @param[in]    trx_commit

  @retval       true        Add success
  @retval       false       Add failure
*/
bool Cleanout_cursors::push_trx(trx_id_t trx_id, txn_commit_t txn_commit) {
  auto it =
      m_txns.insert(std::pair<trx_id_t, txn_commit_t>(trx_id, txn_commit));

  if (it.second) m_txn_num++;
  return it.second;
}

/**
  Put the cursor that needed to cleanout into vector.

  @param[in]      page_id
  @param[in]      index
*/
void Cleanout_cursors::push_cursor(const Cursor &cursor) {
  m_cursors.push_back(cursor);
  m_cursor_num++;
}

/**
  Put the cursor that needed to cleanout into vector.

  @param[in]      page_id
  @param[in]      index
*/
void Cleanout_cursors::push_cursor_by_page(const Cursor &cursor,
                                           trx_id_t trx_id,
                                           txn_commit_t txn_commit) {
  ut_ad(cursor.used_by_tcn());
  if (m_cursors.size() > 0 &&
      m_cursors.back().page_id() == (cursor.page_id())) {
    m_cursors.back().push_back(trx_id, txn_commit);
  } else {
    /** Attention Txn_commits copy */
    m_cursors.push_back(cursor);
    m_cursors.back().push_back(trx_id, txn_commit);
    m_cursor_num++;
  }
}

bool Cleanout_cursors::is_empty() {
  if (m_cursor_num == 0 && m_txn_num == 0) return true;

  return false;
}

void Cleanout_cursors::init() {
  if (m_cursor_num > 0) {
    m_cursors.clear();
    m_cursor_num = 0;
  }
  if (m_txn_num > 0) {
    m_txns.clear();
    m_txn_num = 0;
  }
}

Cleanout_cursors::~Cleanout_cursors() {
  m_cursors.clear();
  m_txns.clear();
  m_cursor_num = 0;
  m_txn_num = 0;
}

}  // namespace lizard
