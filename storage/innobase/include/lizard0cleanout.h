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

#ifndef lizard0cleanout_h
#define lizard0cleanout_h

#include "buf0block_hint.h"
#include "buf0types.h"
#include "fil0fil.h"
#include "rem0types.h"
#include "trx0types.h"

#include "lizard0mon.h"
#include "lizard0rem0types.h"
#include "lizard0txn0rec0types.h"

struct mtr_t;
struct dict_index_t;
struct page_zip_des_t;
struct txn_rec_t;
struct btr_pcur_t;
struct btr_cur_t;
struct que_thr_t;
struct row_prebuilt_t;

namespace lizard {
/*----------------------------------------------------------------*/
/* Lizard cleanout structure and function. */
/*----------------------------------------------------------------*/
/* Revision:
 *
 * We take a new strategy to do cleanout when opt_cleanout_write_redo
 * setting is false.
 *
 * 1. try see
 *	MTR_NONE
 *
 * 2. real see
 * 	MTR_NO_REDO
 *
 * 3. modify
 * 	MTR_NONE:
 *
 * 4. ddl
 * 	MTR_NO_REDO
 *
 * 5. commit
 * 	MTR_NONE
 *
 * 6. gpp
 * 	MTR_NO_REDO
 *
 * Make dirty through MTR_NO_REDO if page cleanouts has been more than
 * srv_cleanout_dirty_threshold and was still clean page.
 */

/** Whether to write redo log when cleanout */
extern bool opt_cleanout_write_redo;

/** Whether disable the delayed cleanout when read */
extern bool opt_txn_cleanout_disable;

/** Whether disable the gpp cleanout when read */
extern bool opt_gpp_cleanout_disable;

/** Whether disable the ddl cleanout when read */
extern bool opt_ddl_cleanout_disable;

/** Whether disable the commit cleanout when dml. */
extern bool opt_commit_cleanout_disable;

/** Whether disable the parallel cleanout when count star */
extern bool opt_count_star_cleanout_disable;

/* Commit cleanout max num. */
extern ulint srv_commit_cleanout_max_rows;

/** Make page dirty if cleaned records are more than threshold and page was
 * still clean */
extern ulint srv_cleanout_dirty_threshold;

/** Query cleanout level */
extern uint32_t srv_query_cleanout_level;

/** Query cleanout level */
enum query_cleanout_level_t {
  /** Ignore cleanout if try see ok. */
  QC_NONE = 0,
  /** Try to cleanout if tcn hit, avoid by lookuping txn slot.*/
  QC_NO_SLOT = 1,
  /** skip try see and make sure cleanout and generate full redo log */
  QC_ALL = 2
};

typedef enum query_cleanout_level_t qc_level_t;
/*----------------------------------------------------------------*/
/* Cleanout by cursor.
 *
 * We defined different cursor about what to cleanout.
----------------------------------------------------------------*/
class Cursor {
 public:
  explicit Cursor()
      : m_old_stored(false),
        m_old_reg(),
        m_block(nullptr),
        m_index(nullptr),
        m_modify_clock(0),
        m_block_when_stored(),
        m_log_mode(MTR_LOG_ALL),
        m_restored(false) {
    m_block_when_stored.clear();
  }

  virtual ~Cursor() { reset(); }

  Cursor(const Cursor &other);

  Cursor &operator=(const Cursor &);

  /** Store the record position.
   *
   * @param[in]		cursor
   * @param[in]		mtr log mode
   *
   * @retval		true	successful */
  bool store_position(const btr_cur_t *bcur, mtr_log_t log_mode);

  bool restore_position(mtr_t *mtr, ut::Location location, ulint n_recs);

  ulint cleanout_txn(reg_t reg, txn_rec_t &txn_rec, txn_layout_t layout,
                     mtr_t *mtr);

  ulint cleanout_gpp(ulint gpp_no, ulint gpp_no_offset, mtr_t *mtr);

  /** inc page cleanouts */
  void inc_cleanouts(mtr_t *mtr);

  virtual ulint cleanout() = 0;

  /* Reset the cursor. */
  void reset() {
    m_old_stored = false;
    m_old_reg = REG_NULL;
    m_block = nullptr;
    m_index = nullptr;
    m_modify_clock = 0;
    m_block_when_stored.clear();
    m_log_mode = MTR_LOG_ALL;
    m_restored = false;
  }

  bool stored() const { return m_old_stored; }

  buf_block_t *get_block() const { return m_block; }
  dict_index_t *get_index() const { return m_index; }
  reg_t get_old_reg() const { return m_old_reg; }

 private:
  void set_log_mode(mtr_t *mtr, ulint n_recs);

 protected:
  bool m_old_stored;

  reg_t m_old_reg;

  buf_block_t *m_block;

  dict_index_t *m_index;

  uint64_t m_modify_clock;

  buf::Block_hint m_block_when_stored;

  mtr_log_t m_log_mode;

  bool m_restored;
};

/*----------------------------------------------------------------*/
/* TCursor extends Cursor for indexes w/ transactional fields cleanout.  */
/*----------------------------------------------------------------*/
class TCursor : public Cursor {
 public:
  explicit TCursor()
      : Cursor(), m_txn_rec(), m_layout(TL_NONE), m_stored(false) {}

  TCursor(const TCursor &other)
      : Cursor(other),
        m_txn_rec(other.m_txn_rec),
        m_layout(other.m_layout),
        m_stored(other.m_stored) {}

  TCursor &operator=(const TCursor &other) {
    if (this != &other) {
      Cursor::operator=(other);
      m_txn_rec = other.m_txn_rec;
      m_layout = other.m_layout;
      m_stored = other.m_stored;
    }
    return (*this);
  }

  void commit(const txn_rec_t &txn_rec) {
    ut_ad(txn_rec.is_committed());
    ut_ad(m_old_stored == true);

    m_txn_rec = txn_rec;
    m_stored = true;
  }

  /** Store position and txn info.
   *
   * @retval	true	if successful
   * */
  bool store(const btr_cur_t *bcur, const txn_rec_t &txn_rec,
             const txn_layout_t &layout, mtr_log_t log_mode) {
    ut_ad(txn_rec.is_committed());

    m_txn_rec = txn_rec;
    m_layout = layout;
    m_stored = true;
    return store_position(bcur, log_mode);
  }

  /** Store position and layout
   *
   * @retval	true	if successful
   * */
  bool store(const btr_cur_t *bcur, const txn_layout_t &layout,
             mtr_log_t log_mode) {
    m_layout = layout;
    return store_position(bcur, log_mode);
  }

  /** Cleanout txn field in record, include Clover or Bamboo layout
   *
   * @retval	how many records were cleanouted. */
  virtual ulint cleanout() override;

  void reset() {
    clear();
    Cursor::reset();
  }

  bool is_on_same_page(const btr_cur_t *bcur) const;

  ~TCursor() { clear(); }

  txn_layout_t get_layout() const { return m_layout; }

  bool is_txn_rec_committed() const { return m_txn_rec.is_committed(); }

 private:
  void clear() {
    m_txn_rec.reset();
    m_layout = TL_NONE;
    m_stored = false;
  }

 private:
  txn_rec_t m_txn_rec;

  txn_layout_t m_layout;

  bool m_stored;
};

/** TCodex has a TCursor, and stores extra records. */
class TCodex {
 private:
  /** How many recs on same page can be saved to cleanout after scan. */
  constexpr static size_t MAX_SCAN_REC_NUM = 5;

 public:
  explicit TCodex() : m_cursor(), m_n_recs(0) {}
  TCodex(const TCodex &other)
      : m_cursor(other.m_cursor), m_n_recs(other.m_n_recs) {
    std::copy(other.m_recs, other.m_recs + MAX_SCAN_REC_NUM, m_recs);
  }

  TCodex &operator=(const TCodex &other) {
    if (this != &other) {
      m_cursor = other.m_cursor;
      m_n_recs = other.m_n_recs;
      std::copy(other.m_recs, other.m_recs + MAX_SCAN_REC_NUM, m_recs);
    }
    return (*this);
  }

  ulint cleanout();

  bool is_on_same_page(const btr_cur_t *bcur) const {
    return m_cursor.is_on_same_page(bcur);
  }

  /** Clean records and reset the cursor. */
  void reset() {
    m_cursor.reset();
    m_n_recs = 0;
  }

  /** Store position, layout and extra record
   *
   * @retval	true	if successful
   * */
  bool store(const btr_cur_t *bcur, const txn_layout_t &layout,
             mtr_log_t log_mode, reg_t reg, txn_rec_t txn_rec) {
    if (m_n_recs >= MAX_SCAN_REC_NUM) return false;
    m_recs[m_n_recs].old_reg = reg;
    m_recs[m_n_recs].old_txn_rec = txn_rec;
    m_n_recs++;
    return m_cursor.store(bcur, txn_rec, layout, log_mode);
  }
  /** Store extra record
   *
   * @retval	true	if successful
   * */
  bool store(reg_t reg, txn_rec_t txn_rec) {
    if (m_n_recs >= MAX_SCAN_REC_NUM) return false;
    m_recs[m_n_recs].old_reg = reg;
    m_recs[m_n_recs].old_txn_rec = txn_rec;
    m_n_recs++;
    return true;
  }

 private:
  /** Cleanout Clover layout txn record.
   *
   * @retval	How many record was cleanouted.*/
  ulint cleanout_clover_rec();
  /** Cleanout Bamboo layout txn record.
   *
   * @retval	How many record was cleanouted.*/
  ulint cleanout_bamboo_rec();

  TCursor m_cursor;
  struct extra_rec_t {
    reg_t old_reg;
    txn_rec_t old_txn_rec;
  };
  /** Extra records stored in same page as current cursor for cleanout. */
  extra_rec_t m_recs[MAX_SCAN_REC_NUM];
  /** How many extra records stored in same page as current cursor for cleanout.
   */
  ulint m_n_recs;
};

/*----------------------------------------------------------------*/
/* GCursor extends Cursor for gpp cleanout. */
/*----------------------------------------------------------------*/

class GCursor : public Cursor {
 public:
  explicit GCursor()
      : Cursor(),
        m_gpp_no(FIL_NULL),
        m_gpp_no_offset(ULINT_UNDEFINED),
        m_stored(false) {}

  GCursor(const GCursor &other)
      : Cursor(other),
        m_gpp_no(other.m_gpp_no),
        m_gpp_no_offset(other.m_gpp_no_offset),
        m_stored(other.m_stored) {}

  GCursor &operator=(const GCursor &other) {
    if (this != &other) {
      Cursor::operator=(other);
      m_gpp_no = other.m_gpp_no;
      m_gpp_no_offset = other.m_gpp_no_offset;
      m_stored = other.m_stored;
    }
    return (*this);
  }

  /** Store position and save gpp no offset.
   *
   * @retval	true	if successful */
  bool store(const btr_cur_t *bcur, const ulint &gpp_no_offset,
             mtr_log_t log_mode) {
    ut_a(gpp_no_offset != ULINT_UNDEFINED);
    m_gpp_no_offset = gpp_no_offset;
    return store_position(bcur, log_mode);
  }

  virtual ulint cleanout() override;

  void reset() {
    clear();
    Cursor::reset();
  }

  void set_gpp_no(const page_no_t &gpp_no) {
    ut_ad(m_gpp_no_offset != ULINT_UNDEFINED && gpp_no != FIL_NULL);
    m_gpp_no = gpp_no;
    m_stored = true;
  }

  ~GCursor() { clear(); }

 private:
  void clear() {
    m_gpp_no = FIL_NULL;
    m_gpp_no_offset = ULINT_UNDEFINED;
    m_stored = false;
  }

 private:
  /* Primary key page_no for gpp backfill. */
  page_no_t m_gpp_no;

  /* Offset of gpp_no in record. */
  ulint m_gpp_no_offset;

  bool m_stored;
};

/*----------------------------------------------------------------*/
/* Cleanout interface */
/*----------------------------------------------------------------*/

class Cleanout {
 public:
  Cleanout() {}

  virtual ~Cleanout() {}

  /** Execute cleanout work. */
  virtual void execute() = 0;

  /** Collect txn record for cleanout.
   *
   * @param[in]		btree cursor
   * @param[in]		commit info
   * @param[in]		rec layout
   *
   * @retval		txn cursor for cleanout.
   * */
  virtual void collect_txn(const btr_cur_t *bcur, const txn_rec_t &txn_rec,
                               const txn_layout_t &layout,
                               mtr_log_t log_mode) = 0;

  /** Collect gpp record for cleanout.
   *
   * @param[in]		btree cursor
   * @param[in]		gpp no offset within record
   *
   * @retval		gpp cursor for cleanout.
   * */
  virtual GCursor *collect_gpp(const btr_cur_t *btr, const ulint gpp_no_offset,
                               mtr_log_t log_mode) = 0;
};

/*------------------------------------------------------------------------*/
/* Scan_cleanout extends Cleanout for lizard and gpp backfill cleanout.   */
/*------------------------------------------------------------------------*/
class Scan_cleanout : public Cleanout {
 private:
  /** How many cursors can be saved to cleanout for txn after scan. */
  constexpr static size_t MAX_TXN_CURSORS = 2;
  /** How many cursors can be saved to cleanout for gpp after scan. */
  constexpr static size_t MAX_GPP_CURSORS = 3;

 public:
  explicit Scan_cleanout()
      : Cleanout(),
        m_txn_cursors(),
        m_txn_num(0),
        m_gpp_cursors(),
        m_gpp_num(0) {}

  virtual ~Scan_cleanout() { clear(); }

  /** Collect txn record for cleanout.
   *
   * @param[in]		btree cursor
   * @param[in]		commit info
   * @param[in]		rec layout
   *
   * @retval		txn cursor for cleanout.
   * */
  virtual void collect_txn(const btr_cur_t *bcur, const txn_rec_t &txn_rec,
                               const txn_layout_t &layout,
                               mtr_log_t log_mode) override;

  /** Collect gpp record for cleanout.
   *
   * @param[in]		btree cursor
   * @param[in]		gpp no offset within record
   *
   * @retval		gpp cursor for cleanout.
   * */
  virtual GCursor *collect_gpp(const btr_cur_t *btr, const ulint gpp_no_offset,
                               mtr_log_t log_mode) override;

  virtual void execute() override {
    ulint cleaned = 0;
    for (ulint i = 0; i < m_txn_num; i++) {
      cleaned += m_txn_cursors[i].cleanout();
    }
    scan_cleanout_txn_clean_stat(cleaned);

    cleaned = 0;
    for (ulint i = 0; i < m_gpp_num; i++) {
      cleaned += m_gpp_cursors[i].cleanout();
    }
    scan_cleanout_gpp_clean_stat(cleaned);

    clear();
  }

  bool is_empty() const { return m_txn_num == 0 && m_gpp_num == 0; }

  /** Acquire a new codex for txn.
   *
   * @param[in]		bcur
   * @param[in]		txn_rec
   * @param[in]		layout
   * @param[in]		log_mode
   *
   * @retval		cursor or nullptr if unavailable slot
   */
  TCodex *acquire_new_codex(const btr_cur_t *bcur, const txn_layout_t &layout,
                            mtr_log_t log_mode, reg_t reg,
                            const txn_rec_t &txn_rec);

  /** Acquire a txn cursor with extra records and store record position for
   * cleanout.
   *
   * @param[in]		pcursor
   * @param[in]		committed txn rec
   *
   * @retval		cursor or nullptr if disable or unavailable slot */
  TCodex *request_codex(const btr_cur_t *bcur, const txn_layout_t &layout,
                       mtr_log_t log_mode, const txn_rec_t &txn_rec);

  /** Acquire a gpp cursor and store record position for cleanout.
   *
   * @param[in]		pcursor
   * @param[in]		committed txn rec
   *
   * @retval		cursor or nullptr if disable or unavailable slot */
  GCursor *request_gpp(const btr_cur_t *bcur, const ulint gpp_no_offset,
                       mtr_log_t log_mode) {
    GCursor *cur = nullptr;
    if (m_gpp_num < MAX_GPP_CURSORS) {
      m_gpp_cursors[m_gpp_num].reset();
      cur = &m_gpp_cursors[m_gpp_num++];
      cur->store(bcur, gpp_no_offset, log_mode);
    }

    return cur;
  }

  void clear() {
    m_txn_num = 0;
    m_gpp_num = 0;
  }

 private:
  TCodex m_txn_cursors[MAX_TXN_CURSORS];
  ulint m_txn_num;

  GCursor m_gpp_cursors[MAX_GPP_CURSORS];
  ulint m_gpp_num;
};

/*------------------------------------------------------------------------*/
/* Commit_cleanout extends Cleanout for lizard cleanout when commit.      */
/*------------------------------------------------------------------------*/
class Commit_cleanout : public Cleanout {
 public:
  /** How many cursors can be saved to static array after commit. If it
  is greater than STATIC_CURSORS, it will be saved to dynamic array. */
  constexpr static size_t STATIC_CURSORS = 3;

  constexpr static size_t MAX_CURSORS = 4096;

 public:
  explicit Commit_cleanout()
      : Cleanout(),
        m_dynamic_cursors(),
        m_static_num(0),
        m_dynamic_num(0),
        m_txn_rec() {}

  virtual ~Commit_cleanout() { clear(); }

  void commit(const txn_rec_t &txn_rec) { m_txn_rec = txn_rec; }

  /** Collect txn record for cleanout.
   *
   * @param[in]		btree cursor
   * @param[in]		commit info
   * @param[in]		rec layout
   *
   * @retval		txn cursor for cleanout.
   * */
  virtual void collect_txn(const btr_cur_t *bcur, const txn_rec_t &txn_rec,
                               const txn_layout_t &layout,
                               mtr_log_t log_mode) override;

  /** Collect gpp record for cleanout.
   *
   * @param[in]		btree cursor
   * @param[in]		gpp no offset within record
   *
   * @retval		gpp cursor for cleanout.
   * */
  virtual GCursor *collect_gpp(const btr_cur_t *btr, const ulint gpp_no_offset,
                               mtr_log_t log_mode) override {
    /** Commit cleanout didn't support gpp.*/
    ut_ad(0);
    return nullptr;
  }

  virtual void execute() override {
    if (m_static_num == 0 && m_dynamic_num == 0) {
      ut_ad(m_dynamic_cursors.size() == 0);
      return;
    }

    ulint cleaned = 0;

    for (ulint i = 0; i < m_static_num; i++) {
      m_static_cursors[i].commit(m_txn_rec);
      cleaned += m_static_cursors[i].cleanout();
    }

    for (auto &it : m_dynamic_cursors) {
      it.commit(m_txn_rec);
      cleaned += it.cleanout();
    }

    commit_cleanout_clean_stat(cleaned);

    clear();
  }
  ulint count() const { return m_static_num + m_dynamic_num; }
  bool is_empty() const { return count() == 0; }

  /** Acquire a txn cursor and store record position for cleanout.
   *
   * @param[in]		pcursor
   * @param[in]		committed txn rec
   *
   * @retval		cursor or nullptr if disable or unavailable slot */
  TCursor *request_txn(const btr_cur_t *bcur, const txn_layout_t &layout,
                       mtr_log_t log_mode) {
    TCursor *cur = nullptr;
    if (count() < srv_commit_cleanout_max_rows) {
      TCursor tcursor;
      tcursor.store(bcur, layout, log_mode);
      cur = push_back(tcursor);
    }
    return cur;
  }

  void clear() {
    m_dynamic_cursors.clear();
    m_static_num = 0;
    m_dynamic_num = 0;

    m_txn_rec.reset();
  }

 private:
  TCursor *push_back(const TCursor &cursor) {
    TCursor *cur = nullptr;
    if (m_static_num < STATIC_CURSORS) {
      cur = &m_static_cursors[m_static_num++];
      *cur = cursor;
    } else {
      m_dynamic_cursors.push_back(cursor);
      m_dynamic_num++;
      cur = &m_dynamic_cursors.back();
    }

    return cur;
  }

 private:
  TCursor m_static_cursors[STATIC_CURSORS];

  /* If m_static_num extends STATIC_CURSORS, use m_dynamic_cursors. */
  std::vector<TCursor, ut::allocator<TCursor>> m_dynamic_cursors;

  ulint m_static_num;
  ulint m_dynamic_num;

  txn_rec_t m_txn_rec;
};

class Pread_cleanout : public Cleanout {
 private:
  /** How many cursors can be saved to cleanout ddl. */
  constexpr static size_t MAX_CURSORS = 1024;

 public:
  explicit Pread_cleanout()
      : Cleanout(), m_cursor(), m_old_regs(), m_txn_recs(), m_rec_nums{0} {}

  virtual ~Pread_cleanout() override {
    ut_ad(m_rec_nums == 0);
    clear();
  }

  virtual void execute() override;

  /** Collect txn record for cleanout.
   *
   * @param[in]		btree cursor
   * @param[in]		commit info
   * @param[in]		rec layout
   *
   * @retval		txn cursor for cleanout.
   * */
  virtual void collect_txn(const btr_cur_t *bcur, const txn_rec_t &txn_rec,
                               const txn_layout_t &layout,
                               mtr_log_t log_mode) override;

  /** Collect gpp record for cleanout.
   *
   * @param[in]		btree cursor
   * @param[in]		gpp no offset within record
   *
   * @retval		gpp cursor for cleanout.
   * */
  virtual GCursor *collect_gpp(const btr_cur_t *btr, const ulint gpp_no_offset,
                               mtr_log_t log_mode) override {
    ut_ad(0);
    return nullptr;
  }
  
 private:
  void clear() {
    m_rec_nums = 0;
    m_cursor.reset();
  }

 private:
  /** If the cursor can restore, it means that the offsets of other records
   * haven't change. */
  TCursor m_cursor;

  reg_t m_old_regs[MAX_CURSORS];
  txn_rec_t m_txn_recs[MAX_CURSORS];

  ulint m_rec_nums;
};

/** Cleanout runtime context for pcursor. */
struct cleanout_ctx_t {
 public:
  explicit cleanout_ctx_t(trx_t *trx, btr_pcur_t *pcur);

  explicit cleanout_ctx_t(btr_pcur_t *pcur, Cleanout *cleanout);

  explicit cleanout_ctx_t(btr_cur_t *bcur, Cleanout *cleanout);

  /**Setting mtr log mode. */
  cleanout_ctx_t &operator()(const mtr_log_t log_mode) {
    m_qc_level = static_cast<qc_level_t>(srv_query_cleanout_level);

    if (opt_cleanout_write_redo || m_qc_level == QC_ALL) {
      m_log_mode = MTR_LOG_ALL;
    } else {
      m_log_mode = log_mode;
    }

    m_setting = true;

    return *this;
  }

  /** Whether cleanout context is usable. */
  bool is_usable() const { return m_cleanout != nullptr && m_bcur != nullptr; }

  /** Get query cleanout level. */
  qc_level_t qc_level() const {
    ut_ad(m_setting);
    return m_qc_level;
  }

  Cleanout *cleanout() { return m_cleanout; }

  /** Collect txn record for cleanout.
   @param[in]		txn rec
   @param[in]		rec layout */
  void collect_txn(const txn_rec_t &txn_rec, const txn_layout_t &layout) {
    ut_ad(is_usable());
    ut_ad(m_setting);
    m_cleanout->collect_txn(m_bcur, txn_rec, layout, m_log_mode);

    return;
  }
  /** Collect gpp record for cleanout.
   @param[in]		gpp no offset within record */
  void collect_gpp(const ulint gpp_no_offset) {
    ut_ad(is_usable());
    ut_ad(m_setting);
    m_gcursor = m_cleanout->collect_gpp(m_bcur, gpp_no_offset, m_log_mode);

    return;
  }

  /** Address gpp if missing.
   @param[in]	gpp no */
  void address_gpp_if_missing(const gpp_no_t gpp_no) {
    if (m_gcursor) {
      m_gcursor->set_gpp_no(gpp_no);
    }
  }

  btr_cur_t *btr_cur() { return m_bcur; }

  trx_t *trx() { return m_trx; }

 public:
  trx_t *m_trx;
  /** Btree position of record */
  btr_cur_t *m_bcur;
  /** Container which save tcursor or gcursor to cleanout later.*/
  Cleanout *m_cleanout;
  /** Saved gpp cursor. */
  GCursor *m_gcursor;
  /** future cleanout mtr log mode. */
  mtr_log_t m_log_mode;
  /** whether log mode was set. */
  bool m_setting;
  /** query cleanout level. */
  qc_level_t m_qc_level;
};

/**
  Collect rows updated in current transaction.

  @param[in]        thr             current session
  @param[in]        cursor          btr cursor
  @param[in]        rec             current rec
  @param[in]        flags           mode flags for btr_cur operations
*/
extern void commit_cleanout_collect(que_thr_t *thr, btr_cur_t *cursor,
                                    rec_t *rec, ulint flags,
                                    const txn_layout_t &layout);
/**
  After search row complete, do the cleanout.

  @param[in]      prebuilt

  @retval         count       cleaned records count
*/
extern void cleanout_after_read(row_prebuilt_t *prebuilt);
/**
  Cleanout rows at transaction commit.
*/
extern void cleanout_after_commit(trx_t *trx, bool serialised);

}  // namespace lizard

using Cleanout_ctx_t = lizard::cleanout_ctx_t;

#endif
