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
#include "lizard0mon.h"
#include "page0types.h"
#include "rem0types.h"
#include "trx0types.h"
#include "ut0mutex.h"

#include "lizard0txn0rec0types.h"
#include "lizard0ut.h"

struct mtr_t;
struct dict_index_t;
struct page_zip_des_t;
struct txn_rec_t;
struct btr_pcur_t;

namespace lizard {

/*----------------------------------------------------------------*/
/* Lizard cleanout structure and function. */
/*----------------------------------------------------------------*/
/** Whether to write redo log when cleanout */
extern bool opt_cleanout_write_redo;

/** Whether disable the delayed cleanout when read */
extern bool opt_cleanout_disable;

/** Whether disable the gpp cleanout when read */
extern bool opt_gpp_cleanout_disable;

/** Whether disable the ddl cleanout when read */
extern bool opt_ddl_cleanout_disable;

/* Commit cleanout max num. */
extern ulint srv_commit_cleanout_max_rows;

/*----------------------------------------------------------------*/
/* Lizard cleanout by cursor. */
/*----------------------------------------------------------------*/
class Cursor {
 public:
  explicit Cursor()
      : m_old_stored(false),
        m_old_rec(nullptr),
        m_block(nullptr),
        m_index(nullptr),
        m_modify_clock(0),
        m_block_when_stored() {
    m_block_when_stored.clear();
  }

  Cursor(const Cursor &other);

  Cursor &operator=(const Cursor &);

  bool store_position(btr_pcur_t *pcur);

  bool store_position(dict_index_t *index, buf_block_t *block, rec_t *rec);

  bool restore_position(mtr_t *mtr, ut::Location location);

  virtual ulint cleanout() = 0;

  /* Reset the cursor. */
  virtual void reset() {
    m_old_stored = false;
    m_old_rec = nullptr;
    m_block = nullptr;
    m_index = nullptr;
    m_modify_clock = 0;
    m_block_when_stored.clear();
  }
  bool stored() const { return m_old_stored; }

  virtual ~Cursor() { reset(); }

 protected:
  bool m_old_stored;

  rec_t *m_old_rec;

  buf_block_t *m_block;

  dict_index_t *m_index;

  uint64_t m_modify_clock;

  buf::Block_hint m_block_when_stored;
};

/*----------------------------------------------------------------*/
/* CCursor extends Cursor for lizard primary key cleanout.  */
/*----------------------------------------------------------------*/
class CCursor : public Cursor {
 public:
  explicit CCursor() : Cursor(), m_txn_rec(), m_stored(false) {}

  CCursor(const CCursor &other)
      : Cursor(other), m_txn_rec(other.m_txn_rec), m_stored(other.m_stored) {}

  CCursor &operator=(const CCursor &other) {
    if (this != &other) {
      Cursor::operator=(other);
      m_txn_rec = other.m_txn_rec;
      m_stored = other.m_stored;
    }
    return (*this);
  }

  void set_txn_rec(const txn_rec_t &txn_rec) {
    ut_ad(txn_rec.is_committed());
    ut_ad(m_old_stored == true);

    m_txn_rec = txn_rec;
    m_stored = true;
  }

  bool store(btr_pcur_t *pcur, const txn_rec_t &txn_rec) {
    ut_ad(txn_rec.is_committed());

    m_txn_rec = txn_rec;
    m_stored = true;
    return store_position(pcur);
  }

  bool store(dict_index_t *index, buf_block_t *block, rec_t *rec,
             const txn_rec_t &txn_rec) {
    ut_ad(m_txn_rec.is_committed());

    m_txn_rec = txn_rec;
    m_stored = true;
    return store_position(index, block, rec);
  }

  ulint cleanout() override;

  virtual void reset() override {
    m_txn_rec.reset();
    m_stored = false;
    Cursor::reset();
  }

  bool is_on_same_page(btr_pcur_t *pcur) const;

  ~CCursor() { reset(); }

  buf_block_t *get_block() const { return m_block; }
  dict_index_t *get_index() const { return m_index; }

  rec_t* get_old_rec() const { return m_old_rec; }

 private:
  txn_rec_t m_txn_rec;
  bool m_stored;
};

/*----------------------------------------------------------------*/
/* SCursor extends Cursor for gpp secondary key cleanout. */
/*----------------------------------------------------------------*/

class SCursor : public Cursor {
 public:
  explicit SCursor() : Cursor(), m_gpp_no(FIL_NULL), m_gpp_no_offset(0) {}

  SCursor(const SCursor &other)
      : Cursor(other),
        m_gpp_no(other.m_gpp_no),
        m_gpp_no_offset(other.m_gpp_no_offset),
        m_stored(other.m_stored) {}

  SCursor &operator=(const SCursor &other) {
    if (this != &other) {
      Cursor::operator=(other);
      m_gpp_no = other.m_gpp_no;
      m_gpp_no_offset = other.m_gpp_no_offset;
      m_stored = other.m_stored;
    }
    return (*this);
  }

  bool store(btr_pcur_t *pcur, const ulint &gpp_no_offset) {
    m_gpp_no_offset = gpp_no_offset;
    return store_position(pcur);
  }

  ulint cleanout() override;

  virtual void reset() override {
    m_gpp_no = FIL_NULL;
    m_gpp_no_offset = 0;
    m_stored = false;

    Cursor::reset();
  }

  void set_gpp_no(const page_no_t &gpp_no) {
    ut_ad(m_gpp_no_offset != 0 && gpp_no != FIL_NULL);
    m_gpp_no = gpp_no;
    m_stored = true;
  }

  ~SCursor() { reset(); }

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

  virtual void clear() = 0;

  /** Execute cleanout work. */
  virtual void execute() = 0;

  /** Collect record */
  virtual void collect(const trx_id_t trx_id, const txn_rec_t &txn_rec,
                       const rec_t *rec, const dict_index_t *index,
                       const ulint *offsets, btr_pcur_t *pcur) = 0;
};

/** Cleanout runtime context for pcursor. */
struct cleanout_ctx_t {
 public:
  explicit cleanout_ctx_t(btr_pcur_t *pcur);

  explicit cleanout_ctx_t(btr_pcur_t *pcur, Cleanout *cleanout)
      : m_pcur(pcur), m_cleanout(cleanout) {}

  bool is_active() const { return m_cleanout != nullptr && m_pcur != nullptr; }

  Cleanout *cleanout() { return m_cleanout; }

  btr_pcur_t *pcur() { return m_pcur; }

 public:
  btr_pcur_t *m_pcur;
  Cleanout *m_cleanout;
};

/*------------------------------------------------------------------------*/
/* Scan_cleanout extends Cleanout for lizard and gpp backfill cleanout.   */
/*------------------------------------------------------------------------*/
class Scan_cleanout : public Cleanout {
 private:
  /** How many cursors can be saved to cleanout after scan. */
  constexpr static size_t MAX_CURSORS = 3;

 public:


  explicit Scan_cleanout()
      : Cleanout(),
        m_clust_cursors(),
        m_clust_num(0),
        m_sec_cursors(),
        m_sec_num(0) {}

  virtual ~Scan_cleanout() { clear(); }

  virtual void collect(const trx_id_t trx_id, const txn_rec_t &txn_rec,
                       const rec_t *rec, const dict_index_t *index,
                       const ulint *offsets, btr_pcur_t *pcur) override;

  virtual void execute() override {
    ulint cleaned = 0;
    for (ulint i = 0; i < m_clust_num; i++) {
      cleaned += m_clust_cursors[i].cleanout();
    }
    if (cleaned > 0) lizard_stats.scan_cleanout_clust_clean.add(cleaned);

    cleaned = 0;
    for (ulint i = 0; i < m_sec_num; i++) {
      cleaned += m_sec_cursors[i].cleanout();
    }
    if (cleaned > 0) lizard_stats.scan_cleanout_sec_clean.add(cleaned);

    clear();
  }

  virtual void clear() override {
    m_clust_num = 0;
    m_sec_num = 0;
  }

  bool is_empty() const { return m_clust_num == 0 && m_sec_num == 0; }

  /** Acquire a secondary cursor and store record position for gpp cleanout.
   *
   * @param[in]		pcursor
   * @param[in]		gpp no offset within sec record.
   *
   * @retval		cursor or nullptr if disable or unavailable slot */
  SCursor *acquire_for_gpp(btr_pcur_t *pcur, ulint gpp_no_offset) {
    SCursor *cur =nullptr;
    if (opt_gpp_cleanout_disable) {
      return nullptr;
    }
    if ((cur = acquire_sec()) != nullptr) {
      cur->store(pcur, gpp_no_offset);
      lizard_stats.cleanout_sec_collect.inc();
    }
    return cur;
  }

  /** Acquire a primary cursor and store record position for lizard cleanout.
   *
   * @param[in]		pcursor
   * @param[in]		committed txn rec
   *
   * @retval		cursor or nullptr if disable or unavailable slot */
  CCursor *acquire_for_lizard(btr_pcur_t *pcur, const txn_rec_t &txn_rec) {
    CCursor *cur = nullptr;
    if ((cur = acquire_clust()) != nullptr) {
      cur->store(pcur, txn_rec);
      lizard_stats.cleanout_clust_collect.inc();
    }
    return cur;
  }

 private:
  /** Acquire a secondary cursor for cleanout.
   *
   * @retval		cursor or nullptr if disable or unavailable slot */
  SCursor *acquire_sec() {
    if (m_sec_num < MAX_CURSORS) {
      m_sec_cursors[m_sec_num].reset();
      return &m_sec_cursors[m_sec_num++];
    }
    return nullptr;
  }
  /** Acquire a primary cursor for cleanout.
   *
   * @retval		cursor or nullptr if disable or unavailable slot */
  CCursor *acquire_clust() {
    if (m_clust_num < MAX_CURSORS) {
      m_clust_cursors[m_clust_num].reset();
      return &m_clust_cursors[m_clust_num++];
    }
    return nullptr;
  }

 private:
  CCursor m_clust_cursors[MAX_CURSORS];
  ulint m_clust_num;

  SCursor m_sec_cursors[MAX_CURSORS];
  ulint m_sec_num;
};

/*------------------------------------------------------------------------*/
/* Commit_cleanout extends Cleanout for lizard cleanout when commit.      */
/*------------------------------------------------------------------------*/
class Commit_cleanout : public Cleanout {
 public:
  /** How many cursors can be saved to static array after commit. If it
  is greater than MAX_CURSORS, it will be saved to dynamic array. */
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

  virtual void clear() override {
    m_dynamic_cursors.clear();
    m_static_num = 0;
    m_dynamic_num = 0;

    m_txn_rec.reset();
  }

  void set_commit(const txn_rec_t &txn_rec) { m_txn_rec = txn_rec; }


  /** Collect record */
  virtual void collect(const trx_id_t trx_id, const txn_rec_t &txn_rec,
                       const rec_t *rec, const dict_index_t *index,
                       const ulint *offsets, btr_pcur_t *pcur) override {
    ut_a(0);
  }

  virtual void execute() override {
    if (m_static_num == 0 && m_dynamic_num == 0) {
      ut_ad(m_dynamic_cursors.size() == 0);
      return;
    }

    ulint cleaned = 0;

    for (ulint i = 0; i < m_static_num; i++) {
      m_static_cursors[i].set_txn_rec(m_txn_rec);
      cleaned += m_static_cursors[i].cleanout();
    }

    for (auto &it : m_dynamic_cursors) {
      it.set_txn_rec(m_txn_rec);
      cleaned += it.cleanout();
    }
    if (stat_enabled) {
      lizard_stats.commit_cleanout_collects.add(count());
      lizard_stats.commit_cleanout_cleaned.add(cleaned);
    }

    clear();
  }
  ulint count() const { return m_static_num + m_dynamic_num; }
  bool is_empty() const { return count() == 0; }

  /** Push cusor into commit lizard cleanout and store record position.
   *
   * @param[in]		index     index
   * @param[in]		block     buffer block
   * @param[in]		rec       current rec
   */
  void push_cursor(dict_index_t *index, buf_block_t *block, rec_t *rec) {
    CCursor ccursor;
    if (count() < srv_commit_cleanout_max_rows) {
      ccursor.store_position(index, block, rec);
      push_clust(ccursor);
    } else if (stat_enabled) {
      lizard_stats.commit_cleanout_skip.inc();
    }
  }

 private:
  void push_clust(const CCursor &cursor) {
    if (m_static_num < STATIC_CURSORS) {
      m_static_cursors[m_static_num++] = cursor;
    } else {
      m_dynamic_cursors.push_back(cursor);
      m_dynamic_num++;
    }
  }

 private:
  CCursor m_static_cursors[STATIC_CURSORS];

  /* If m_static_num extends MAX_CURSORS, use m_dynamic_cursors. */
  std::vector<CCursor, ut::allocator<CCursor>> m_dynamic_cursors;

  ulint m_static_num;
  ulint m_dynamic_num;

  txn_rec_t m_txn_rec;
};

class DDL_cleanout : public Cleanout {
 private:
  /** How many cursors can be saved to cleanout ddl. */
  constexpr static size_t MAX_CURSORS = 1024;

 public:
  explicit DDL_cleanout()
      : Cleanout(), m_cursor(), m_old_recs(), m_txn_recs(), m_rec_nums{0} {}

  virtual ~DDL_cleanout() override { clear(); }

  virtual void clear() override {
    m_rec_nums = 0;
    m_cursor.reset();
  }

  virtual void execute() override {
    ulint cleaned = do_cleanout();

    clear();
    lizard::lizard_stats.ddl_cleanout_clust_clean.add(cleaned);
  }

  /** Collect record */
  virtual void collect(const trx_id_t trx_id, const txn_rec_t &txn_rec,
                       const rec_t *rec, const dict_index_t *index,
                       const ulint *offsets, btr_pcur_t *pcur) override;

 private:
  ulint do_cleanout();

  void store(const rec_t *rec, const txn_rec_t &txn_rec, btr_pcur_t *pcur) {
    /** Store the first record of new page. */
    if (!m_cursor.stored()) {
      ut_ad(m_rec_nums == 0);
      m_cursor.store_position(pcur);
    }

    /** If the record is on the different page, return. */
    if (!m_cursor.is_on_same_page(pcur)) {
      clear();
      return;
    }

    if (m_rec_nums >= MAX_CURSORS) return;

    m_old_recs[m_rec_nums] = const_cast<rec_t *>(rec);
    m_txn_recs[m_rec_nums] = txn_rec;
    m_rec_nums++;
  }

 private:
  /** If the cursor can restore, it means that the offsets of other records
   * haven't change. */
  CCursor m_cursor;

  rec_t* m_old_recs[MAX_CURSORS];
  txn_rec_t m_txn_recs[MAX_CURSORS];

  ulint m_rec_nums;
};

/**
  Collect rows updated in current transaction.

  @param[in]        thr             current session
  @param[in]        cursor          btr cursor
  @param[in]        rec             current rec
  @param[in]        flags           mode flags for btr_cur operations
*/
extern void commit_cleanout_collect(que_thr_t *thr, btr_cur_t *cursor,
                                    rec_t *rec, ulint flags);
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

#endif
