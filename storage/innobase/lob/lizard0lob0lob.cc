/*****************************************************************************

Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file lob/lizard0lob0lob.cc
  LOB page pre-allocation for InnoDB clustered-index inserts and updates.

 Created 2026-05-11 by Jiyang.zhang
 *******************************************************/

#include "mtr0mtr.h"
#include "fsp0fsp.h"
#include "btr0btr.h"
#include "lizard0lob0lob.h"
#include "lob0lob.h"
#include "lob0first.h"
#include "lob0pages.h"
#include "lob0impl.h"
#include "row0upd.h"
#include "log0chkp.h"
#include "lizard0mon.h"

/** Upper bound on a single LOB's prealloc page count.
    Sized to cover a full LONGBLOB (~4 GB) under the default 16 KB page
    size: 4 GB / 16 KB = 262144 pages.  Larger page sizes can store the
    same 4 GB in fewer pages, so this bound is also safe for 32 KB / 64 KB
    pages.  Smaller page sizes (4 KB / 8 KB) cap effective prealloc size
    at 1 GB / 2 GB respectively, beyond which the LOB falls back to the
    lazy path. */
static constexpr page_no_t LOB_MAX_PREALLOC_PAGES = 262144;

/** Commit & restart the prealloc mtr every this many pages, so the
    per-mtr redo size stays bounded. No buffer-pool pages are created
    here, only fsp metadata is touched, so the restart is cheap. */
static constexpr page_no_t LOB_PREALLOC_BATCH_PAGES = 1024;

/** Emergency refill batch size for InsertContext::fill_lob_page().
    Invoked only when the pre-allocated pool underestimates and runs dry
    mid-write; at that point the remaining page count is unknown, so we
    claim this many pages at a time until the write finishes. */
static constexpr page_no_t LOB_PREALLOC_HEURISTIC_PAGES = 8;

namespace lizard {

/********************************************************************/
/** Lob_alloc */
/********************************************************************/
ulint Lob_alloc::latchmode() const { return BTR_MODIFY_TREE; }

dberr_t Lob_alloc::fill(page_no_t, ulint, page_no_t, Flush_observer *, bool) {
  return DB_OUT_OF_FILE_SPACE;
}

/********************************************************************/
/** Lob_alloc_lazy */
/********************************************************************/
class Lob_alloc_lazy : public Lob_alloc {
 public:
  Lob_alloc_lazy(dict_index_t *index, bool bulk);

  virtual buf_block_t *get(page_no_t hint, ulint direction,
                           mtr_t *mtr) override;

 private:
  bool m_bulk;
};

Lob_alloc_lazy::Lob_alloc_lazy(dict_index_t *index, bool bulk)
    : Lob_alloc(index), m_bulk(bulk) {}

/** actually lob::alloc_lob_page */
buf_block_t *Lob_alloc_lazy::get(page_no_t hint, ulint direction,
                                 mtr_t *lob_mtr) {
  ulint r_extents;
  mtr_t mtr_bulk;
  mtr_t *alloc_mtr;
  btr_alloc_t alloc;

  space_id_t space_id = dict_index_get_space(m_index);

  ut_ad(fsp_check_tablespace_size(space_id));

  if (m_bulk) {
    mtr_start(&mtr_bulk);
    alloc_mtr = &mtr_bulk;
  } else {
    alloc_mtr = lob_mtr;
  }

  bool success =
      fsp_reserve_free_extents(&r_extents, space_id, 1, FSP_BLOB, alloc_mtr, 1);

  DBUG_EXECUTE_IF("innodb_alloc_lob_page_failed", success = false;);

  if (!success) {
    if (m_bulk) {
      ut_ad(alloc_mtr == &mtr_bulk);
      alloc_mtr->commit();
    }
    return nullptr;
  }

  ut_ad(direction == FSP_NO_DIR);
  alloc = btr_page_alloc(m_index, hint, direction, 0, alloc_mtr, lob_mtr);

  fil_space_release_free_extents(space_id, r_extents);

  if (m_bulk) {
    ut_ad(alloc_mtr == &mtr_bulk);
    alloc_mtr->commit();
  }

  return alloc.new_block;
}

/********************************************************************/
/** Lob_alloc_eager::Backup_latchmode */
/********************************************************************/
/** Eager pre-allocator: fills a per-index free-page pool up front and
    dispenses pages from it without touching FSP metadata on every LOB
    data page write. */
class Lob_alloc_eager : public Lob_alloc {
 private:
  typedef std::vector<page_no_t, mem_heap_allocator<page_no_t>> Pages;

  /** RAII-like helper that backs up the latch_mode of a btr_pcur_t and
      restores it later. Encapsulates the (m_backup_latchmode, m_has_backup)
      pair so that the sentinel value 0 is no longer relied on.

      Scoped inside Lob_alloc_eager because it is only used by the eager
      prealloc path; do not expose it as a generic utility. */
  class Backup_latchmode {
   public:
    Backup_latchmode()
        : m_backup_latchmode(0), m_has_backup(false), m_pcur(nullptr) {}

    /** Defensive: caller must explicitly restore before letting the helper
        die, otherwise the latch_mode change leaks. */
    ~Backup_latchmode() { ut_ad(!m_has_backup); }

    /** Backup the current latch_mode of pcur and overwrite it with new_mode.
        Must not be called when a backup is already in place. */
    void backup_and_set(btr_pcur_t *pcur, ulint new_mode);

    /** Restore the previously backed-up latch_mode to pcur and clear the
        backup slot. No-op if no backup was taken. */
    void restore(btr_pcur_t *pcur, mtr_t *mtr);

    /** Whether a backup is currently in place. */
    bool has_backup() const { return m_has_backup; }

   private:
    ulint m_backup_latchmode;
    bool m_has_backup;
    btr_pcur_t *m_pcur;
  };

 public:
  Lob_alloc_eager(dict_index_t *index, mem_heap_t *heap);

  virtual ~Lob_alloc_eager() override;

  virtual dberr_t fill(page_no_t hint, ulint direction, page_no_t n_pages,
                       Flush_observer *observer, bool refill) override;

  virtual buf_block_t *get(page_no_t hint, ulint direction,
                           mtr_t *mtr) override;

  virtual ulint latchmode() const override;

  virtual void backup_and_set_latchmode(btr_pcur_t *, ulint) override;

  virtual void restore_latchmode(btr_pcur_t *, mtr_t *) override;

#ifdef UNIV_DEBUG
  virtual bool is_prealloc() const override { return true; }
#endif /* UNIV_DEBUG */

 private:
  void clear(bool expect_unused);

  page_no_t fill_one(page_no_t hint, ulint direction, mtr_t *mtr);

  Pages m_unused;
  Backup_latchmode m_backup_latchmode;
  space_id_t m_space_id;
  page_size_t m_page_size;
};

void Lob_alloc_eager::Backup_latchmode::backup_and_set(btr_pcur_t *pcur,
                                                       ulint new_mode) {
  ut_ad(pcur);
  ut_ad(!m_has_backup);

  m_pcur = pcur;
  m_backup_latchmode = pcur->m_latch_mode;
  m_has_backup = true;
  pcur->m_latch_mode = new_mode;
}

void Lob_alloc_eager::Backup_latchmode::restore(btr_pcur_t *pcur, mtr_t *mtr) {
  ut_ad(pcur == m_pcur);
  ut_ad(m_has_backup);

  pcur->restore_position(m_backup_latchmode, mtr, UT_LOCATION_HERE);
  m_has_backup = false;
  m_backup_latchmode = 0;
}

/********************************************************************/
/** Lob_alloc_eager */
/********************************************************************/
Lob_alloc_eager::Lob_alloc_eager(dict_index_t *index, mem_heap_t *heap)
    : Lob_alloc(index),
      m_unused(mem_heap_allocator<page_no_t>(heap)),
      m_backup_latchmode(),
      /** See btr_root_block_get */
      m_space_id(dict_index_get_space(index)),
      m_page_size(dict_table_page_size(index->table)) {}

void Lob_alloc_eager::backup_and_set_latchmode(btr_pcur_t *pcur,
                                               ulint new_mode) {
  if (pcur == nullptr) return;

  ut_ad(pcur->m_latch_mode == BTR_MODIFY_TREE);
  m_backup_latchmode.backup_and_set(pcur, BTR_MODIFY_LEAF);
}

/** Destructor invariant: by the time a Lob_alloc_eager is destroyed,
    m_unused must already be empty on every non-error path.

    On the success path the LOB write loop drains the pool through get(),
    leaving m_unused empty before the InsertContext (and therefore this
    allocator) is destroyed.  On the OOS-during-fill path, fill() reclaims
    its partial pages via clear(true) before returning DB_OUT_OF_FILE_SPACE.
    The only situations where m_unused can still hold pages here are the
    DBUG fault-injection points listed inside clear(): underestimate /
    overestimate refill scenarios and synthetic alloc failures, all of
    which are guarded by the DBUG_EVALUATE_IF clause in clear()'s ut_ad.

    We therefore call clear(false) from the destructor: it asserts the
    invariant on production builds, and falls back to freeing the leaked
    pages so that we never leak FSP space even if the invariant is
    violated by a bug or a fault-injection test. */
Lob_alloc_eager::~Lob_alloc_eager() { clear(false); }

ulint Lob_alloc_eager::latchmode() const { return BTR_MODIFY_LEAF; }

buf_block_t *Lob_alloc_eager::get(page_no_t hint, ulint direction, mtr_t *mtr) {
  page_no_t page_no;
  buf_block_t *block;

  if (m_unused.empty()) {
    return nullptr;
  }

  page_no = m_unused.back();
  m_unused.pop_back();

  block = fsp_page_create({m_space_id, page_no}, m_page_size, RW_X_LATCH, mtr,
                          mtr);
  ut_a(block);

  return block;
}

void Lob_alloc_eager::restore_latchmode(btr_pcur_t *pcur, mtr_t *btr_mtr) {
  const bool restore_latchmode = m_backup_latchmode.has_backup();

  if (!restore_latchmode) {
    return;
  }

  /** No need to Flush_observer. */
  ut_ad(pcur);
  ut_ad(btr_mtr);

  pcur->store_position(btr_mtr);
  mtr_commit(btr_mtr);

  clear(false);

  mtr_start(btr_mtr);
  m_backup_latchmode.restore(pcur, btr_mtr);
}

/** Free any pages still sitting in the prealloc pool back to the LOB
    segment, then empty m_unused.

    The @p expect_unused name reads backwards on first sight, but it means:
      - false: "do NOT expect the pool to hold pages"; the caller asserts
               that the normal write path has drained it.  Used by the
               destructor and by restore_latchmode(), where the LOB write
               must already have consumed every preallocated page.
      - true : "the pool MAY still hold pages"; the caller is on a known
               cleanup path that has to free whatever is left.  Used by
               fill() when an mid-prealloc OOS forces it to release the
               partial pages it has already claimed.

    The DBUG_EVALUATE_IF chain in the assertion enumerates the synthetic
    failure points that legitimately leave residual pages in the pool even
    when expect_unused is false:

      - innodb_lob_prealloc_overestimate / _underestimate:
          calc_lob_prealloc_pages was off, so a page was never popped
          (overestimate) or refill claimed extra pages that were never
          consumed (underestimate).
      - innodb_lob_data_page_alloc_failed / _alloc_node_page_failed:
          a downstream LOB data / node-page alloc returned failure mid
          write, leaving the rest of the pool unused.

    Every path eventually reaches fseg_free_page on its own private mtr -
    we never piggy-back on the caller's btr_mtr because clear() is invoked
    in places where btr_mtr is committed (restore_latchmode) or has never
    existed (destructor of a fully-drained allocator). */
void Lob_alloc_eager::clear(bool expect_unused) {
  page_t *root;
  fseg_header_t *seg_header;
  space_id_t space_id;
  mtr_t mtr;
  ulint n_freed = 0;

  ut_ad(expect_unused || m_unused.empty() ||
        DBUG_EVALUATE_IF("innodb_lob_prealloc_overestimate", true, false) ||
        DBUG_EVALUATE_IF("innodb_lob_prealloc_underestimate", true, false) ||
        DBUG_EVALUATE_IF("innodb_lob_data_page_alloc_failed", true, false) ||
        DBUG_EVALUATE_IF("innodb_lob_alloc_node_page_failed", true, false));
  if (m_unused.empty()) return;

  mtr_start(&mtr);
  root = btr_root_get(m_index, &mtr);
  seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
  space_id = dict_index_get_space(m_index);

  for (page_no_t page_no : m_unused) {
    if (page_no != FIL_NULL) {
      fseg_free_page(seg_header, space_id, page_no, /*ahi=*/false, &mtr);
      n_freed++;
    }
  }

  mtr_commit(&mtr);

  m_unused.clear();

  generic_stats.lob_prealloc_overestimate_pages.add(n_freed);
}

dberr_t Lob_alloc_eager::fill(page_no_t hint, ulint direction,
                              page_no_t n_pages, Flush_observer *observer,
                              bool refill) {
  mtr_t mtr;

  ut_ad(hint == FIL_NULL);
  ut_ad(direction == FSP_NO_DIR);
  ut_ad(n_pages > 0);

  ut_ad(m_unused.empty());
  m_unused.assign(n_pages, FIL_NULL);

  mtr_start(&mtr);
  /** Hand the flush_observer over so that any redo / page modification
      done in this independent prealloc mtr is still tracked by the same
      observer that owns the outer btr_mtr (e.g. online DDL). */
  mtr.set_flush_observer(observer);

  for (ulint i = 0; i < n_pages; i++) {
    const page_no_t page_no = fill_one(hint, direction, &mtr);

    if (page_no == FIL_NULL) {
      generic_stats.lob_prealloc_fail_no_space.inc();
      mtr_commit(&mtr);
      /* Reclaim partial pages so m_unused is empty when fill returns
         non-success.  Safe here: both callers (active_lob_page_allocator
         and InsertContext::fill_lob_page) wrap fill() inside
         check_redolog, which commits btr_mtr before invoking us, so no
         leaf-page or cluster-index latch is held; fill's own mtr was
         just committed above.  clear() opens its own short mtr to free
         the seg pages. */
      clear(true);
      return DB_OUT_OF_FILE_SPACE;
    }

    /** Fill and get using FIFO */
    m_unused.rbegin()[i] = page_no;

    /** Pure redo-checkpoint: bound the per-mtr redo size by committing
        and restarting the mtr every LOB_PREALLOC_BATCH_PAGES pages.
        No pcur position needs to be saved/restored because the prealloc
        mtr is independent from any btr_mtr.
        The flush_observer must be re-attached after each restart so the
        observer keeps tracking redo/dirty-page state across batches. */
    if ((i + 1) % LOB_PREALLOC_BATCH_PAGES == 0 && (i + 1) < n_pages) {
      mtr_commit(&mtr);
      log_free_check();
      mtr_start(&mtr);
      mtr.set_flush_observer(observer);
    }
  }

  /** Should not happend if calc_lob_prealloc_pages rightly. */
  ut_ad(!refill ||
        DBUG_EVALUATE_IF("innodb_lob_prealloc_underestimate", true, false));
  if (refill) generic_stats.lob_prealloc_underestimate_pages.add(n_pages);
  generic_stats.lob_prealloc_pages.add(n_pages);

  mtr_commit(&mtr);
  return DB_SUCCESS;
}

page_no_t Lob_alloc_eager::fill_one(page_no_t hint, ulint direction,
                                    mtr_t *mtr) {
  page_no_t ret_page;
  page_t *root;
  fseg_header_t *seg_header;

  ut_ad(!dict_index_is_ibuf(m_index));
  ut_ad(hint == FIL_NULL);
  ut_ad(direction == FSP_NO_DIR);

  DBUG_EXECUTE_IF("innodb_alloc_lob_page_failed", { return FIL_NULL; });

  {
    /** Actually like btr_page_alloc but only claim. */
    // alloc = btr_page_alloc(index, hint, FSP_NO_DIR, 0, alloc_mtr, lob_mtr);
    root = btr_root_get(m_index, mtr);
    seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;

    ret_page = fseg_claim_free_page_general(seg_header, FIL_NULL, FSP_NO_DIR,
                                            false, mtr);
  }

  return ret_page;
}

// Lob_alloc_eager::~Lob_alloc_eager() { cleanup(); }

bool srv_lob_prealloc = true;

static inline bool can_lob_prealloc(const dict_index_t *index) {
  ut_ad(index->is_clustered());

  /* lob_insert_noindex forces the noindex path which uses no node pages;
     pre-allocating them would leave m_unused non-empty and fire the
     Lob_alloc_eager destructor assertion. */
  DBUG_EXECUTE_IF("lob_insert_noindex", { return false; });

  return srv_lob_prealloc && !dict_index_is_online_ddl(index) &&
         !(index->type & DICT_FTS) && !dict_index_is_sdi(index) &&
         index->table && !index->table->is_compressed() &&
         !index->table->is_temporary() && !index->table->is_system_table;
}

/** Calculate the number of pages needed to store one LOB field externally.

  Derived directly from the write path in lob::insert() (lob0impl.cc):

  1. first_page_t::alloc() allocates one page; its inline free-list is
     pre-populated with K (= first_page_t::node_count()) index entries.
  2. first_page_t::write() stores min(len, A) bytes of payload, where
     A = first_page_t::max_space_available().
  3. first.alloc_index_entry() consumes one entry for the first page itself.
  4. While bytes remain, each loop iteration unconditionally allocates one
     data_page (write stores min(remaining, B) bytes, B = data_page_t::
     payload()) and consumes one index entry. When the free-list is empty,
     node_page_t::alloc() is called first, refilling the free-list with M
     entries (M = node_page_t::node_count()).

  Total entries consumed = 1 + D where D is the number of data pages.
  Free-list supplies K inline + n_node * M from node pages, so the smallest
  n_node satisfying K + n_node * M >= 1 + D is

       n_node = ceil( max(0, 1 + D - K) / M ),
  with D = ceil( max(0, len - A) / B ).

  Total pages = 1 + D + n_node.

  @param[in]    len     LOB field length in bytes.
  @return number of pages this field will consume on the LOB segment. */
static ulint calc_lob_pages_needed(ulint len) {
  /* 15680 on a 16K page. */
  static const ulint A = lob::first_page_t::max_space_available();
  /* 16327 on a 16K page. */
  static const ulint B = lob::data_page_t::payload();
  /* 10. */
  static const ulint K = lob::first_page_t::node_count();
  /* 272 on a 16K page. */
  static const ulint M = lob::node_page_t::node_count();
  ulint d_pages = 0;
  ulint n_entries;
  ulint n_node_pages = 0;

  if (len > A) {
    /* ceil((len - A) / B). */
    d_pages = (len - A - 1) / B + 1;
  }

  /* One inline entry for the first_page itself + one per data_page. */
  n_entries = 1 + d_pages;

  if (n_entries > K) {
    /* ceil((n_entries - K) / M). */
    n_node_pages = (n_entries - K - 1) / M + 1;
  }

  return 1 + d_pages + n_node_pages;
}

/** Layer 2 prealloc check: validate all LOB fields, reject partial-update
fields or oversized blobs, and return total page count (0 = not feasible).
@param[in]  big_rec  externally stored fields
@param[in]  upd      update vector (nullptr for INSERT)
@return total page count, or 0 on rejection. */
static page_no_t calc_lob_prealloc_pages(const big_rec_t *big_rec,
                                         const upd_t *upd) {
  page_no_t n_pages = 0;
  ulint total_blob_len = 0;

  for (uint i = 0; i < big_rec->n_fields; i++) {
    const big_rec_field_t &field = big_rec->fields[i];

    if (upd != nullptr && upd->is_partially_updated(field.field_no)) {
      return 0;
    }

    ulint pages_for_field = calc_lob_pages_needed(field.len);
    if (pages_for_field == 0) {
      return 0;
    }

    DBUG_EXECUTE_IF("innodb_lob_prealloc_overestimate", pages_for_field += 5;);
    DBUG_EXECUTE_IF("innodb_lob_prealloc_underestimate",
                    if (pages_for_field > 1) pages_for_field--;);

    n_pages += pages_for_field;
    total_blob_len += field.len;
  }

  return n_pages;
}

Lob_alloc *active_lob_page_allocator(const big_rec_t *big_rec_vec,
                                     const upd_t *upd, bool is_bulk,
                                     btr_pcur_t *pcur, dict_index_t *index,
                                     Flush_observer *observer) {
  dberr_t err = DB_SUCCESS;
  mem_heap_t *heap;
  page_no_t n_prealloc = 0;
  Lob_alloc *alloc;

  ut_ad(big_rec_vec->heap != nullptr);
  heap = big_rec_vec->heap;

  /**
    Use prealloc when:
    1. Not bulk
    2. Not special index like compressed or other.
    3. Not partial-update
  */
  if (!is_bulk && can_lob_prealloc(index)) {
    n_prealloc = lizard::calc_lob_prealloc_pages(big_rec_vec, upd);

    if (n_prealloc > LOB_MAX_PREALLOC_PAGES) {
      lizard::generic_stats.lob_prealloc_reject_too_large.inc();
      n_prealloc = 0;
    }
  }

  if (n_prealloc > 0) {
    /** Prealloc can not use hint page. */
    /** Try use eager_alloc. */
    lizard::Lob_alloc_eager *eager_alloc =
        new (mem_heap_alloc(heap, sizeof(Lob_alloc_eager)))
            Lob_alloc_eager(index, heap);

    err = eager_alloc->fill(FIL_NULL, FSP_NO_DIR, n_prealloc, observer,
                            /*refill=*/false);

    if (err != DB_SUCCESS) {
      /* fill() already freed any partial pages on failure. */
      return nullptr;
    }

    alloc = eager_alloc;
  } else {
    alloc = new (mem_heap_alloc(heap, sizeof(lizard::Lob_alloc_lazy)))
        lizard::Lob_alloc_lazy(index, is_bulk);
  }

  /** Backup pcur latchmode only after prealloc has succeeded, so a
      fill failure does not leave the caller's pcur in a half-mutated
      state. */
  alloc->backup_and_set_latchmode(pcur, alloc->latchmode());

  return alloc;
}
}

namespace lob {

InsertContext::InsertContext(const BtrContext &btr_ctx,
                             const big_rec_t *big_rec_vec, const upd_t *upd,
                             lizard::Lob_alloc *alloc)
    : BtrContext(btr_ctx),
      m_big_rec_vec(big_rec_vec),
      m_upd(upd),
      m_page_alloc(alloc) {}

InsertContext::~InsertContext() {
  if (m_page_alloc) {
    m_page_alloc->restore_latchmode(m_pcur, m_mtr);
    m_page_alloc = nullptr;
  }
}

bool InsertContext::fill_lob_page(page_no_t hint) {
  dberr_t err;
  Flush_observer *obs;

  obs = m_mtr->get_flush_observer();
  err = check_redolog([this, obs, hint]() {
    return m_page_alloc->fill(hint, FSP_NO_DIR, LOB_PREALLOC_HEURISTIC_PAGES,
                              obs,
                              /*refill=*/true);
  });

  return (err == DB_SUCCESS);
}

buf_block_t *InsertContext::alloc_lob_page(page_no_t hint, mtr_t *lob_mtr) {
  buf_block_t *block = m_page_alloc->get(hint, FSP_NO_DIR, lob_mtr);

  if (block == nullptr) {
    if (fill_lob_page(hint)) {
      /* Only fill and get once */
      block = m_page_alloc->get(hint, FSP_NO_DIR, lob_mtr);
    }
  }

  return block;
}

/** Build the page allocator inside a check_redolog pre_restore window.

    Why the construction must run in the latch-free window (i.e. as a
    pre_restore callback rather than at InsertContext build time):

    active_lob_page_allocator() picks Lob_alloc_eager whenever the request
    fits the prealloc envelope, and Lob_alloc_eager::fill() runs an
    independent prealloc mtr that walks the LOB segment to claim pages
    from FSP.  If we invoked fill() while the caller's btr_mtr was still
    alive, the prealloc mtr would acquire FSP latches that the btr_mtr
    already covers indirectly, and we would self-deadlock against
    ourselves through the latch ordering rules (see Lob_alloc::fill()'s
    Doxygen for the full chain).

    Wrapping construction in check_redolog() guarantees that btr_mtr has
    been committed before fill() is invoked and is freshly restarted only
    after fill() returns, so the prealloc mtr always runs alone. */
dberr_t InsertContext::check_redolog_and_active_allocator() {
  Flush_observer *flush_observer;
  flush_observer = m_mtr->get_flush_observer();

  return check_redolog([this, flush_observer]() {
    if (m_page_alloc) return DB_SUCCESS;

    m_page_alloc = lizard::active_lob_page_allocator(
        m_big_rec_vec, m_upd, is_bulk(), m_pcur, m_index, flush_observer);

    return m_page_alloc ? DB_SUCCESS : DB_OUT_OF_FILE_SPACE;
  });
}

/** Construct the GuardFirstPage RAII guard.

    The post_restore lambda re-derives ref->set_ref from
    ctx->get_field_ref(field->field_no) on every check_redolog cycle.
    The reason is that the external-field reference inside the cluster
    record is addressed by a raw pointer into the leaf page's record
    area: when check_redolog() commits and restarts btr_mtr the leaf
    page may be re-pinned at a different buffer-frame address, or the
    record's offset within the frame can shift due to page reorganization
    triggered by a concurrent write, so the pointer captured in @p ref
    becomes stale.  Re-deriving it from ctx->get_field_ref() is what
    keeps subsequent ref writes landing inside the right cluster
    record. */
GuardFirstPage::GuardFirstPage(InsertContext *ctx, first_page_t *first,
                               ref_t *ref, big_rec_field_t *field,
                               const page_id_t &page_id,
                               const page_size_t &page_size)
    : m_ctx(ctx), m_fn([ctx, first, ref, field, page_id, page_size]() {
        ref->set_ref(ctx->get_field_ref(field->field_no));
        first->load_x(page_id, page_size);
      }) {
  ctx->set_post_restore(&m_fn);
}

GuardFirstPage::~GuardFirstPage() { m_ctx->set_post_restore(nullptr); }

}  // namespace lob

