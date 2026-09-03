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

/** @file include/lizard0lob0lob.h
  LOB page pre-allocation for InnoDB clustered-index inserts and updates.

 Created 2026-05-11 by Jiyang.zhang
 *******************************************************/

#ifndef lizard0lob0lob_h
#define lizard0lob0lob_h

#include <functional>

#include "db0err.h"
#include "fsp0types.h"
#include "mem0mem.h"
#include "page0size.h"
#include "buf0types.h"
#include "univ.i"

class dict_index_t;
struct btr_pcur_t;
class Flush_observer;
class big_rec_t;
class upd_t;
struct big_rec_field_t;

namespace lizard {

extern bool srv_lob_prealloc;

class Lob_alloc {
 public:
  Lob_alloc(dict_index_t *index) : m_index(index) {}
  virtual ~Lob_alloc() = default;

  /**
    Pre-fill the internal page pool with @p n_pages pages claimed from the
    LOB segment.  Only meaningful for the eager pre-allocator; the base
    implementation always reports out-of-space so that the lazy path falls
    back to per-page allocation through get().

    The fsp reservation type is hard-coded to FSP_BLOB inside the
    implementations - all current callers are LOB writes and there is no
    other fsp_reserve_t value that makes sense here.

    @param[in]  hint        hint page-no for the allocator (FIL_NULL = none).
    @param[in]  direction   FSP_UP / FSP_DOWN / FSP_NO_DIR.
    @param[in]  n_pages     number of pages to reserve up front.
    @param[in]  observer    flush observer attached to the prealloc mtr.
    @param[in]  refill      true if invoked as an emergency refill (the pool
                            was drained mid-write); false on the initial fill.
    @retval DB_SUCCESS              all pages reserved.
    @retval DB_OUT_OF_FILE_SPACE    not enough free pages on the segment, or
                                    this allocator does not support fill().
  */
  virtual dberr_t fill(page_no_t hint, ulint direction, page_no_t n_pages,
                       Flush_observer *observer, bool refill);

  /**
    Dispense one LOB page to the caller.  The eager allocator pops a
    page from its pre-filled pool without touching FSP metadata; the lazy
    allocator forwards directly to fsp/fseg routines under @p mtr.

    Like fill(), the fsp reservation type is hard-coded to FSP_BLOB.

    @param[in]  hint        hint page-no for the allocator (FIL_NULL = none).
    @param[in]  direction   FSP_UP / FSP_DOWN / FSP_NO_DIR.
    @param[in]  mtr         mtr that latches the returned block.
    @return latched buffer block, or nullptr on out-of-space.
  */
  virtual buf_block_t *get(page_no_t hint, ulint direction, mtr_t *mtr) = 0;

  /**
    Latch mode required on the clustered-index pcur held by the caller
    while this allocator is active.  The lazy path needs BTR_MODIFY_TREE
    because each get() walks FSP/segment metadata; the eager path can
    relax to BTR_MODIFY_LEAF since the pool is filled up front and no
    further tree-level latches are taken on the LOB write loop.
  */
  virtual ulint latchmode() const;

  /**
    Hook invoked once before LOB write begins so that the allocator can
    temporarily swap the pcur's latch_mode to whatever it expects (see
    latchmode()).  Base implementation is a no-op since the default mode
    is BTR_MODIFY_TREE which matches what the caller already holds.
  */
  virtual void backup_and_set_latchmode(btr_pcur_t *, ulint) {}

  /**
    Counterpart of backup_and_set_latchmode(): restore the pcur's
    original latch_mode after the LOB write completes.  Base
    implementation is a no-op.
  */
  virtual void restore_latchmode(btr_pcur_t *, mtr_t *) {}

#ifdef UNIV_DEBUG
  /** Returns true iff this allocator is an eager pre-allocator.
      Only declared in debug builds; used by ut_ad() guard sites to verify
      that certain code paths never see a prealloc allocator. */
  virtual bool is_prealloc() const { return false; }
#endif /* UNIV_DEBUG */

 protected:
  dict_index_t *m_index;
};

/**
  Pick and construct the appropriate Lob_alloc for one LOB write call.

  Decision tree:
    - bulk load, online DDL, FTS / SDI / compressed / temporary / system
      tables, or any partially-updated LOB field => fall back to lazy
      per-page allocation (Lob_alloc_lazy).
    - else compute the exact page demand from @p big_rec_vec / @p upd; if
      it is within LOB_MAX_PREALLOC_PAGES, build a Lob_alloc_eager and
      pre-fill the pool in a single FSP traversal.
    - if the eager fill fails (out of space, etc.) the function returns
      nullptr; the caller must abort the LOB write with DB_OUT_OF_FILE_SPACE.

  On success the returned allocator owns a backup of @p pcur's latch_mode
  and has already swapped it to allocator->latchmode(); the caller must
  destruct the allocator (which calls restore_latchmode()) before pcur is
  closed.  The allocator is allocated from big_rec_vec->heap and therefore
  must NOT be deleted with operator delete - just invoke its destructor.

  @param[in]      big_rec_vec  externally stored fields of the row.
  @param[in]      upd          update vector (nullptr for INSERT).
  @param[in]      is_bulk      true for OPCODE_INSERT_BULK.
  @param[in,out]  pcur         clustered-index pcur whose latch_mode may be
                               temporarily swapped.
  @param[in]      index        clustered index being written.
  @param[in]      observer     flush observer attached to the prealloc mtr.
  @return owning Lob_alloc, or nullptr on out-of-space during prealloc.
*/
Lob_alloc *active_lob_page_allocator(const big_rec_t *big_rec_vec,
                                     const upd_t *upd, bool is_bulk,
                                     btr_pcur_t *pcur, dict_index_t *index,
                                     Flush_observer *observer);

}  // namespace lizard

namespace lob {

class InsertContext;
class first_page_t;
class ref_t;

/** RAII guard that registers a post-restore callback on InsertContext.
    On every check_redolog cycle the callback re-latches the first LOB page
    and refreshes the external-field-reference pointer, both of which are
    invalidated when the btr_mtr is committed and restarted.
    The callback is deregistered on destruction, so it is never invoked
    after the local variables it captures go out of scope. */
class GuardFirstPage {
 public:
  GuardFirstPage(InsertContext *ctx, first_page_t *first, ref_t *ref,
                 big_rec_field_t *field, const page_id_t &page_id,
                 const page_size_t &page_size);

  ~GuardFirstPage();

 private:
  InsertContext *m_ctx;
  std::function<void()> m_fn;
};

}  // namespace lob

#endif
