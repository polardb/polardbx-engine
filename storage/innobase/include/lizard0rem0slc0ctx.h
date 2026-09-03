/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0rem0slc0ctx.h
 Slice Context implementation.

 Created 2025-12-12 by Yichang Song
 *******************************************************/

#ifndef lizard0rem0slc0ctx_h
#define lizard0rem0slc0ctx_h

#include "lizard0rem0slc.h"
#include "mem0mem.h"

namespace ddl {
using mrec_t = byte;
}

class Slice_ctx final : public Slice_context_iface {
 public:
  Slice_ctx()
      : m_dslc(),
        m_pslc(),
        m_slc_which(nullptr),
        m_inner_heap(nullptr),
        m_heap_which(&m_inner_heap) {
    m_offsets = m_offsets_;
    rec_offs_init(m_offsets_);
  }

  Slice_ctx(ulint heap_size) : Slice_ctx() {
    m_inner_heap = mem_heap_create(heap_size, UT_LOCATION_HERE);
    ut_ad(m_heap_which == &m_inner_heap);
  }

  Slice_ctx(mem_heap_t **heap)
      : m_dslc(),
        m_pslc(),
        m_slc_which(nullptr),
        m_offsets(nullptr),
        m_inner_heap(nullptr),
        m_heap_which(heap) {
    m_offsets = m_offsets_;
    rec_offs_init(m_offsets_);

    ut_ad(m_heap_which);
  }

  ~Slice_ctx() {
    ut_ad(m_heap_which == &m_inner_heap || !m_inner_heap);
    if (m_inner_heap) {
      mem_heap_free(m_inner_heap);
      m_inner_heap = nullptr;
    }
  }

  Slice *slice() { return m_slc_which; }

  bool is_positioned(const Slice *slc) const { return slc == m_slc_which; }

  Slice *get_offsets(creg_t reg, const dict_index_t *index, ulint n_fields);

  /** Reposition origin_slc to appropriate slice which represent new copied
   * record, we only copy the offsets since they have the same offsets.
   *
   * @param[in]		copied rec
   * @param[in]		index
   * @param[in]		original slice.
   *
   * @retval		copied slice.
   * */
  Slice *copy_offsets(creg_t copied, const dict_index_t *index,
                      const Slice *origin_slc);

  /** Reset slice to initial state.
   *
   * @retval	null slice. */
  Slice *reset_slice() {
    m_slc_which = nullptr;
    m_offsets = m_offsets_;
    rec_offs_init(m_offsets_);

    /** Empty myself heap. */
    if (m_inner_heap) {
      ut_ad(m_heap_which == &m_inner_heap);
      mem_heap_empty(m_inner_heap);
    }
    return m_slc_which;
  }

  /** Borrow heap for temporary usage, take care of the lifecycle. */
  mem_heap_t *borrow_heap() {
    ut_ad(*m_heap_which);
    return *m_heap_which;
  }

 private:
  DSlice m_dslc;
  PSlice m_pslc;
  Slice *m_slc_which;

  ulint m_offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *m_offsets; /** Pointer to real offsets. */

  mem_heap_t *m_inner_heap;
  mem_heap_t **m_heap_which;
};

class Rlog_slice_ctx final : public Slice_context_iface {
 public:
  Rlog_slice_ctx() : m_mslc(), m_heap(nullptr) {
    m_offsets = m_offsets_;
    rec_offs_init(m_offsets_);
  }

  ~Rlog_slice_ctx() {
    if (m_heap) {
      mem_heap_free(m_heap);
    }
  }

  void set_n_alloc(ulint n_alloc);
  void set_n_fields(ulint n_fields);

  MSlice *deserialize_init_offsets(const ddl::mrec_t *rec,
                                   const dict_index_t *index);

  MSlice *mslc() { return &m_mslc; }
  const MSlice *mslc() const { return &m_mslc; }

 private:
  MSlice m_mslc;
  ulint m_offsets_[REC_OFFS_NORMAL_SIZE];
  mem_heap_t *m_heap;
  ulint *m_offsets; /** Pointer to real offsets. */
};

class DDL_slice_ctx final: public Slice_context_iface {
 public:
  DDL_slice_ctx() : m_offsets(), m_mslc() {}

  ~DDL_slice_ctx() = default;

  void set_n_alloc(ulint n_alloc);
  void set_n_fields(ulint n_fields);
  void set_mrec(byte *mrec);

  MSlice *deserialize_init_offsets(creg_t reg, const dict_index_t *index);

  void deep_copy_offsets_body(const MSlice *slc);
  void reset_offsets_body();

  MSlice *mslc() { return &m_mslc; }
  const MSlice *mslc() const { return &m_mslc; }

  const byte *mrec() { return m_mslc.reg().rec; }

  bool empty() const { return m_offsets.empty(); }

 private:
  using Offsets = std::vector<ulint, ut::allocator<ulint>>;
  Offsets m_offsets{};
  MSlice m_mslc;
};

/** Used for unzipping pages for ZIP Page. */
class Unzip_slice_ctx final : public Slice_context_iface {
 public:
  Unzip_slice_ctx(ulint pre_alloc_size, mem_heap_t *heap)
      : m_dslc(), m_heap(heap) {
    if (pre_alloc_size > REC_OFFS_NORMAL_SIZE) {
      m_offsets = static_cast<ulint *>(
          mem_heap_alloc(heap, pre_alloc_size * sizeof(ulint)));
      rec_offs_set_n_alloc(m_offsets, pre_alloc_size);
    } else {
      m_offsets = m_offsets_;
      rec_offs_init(m_offsets_);
    }
  }

  ~Unzip_slice_ctx() = default;

  DSlice *get_offsets(creg_t reg, const dict_index_t *index, ulint n_fields);

  DSlice *get_offsets_reverse(creg_t reg, const byte *data, const byte *end,
                              const dict_index_t *index, ulint node_ptr);

  DSlice *dslc() { return &m_dslc; }

 private:
  DSlice m_dslc;
  ulint m_offsets_[REC_OFFS_NORMAL_SIZE];
  mem_heap_t *m_heap;
  ulint *m_offsets; /** Pointer to real offsets. */
};

/** Slice context containing slice where stored in heap.
 */
class Inheap_slice_ctx final : public Slice_context_iface {
 public:
  Inheap_slice_ctx()
      : m_pslc(),
        m_dslc(),
        m_slc_which(nullptr),
        m_inner_heap(nullptr),
        m_heap_which(&m_inner_heap) {
    m_offsets = m_offsets_;
    rec_offs_init(m_offsets_);
  }

  Inheap_slice_ctx(mem_heap_t **heap)
      : m_pslc(), m_dslc(), m_slc_which(nullptr), m_inner_heap(nullptr) {
    m_offsets = m_offsets_;
    rec_offs_init(m_offsets_);

    ut_ad(*heap);
    m_heap_which = heap;
  }

  ~Inheap_slice_ctx() {
    ut_ad(m_heap_which == &m_inner_heap || !m_inner_heap);
    if (m_inner_heap) {
      mem_heap_free(m_inner_heap);
      m_inner_heap = nullptr;
    }
  }

  Slice *get_offsets(creg_t reg, const page_t *page, const dict_index_t *index,
                     ulint n_fields);

 private:
  PSlice m_pslc;
  DSlice m_dslc;
  Slice *m_slc_which;

  ulint m_offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *m_offsets; /** Pointer to real offsets. */

  mem_heap_t *m_inner_heap;
  mem_heap_t **m_heap_which;
};

class DSlice_ctx final : public Slice_context_iface {
 public:
  DSlice_ctx() : m_dslc(), m_heap(nullptr) {
    m_offsets = m_offsets_;
    rec_offs_init(m_offsets_);
  }

  ~DSlice_ctx() {
    if (m_heap) {
      mem_heap_free(m_heap);
      m_heap = nullptr;
    }
  }

  DSlice *dslc() { return &m_dslc; }

  DSlice *get_offsets(creg_t reg, const dict_index_t *index, ulint n_fields);

  /** Copy the whole offsets content
   *
   * @param[in]		copied record
   * @param[in]		index
   * @param[in]		source slice.
   *
   * @retval	copied dslice. */
  DSlice *copy_offsets(creg_t reg, const dict_index_t *index,
                       const Slice *origin_slc);

 private:
  DSlice m_dslc;
  ulint m_offsets_[REC_OFFS_NORMAL_SIZE];
  mem_heap_t *m_heap;
  ulint *m_offsets; /** Pointer to real offsets. */
};

class Cached_slice_ctx final : public Slice_context_iface {
 public:
  Cached_slice_ctx(const dict_index_t *index)
      : m_index(index), m_offsets(index->rec_cache.offsets), m_dslc() {}
  ~Cached_slice_ctx() = default;

  bool cached() const { return m_offsets != nullptr; }

  DSlice *get_offsets(creg_t reg);

 private:
  const dict_index_t *m_index;
  const ulint *m_offsets; /** Cached and fixed offsets for index. */
  DSlice m_dslc;
};

reg_t rec_copy(boc_t cbuf, Slice *origin_slc);

/** Copy the complete record to a contiguous address space
 *
 * @param[in/out]	buffer address.
 * @param[in]		original record.
 *
 * @retval	copied record. */
reg_t row_copy(boc_t cbuf, const Slice *origin_slc);

/** Copy the complete record to a contiguous address space
 *
 * @param[in/out]	buffer address.
 * @param[in]		original record.
 * @param[in/out]	dslice context.
 *
 * @retval	dslice which held the copied record. */
DSlice *row_copy(boc_t cbuf, const Slice *origin_slc, DSlice_ctx *dslc_ctx);

/** Use the same slice context to reposition to new copied record which
 * have the same offsets.
 *
 * @param[in/out]	copied buf
 * @param[in]		original slice.
 * @param[in/out]	slice context.
 *
 * @retval		copied slice. */
Slice *row_copy(boc_t cbuf, const Slice *origin_slc, Slice_ctx *sctx);

/** Copies the first n fields of a physical record to a new physical record in
 *  a buffer.
 * @param[in,out]  boc          memory buffer for the copied prefix, or NULL
 * @param[in,out]  buf_size     buffer size
 * @param[in]      slc          slice to copy from
 * @param[in]      n_fields     number of fields to copy
 * @return own: copied record */
reg_t row_copy_prefix(boc_t *boc, size_t *buf_size, const Slice *slc,
                      ulint n_fields);

#endif
