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

/** @file rem/lizard0rem0slc0ctx.cc
 Slice Context implementation.

 Created 2025-12-12 by Yichang Song
 *******************************************************/

#include "ddl0ddl.h"
#include "univ.i"

#include "lizard0rem0slc.h"
#include "lizard0rem0slc0ctx.h"
#include "lizard0rem0types.h"

/**************************************************/
/** Slice Context */
/**************************************************/

Slice *Slice_ctx::get_offsets(creg_t reg, const dict_index_t *index,
                              ulint n_fields) {
  m_offsets = rec_get_offsets_low(reg, index, m_offsets, n_fields,
                                  UT_LOCATION_HERE, m_heap_which);

  if (rec_get_spc_flag(reg.rec, index)) {
    m_pslc.set_offsets(reg, m_offsets, index, m_heap_which);
    m_slc_which = &m_pslc;
  } else {
    m_dslc.set_offsets(reg, m_offsets, index);
    m_slc_which = &m_dslc;
  }
  return m_slc_which;
}

/** Reposition origin_slc to appropriate slice which represent new copied
 * record, we only copy the offsets since they have the same offsets.
 *
 * @param[in]		copied rec
 * @param[in]		index
 * @param[in]		original slice.
 *
 * @retval		copied slice.
 * */
Slice *Slice_ctx::copy_offsets(creg_t copied, const dict_index_t *index,
                               const Slice *origin_slc) {
  ut_ad(origin_slc == &m_pslc || origin_slc == &m_dslc);
  ut_ad(origin_slc == m_slc_which);
  ut_ad(index == origin_slc->index());
  ut_ad(origin_slc->offsets() == m_offsets);

  /** rec, index */
  rec_offs_make_valid(copied.rec, index, m_offsets, true);

  /** extra_size: only subtract from the size portion (low REC_OFFS_MASK
   * bits), preserve high-bit flags (REC_OFFS_COMPACT / REC_OFFS_EXTERNAL). */
  ulint extra = rec_offs_base(m_offsets)[0];
  rec_offs_base(m_offsets)[0] =
      ((extra & REC_OFFS_MASK) - origin_slc->rec_pack().spc_size()) |
      (extra & ~REC_OFFS_MASK);

  m_dslc.set_offsets(copied, m_offsets, index);

  m_slc_which = &m_dslc;

  return &m_dslc;
}

void Rlog_slice_ctx::set_n_alloc(ulint n_alloc) {
  if (UNIV_UNLIKELY(rec_offs_get_n_alloc(m_offsets) < n_alloc)) {
    if (UNIV_LIKELY(!m_heap)) {
      m_heap = mem_heap_create(n_alloc * sizeof(ulint), UT_LOCATION_HERE);
    }
    m_offsets =
        static_cast<ulint *>(mem_heap_alloc(m_heap, n_alloc * sizeof(ulint)));
    rec_offs_set_n_alloc(m_offsets, n_alloc);
  }
}

void Rlog_slice_ctx::set_n_fields(ulint n_fields) {
  ut_ad(rec_offs_get_n_alloc(m_offsets) >= n_fields);
  rec_offs_set_n_fields(m_offsets, n_fields);
}

MSlice *Rlog_slice_ctx::deserialize_init_offsets(const ddl::mrec_t *rec,
                                                 const dict_index_t *index) {
  creg_t reg = {rec, MLOC_IN_HEAP};
  ut_ad(rec_offs_get_n_alloc(m_offsets) >= rec_offs_n_fields(m_offsets));
  ut_ad(rec_offs_n_fields(m_offsets) > 0);

  rec_init_offsets_comp_ordinary(rec, true, index, m_offsets);

  m_mslc.set_offsets(reg, m_offsets, index);
  return &m_mslc;
}

void DDL_slice_ctx::set_n_alloc(ulint n_alloc) {
  m_offsets.resize(n_alloc);
  m_offsets[0] = n_alloc;
}

void DDL_slice_ctx::set_n_fields(ulint n_fields) {
  ut_ad(m_offsets.size() >= n_fields);
  ut_ad(m_offsets[0] >= n_fields);
  m_offsets[1] = n_fields;
}

void DDL_slice_ctx::set_mrec(byte *mrec) {
  m_mslc.store_position({const_cast<rec_t *>(mrec), MLOC_IN_HEAP},
                        m_mslc.index(), false);
}
MSlice *DDL_slice_ctx::deserialize_init_offsets(creg_t reg,
                                                const dict_index_t *index) {
  // ut_ad(!rec_get_spc_flag(reg.rec, index));
  ut_ad(!m_offsets.empty() && m_offsets.size() == m_offsets[0]);
  ut_ad(m_offsets[0] >= m_offsets[1]);

  rec_init_offsets_comp_ordinary(reg.rec, true, index, m_offsets.data());
  m_mslc.set_offsets(reg, m_offsets.data(), index);
  return &m_mslc;
}

void DDL_slice_ctx::deep_copy_offsets_body(const MSlice *slc) {
  ut_ad(!m_offsets.empty() && rec_offs_get_n_alloc(m_offsets.data()) <=
                                  rec_offs_get_n_alloc(slc->offsets()));
  memcpy(&m_offsets[2], &slc->offsets()[2],
         (m_offsets.size() - 2) * sizeof(ulint));
  m_mslc.set_offsets(m_mslc.reg(), m_offsets.data(), slc->index());
}

void DDL_slice_ctx::reset_offsets_body() {
  ut_ad(!m_offsets.empty() && m_offsets.size() == m_offsets[0]);
  memset(&m_offsets[2], 0x0, (m_offsets.size() - 2) * sizeof(ulint));
  m_mslc.set_offsets(m_mslc.reg(), m_offsets.data(), m_mslc.index());
}

DSlice *Unzip_slice_ctx::get_offsets(creg_t reg, const dict_index_t *index,
                                     ulint n_fields) {
  ut_ad(!index->is_spc());

  m_offsets = rec_get_offsets_low(reg, index, m_offsets, n_fields,
                                  UT_LOCATION_HERE, &m_heap);

  m_dslc.set_offsets(reg, m_offsets, index);

  return &m_dslc;
}

DSlice *Unzip_slice_ctx::get_offsets_reverse(creg_t reg, const byte *data,
                                             const byte *end,
                                             const dict_index_t *index,
                                             ulint node_ptr) {
  ut_ad(!index->is_spc());
  rec_get_offsets_reverse_low(data, index, node_ptr, m_offsets);
  rec_offs_make_valid(reg.rec, index, m_offsets, true);

  m_dslc.set_offsets(reg, m_offsets, index);
  return &m_dslc;
}

Slice *Inheap_slice_ctx::get_offsets(creg_t reg, const page_t *page,
                                     const dict_index_t *index,
                                     ulint n_fields) {
  ut_ad(reg.in_heap());

  m_offsets = rec_get_offsets_low(reg, index, m_offsets, n_fields,
                                  UT_LOCATION_HERE, m_heap_which);

  if (rec_get_spc_flag(reg.rec, index)) {
    m_pslc.set_offsets(reg, page, m_offsets, index, m_heap_which);
    m_slc_which = &m_pslc;
  } else {
    m_dslc.set_offsets(reg, m_offsets, index);
    m_slc_which = &m_dslc;
  }
  return m_slc_which;
}

DSlice *DSlice_ctx::get_offsets(creg_t reg, const dict_index_t *index,
                                ulint n_fields) {
  m_offsets = rec_get_offsets_low(reg, index, m_offsets, n_fields,
                                  UT_LOCATION_HERE, &m_heap);
  m_dslc.set_offsets(reg, m_offsets, index);
  return &m_dslc;
}

/** Copy the whole offsets content
 *
 * @param[in]		copied record
 * @param[in]		source slince
 *
 * @retval	copied dslice. */
DSlice *DSlice_ctx::copy_offsets(creg_t copied, const dict_index_t *index,
                                 const Slice *origin_slc) {
  const ulint *origin_offsets = origin_slc->offsets();
  ulint n_alloc = rec_offs_get_n_alloc(origin_offsets);
  ulint n_fields = rec_offs_n_fields(origin_offsets);

  /** n alloc size. */
  if (rec_offs_get_n_alloc(m_offsets) < n_alloc) {
    if (!m_heap) {
      m_heap = mem_heap_create(n_alloc * sizeof(ulint), UT_LOCATION_HERE);
    }

    m_offsets =
        static_cast<ulint *>(mem_heap_alloc(m_heap, n_alloc * sizeof(ulint)));
  }
  /** n_alloc, n_fields, rec, index. */
  rec_offs_set_n_alloc(m_offsets, n_alloc);
  rec_offs_set_n_fields(m_offsets, n_fields);
  rec_offs_make_valid(copied.rec, index, m_offsets, false);

  /** extra size: only subtract from the size portion (low REC_OFFS_MASK
   * bits), preserve high-bit flags (REC_OFFS_COMPACT / REC_OFFS_EXTERNAL). */
  ulint extra = rec_offs_base(origin_offsets)[0];
  rec_offs_base(m_offsets)[0] =
      ((extra & REC_OFFS_MASK) - origin_slc->rec_pack().spc_size()) |
      (extra & ~REC_OFFS_MASK);

  /** n_fields.*/
  for (ulint i = 1; i <= n_fields; i++) {
    rec_offs_base(m_offsets)[i] = rec_offs_base(origin_offsets)[i];
  }

  m_dslc.set_offsets(copied, m_offsets, index);

  return &m_dslc;
}

DSlice *Cached_slice_ctx::get_offsets(creg_t reg) {
  ut_ad(!rec_get_spc_flag(reg.rec, m_index));
  ut_ad(m_offsets);
  m_dslc.set_offsets(reg, const_cast<ulint *>(m_offsets), m_index);
  return &m_dslc;
}

reg_t rec_copy(boc_t cbuf, Slice *origin_slc) {
  ut_ad(cbuf.loc == MLOC_IN_PAGE);
  reg_t reg = origin_slc->rec_copy(cbuf);
  origin_slc->store_position(reg, origin_slc->index());
  return reg;
}

/** Copy the complete record to a contiguous address space
 *
 * @param[in/out]	buffer address.
 * @param[in]		original record.
 *
 * @retval	copied record. */
reg_t row_copy(boc_t cbuf, const Slice *origin_slc) {
  ut_ad(cbuf.loc == MLOC_IN_HEAP);
  reg_t reg = origin_slc->row_copy(cbuf);
  return reg;
}

/** Copy the complete record to a contiguous address space
 *
 * @param[in/out]	buffer address.
 * @param[in]		original record.
 * @param[in/out]	dslice context.
 *
 * @retval	dslice which held the copied record. */
DSlice *row_copy(boc_t cbuf, const Slice *origin_slc, DSlice_ctx *dslc_ctx) {
  reg_t reg = row_copy(cbuf, origin_slc);
  dslc_ctx->copy_offsets(reg, origin_slc->index(), origin_slc);
  return dslc_ctx->dslc();
}

/** Use the same slice context to reposition to new copied record which
 * have the same offsets.
 *
 * @param[in/out]	copied buf
 * @param[in]		original slice.
 * @param[in/out]	slice context.
 *
 * @retval		copied slice. */
Slice *row_copy(boc_t cbuf, const Slice *origin_slc, Slice_ctx *sctx) {
  reg_t reg = row_copy(cbuf, origin_slc);

  return sctx->copy_offsets(reg, origin_slc->index(), origin_slc);
}

/** Copies the first n fields of a physical record to a new physical record in
 *  a buffer.
 * @param[in,out]  boc          memory buffer for the copied prefix, or NULL
 * @param[in,out]  buf_size     buffer size
 * @param[in]      slc          slice to copy from
 * @param[in]      n_fields     number of fields to copy
 * @return own: copied record */
reg_t row_copy_prefix(boc_t *boc, size_t *buf_size, const Slice *slc,
                      ulint n_fields) {
  return slc->row_copy_prefix(boc, buf_size, n_fields);
}

/**************************************************/
/** Helper / Wrapper function. */
/**************************************************/
