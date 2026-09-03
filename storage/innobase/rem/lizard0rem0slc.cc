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

/** @file rem/lizard0rem0slc.cc
 Slice implementation.

 Created 2025-12-12 by Yichang Song
 *******************************************************/

#include "data0type.h"
#include "lizard0spc0types.h"
#include "mysqld_error.h"
#include "page0page.h"
#include "rec.h"
#include "rem/rec.h"
#include "rem0lrec.h"
#include "ut0core.h"
#include "ut0log.h"

#include "lizard0dict0dict.h"
#include "lizard0rec0spc.h"
#include "lizard0rec0spc0types.h"
#include "lizard0rem0slc.h"
#include "lizard0rem0types.h"
#include "lizard0page0dict.h"

#ifdef UNIV_DEBUG
[[nodiscard]] bool rec_offs_validate(const ulint *offsets) {
  ulint i = rec_offs_n_fields(offsets);
  ulint last = ULINT_MAX;
  while (i--) {
    ulint curr = rec_offs_base(offsets)[1 + i] & REC_OFFS_MASK;
    ut_a(curr <= last);
    last = curr;
  }
  return true;
}
#endif /* UNIV_DEBUG */

/** Calculate extra and data size if copy prefix n_fields.
 *
 * @param[in]		rec
 * @param[in]		index
 * @param[in]		n_fields
 * @param[in/out]	extra length
 * @param[in/out]	data length */
static inline void rec_get_prefix_range_new(const rec_t *rec,
                                            const dict_index_t *index,
                                            ulint n_fields, ulint *extra_len,
                                            ulint *data_len) {
  const byte *nulls;
  const byte *lens;
  uint16_t n_null = 0;
  ulint i;
  ulint prefix_len;
  ulint null_mask;
  ulint status;
  bool is_rtr_node_ptr = false;

  ut_ad(dict_table_is_comp(index->table));

  status = rec_get_status(rec);

  switch (status) {
    case REC_STATUS_ORDINARY:
      ut_ad(n_fields <= dict_index_get_n_fields(index));
      break;
    case REC_STATUS_NODE_PTR:
      /* For R-tree, we need to copy the child page number field. */
      if (dict_index_is_spatial(index)) {
        ut_ad(n_fields == DICT_INDEX_SPATIAL_NODEPTR_SIZE + 1);
        is_rtr_node_ptr = true;
      } else {
        /* it doesn't make sense to copy the child page number
        field */
        ut_ad(n_fields <= dict_index_get_n_unique_in_tree_nonleaf(index));
      }
      break;
    case REC_STATUS_INFIMUM:
    case REC_STATUS_SUPREMUM:
    /* infimum or supremum record: no sense to copy anything */
    default:
      ut_error;
  }

  {
    uint16_t ndf = 0;
    uint8_t row_version = UINT8_UNDEFINED;
    ut_d(enum REC_INSERT_STATE rec_ins_state =) rec_init_null_and_len_comp(
        rec, index, &nulls, &lens, &n_null, ndf, row_version);
    ut_ad(rec_ins_state != NONE);
    ut_ad(rec_ins_state == INSERTED_INTO_TABLE_WITH_NO_INSTANT_NO_VERSION ||
          index->has_instant_cols_or_row_versions());
    ut_ad(!rec_new_is_versioned(rec) ||
          (is_valid_row_version(row_version) &&
           row_version <= index->table->current_row_version));
  }

  UNIV_PREFETCH_R(lens);
  prefix_len = 0;
  null_mask = 1;

  /* read the lengths of fields 0..n */
  for (i = 0; i < n_fields; i++) {
    const dict_field_t *field;
    const dict_col_t *col;

    field = index->get_field(i);
    col = field->col;

    if (!(col->prtype & DATA_NOT_NULL)) {
      /* nullable field => read the null flag */
      if (UNIV_UNLIKELY(!(byte)null_mask)) {
        nulls--;
        null_mask = 1;
      }

      if (*nulls & null_mask) {
        null_mask <<= 1;
        continue;
      }

      null_mask <<= 1;
    }

    if (is_rtr_node_ptr && i == 1) {
      /* For rtree node ptr rec, we need to
      copy the page no field with 4 bytes len. */
      prefix_len += 4;
    } else if (field->fixed_len) {
      prefix_len += field->fixed_len;
    } else {
      ulint len = *lens--;
      /* If the maximum length of the column is up
      to 255 bytes, the actual length is always
      stored in one byte. If the maximum length is
      more than 255 bytes, the actual length is
      stored in one byte for 0..127.  The length
      will be encoded in two bytes when it is 128 or
      more, or when the column is stored externally. */
      if (DATA_BIG_COL(col)) {
        if (len & 0x80) {
          /* 1exxxxxx */
          len &= 0x3f;
          len <<= 8;
          len |= *lens--;
          UNIV_PREFETCH_R(lens);
        }
      }
      prefix_len += len;
    }
  }

  UNIV_PREFETCH_R(rec + prefix_len);

  *extra_len = rec - (lens + 1);
  *data_len = prefix_len;
}

bool rec_offs_comp(const Slice *slc) { return rec_offs_comp(slc->offsets()); }

/** --------------------------------------------------------------------*/
/** Slice */
/** --------------------------------------------------------------------*/

/** Size of data stored on heap space which is contiguous.
 *
 * @retval	data size (bytes) */
ulint Slice::data_size() const {
  ulint size;
  rec_pack_t pack;
  ut_ad(rec_offs_validate(m_offsets));

  pack = rec_pack();
  size = rec_offs_base(m_offsets)[rec_offs_n_fields(m_offsets)] & REC_OFFS_MASK;

  ut_ad(size < UNIV_PAGE_SIZE);

  return size - pack.len;
}

/** Size of extra stored on heap space which is contiguous.
 *
 * @retval	extra size (bytes) */
ulint Slice::extra_size() const { 
  ulint size;
  ut_ad(rec_offs_validate(m_offsets));

  size = *rec_offs_base(m_offsets) & ~(REC_OFFS_COMPACT | REC_OFFS_EXTERNAL);

  ut_ad(size < UNIV_PAGE_SIZE);

  return (size);
}
/** Copy the rec that stored on heap space
 *
 * @param[in/out]	buffer
 *
 * @retval	copied rec.*/
reg_t Slice::rec_copy(boc_t &cbuf) const {
  ut_ad(m_reg.rec != nullptr);
  ut_ad(cbuf.buf != nullptr);
  ut_ad(rec_offs_validate(m_reg.rec, nullptr, m_offsets));

  ulint extra_len = extra_size();
  ulint data_len = data_size();

  ut_memcpy(cbuf.buf, m_reg.rec - extra_len, extra_len + data_len);

  return {cbuf.buf + extra_len, cbuf.loc};
}

/**
 * Make slice positioned on new record and correct related offsets.
 *
 * @param[in]		new record
 * @param[in]		index
 * @param[in]		check status or not. */
void Slice::store_position(creg_t reg, const dict_index_t *index,
                           bool check_status) {
  m_reg = {const_cast<rec_t *>(reg.rec), reg.loc};
  ut_ad(m_index == index);
  m_index = index;
  if (m_offsets) {
    rec_offs_make_valid(m_reg.rec, index, m_offsets, check_status);
    ut_ad(validate());
  }
}

/** Get nth field length
 *
 * @param[in]		index
 * @param[in]		nth field
 *
 * @retval	field length. */
ulint Slice::get_nth_field_len(const dict_index_t *index, ulint n) const {
  ulint len;
  ut_ad(!index || m_index == index);
  rec_get_nth_field_offs_low(index, m_offsets, n, &len);
  return len;
}

/** Validate record which stored on page or buffer. */
bool Slice::validate() const {
  if (m_reg.loc == MLOC_IN_PAGE) {
    ut_ad(page_rec_check(m_reg.rec));
  }

  /* Assert index and page are consistent. */
  // ut_ad((m_index->is_spc()) == (m_index->pd_capacity > 0));
  if (m_reg.in_page()) {
    page_t *page = page_align(m_reg.rec);
    /** TODO: fix recover. <30-01-26, zanye.zjy> */
    // ut_ad(m_index->is_spc() == fil_page_is_spc(page));
    if (fil_page_is_spc(page)) {
      ut_ad(page_is_leaf(page) || page_dict_get_n_slots(page) == 0);
    }
  }
  return true;
}

/** Validates the consistency of a physical record.
@return true if ok */
bool Slice::rec_validate_low() const {
  ulint len;
  uint16_t n_fields;
  ulint len_sum = 0;
  ulint i;
  uint16_t n_defaults = 0;
  uint16_t n_inst_dropped = 0;

  ut_a(rec());
  n_fields = static_cast<uint16_t>(rec_offs_n_fields(offsets()));

  if ((n_fields == 0) || (n_fields > REC_MAX_N_FIELDS)) {
#ifndef UNIV_LIBRARY
    ib::error(ER_IB_MSG_925) << "Record has " << n_fields << " fields";
#endif
    return false;
  }

  for (i = 0; i < n_fields; i++) {
    /* nullptr for index as i is physical here */
    len = Slice::get_nth_field_len(nullptr, i);

    switch (len) {
      case UNIV_SQL_NULL:
        if (!rec_offs_comp(offsets())) {
          if (i - n_inst_dropped < rec_get_n_fields_old_raw(rec())) {
            len_sum +=
                rec_get_nth_field_size_old_low(rec(), i - n_inst_dropped);
          } else {
            /* INSTANT ADD column with NULL default which is not on this row */
            ++n_defaults;
          }
        }
        break;
      case UNIV_SQL_ADD_COL_DEFAULT:
        /* INSTANT ADD Column */
        ++n_defaults;
        break;
      case UNIV_SQL_INSTANT_DROP_COL:
        /* INSTANT DROP Column */
        ++n_inst_dropped;
        break;
      default:
        /* Normal field with non null value */

        /* INSTANT ADD columns will come (physically) after original ones */
        ut_a(n_defaults == 0);

        if (len >= UNIV_PAGE_SIZE) {
#ifndef UNIV_LIBRARY
          ib::error(ER_IB_MSG_926) << "Record field " << i << " len " << len;
#endif
          return false;
        }

        len_sum += len;
        break;
    }
  }

  ut_a(rec_offs_comp(offsets()) ||
       (n_fields - n_inst_dropped) <=
           rec_get_n_fields_old_raw(rec()) + n_defaults);

  if (len_sum != row_size() - (extra_size() - rec_pack().spc_size())) {
#ifndef UNIV_LIBRARY
    ib::error(ER_IB_MSG_927) << "Record len should be " << len_sum << ", len "
                             << row_size() - extra_size();
#endif
    return false;
  }

#ifndef UNIV_LIBRARY
  if (!rec_offs_comp(offsets())) {
    ut_a(rec_validate_old(rec()));
  }
#endif
  return true;
}
/** --------------------------------------------------------------------*/


/**************************************************/
/** Default Slice */
/**************************************************/
void DSlice::set_offsets(creg_t reg, ulint *offsets,
                         const dict_index_t *index) {
  m_reg = {const_cast<rec_t *>(reg.rec), reg.loc};
  m_offsets = offsets;
  m_index = index;
}

/** Copy the row first n fields prefix to buffer using compact format.
 *
 * @param[in/out]	target buffer
 * @param[in/out]	target buffer size
 * @param[in]		n fields
 *
 * @retval	copied rec */
reg_t DSlice::row_copy_prefix(boc_t *boc, size_t *buf_size,
                              ulint n_fields) const {
  UNIV_PREFETCH_RW(boc->buf);
  if (!dict_table_is_comp(m_index->table)) {
#ifndef UNIV_LIBRARY
    ut_ad(rec_validate_old(m_reg.rec));
#endif
    return (rec_copy_prefix_to_buf_old(
        m_reg.rec, n_fields, rec_get_field_start_offs_low(m_reg.rec, n_fields),
        boc, buf_size));
  }

  ulint extra_len;
  ulint data_len;
  rec_get_prefix_range_new(m_reg.rec, m_index, n_fields, &extra_len, &data_len);

  if ((boc->buf == nullptr) || (*buf_size < extra_len + data_len)) {
    ut::free(boc->buf);
    *buf_size = extra_len + data_len;
    *boc =
        ut::malloc_boc_withkey(UT_NEW_THIS_FILE_PSI_KEY, extra_len + data_len);
  }
  memcpy(boc->buf, m_reg.rec - extra_len, extra_len + data_len);
  return {boc->buf + extra_len, boc->loc};
}

/** Copy the logical contigous data section to dest
 *
 * @param[in]		target buffer
 * @param[in]		prefix data length */
void DSlice::dtuple_copy(byte *dest, ulint copy_len) const {
  ut_a(copy_len <= data_size());
  memcpy(dest, m_reg.rec, copy_len);
}

/** Get nth-field, maybe instant column. */
const byte *DSlice::get_nth_field_instant(const dict_index_t *index, ulint n,
                                          ulint *len) {
  ut_ad(index == nullptr || m_index == index);
  return rec_get_nth_field_instant_low(m_reg.rec, m_offsets, n, index, len);
}

/** Get memory contigous nth-field value address by calculate directly.
 *
 * @param[in]		index
 * @param[in]		nth field
 * @param[in/out]	field length
 *
 * @retval	field value address. */
byte *DSlice::get_nth_field(const dict_index_t *index, ulint n, ulint *len) {
  ut_ad(!index || m_index == index);
  ut_ad(validate());

  return m_reg.rec + rec_get_nth_field_offs_low(index, m_offsets, n, len);
}

/** Get system column physical offset on rec.
 *
 * @param[in] 	system column type
 * @param[in]		nth column
 * @param[in/out]	field length.
 *
 * @retval	physical offset. */
ulint DSlice::get_nth_sys_field_offs(ulint sys_col_type, ulint n,
                                     ulint *len) const {
  if (n == ULINT_UNDEFINED) {
    n = m_index->get_sys_col_pos(sys_col_type);
  }

  ut_ad(n != ULINT_UNDEFINED && n > 0);

  return rec_get_nth_field_offs_low(
      nullptr /* row versions don't affect sys fields */, m_offsets, n, len);
}

/** Overwrite nth field value, only suitable for dslice.
 *
 * @param[in]		index
 * @param[in]		nth field
 * @param[in]		data source
 * @param[in]		data length
 * */
void DSlice::set_nth_field(const dict_index_t *index, ulint n, const void *data,
                           ulint len) {
  byte *data2;
  ulint len2;

  ut_ad(validate());
  ut_ad(!index || index == m_index);

  if (index && index->has_row_versions()) {
    n = index->get_field_off_pos(n);
  }

  auto fn = [&](const ulint *offsets, ulint n) {
    ulint n_drop = 0;
    for (size_t i = 0; i < n; i++) {
      ulint len = rec_offs_base(offsets)[1 + i];
      if (len & REC_OFFS_DROP) {
        n_drop++;
      }
    }
    return n_drop;
  };

  if (len == UNIV_SQL_NULL) {
    if (!rec_offs_nth_sql_null_low(m_offsets, n)) {
      ut_a(!rec_offs_comp(m_offsets));
      ulint n_drop = rec_old_is_versioned(m_reg.rec) ? fn(m_offsets, n) : 0;
      rec_set_nth_field_sql_null_low(m_reg.rec, n - n_drop);
    }

    return;
  }

  ut_ad(!rec_offs_nth_default_low(m_offsets, n));

  /* nullptr for index as n is physical here */
  // data2 = rec_get_nth_field(nullptr, slc->rec(), slc, n, &len2);
  data2 = get_nth_field(nullptr, n, &len2);

  if (len2 == UNIV_SQL_NULL) {
    ut_ad(!rec_offs_comp(m_offsets));

    ulint n_drop = rec_old_is_versioned(m_reg.rec) ? fn(m_offsets, n) : 0;
    rec_set_nth_field_null_bit_low(m_reg.rec, n - n_drop, false);

    ut_ad(len == rec_get_nth_field_size_old_low(m_reg.rec, n - n_drop));
  } else {
    ut_ad(len2 == len || (len2 == UNIV_SQL_INSTANT_DROP_COL && len == 0));
  }

  ut_memcpy(data2, data, len);
}

/** Validate slice according to original innodb rec format. */
bool DSlice::validate() const {
  ut_ad(Slice::validate());
  ut_ad(m_reg.rec && m_offsets);
  ut_ad(!rec_get_spc_flag(m_reg.rec, m_index));
  ut_ad(rec_offs_validate(m_reg.rec, m_index, m_offsets));
#ifndef UNIV_LIBRARY
  ut_a(rec_validate_low());
#endif
  return true;
}

/* Correspond to btr_rec_get_field_ref */
byte *DSlice::get_lob_field_ref(ulint n) {
  return const_cast<byte *>(
      const_cast<const DSlice *>(this)->get_lob_field_ref(n));
}

/* Correspond to btr_rec_get_field_ref */
const byte *DSlice::get_lob_field_ref(ulint n) const {
  ulint field_ref_offs;
  ulint local_len;
  ut_a(rec_offs_nth_extern(m_index, m_offsets, n));

  field_ref_offs =
      rec_get_nth_field_offs_low(m_index, m_offsets, n, &local_len);

  ut_a(rec_field_not_null_not_add_col_def(local_len));
  ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

  return m_reg.rec + field_ref_offs + local_len - BTR_EXTERN_FIELD_REF_SIZE;
}

/**************************************************/
/** Prefix Compress Slice */
/**************************************************/

/** Setter of pslice record offsets.
 *
 * @param[in]		rec_t
 * @param[in]		offsets of rec_t
 * @param[in]		index
 * @param[in]		spc rec pack info.
 * @param[in]		page dict record address.
 * @param[in]		heap */
void PSlice::set_offsets_low(creg_t reg, ulint *offsets,
                             const dict_index_t *index, rec_pack_t rec_pack,
                             const dict_rec_t *drec, mem_heap_t **heap) {
  /** Slice */
  m_reg = {const_cast<rec_t *>(reg.rec), reg.loc};
  m_offsets = offsets;
  m_index = index;

  /** Pslice */
  m_rec_pack = rec_pack;
  m_drec = drec;
  m_heap = heap;
}

/** Set slice attributes through slice context -> get_offsets() */
void PSlice::set_offsets(creg_t reg, ulint *offsets, const dict_index_t *index,
                         mem_heap_t **heap) {
  switch (reg.loc) {
    case MLOC_IN_PAGE: {
#ifndef UNIV_LIBRARY
      /* Validate the rec is on page and the page is valid. */
      /** TODO: fix it <16-01-26, zanye.zjy> */
      // ut_ad(lizard::page_validate_spc_area(page_align(reg), nullptr));
#endif
      rec_pack_t rec_pack = rec_get_spc_pack(reg.rec, index);
      ut_ad(rec_pack.is_spc());

      dict_rec_t *drec =
          page_dict_get_nth_field(page_align(reg), rec_pack.id, nullptr);

      set_offsets_low(reg, offsets, index, rec_pack, drec, heap);
      break;
    }
    case MLOC_IN_HEAP:
    case MLOC_NONE:
      ut_error;
  }
}

/** Set slice attributes through slice context -> get_offsets(),
 *  special for Inheap_slice_ctx.
 * */
void PSlice::set_offsets(creg_t reg, const page_t *page, ulint *offsets,
                         const dict_index_t *index, mem_heap_t **heap) {
  rec_pack_t rec_pack;
  const dict_rec_t *drec = nullptr;
  ut_ad(reg.loc == MLOC_IN_HEAP && rec_get_spc_flag(reg.rec, index));

  rec_pack = rec_get_spc_pack(reg.rec, index);
  ut_ad(rec_pack.is_spc());

  drec = page_dict_get_nth_field(page, rec_pack.id, nullptr);

  set_offsets_low(reg, offsets, index, rec_pack, drec, heap);
}

/** Get memory contigous nth-field value address by calculate or realloc.
 *
 * @param[in]		index
 * @param[in]		nth field
 * @param[in/out]	field length
 *
 * @retval	field value address. */
byte *PSlice::get_nth_field(const dict_index_t *index, ulint n, ulint *len) {
  pack_len_t pack_len;
  ulint field_len;
  ulint field_off;

  byte *field;

  /** Get from prefix area */
  ut_ad(validate());

  pack_len = m_rec_pack.len;
  field_off = rec_get_nth_field_offs_low(index, m_offsets, n, &field_len);

  *len = field_len;

  if (field_len == UNIV_SQL_NULL) {
    return m_reg.rec + field_off;
#ifdef UNIV_DEBUG
  } else if (field_len >= UNIV_EXTERN_STORAGE_FIELD) {
    ut_error;
#endif
  } else if (pack_len <= field_off) {
    /** Locate on rec */
    return m_reg.rec + (field_off - pack_len);
  } else if (field_off + field_len <= pack_len) {
    /** Locate on dict */
    return const_cast<byte *>(m_drec + field_off);
  } else {
    /** fragmental field on dict and rec */
    if (!(*m_heap)) *m_heap = mem_heap_create(256, UT_LOCATION_HERE);

    field = (byte *)mem_heap_alloc(*m_heap, field_len);
    memcpy(field, m_drec + field_off, (pack_len - field_off));
    memcpy(field + (pack_len - field_off), m_reg.rec,
           field_len - (pack_len - field_off));
    return field;
  }
}

/** Get nth-field, impossible instant column. */
const byte *PSlice::get_nth_field_instant(const dict_index_t *index, ulint n,
                                          ulint *len) {
  ut_ad(!index->has_row_versions());

  return get_nth_field(index, n, len);
}

/** Get system column physical offset on rec.
 *
 * @param[in]		system column type
 * @param[in]		nth column
 * @param[in/out]	field length.
 *
 * @retval	physical offset. */
ulint PSlice::get_nth_sys_field_offs(ulint sys_col_type, ulint n,
                                     ulint *len) const {
  ulint logic_offset = 0;

  if (n == ULINT_UNDEFINED) {
    n = m_index->get_sys_col_pos(sys_col_type);
  }
  ut_ad(n != ULINT_UNDEFINED && n > 0 &&
        n == m_index->get_sys_col_pos(sys_col_type));

  /** System column didn't allowed to store on page dict. */
  logic_offset = rec_get_nth_field_offs_low(
      nullptr /* row versions don't affect sys fields */, m_offsets, n, len);

  ut_ad(logic_offset >= m_rec_pack.len);
  return logic_offset - m_rec_pack.len;
}

/** Overwrite nth field value, only suitable for dslice.
 *
 * @param[in]		index
 * @param[in]		nth field
 * @param[in]		data source
 * @param[in]		data length
 * */
void PSlice::set_nth_field(const dict_index_t *index, ulint n, const void *data,
                           ulint len) {
  ut_a(0);
}

/** Row logical size (bytes), which is calculated by compact format,
 *
 * @retval	size.*/
ulint PSlice::row_size() const {
  ut_ad(m_rec_pack.is_spc());
  return rec_size() + m_rec_pack.len - m_rec_pack.spc_size();
}

/** Copy the whole row to buffer using compact format.
 *
 * @param[in/out]	target buffer
 *
 * @retval	copied rec */
reg_t PSlice::row_copy(boc_t &cbuf) const {
  ut_ad(m_drec && rec_get_spc_flag(m_reg.rec, m_index));
  ut_ad(m_rec_pack.is_spc());

#ifndef UNIV_LIBRARY
  ut_ad(rec_offs_validate(m_reg.rec, nullptr, m_offsets));
  ut_ad(rec_validate_low());
#endif

  byte *buf = cbuf.buf;

  /** size of spc_header in SPC record. */
  uint8_t size_spc = m_rec_pack.spc_size();

  /* size of extra size in spc record. Including lens + nulls + spc_header +
   * extra_header(5B). */
  uint8_t size_extra = extra_size();

  /* size of extra size in unencoded record. Including lens + nulls +
   * extra_header(5B). */
  uint8_t size_compact_extra = size_extra - size_spc;

  /** Pointer to the normal record. */
  rec_t *res = (byte *)buf + size_compact_extra;

  /** 1. lens and nulls */
  ut_memcpy(buf, m_reg.rec - size_extra,
            size_compact_extra - REC_N_NEW_EXTRA_BYTES);
  buf = (byte *)buf + size_compact_extra - REC_N_NEW_EXTRA_BYTES;

  /* 2. normal compact extra header. (5 bytes) */
  ut_memcpy(buf, m_reg.rec - REC_N_NEW_EXTRA_BYTES, REC_N_NEW_EXTRA_BYTES);
  rec_set_spc_flag(res, m_index, false);
  buf = (byte *)buf + REC_N_NEW_EXTRA_BYTES;
  ut_ad(buf == res);

  /** 3. data, including prefix */
  dtuple_copy(buf, ULINT_UNDEFINED);

  return {res, cbuf.loc};
}

/** Copy the row first n fields prefix to buffer using compact format.
 *
 * @param[in/out]	target buffer
 * @param[in/out]	target buffer size
 * @param[in]		n fields
 *
 * @retval	copied rec */
reg_t PSlice::row_copy_prefix(boc_t *boc, size_t *buf_size,
                              ulint n_fields) const {
  UNIV_PREFETCH_RW(boc->buf);

  ulint extra_len;
  ulint data_len;
  rec_get_prefix_range_new(m_reg.rec, m_index, n_fields, &extra_len, &data_len);

  ulint pack_size = m_rec_pack.spc_size();
  ulint row_extra_len = extra_len - pack_size;

  if ((boc->buf == nullptr) || (*buf_size < row_extra_len + data_len)) {
    ut::free(boc->buf);
    *buf_size = row_extra_len + data_len;
    *boc = ut::malloc_boc_withkey(UT_NEW_THIS_FILE_PSI_KEY,
                                  row_extra_len + data_len);
  }

  byte *ptr = boc->buf;

  /** Pointer to the normal record. */
  rec_t *res = boc->buf + row_extra_len;

  /* 1. memcpy lens and nulls */
  ut_memcpy(ptr, m_reg.rec - extra_len,
            extra_len - REC_N_NEW_EXTRA_BYTES - pack_size);
  ptr = (byte *)ptr + extra_len - REC_N_NEW_EXTRA_BYTES - pack_size;

  /* 2. memcpy normal compact extra header. (5 bytes) */
  ut_memcpy(ptr, m_reg.rec - REC_N_NEW_EXTRA_BYTES, REC_N_NEW_EXTRA_BYTES);
  rec_set_spc_flag(res, m_index, false);
  ptr = (byte *)ptr + REC_N_NEW_EXTRA_BYTES;
  ut_ad(ptr == res);

  /* 3. data, including prefix. */
  dtuple_copy(ptr, data_len);
  ptr += data_len;

  ut_ad((size_t)(ptr - boc->buf) == row_extra_len + data_len);

  return {res, boc->loc};
}

/** Validate slice according to compressed rec format. */
bool PSlice::validate() const {
  ut_ad(Slice::validate());
  ut_ad(m_reg.rec && m_offsets && m_drec);
  ut_ad(rec_get_spc_flag(m_reg.rec, m_index));
  ut_ad(rec_offs_validate(m_reg.rec, m_index, m_offsets));
#ifndef UNIV_LIBRARY
  ut_a(rec_validate_low());
#endif

  // TODO: more check
#ifdef UNIV_DEBUG
  if (m_reg.in_page()) {
    page_t *page = page_align(m_reg.rec);
    ulint len;
    const dict_rec_t *drec = page_dict_get_nth_field(page, m_rec_pack.id, &len);
    ut_ad(0 == memcmp(m_drec, drec, len));
  }
#endif
  return true;
}

/** Copy the logical contigous data section to dest
 *
 * @param[in]		target buffer
 * @param[in]		prefix data length */
void PSlice::dtuple_copy(byte *dest, ulint copy_len) const {
  ulint pack_len = m_rec_pack.len;
  if (copy_len == ULINT_UNDEFINED) {
    copy_len = data_size() + pack_len;
  }
  ut_ad(copy_len <= data_size() + pack_len);

  /* Copy dict_rec. */
  memcpy(dest, m_drec, std::min(copy_len, pack_len));

  if (copy_len > pack_len) {
    memcpy(dest + pack_len, m_reg.rec, copy_len - pack_len);
  }
}

/**The nth byte of logical contigous data section.*/
byte PSlice::operator[](size_t i) const {
  /** TODO: fix it <25-12-25, zanye.zjy> */
  pack_len_t pack_len = m_rec_pack.len;
  if (i < pack_len) {
    return m_drec[i];
  } else {
    return m_reg.rec[i - pack_len];
  }
}

/**************************************************/
/** MSlice */
/**************************************************/
bool MSlice::validate() const {
  ut_ad(Slice::validate());
  /* mslc can't be validated by rec_offs_validate because there are no
   * REC_N_NEW_EXTRA_BYTES in mrec. */
  ut_ad(m_reg.rec && m_offsets);
#ifndef UNIV_LIBRARY
  ut_a(rec_validate_low());
#endif
  return true;
}
