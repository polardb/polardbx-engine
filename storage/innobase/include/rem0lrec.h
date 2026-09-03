/*****************************************************************************

Copyright (c) 2021, 2022, Oracle and/or its affiliates.

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

/** @file include/rem0lrec.h
 Record manager.
 This file contains low level functions which deals with physical index of
 fields in a physical record.

 After INSTANT ADD/DROP feature, fields index on logical record might not be
 same as field index on physical record. So a wrapper (rem0wrec.h) is
 implemented which translates logical index to physical index. And then
 functions of this file are called with physical index of the field.

 Created 13/08/2021 Mayank Prasad
*****************************************************************************/

#ifndef rem0lrec_h
#define rem0lrec_h

#include "rem0rec.h"

/** Set nth field value to SQL NULL.
@param[in,out]  rec  record
@param[in]      n    index of the field. */
static inline void rec_set_nth_field_sql_null_low(rec_t *rec, ulint n);

/** Returns nonzero if the SQL NULL bit is set in nth field of rec.
@param[in]  offsets  array returned by rec_get_offsets()
@param[in]  n        index of the field.
@return nonzero if SQL NULL */
static inline ulint rec_offs_nth_sql_null_low(const ulint *offsets, ulint n);

/** Read the offset of the start of a data field in the record. The start of an
SQL null field is the end offset of the previous non-null field, or 0, if none
exists. If n is the number of the last field + 1, then the end offset of the
last field is returned.
@param[in]  rec  record
@param[in]  n    index of the field
@return offset of the start of the field */
static inline ulint rec_get_field_start_offs_low(const rec_t *rec, ulint n);

/** Returns the offset of nth field start if the record is stored in the 1-byte
offsets form.
@param[in]  rec  record
@param[in]  n    index of the field
@return offset of the start of the field */
static inline ulint rec_1_get_field_start_offs_low(const rec_t *rec, ulint n);

/** Returns the offset of nth field end if the record is stored in the 1-byte
offsets form. If the field is SQL null, the flag is ORed in the returned value.
@param[in]  rec  record
@param[in]  n    index of the field
@return offset of the start of the field, SQL null flag ORed */
static inline ulint rec_1_get_field_end_info_low(const rec_t *rec, ulint n);

/** Sets the field end info for the nth field if the record is stored in the
1-byte format.
@param[in,out]  rec   record
@param[in]      n     index of the field
@param[in]      info  value to set */
static inline void rec_1_set_field_end_info_low(rec_t *rec, ulint n,
                                                ulint info);
/** Returns the offset of nth field start if the record is stored in the 2-byte
offsets form.
@param[in]  rec   record
@param[in]  n     index of the field
@return offset of the start of the field */
static inline ulint rec_2_get_field_start_offs_low(const rec_t *rec, ulint n);

/** Returns the offset of nth field end if the record is stored in the 2-byte
offsets form. If the field is SQL null, the flag is ORed in the returned value.
@param[in]  rec   record
@param[in]  n     index of the field
@return offset of the start of the field, SQL null flag and extern storage flag
ORed */
static inline ulint rec_2_get_field_end_info_low(const rec_t *rec, ulint n);

/** Sets the field end info for the nth field if the record is stored in the
2-byte format.
@param[in]  rec   record
@param[in]  n     index of the field
@param[out] info  end info */
static inline void rec_2_set_field_end_info_low(rec_t *rec, ulint n,
                                                ulint info);

/** Sets the value of the ith field SQL null bit of an old-style record.
@param[in]  rec   record
@param[in]  i     index of the field
@param[in]  val   value to set */
static inline void rec_set_nth_field_null_bit_low(rec_t *rec, ulint i,
                                                  bool val);

/** Gets the physical size of an old-style field.
Also an SQL null may have a field of size > 0, if the data type is of a fixed
size.
@param[in]  rec   record
@param[in]  n     index of the field
@return field size in bytes */
static inline ulint rec_get_nth_field_size_old_low(const rec_t *rec, ulint n) {
  ulint os;
  ulint next_os;

  os = rec_get_field_start_offs_low(rec, n);
  next_os = rec_get_field_start_offs_low(rec, n + 1);

  ut_ad(next_os - os < UNIV_PAGE_SIZE);

  return (next_os - os);
}

/** The following function is used to get the offset to the nth
data field in an old-style record.
@param[in]   rec  record
@param[in]   n    index of the field
@param[out]  len  length of the field; UNIV_SQL_NULL if SQL null;
@return offset to the field */
static inline ulint rec_get_nth_field_offs_old_low(const rec_t *rec, ulint n,
                                                   ulint *len) {
  ulint os;
  ulint next_os;

  ut_ad(len);
  ut_a(rec);
  ut_a(n < rec_get_n_fields_old_raw(rec));

  if (rec_get_1byte_offs_flag(rec)) {
    os = rec_1_get_field_start_offs_low(rec, n);

    next_os = rec_1_get_field_end_info_low(rec, n);

    if (next_os & REC_1BYTE_SQL_NULL_MASK) {
      *len = UNIV_SQL_NULL;

      return (os);
    }

    next_os = next_os & ~REC_1BYTE_SQL_NULL_MASK;
  } else {
    os = rec_2_get_field_start_offs_low(rec, n);

    next_os = rec_2_get_field_end_info_low(rec, n);

    if (next_os & REC_2BYTE_SQL_NULL_MASK) {
      *len = UNIV_SQL_NULL;

      return (os);
    }

    next_os = next_os & ~(REC_2BYTE_SQL_NULL_MASK | REC_2BYTE_EXTERN_MASK);
  }

  *len = next_os - os;

  ut_ad(*len < UNIV_PAGE_SIZE);

  return (os);
}

/** Mark the nth field as externally stored.
@param[in]  offsets  array returned by rec_get_offsets()
@param[in]  n       index of the field */
static inline void rec_offs_make_nth_extern_low(ulint *offsets, const ulint n) {
  ut_ad(!rec_offs_nth_sql_null_low(offsets, n));
  rec_offs_base(offsets)[1 + n] |= REC_OFFS_EXTERNAL;
}

static inline ulint rec_offs_nth_sql_null_low(const ulint *offsets, ulint n) {
  return (rec_offs_base(offsets)[1 + n] & REC_OFFS_SQL_NULL);
}

/** Returns nonzero if the default bit is set in nth field of rec.
@param[in]  offsets  array returned by rec_get_offsets()
@param[in]  n        index of the field
@return nonzero if default bit is set */
static inline ulint rec_offs_nth_default_low(const ulint *offsets, ulint n) {
  return (rec_offs_base(offsets)[1 + n] & REC_OFFS_DEFAULT);
}

/** Gets the physical size of a field.
@param[in]  offsets array returned by rec_get_offsets()
@param[in]  n       index of the field
@return length of field */
static inline ulint rec_offs_nth_size_low(const ulint *offsets, ulint n) {
  if (!n) {
    return (rec_offs_base(offsets)[1 + n] & REC_OFFS_MASK);
  }
  return ((rec_offs_base(offsets)[1 + n] - rec_offs_base(offsets)[n]) &
          REC_OFFS_MASK);
}

// rec_set_nth_field has been replaced by DSlice::set_nth_field
// void DSlice::set_nth_field(const dict_index_t *index, ulint n, const void
// *data,
//                            ulint len) {
//   byte *data2;
//   ulint len2;

//   ut_ad(validate());
//   ut_ad(!index || index == m_index);

//   if (index && index->has_row_versions()) {
//     n = index->get_field_off_pos(n);
//   }

//   auto fn = [&](const ulint *offsets, ulint n) {
//     ulint n_drop = 0;
//     for (size_t i = 0; i < n; i++) {
//       ulint len = rec_offs_base(offsets)[1 + i];
//       if (len & REC_OFFS_DROP) {
//         n_drop++;
//       }
//     }
//     return n_drop;
//   };

//   if (len == UNIV_SQL_NULL) {
//     if (!rec_offs_nth_sql_null_low(m_offsets, n)) {
//       ut_a(!rec_offs_comp(m_offsets));
//       ulint n_drop = rec_old_is_versioned(m_reg.rec) ? fn(m_offsets, n) : 0;
//       rec_set_nth_field_sql_null_low(m_reg.rec, n - n_drop);
//     }

//     return;
//   }

//   ut_ad(!rec_offs_nth_default_low(m_offsets, n));

//   /* nullptr for index as n is physical here */
//   data2 = get_nth_field(nullptr, n, &len2);

//   if (len2 == UNIV_SQL_NULL) {
//     ut_ad(!rec_offs_comp(m_offsets));

//     ulint n_drop = rec_old_is_versioned(m_reg.rec) ? fn(m_offsets, n) : 0;
//     rec_set_nth_field_null_bit_low(m_reg.rec, n - n_drop, false);

//     ut_ad(len == rec_get_nth_field_size_old_low(m_reg.rec, n - n_drop));
//   } else {
//     ut_ad(len2 == len);
//   }

//   ut_memcpy(data2, data, len);
// }

static inline void rec_set_nth_field_sql_null_low(rec_t *rec, ulint n) {
  ulint offset;

  offset = rec_get_field_start_offs_low(rec, n);

  data_write_sql_null(rec + offset, rec_get_nth_field_size_old_low(rec, n));

  rec_set_nth_field_null_bit_low(rec, n, true);
}

static inline void rec_set_nth_field_null_bit_low(rec_t *rec, ulint i,
                                                  bool val) {
  ulint info;

  if (rec_get_1byte_offs_flag(rec)) {
    info = rec_1_get_field_end_info_low(rec, i);

    if (val) {
      info = info | REC_1BYTE_SQL_NULL_MASK;
    } else {
      info = info & ~REC_1BYTE_SQL_NULL_MASK;
    }

    rec_1_set_field_end_info_low(rec, i, info);

    return;
  }

  info = rec_2_get_field_end_info_low(rec, i);

  if (val) {
    info = info | REC_2BYTE_SQL_NULL_MASK;
  } else {
    info = info & ~REC_2BYTE_SQL_NULL_MASK;
  }

  rec_2_set_field_end_info_low(rec, i, info);
}

static inline ulint rec_get_field_start_offs_low(const rec_t *rec, ulint n) {
  ut_ad(rec);
  ut_ad(n <= rec_get_n_fields_old_raw(rec));

  if (n == 0) {
    return (0);
  }

  if (rec_get_1byte_offs_flag(rec)) {
    return (rec_1_get_field_start_offs_low(rec, n));
  }

  return (rec_2_get_field_start_offs_low(rec, n));
}

static inline ulint rec_1_get_field_start_offs_low(const rec_t *rec, ulint n) {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields_old_raw(rec));

  if (n == 0) {
    return (0);
  }

  return (rec_1_get_prev_field_end_info(rec, n) & ~REC_1BYTE_SQL_NULL_MASK);
}

static inline ulint rec_2_get_field_start_offs_low(const rec_t *rec, ulint n) {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n <= rec_get_n_fields_old_raw(rec));

  if (n == 0) {
    return (0);
  }

  return (rec_2_get_prev_field_end_info(rec, n) &
          ~(REC_2BYTE_SQL_NULL_MASK | REC_2BYTE_EXTERN_MASK));
}

static inline ulint rec_1_get_field_end_info_low(const rec_t *rec, ulint n) {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old_raw(rec));

  uint32_t version_size = 0;
  if (rec_old_is_versioned(rec)) {
    version_size = 1;
  }

  return (
      mach_read_from_1(rec - (REC_N_OLD_EXTRA_BYTES + version_size + n + 1)));
}

static inline ulint rec_2_get_field_end_info_low(const rec_t *rec, ulint n) {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old_raw(rec));

  uint32_t version_size = 0;
  if (rec_old_is_versioned(rec)) {
    version_size = 1;
  }

  return (mach_read_from_2(rec -
                           (REC_N_OLD_EXTRA_BYTES + version_size + 2 * n + 2)));
}

static inline void rec_1_set_field_end_info_low(rec_t *rec, ulint n,
                                                ulint info) {
  ut_ad(rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old_raw(rec));

  uint32_t version_length = 0;
  if (rec_old_is_versioned(rec)) {
    version_length = 1;
  }

  mach_write_to_1(rec - (REC_N_OLD_EXTRA_BYTES + version_length + n + 1), info);
}

static inline void rec_2_set_field_end_info_low(rec_t *rec, ulint n,
                                                ulint info) {
  ut_ad(!rec_get_1byte_offs_flag(rec));
  ut_ad(n < rec_get_n_fields_old_raw(rec));

  uint32_t version_length = 0;
  if (rec_old_is_versioned(rec)) {
    version_length = 1;
  }

  mach_write_to_2(rec - (REC_N_OLD_EXTRA_BYTES + version_length + 2 * n + 2),
                  info);
}

#endif
