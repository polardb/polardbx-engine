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
ANY WARRANTY; without even the lizardplied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License,
version 2.0, for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/lizard0rem0lrec.h
 Record manager.
 This file contains low level functions which deals with physical index of
 fields in a physical record.

 Created 2025-01-21 by Jiyang.zhang
 *******************************************************/
#include "rem0rec.h"
#include "rem0lrec.h"

namespace lizard {

static inline bool rec_panda_contains_null_in_unique_old_1byte(
    const dict_index_t *index, const rec_t *rec) {
  ulint i = 0;
  ulint offs = REC_N_OLD_EXTRA_BYTES;
  auto n_fields = rec_get_n_fields_old(rec, index);
  auto n_unique = dict_index_get_n_unique(index);
  /** If infimum or supremum, n_fields should be 1. */
  if (n_unique < n_fields) {
    n_fields = n_unique;
  }

  ut_ad(!index->has_instant_cols_or_row_versions());

  ut_a(!rec_old_is_versioned(rec));

  ut_ad(dict_index_is_panda(index));

  do {
    /* i is physical pos here */
    offs = rec_1_get_field_end_info_low(rec, i);

    if (offs & REC_1BYTE_SQL_NULL_MASK) {
      return true;
    }

  } while (++i < n_fields);

  return false;
}

static inline bool rec_panda_contains_null_in_unique_old_2byte(
    const dict_index_t *index, const rec_t *rec) {
  ulint i = 0;
  ulint offs = REC_N_OLD_EXTRA_BYTES;
  auto n_fields = rec_get_n_fields_old(rec, index);
  auto n_unique = dict_index_get_n_unique(index);
  /** If infimum or supremum, n_fields should be 1. */
  if (n_unique < n_fields) {
    n_fields = n_unique;
  }

  ut_ad(!rec_old_is_versioned(rec));

  do {
    /* i is physical pos here */
    offs = rec_2_get_field_end_info_low(rec, i);
    if (offs & REC_2BYTE_SQL_NULL_MASK) {
      return true;
    }
  } while (++i < n_fields);

  return false;
}

static inline bool rec_panda_contains_null_in_unique_old(const dict_index_t *index,
                                           const rec_t *rec) {
  ut_a(!rec_old_is_versioned(rec));

  if (rec_get_1byte_offs_flag(rec)) {
    return rec_panda_contains_null_in_unique_old_1byte(index, rec);
  } else {
    return rec_panda_contains_null_in_unique_old_2byte(index, rec);
  }
}

static inline bool rec_panda_contains_null_in_unique_new(
    const dict_index_t *index, const rec_t *rec) {

  const byte *nulls = nullptr;
  const byte *lens = nullptr;
  uint16_t n_null = 0;
  ulint status;
  enum REC_INSERT_STATE rec_insert_state = REC_INSERT_STATE::NONE;
  uint8_t row_version = UINT8_UNDEFINED;
  uint16_t non_default_fields = 0;

  auto n_unique = dict_index_get_n_unique(index);

  ut_ad(dict_index_is_panda(index));
  ut_ad(dict_table_is_comp(index->table));

  status = rec_get_status(rec);
  switch (status) {
    case REC_STATUS_ORDINARY:
      break;
    case REC_STATUS_NODE_PTR:
      /* For R-tree, we need to copy the child page number field. */
      ut_ad(!dict_index_is_spatial(index));
      break;
    case REC_STATUS_INFIMUM:
    case REC_STATUS_SUPREMUM:
      /* infimum or supremum record: no sense to copy anything */
      return false;
    default:
      ut_error;
  }

  rec_insert_state = rec_init_null_and_len_comp(
      rec, index, &nulls, &lens, &n_null, non_default_fields, row_version);

  switch (rec_insert_state) {
    case INSERTED_INTO_TABLE_WITH_NO_INSTANT_NO_VERSION:
      ut_ad(!index->has_instant_cols_or_row_versions());
      break;
    default:
      ut_error;
  }

  /* read the lengths of fields 0..n */
  ulint null_mask = 1;
  uint16_t i = 0;
  do {
    /* Fields are stored on disk in version they are added in and are
    maintained in fields_array in the same order. Get the right field. */
    const dict_field_t *field = index->get_physical_field(i);
    const dict_col_t *col = field->col;

    if (!(col->prtype & DATA_NOT_NULL)) {
      /* nullable field => read the null flag */
      ut_ad(n_null--);

      if (UNIV_UNLIKELY(!(byte)null_mask)) {
        nulls--;
        null_mask = 1;
      }

      if (*nulls & null_mask) {
        null_mask <<= 1;
        /* No length is stored for NULL fields.
        We do not advance offs, and we set
        the length to zero and enable the
        SQL NULL flag in offsets[]. */
        // len = offs | REC_OFFS_SQL_NULL;
        // goto resolved;
        return true;
      }
      null_mask <<= 1;
    }
  } while (++i < n_unique);

  return false;
}

static inline bool rec_panda_contains_null_in_unique(const dict_index_t *index,
                                                     const rec_t *rec) {
  if (dict_table_is_comp(index->table)) {
    return rec_panda_contains_null_in_unique_new(index, rec);
  } else {
    return rec_panda_contains_null_in_unique_old(index, rec);
  }
}
}

