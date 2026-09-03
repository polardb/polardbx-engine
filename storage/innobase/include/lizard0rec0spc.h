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

/** @file include/lizard0rec0spc.h
  Record of Secondary Prefix Compression.

 Created 2025-09-29 by Yichang Song
 *******************************************************/

#ifndef lizard0rec0spc_h
#define lizard0rec0spc_h

#include "rem0rec.h"

#include "lizard0rec0spc0types.h"

/** Retrieves the SPC pack id of a new-style record.

 * @param[in] rec The record to retrieve the SPC pack ID from.
 * @param[in] index dict index

 * @return SPC prefix ID */
static inline pack_id_t rec_get_spc_pack_id(const rec_t *rec,
                                            const dict_index_t *index) {
  ut_ad(rec_get_spc_flag(rec, index));

  ut_ad(rec_get_bit_field_1(rec, REC_RESERVED_BIT, REC_RESERVED_BIT_MASK,
                            REC_RESERVED_BIT_SHIFT) == 0);

  return rec_get_bit_field_1(rec, REC_PACK_ID, REC_PACK_ID_MASK,
                             REC_PACK_ID_SHIFT);
}

/** Retrieves the SPC pack length of a new-style record.

 * @param[in] rec         The record to retrieve the SPC slice length from.
 * @param[in] index       dict index
 * @param[out] byte_count The number of bytes used by the SPC slice length.

 * @return SPC pack length */
static inline pack_len_t rec_get_spc_pack_len(const rec_t *rec,
                                              const dict_index_t *index,
                                              ulint *bytes) {
  size_t size = 0;
  ut_ad(rec_get_spc_flag(rec, index));

  /** First byte */
  pack_len_t pack_len =
      rec_get_bit_field_1(rec, REC_PACK_LEN_1BYTE, REC_PACK_LEN_1BYTE_MASK,
                          REC_PACK_LEN_1BYTE_SHIFT);
  size++;

  /** Second byte */
  if (rec_get_bit_field_1(rec, REC_PACK_LEN_FLAG, REC_PACK_LEN_FLAG_MASK,
                          REC_PACK_LEN_FLAG_SHIFT)) {
    pack_len <<= 8;
    pack_len |=
        rec_get_bit_field_1(rec, REC_PACK_LEN_2BYTE, REC_PACK_LEN_2BYTE_MASK,
                            REC_PACK_LEN_2BYTE_SHIFT);
    size++;
  }

  if (bytes) {
    *bytes = size;
  }
  return pack_len;
}

/** Retrieves the SPC pack length of a new-style record.

 * @param[in] rec         The record to retrieve the SPC slice length from.
 * @param[in] index       dict index
 * @param[out] byte_count The number of bytes used by the SPC slice length.

 * @return SPC pack length */
static inline pack_len_t rec_get_spc_pack_len(const rec_t *rec,
                                              const dict_index_t *index) {
  return rec_get_spc_pack_len(rec, index, nullptr);
}

/** Sets the SPC pack id of a new-style record.

 * @param[in,out] rec The record to set the SPC prefix ID to.
 * @param[in] index dict index
 * @param[in] prefix_id The SPC prefix ID to set. */

static inline void rec_set_spc_pack_id(rec_t *rec, const dict_index_t *index,
                                pack_id_t pack_id) {
  ut_ad(rec_get_spc_flag(rec, index));
  ut_ad(pack_id <= PACK_ID_MAX);

  rec_set_bit_field_1(rec, pack_id, REC_PACK_ID, REC_PACK_ID_MASK,
                      REC_PACK_ID_SHIFT);

  rec_set_bit_field_1(rec, 0, REC_RESERVED_BIT, REC_RESERVED_BIT_MASK,
                      REC_RESERVED_BIT_SHIFT);
}

/** Sets the SPC slice length of a new-style record.

 * @param[in,out] rec The record to set the SPC slice length to.
 * @param[in] index dict index
 * @param[in] slice_len The SPC slice length to set. */

static inline void rec_set_spc_pack_len(rec_t *rec, const dict_index_t *index,
                                        pack_len_t pack_len) {
  ut_ad(rec_get_spc_flag(rec, index));
  ut_ad(pack_len > 0 && pack_len <= PACK_LEN_MAX);

  if (pack_len <= REC_MAX_PACK_LEN_1BYTE) {
    rec_set_bit_field_1(rec, pack_len, REC_PACK_LEN_1BYTE,
                        REC_PACK_LEN_1BYTE_MASK, REC_PACK_LEN_1BYTE_SHIFT);

    rec_set_bit_field_1(rec, 0, REC_PACK_LEN_FLAG, REC_PACK_LEN_FLAG_MASK,
                        REC_PACK_LEN_FLAG_SHIFT);
  } else {
    rec_set_bit_field_1(rec, pack_len >> 8, REC_PACK_LEN_1BYTE,
                        REC_PACK_LEN_1BYTE_MASK, REC_PACK_LEN_1BYTE_SHIFT);

    rec_set_bit_field_1(rec, 1, REC_PACK_LEN_FLAG, REC_PACK_LEN_FLAG_MASK,
                        REC_PACK_LEN_FLAG_SHIFT);
    rec_set_bit_field_1(rec, pack_len & 0xFF, REC_PACK_LEN_2BYTE,
                        REC_PACK_LEN_2BYTE_MASK, REC_PACK_LEN_2BYTE_SHIFT);
  }
}

/** Mark spc flag and set pack id/len on record. */
static inline void rec_set_spc(rec_t *rec, const dict_index_t *index,
                               const rec_pack_t &pack) {
  ut_ad(pack.is_spc());

  rec_set_spc_flag(rec, index, true);
  /* Pack id and length match flag (1 byte) */
  rec_set_spc_pack_id(rec, index, pack.id);
  /* Pack len (1/2 bytes) */
  rec_set_spc_pack_len(rec, index, pack.len);
}

/** Retrieves the SPC pack of a new-style record.

 * @param[in] rec         The record to retrieve the SPC slice length from.
 * @param[in] index       dict index
 *
 * retval	spc pack id and pack len.
 * */
static inline rec_pack_t rec_get_spc_pack(const rec_t *rec,
                                          const dict_index_t *index) {
  rec_pack_t rec_pack;
  /** SPC record. */
  if (rec_get_spc_flag(rec, index)) {
    rec_pack = {rec_get_spc_pack_id(rec, index),
                rec_get_spc_pack_len(rec, index)};
  }

  return rec_pack;
}

/** Retrieves the SPC size (bytes) of a new-style record.

 * @param[in] rec         The record.
 * @param[in] index       dict index
 *
 * retval	spc size on record.
 * */
static inline ulint rec_get_spc_extra_size(const rec_t *rec,
                                           const dict_index_t *index) {
  return rec_get_spc_pack(rec, index).spc_size();
}

/** Estimate SPC size on record by pack length.
 *
 * @param[in]	rec pack length
 *
 * retval	spc size on record.
 * */
static inline ulint rec_eval_spc_extra_size(const pack_len_t len) {
  ut_ad(len >= 0 && len <= PACK_LEN_MAX);

  if (len == 0) return 0;

  rec_pack_t pack = {0, len};
  return pack.spc_size();
}

#endif

