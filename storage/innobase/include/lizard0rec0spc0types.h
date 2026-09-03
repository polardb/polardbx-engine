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

/** @file include/lizard0rec0spc0types.h
  Types of Record in Secondary Prefix Compression.

 Created 2025-10-13 by Yichang Song
 *******************************************************/

#ifndef lizard0rec0spc0types_h
#define lizard0rec0spc0types_h

#include "lizard0spc0types.h"

/********************************************************************
 *          Format of SPC Record
 ********************************************************************
 * The SPC Record format is as follows:
 * |-----------------------------------------|
 * |              SPC Record                 |
 * | :-------------------------------------: |
 * |      Lengths of Variable Fields         |
 * |         Nullable fields bitmap          |
 * |           *SPC Header*                  | spc header size=2/3B
 * |                Header                   | extra_size=5B
 * |-----------------------------------------| <----- rec_t
 * |                 Data                    |
 * |-----------------------------------------|
 *********************************************************************/

/********************************************************************
 *          Format of SPC Record Header
 ********************************************************************
 * The Record Header format is as follows:
 * |-------------------------------------------|
 * |            SPC Record Header              |
 * | :---------------------------------------: |
 * |    *Pack ID*             (7 bits)         |----|
 * |    *Reserved Bit*        (1 bit)          |    |
 * |    *Pack Len*            (7/15 bits)      |    |-> SPC Header
 * |    *Len Flag*            (1 bit)          |----|     (2~3 Bytes)
 * |   -------------------------------------   |
 * |    N_OWNED               (4 bits)         |----|
 * |    INFO BITS             (4 bits)         |    |
 * |      - MIN_REC_FLAG      (1 bit)          |    |
 * |      - DELETED_FLAG      (1 bit)          |    |
 * |      - VERSION_FLAG      (1 bit)          |    | -> Normal Header (5 Bytes)
 * |      - INSTANT_FLAG / *SPC_FLAG* (1 bit)  |    |
 * |    STATUS                (3 bits)         |    |
 * |    HEAP_NO               (13 bits)        |    |
 * |    REC_NEXT              (16 bits)        |----|
 * |-------------------------------------------|
 *
 * - The SPC Record has two parts: SPC Header and Normal Header.
 * - The fields marked with '*' are unique to SPC records.
 * - Pack ID: The identifier for the prefix used from the page's SPC
 *   metadata area. It is 7 bits long, with a valid range of [0, 127]
 *   (PACK_ID_MAX = 127).
 * - Reserved Bit: A 1-bit field reserved for future use.
 * - Pack Len: The length of the prefix stored in the record data. If the
 *   value fits in 7 bits (<= 127), it occupies 1 byte; otherwise, it uses
 *   2 bytes (7 bits for high-order + 8 bits for low-order = 15 bits total).
 * - Len Flag: A 1-bit flag. If set (1), Pack Len is stored in 2 bytes;
 *   if clear (0), Pack Len is stored in 1 byte.
 * - SPC_FLAG: A 1-bit flag that is set to indicate this is an SPC-encoded
 *   record. This bit is reused with the INSTANT_FLAG field (bit 7), as
 *   INSTANT_FLAG is only used for clustered index while SPC_FLAG is only
 *   used for secondary index.
 * - The other fields (N_OWNED, STATUS, HEAP_NO, REC_NEXT, and the remaining
 *   2 bits of STATUS) have the same meaning as in a standard COMPACT format
 *   record.
 *
 *********************************************************************/

/* Rec Pack ID. (lowest 7 bits)*/
constexpr uint32_t REC_PACK_ID = 6; /* This is single byte bit-field */
constexpr uint32_t REC_PACK_ID_MASK = 0x7FUL;
constexpr uint32_t REC_PACK_ID_SHIFT = 0;

/* Rec Reserved Bit. (highest 1 bit)*/
constexpr uint32_t REC_RESERVED_BIT = 6; /* This is single byte bit-field */
constexpr uint32_t REC_RESERVED_BIT_MASK = 0x80UL;
constexpr uint32_t REC_RESERVED_BIT_SHIFT = 7;


/* Rec Slice Length in the first byte. */
constexpr uint32_t REC_PACK_LEN_1BYTE = 7; /* This is single byte bit-field */

constexpr uint8_t REC_PACK_LEN_1BYTE_MASK = 0x7F;
constexpr uint8_t REC_PACK_LEN_1BYTE_SHIFT = 0;

/* Max slice length if stored only in first byte */
constexpr uint8_t REC_MAX_PACK_LEN_1BYTE = 0x7F;

/* Rec Slice length flag, indicate whether the slice length is stored in two
 * bytes
 */
/* This is single byte bit-field */
constexpr uint32_t REC_PACK_LEN_FLAG = 7;
constexpr uint32_t REC_PACK_LEN_FLAG_MASK = 0x80;
constexpr uint32_t REC_PACK_LEN_FLAG_SHIFT = 7;

/* Rec Slice Length in the second byte. */
constexpr uint32_t REC_PACK_LEN_2BYTE = 8; /* This is single byte bit-field */
constexpr uint16_t REC_PACK_LEN_2BYTE_MASK = 0xFF;
constexpr uint16_t REC_PACK_LEN_2BYTE_SHIFT = 0;

/** SPC pack information on rec. */
struct rec_pack_t {
  /** Pack id. */
  pack_id_t id;
  /** Pack length. */
  pack_len_t len;

 public:
  rec_pack_t() : id(PACK_ID_NULL), len(0) {}

  /** Default assign */
  rec_pack_t(pack_id_t id_arg, pack_len_t len_arg) : id(id_arg), len(len_arg) {}

  /** Whether it is spc record. */
  bool is_spc() const { return id != PACK_ID_NULL; }

  /** spc size on record
   *
   * @retval	size of bytes or 0
   * */
  ulint spc_size() const  {
    if (is_spc()) {
      ut_ad(len > 0 && len <= PACK_LEN_MAX);
      /** Pack id size = 1 and pack len size = [1|2] */
      return len <= REC_MAX_PACK_LEN_1BYTE ? 1 + 1 : 1 + 2;
    }
    return 0;
  }

  bool operator==(const rec_pack_t &other) const {
    return id == other.id && len == other.len;
  }
  bool operator!=(const rec_pack_t &other) const { return !(*this == other); }
};

/********************************************************************
 *          Record Offsets of SPC Record
 ********************************************************************
 * The record offsets of an SPC record reuse the standard layout:
 * |---------------------------------------------|
 * |             SPC Record Offsets              |
 * | :-----------------------------------------: |
 * |    n_alloc                    offsets[0]    |
 * |    n_fields                   offsets[1]    |
 * |    rec_t *        (debug)     offsets[2]    |
 * |    dict_index_t * (debug)     offsets[3]    |
 * | ------- rec_offs_base(offsets) -------------|
 * |    extra_size and flags  rec_offs_base[0]   |
 * |      - *extra size* (SPC header included)   |
 * |      - REC_OFFS_EXTERNAL ored to extra size |
 * |      - REC_OFFS_COMPACT  ored to extra size |
 * |    offset 1              rec_offs_base[1]   |
 * |    offset 2              rec_offs_base[2]   |
 * |    ...                                      |
 * |    offset n              rec_offs_base[n]   |
 * |---------------------------------------------|
 *
 * - n_alloc: The total number of elements allocated for the offsets array.
 *   This is the same as in standard record offsets.
 * - n_fields: The number of fields in the record. This is the same as in
 *   standard record offsets.
 * - rec_t *: Debug field containing pointer to the record. This is the same
 *   as in standard record offsets.
 * - dict_index_t *: Debug field containing pointer to the dictionary index.
 *   This is the same as in standard record offsets. (Debug builds use
 *   REC_OFFS_HEADER_SIZE = 4, release builds use 2.)
 * - rec_offs_base(offsets): Points to the base of the standard record
 *   offsets array.
 * - extra_size and flags: The first element of rec_offs_base contains size
 *   and flag information. For SPC records, this includes:
 *   - *extra size*: Total size of extra data (Lens + Nulls + *SPC header*
 *     + REC_N_NEW_EXTRA_BYTES). The SPC header size varies with the pack
 *     length encoding (2~3 bytes, see the SPC Record Header above).
 *   - REC_OFFS_EXTERNAL: Flag indicating external field storage (same as
 *     standard).
 *   - REC_OFFS_COMPACT: Flag indicating COMPACT format (same as standard).
 * - offset 1 to offset n: Byte offsets to each field, counting the
 *   compressed prefix as part of the record data. In other words, the
 *   offsets describe the LOGICAL row, as if the prefix were stored in the
 *   record itself. Whether the field content is read from the record or
 *   from the page dict is decided by comparing the field offset with the
 *   pack length (see PSlice::get_nth_field()).
 * - The pack id / pack len are NOT stored in the offsets array. They are
 *   parsed from the record header by rec_get_spc_pack() and cached in
 *   PSlice::m_rec_pack; PSlice::m_drec points to the prefix content in the
 *   page dict area.
 *
 *********************************************************************/

#endif

