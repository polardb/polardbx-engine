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

/** @file include/lizard0page0dict.h
  Metadata area of Secondary Prefix Compression.

 Created 2025-09-28 by Yichang Song
 *******************************************************/

#ifndef lizard0page0dict_h
#define lizard0page0dict_h

#include <cstdint>
#include "lizard0fil0types.h"
#include "lizard0spc0types.h"
#include "mach0data.h"
#include "page0types.h"
#include "rem/rec.h"

/********************************************************************
 *      Format of Secondary Prefix Compression in a Page.
 ********************************************************************
 * SPC uses FIL_PAGE_INDEX_ENCODE Encoded page format as follows:
 * |-----------------------------|
 * | type: FIL_PAGE_INDEX_ENCODE |
 * | :-------------------------: |
 * |         Page Header         |
 * |        Index Header         |
 * |     Infimum & Supremum      |
 * |     * SPC Metadata Area *   |
 * |          *Records*          |
 * |           (free)            |
 * |      Records Directory      |
 * |           Trailer           |
 * |-----------------------------|
 *
 *********************************************************************/

/********************************************************************
 *          SPC Metadata Area
 ********************************************************************
 * The SPC Metadata Area format is as follows:
 * |--------------------------------------------|
 * |          SPC Metadata Area                 |
 * | :----------------------------------------: |
 * |    N_SLOTS (1B)                            |  Number of dictionary entries
 * |    FLAGS   (1B)                            |  Reserved, must be 0x00
 * |    Slot Dir     (n_slots * 2B)             |  offset_0 ... offset_{n-1}
 * |    Dict Data    (variable length)          |  prefix_0 ... prefix_{n-1}
 * |--------------------------------------------|
 *
 * - The pack_id of prefixes has the range of [0, 127] (PACK_ID_MAX).
 *   Max slot count is 128 (PACK_ID_MAX + 1).
 * - Slot Dir contains the end offset of each prefix content. All offsets are
 *   relative to the start of Dict Data area.
 *   - slot[i] stores the end offset of prefix_i (i.e., start offset of
 *     prefix_{i+1}).
 *   - The last slot value equals the total Dict Data size.
 * - Dict Data contains the content of all prefixes. The prefix contents are
 *   tightly packed. Each prefix can be located using:
 *   - prefix_0: starts at offset 0 in Dict Data, length = slot[0]
 *   - prefix_i (i > 0): starts at slot[i-1], length = slot[i] - slot[i-1]
 *
 *********************************************************************/

/*
 * Page Dict Layout (n = n_slots):
 * |--------------------------------------------|
 * |    N_SLOTS     (1B)                        |
 * |    FLAGS       (1B)                        |  Reserved, must be 0x00
 * |    Slot Dir    (n * 2B)                    |  offset_0 ... offset_{n-1}
 * |    Dict Data   (variable)                  |  prefix_0 ... prefix_{n-1}
 * |--------------------------------------------|
 */

/**********************************/
/** Page Dict Field */
/**********************************/
/** Page dict header. */
constexpr uint32_t PAGE_DICT_HDR = PAGE_NEW_SUPREMUM_END;

/* Page dict slot count. */
constexpr uint32_t PAGE_DICT_N_SLOTS = PAGE_DICT_HDR;

/* Page dict reserved bytes. */
constexpr uint32_t PAGE_DICT_FLAGS = PAGE_DICT_N_SLOTS + 1;

/* Page dict slot directory */
constexpr uint32_t PAGE_DICT_DIR = PAGE_DICT_FLAGS + 1;

/* We define a slot in the page dict directory as two bytes */
constexpr uint32_t PAGE_DICT_DIR_SLOT_SIZE = 2;

/** Page dict header size */
constexpr uint32_t PAGE_DICT_HDR_SIZE = 1 /* n_slots */ + 1 /* unused flags */;

/**********************************/
/** Dict Offsets
 * The offsets array is used to cache page dict metadata in memory.
 * Layout: [n_slots, dict_data_ptr, flags, slot_0, slot_1, ..., slot_{n-1}]
 * - offsets[0]: n_slots (number of dictionary entries)
 * - offsets[1]: pointer to dict data area (pd_data_t* cast to ulint)
 * - offsets[2]: flags (reserved, currently 0)
 * - offsets[3...]: slot values (end offset of each prefix in dict data)
 */
/**********************************/
constexpr uint32_t DICT_OFFS_HEADER_SIZE =
    1 /* n_slots */ + 1 /* dict_data_ptr */ + 1 /* flags */;

constexpr uint32_t DICT_OFFS_MAX_SIZE =
    DICT_OFFS_HEADER_SIZE + (PACK_ID_MAX + 1 /* MAX DICT NUMBER */);

static inline ulint *dict_offs_base(ulint *offsets) {
  return offsets + DICT_OFFS_HEADER_SIZE;
}

static inline const ulint *dict_offs_base(const ulint *offsets) {
  return offsets + DICT_OFFS_HEADER_SIZE;
}

static inline void dict_offs_reset(ulint *offsets) {
  offsets[0] = 0;
  offsets[1] = 0;
  offsets[2] = 0;
}

static inline ulint dict_offs_n_slots(const ulint *offsets) {
  return offsets[0];
}

static inline void dict_offs_store_position(const pd_data_t *data,
                                            ulint *offsets) {
  offsets[1] = (ulint)data;
}

static inline ulint dict_offs_flags(const ulint *offsets) { return offsets[2]; }

static inline ulint dict_offs_dir_size(const ulint *offsets) {
  return dict_offs_n_slots(offsets) * PAGE_DICT_DIR_SLOT_SIZE;
}

static inline ulint dict_offs_data_size(const ulint *offsets) {
  ulint n_slots = dict_offs_n_slots(offsets);
  ut_ad(n_slots <= PACK_ID_MAX + 1);
  if (n_slots == 0) {
    return 0;
  }
  return dict_offs_base(offsets)[n_slots - 1];
}

static inline ulint dict_offs_size(const ulint *offsets) {
  return dict_offs_data_size(offsets) + dict_offs_dir_size(offsets) +
         PAGE_DICT_HDR_SIZE;
}

extern bool dict_offs_validate(const pd_data_t *dict, const ulint *offsets,
                               const page_t *page);

/**********************************/
/** Page Dict function */
/**********************************/
inline pack_id_t page_dict_get_n_slots(const page_t *page) {
  ut_ad(fil_page_is_spc(page));
  pack_id_t n_slots = page[PAGE_DICT_N_SLOTS];
  /* The slot count comes from the page image; a corrupt page must crash
  here instead of overflowing the fixed-size dict offsets array in
  page_dict_get_offsets(). */
  ut_a(n_slots <= PACK_ID_MAX + 1);
  return n_slots;
}

inline void page_dict_set_n_slots(page_t *page, pack_id_t n_slots) {
  ut_ad(fil_page_is_spc(page));
  ut_ad(n_slots <= PACK_ID_MAX + 1);
  page[PAGE_DICT_N_SLOTS] = n_slots;
}

/** Set the reserved flags byte in page dict header. */
inline void page_dict_set_flags(page_t *page, const byte val) {
  page[PAGE_DICT_FLAGS] = val;
}

/** Get the reserved flags byte from page dict header.
@note Currently this field is reserved and must be 0x00. */
inline byte page_dict_get_flags(const page_t *page) {
  ut_ad(page[PAGE_DICT_FLAGS] == 0x00);
  return page[PAGE_DICT_FLAGS];
}

inline const byte *page_dict_get_dir(const page_t *page) {
  ut_ad(fil_page_is_spc(page));
  return page + PAGE_DICT_DIR;
}

inline byte *page_dict_get_dir(page_t *page) {
  ut_ad(fil_page_is_spc(page));
  return page + PAGE_DICT_DIR;
}

inline void page_dict_set_dir_slot(page_t *page, pack_id_t id, ulint val) {
  ut_ad(fil_page_is_spc(page));
  mach_write_to_2(page_dict_get_dir(page) + id * PAGE_DICT_DIR_SLOT_SIZE, val);
}

inline ulint page_dict_get_dir_slot(const page_t *page, pack_id_t id) {
  ut_ad(fil_page_is_spc(page));
  ut_ad(id < page_dict_get_n_slots(page));
  return mach_read_from_2(page_dict_get_dir(page) +
                          id * PAGE_DICT_DIR_SLOT_SIZE);
}

inline ulint page_dict_get_dir_size(const page_t *page) {
  ut_ad(fil_page_is_spc(page));
  return page_dict_get_n_slots(page) * PAGE_DICT_DIR_SLOT_SIZE;
}

inline const pd_data_t *page_dict_get_data(const page_t *page) {
  ut_ad(fil_page_is_spc(page));
  ulint dir_size = page_dict_get_dir_size(page);
  if (dir_size == 0) {
    return nullptr;
  } else {
    return page_dict_get_dir(page) + dir_size;
  }
}

inline pd_data_t *page_dict_get_data(page_t *page) {
  ut_ad(fil_page_is_spc(page));
  return const_cast<pd_data_t *>(
      page_dict_get_data(const_cast<const pd_data_t *>(page)));
}

inline const dict_rec_t *page_dict_get_nth_field(const page_t *page,
                                                 pack_id_t id, ulint *len) {
  ulint start_offs = id > 0 ? page_dict_get_dir_slot(page, id - 1) : 0;
  if (len) {
    ulint end_offs = page_dict_get_dir_slot(page, id);
    *len = end_offs - start_offs;
  }
  return page_dict_get_data(page) + start_offs;
}

inline dict_rec_t *page_dict_get_nth_field(page_t *page, pack_id_t id,
                                           ulint *len) {
  return const_cast<dict_rec_t *>(
      page_dict_get_nth_field(const_cast<const page_t *>(page), id, len));
}

inline ulint page_dict_get_data_size(const page_t *page) {
  ut_ad(fil_page_is_spc(page));

  ulint n_slots = page_dict_get_n_slots(page);

  ut_ad(n_slots <= PACK_ID_MAX + 1);
  if (n_slots == 0) return 0;
  return mach_read_from_2(page_dict_get_dir(page) +
                          (n_slots - 1) * PAGE_DICT_DIR_SLOT_SIZE);
}

inline ulint page_dict_get_size(const page_t *page) {
  if (!fil_page_is_spc(page)) return 0;
  return PAGE_DICT_HDR_SIZE + page_dict_get_dir_size(page) +
         page_dict_get_data_size(page);
}

inline byte *page_dict_get_end(page_t *page) {
  return page + PAGE_DICT_HDR + page_dict_get_size(page);
}
inline const byte *page_dict_get_end(const page_t *page) {
  return page_dict_get_end(const_cast<page_t *>(page));
}

/** Extract page dict metadata into an offsets array.
@param[in]  page     SPC page
@param[out] offsets  Output array with layout:
                     - offsets[0]: n_slots (number of dictionary entries)
                     - offsets[1]: pointer to dict data area (cast to ulint)
                     - offsets[2]: flags (reserved, currently 0)
                     - offsets[3...]: slot values (end offset of each prefix)
@return pointer to the offsets array */
inline ulint *page_dict_get_offsets(const page_t *page, ulint *offsets) {
  ulint n_slots = page_dict_get_n_slots(page);
  offsets[0] = n_slots;
  offsets[2] = page_dict_get_flags(page);

  for (ulint i = 0; i < n_slots; ++i) {
    (dict_offs_base(offsets))[i] = page_dict_get_dir_slot(page, i);
  }

  dict_offs_store_position(page_dict_get_data(page), offsets);
  return offsets;
}

extern bool page_dict_validate(const page_t *page,
                               const ulint *dict_offs = nullptr);

extern bool page_dict_equal(const page_t *page1, const page_t *page2);

extern bool page_rows_equal(const page_t *page1, const page_t *page2,
                            const dict_index_t *index);

extern ulint page_get_row_size(const page_t *page, const dict_index_t *index);

/**********************************/
/** Dict data function */
/**********************************/
static inline ulint dict_get_nth_offs(const ulint *offsets, pack_id_t id,
                                      ulint *len) {
  ulint offset;

  ut_ad(id < dict_offs_n_slots(offsets));

  if (id == 0) {
    offset = 0;
  } else {
    offset = dict_offs_base(offsets)[id - 1];
  }

  *len = dict_offs_base(offsets)[id] - offset;

  return offset;
}

static inline const byte *dict_get_nth(const pd_data_t *dict,
                                       const ulint *offsets, pack_id_t id,
                                       ulint *len) {
  ut_ad(dict_offs_validate(dict, offsets, nullptr));
  return dict + dict_get_nth_offs(offsets, id, len);
}

class page_dict_desc_t {
 public:
  page_dict_desc_t() : m_dict_data(nullptr) { dict_offs_reset(m_dict_offs); }

  page_dict_desc_t(const page_t *page) : page_dict_desc_t() {
    if (fil_page_is_spc(page)) {
      page_dict_get_offsets(page, m_dict_offs);
      m_dict_data = page_dict_get_data(page);
    }
  }

  const pd_data_t *data() const { return m_dict_data; }
  ulint *offsets() { return m_dict_offs; }

 private:
  ulint m_dict_offs[DICT_OFFS_MAX_SIZE];
  const pd_data_t *m_dict_data;
};

#endif
