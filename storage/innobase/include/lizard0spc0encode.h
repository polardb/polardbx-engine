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

/** @file include/lizard0spc0encode.h
  Secondary Prefix Compression Encode.

 Created 2025-09-29 by Yichang Song
 *******************************************************/

#ifndef lizard0spc0encode_h
#define lizard0spc0encode_h

#include <string>
#include <vector>

#include "dict0mem.h"

#include "lizard0page0dict.h"
#include "lizard0rec0spc0types.h"
#include "lizard0rem0rec.h"
#include "lizard0spc0types.h"

class Inheap_slice_ctx;

namespace lizard {

#ifdef UNIV_DEBUG
extern uint btr_cur_limit_insert_spc_encode_debug;
#define FORCE_ENCODE_DEBUG(NREC, CODE)                                  \
  if (lizard::btr_cur_limit_insert_spc_encode_debug > 1 &&              \
      (NREC) >= (ulint)lizard::btr_cur_limit_insert_spc_encode_debug) { \
    CODE;                                                               \
  }
/** Assert macro for Page_encoder with debug log */
#define SPC_ENCODER_ASSERT(encoder, condition) \
  do {                                         \
    if (!(condition)) {                        \
      (encoder)->dump_debug_log();             \
      ut_error;                                \
    }                                          \
  } while (0)
#else
#define FORCE_ENCODE_DEBUG(NREC, CODE)
#define SPC_ENCODER_ASSERT(encoder, condition)
#endif /* UNIV_DEBUG */

extern size_t cal_extra_size_after_rearrange(const Slice *slice,
                                             size_t new_prefix_len);

struct Mountain {
  /* Mountain range: [start_idx, end_idx) */
  uint16_t start_idx{UINT16_MAX};
  uint16_t end_idx{UINT16_MAX};

  uint32_t merge_sim_limit{0};

  uint16_t peak_idx{UINT16_MAX};
  uint32_t peak_height{UINT32_MAX};

  UT_LIST_NODE_T(Mountain) list;

  bool empty() const {
    return start_idx == UINT16_MAX && end_idx == UINT16_MAX &&
           peak_idx == UINT16_MAX && peak_height == UINT32_MAX;
  }
  bool completed() const {
    return start_idx != UINT16_MAX && end_idx != UINT16_MAX &&
           peak_idx != UINT16_MAX && peak_height != UINT32_MAX &&
           (end_idx - start_idx) >= 1 && (peak_idx >= start_idx) &&
           (peak_idx <= end_idx) && (peak_height >= merge_sim_limit);
  }

  void build_start(uint16_t start_idx_) {
    ut_ad(empty());
    start_idx = start_idx_;
  }

  void build_end(uint16_t end_idx_, uint32_t merge_sim_limit_) {
    end_idx = end_idx_;
    merge_sim_limit = merge_sim_limit_;
    ut_ad(completed());
  }

  void build_peak(uint16_t peak_idx_, uint32_t peak_height_) {
    peak_idx = peak_idx_;
    peak_height = peak_height_;
  }
};

class Page_encoder {
 private:
  constexpr static uint32_t PACK_MIN_LEN = 4;

  constexpr static uint32_t DICT_DATA_NORMAL_SIZE = 4096;

  /** Encoder debug log printer */
  class encode_printer {
   public:
    encode_printer() : m_enabled(false) {}

    void enable() { m_enabled = true; }
    bool is_enabled() const { return m_enabled; }

    void append(const std::string &line) const {
      if (m_enabled) {
        m_logs.push_back(line);
      }
    }

    void append(const std::ostringstream &oss) const {
      if (m_enabled) {
        std::string str = oss.str();
        if (!str.empty()) {
          m_logs.push_back(str);
        }
      }
    }

    void dump() const;

    void clear() {
      m_logs.clear();
      m_rec_data.clear();
    }

    void reserve_rec_data(size_t n) { m_rec_data.reserve(n); }
    void push_rec_data(std::vector<std::string> &&fields) {
      m_rec_data.push_back(std::move(fields));
    }
    const std::vector<std::vector<std::string>> &rec_data() const {
      return m_rec_data;
    }

   private:
    bool m_enabled;
    mutable std::vector<std::string> m_logs;
    std::vector<std::vector<std::string>> m_rec_data;
  };

 public:
  Page_encoder(ulint pd_capacity, bool enable_print = false)
      : m_pd_capacity(pd_capacity),
        m_n_recs(0),
        m_new_max_rec_size(0),
        m_new_all_rec_size(0),
        m_n_mountains(0),
        m_mountains(),
        m_dict_offs(),
        m_buf_size(DICT_DATA_NORMAL_SIZE),
        m_dict_data(m_dict_data_),
        m_n_packs(0),
        m_packs(nullptr),
        m_row_sizes(nullptr),
        m_heap(nullptr) {
    if (enable_print) {
      m_printer.enable();
    }
  }

  ~Page_encoder() {
    if (m_heap) {
      mem_heap_free(m_heap);
      m_heap = nullptr;
    }
  }

  ulint build(const page_t *page, const dict_index_t *index, ulint n_ins_recs);

  void page_copy_rec_list_end_no_locks(
      buf_block_t *new_block, /*!< in: index page to copy to */
      buf_block_t *block,     /*!< in: index page of rec */
      rec_t *rec,             /*!< in: record on page */
      dict_index_t *index,    /*!< in: record descriptor */
      mtr_t *mtr);            /*!< in: mtr */

  const pd_data_t *dict_data() const;

  ulint *dict_offsets();

 private:
  /** Build "mountains" of similarity curve between records by traversing them
   * @param[in] page index page
   * @param[in] index record descriptor
   */
  void build_mountains(const page_t *page, const dict_index_t *index);

  /** Merge mountains from right to left to maximize storage savings, based on
   * benefit analysis. */
  void merge_mountains();

  /** Build page dicts and decide rec pack for every records.
   * @param[in] page index page
   * @param[in] index record descriptor
   */
  void build_dicts_and_packs(const page_t *page, const dict_index_t *index);

 private:
  /** Common prefix length between two slices.
   * @param[in] s1 slice 1
   * @param[in] s2 slice 2
   * @return common prefix length
   */
  pack_len_t common_prefix(const Slice *s1, const Slice *s2) const;

  /** Common prefix length between the slice and the dict.
   * @param[in] s slice
   * @param[in] dict_id ID of the dict
   * @return common prefix length
   */
  pack_len_t common_prefix(const Slice *s, pack_id_t dict_id) const;

  /** Climb next mountain, prepare new pack id and Mountain object.
   * @return pair of pack id and mountain object */
  std::pair<pack_id_t, Mountain *> climb_next();

  /** Caculate benefit between merge and no merge.
   @param[in] cur current mountain
   @param[in] left left mountain
   @return true if merge is beneficial */
  bool is_beneficial_from_mount_merge(const Mountain *cur,
                                      const Mountain *left) const;

  /** Merge two mountains if beneficial.
   @param[in] cur current mountain
   @param[in] left left mountain
   @param[in] force_merge force merge
   @return true if merge is done */
  bool try_merge_mount(Mountain *cur, Mountain *left, bool force_merge);

  /** Build one dict.
   * @param[in] dict_id dict id
   * @param[in] len dict length
   * @param[in] slice the slice that offers dict contents
   */
  void build_dict(pack_id_t dict_id, ulint len, const Slice *slice);

  /** Build one rec pack.
   * @param[in] cur_idx current record index
   * @param[in] new_dict_id new dict id
   * @param[in] new_pack_len new rec pack length
   */
  void build_pack(uint16_t cur_idx, pack_id_t dict_id, pack_len_t pack_len);

  pack_id_t get_n_slots() const { return dict_offs_n_slots(m_dict_offs); }

  ulint get_max_insert_size(const page_t *page, ulint n_recs) const;

#ifdef UNIV_DEBUG
  bool validate_mountains() const;
  bool validate_packs(const page_t *page, const dict_index_t *index) const;
#endif
  void dump_mountains_with_sim(const char *tag) const;

 public:
  void dump_debug_log() const { m_printer.dump(); }

  ulint get_new_all_rec_size() const { return m_new_all_rec_size; }
  const ulint *dict_offsets() const { return m_dict_offs; }

 private:
  encode_printer m_printer;
  ulint m_pd_capacity;
  ulint m_n_recs;

  uint16_t m_new_max_rec_size;
  ulint m_new_all_rec_size;

  uint16_t m_n_mountains;
  UT_LIST_BASE_NODE_T(Mountain, list) m_mountains;

  ulint m_dict_offs[DICT_OFFS_MAX_SIZE];
  ulint m_buf_size;
  pd_data_t m_dict_data_[DICT_DATA_NORMAL_SIZE];
  pd_data_t *m_dict_data;

  uint16_t m_n_packs;
  rec_pack_t *m_packs;

  uint32_t *m_next_sims;
  uint32_t *m_peak_sims;
  uint16_t *m_row_sizes;

  mem_heap_t *m_heap;
};

extern Slice *rec_decode_to_buf(Slice *slc, Slice_ctx *slc_ctx, boc_t &buf,
                                ulint &buf_len, mem_heap_t **heap,
                                const mtr_t *mtr);

} /* namespace lizard */

#endif
