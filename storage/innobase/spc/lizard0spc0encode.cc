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

/** @file spc/lizard0spc0encode.cc
  Secondary Prefix Compression Encode.

 Created 2025-10-14 by Yichang Song
 *******************************************************/
#include <cctype>
#include <cstring>

#include "btr0btr.h"
#include "mysqld_error.h"
#include "page0cur.h"
#include "rem0rec.h"
#include "ut0lst.h"
#include "ut0mem.h"

#include "lizard0btr0btr.h"
#include "lizard0log.h"
#include "lizard0page0dict.h"
#include "lizard0rec0spc.h"
#include "lizard0rem0slc.h"
#include "lizard0rem0slc0ctx.h"
#include "lizard0spc0encode.h"
#include "lizard0spc0types.h"

namespace lizard {

#ifdef UNIV_DEBUG
uint btr_cur_limit_insert_spc_encode_debug = 0;
#endif /* UNIV_DEBUG */

/*----------------------  Static helper functions  -------------------- */
/** Calculate the size of the first n fields. */
static inline size_t cal_first_n_field_size(const Slice *slc, ulint n) {
  size_t size = 0;
  ulint len;
  for (ulint i = 0; i < n; i++) {
    len = slc->get_nth_field_len(slc->index(), i);
    ut_ad(len != UNIV_SQL_ADD_COL_DEFAULT && len != UNIV_SQL_INSTANT_DROP_COL);
    if (len == UNIV_SQL_NULL) len = 0;
    size += len;
  }
  return size;
}

static Slice *rec_encode_to_buf(Slice *old, const rec_pack_t &new_pack,
                                const page_t *new_page,
                                const dict_index_t *index,
                                Inheap_slice_ctx *inheap_slc_ctx, byte *buf) {
  reg_t reg;
  size_t extra_size;
  const rec_pack_t old_pack = old->rec_pack();

  if (!old_pack.is_spc() && !new_pack.is_spc()) {
    return old;
  }

  extra_size = old->extra_size() - old_pack.spc_size() + new_pack.spc_size();
  reg = {buf + extra_size, MLOC_IN_HEAP};

  /* 1. lens and nulls */
  ut_memcpy(
      buf, old->rec() - old->extra_size(),
      old->extra_size() - REC_N_NEW_EXTRA_BYTES - old_pack.spc_size());

  /* 2. normal compact extra header. (5 bytes) */
  ut_memcpy(reg.rec - REC_N_NEW_EXTRA_BYTES, old->rec() - REC_N_NEW_EXTRA_BYTES,
            REC_N_NEW_EXTRA_BYTES);
  rec_set_spc_flag(reg.rec, index, false);

  /* 3. write spc pack */
  if (new_pack.is_spc()) {
    rec_set_spc(reg.rec, index, new_pack);
  }

  /* 4. data excluded prefix */
  if (new_pack.len < old_pack.len) {
    size_t i = new_pack.len;
    /** Copy data [new_prefix_len, old_prefix_len) */
    while (i < old_pack.len) {
      reg.rec[i - new_pack.len] = (*old)[i];
      ++i;
    }
    /** Copy data [old_prefix_len, data_end] */
    ut_memcpy(&(reg.rec[i - new_pack.len]), old->rec(), old->data_size());
  } else {
    size_t delta = new_pack.len - old_pack.len;
    ut_memcpy(reg.rec, old->rec() + delta, old->data_size() - delta);
  }

  /** 6. Get offsets for new record. */
  return inheap_slc_ctx->get_offsets(reg, new_page, index, ULINT_UNDEFINED);
}

Slice *rec_decode_to_buf(Slice *slc, Slice_ctx *slc_ctx, boc_t &buf,
                         ulint &buf_size, mem_heap_t **heap, const mtr_t *mtr) {
  ulint row_size;

  if (slc->contiguous()) {
    return slc;
  }

  row_size = slc->row_size();

  ut_ad(slc->reg().in_page());
  ut_ad(mtr->is_active());
  ut_ad(mtr->get_log_mode() == MTR_LOG_ALL ||
        mtr->get_log_mode() == MTR_LOG_NO_REDO);
  ut_ad(!slc->index()->table->is_compressed());

  if (buf.is_null() || buf_size < row_size) {
    /** (row_size/2) is preallocated size to avoid realloc. */
    buf_size = row_size + (row_size / 2);

    if (!(*heap)) {
      *heap = mem_heap_create(buf_size, UT_LOCATION_HERE);
    }

    buf = mem_heap_alloc_boc(*heap, buf_size);
  }

  return row_copy(buf, slc, slc_ctx);
}

void Page_encoder::encode_printer::dump() const {
  if (!m_enabled || (m_logs.empty() && m_rec_data.empty())) {
    return;
  }
  lizard_error(ER_LIZARD)
      << "[spc] ========== SPC Encoder Debug Log ==========";
  for (const auto &log : m_logs) {
    lizard_error(ER_LIZARD) << log;
  }
  for (size_t i = 0; i < m_rec_data.size(); ++i) {
    const std::vector<std::string> &fields = m_rec_data[i];
    std::ostringstream oss;
    oss << "[spc] rec[" << i << "] fields(" << fields.size() << ")=[";
    for (size_t f = 0; f < fields.size(); ++f) {
      for (unsigned char c : fields[f]) {
        oss << (std::isprint(c) ? (char)c : '.');
      }
      if (f + 1 < fields.size()) oss << '|';
    }
    oss << "]";
    lizard_error(ER_LIZARD) << oss.str();
  }
  lizard_error(ER_LIZARD) << "[spc] ========== End of Debug Log ==========";
}

#ifdef UNIV_DEBUG
bool Page_encoder::validate_mountains() const {
  if (m_n_mountains == 0) {
    return true;
  }

  uint16_t last_end_idx = 0;
  Mountain *mount = UT_LIST_GET_FIRST(m_mountains);
  for (uint16_t i = 0; i < m_n_mountains; i++) {
    SPC_ENCODER_ASSERT(this, mount != nullptr && mount->completed());
    SPC_ENCODER_ASSERT(this, mount->start_idx == last_end_idx);

    if (i == 0) {
      SPC_ENCODER_ASSERT(this, mount->start_idx == 0);
    }

    /* Validate sims. */
    for (uint16_t j = mount->start_idx; j < mount->end_idx; j++) {
      SPC_ENCODER_ASSERT(this, m_peak_sims[j] <= mount->peak_height);
    }

    last_end_idx = mount->end_idx;
    mount = UT_LIST_GET_NEXT(list, mount);
  }

  return true;
}

bool Page_encoder::validate_packs(const page_t *page,
                                  const dict_index_t *index) const {
  Slice_ctx sctx;
  Slice *slc;
  creg_t cur_reg = page_rec_get_next_creg(page_get_infimum_rec(page));
  uint16_t cur_idx = 0;
  pack_id_t cur_pack_id;
  pack_len_t cur_pack_len;

  while (!page_rec_is_supremum(cur_reg.rec)) {
    slc = sctx.get_offsets(cur_reg, index, ULINT_UNDEFINED);
    cur_pack_id = m_packs[cur_idx].id;
    cur_pack_len = m_packs[cur_idx].len;
    if (cur_pack_id != PACK_ID_NULL) {
      pack_len_t common_prefix_len = common_prefix(slc, cur_pack_id);
      SPC_ENCODER_ASSERT(this, cur_pack_len >= PACK_MIN_LEN &&
                                   cur_pack_len <= common_prefix_len);
    }
    cur_idx++;
    cur_reg = page_rec_get_next_creg(cur_reg.rec);
  }
  SPC_ENCODER_ASSERT(this, cur_idx == m_n_packs);
  return true;
}

#endif

void Page_encoder::dump_mountains_with_sim(const char *tag) const {
  if (!m_printer.is_enabled()) return;

  constexpr ulint kPerLine = 16;

  std::string tag_str = std::string("[spc] ") + tag;
  std::string indent(tag_str.length() + 1, ' ');

  std::ostringstream header;
  header << tag_str << " mountains count=" << m_n_mountains
         << " n_recs=" << m_n_recs;
  m_printer.append(header.str());

  size_t idx = 0;
  for (Mountain *mount = UT_LIST_GET_FIRST(m_mountains); mount != nullptr;
       mount = UT_LIST_GET_NEXT(list, mount)) {
    {
      std::ostringstream oss;
      oss << indent << "mountain[" << idx << "] "
          << "range=[" << mount->start_idx << "," << mount->end_idx << ")"
          << " merge_sim_limit=" << mount->merge_sim_limit
          << " peak_idx=" << mount->peak_idx
          << " peak_height=" << mount->peak_height;

      /* Print peak record prefix bytes from pre-built record data. */
      uint16_t pidx = mount->peak_idx;
      if (mount->peak_height > 0 && pidx < m_printer.rec_data().size()) {
        const std::vector<std::string> &fields = m_printer.rec_data()[pidx];
        oss << " prefix(" << mount->peak_height << ")=[";
        size_t remaining = mount->peak_height;
        for (const std::string &f : fields) {
          size_t take = std::min(remaining, f.size());
          for (size_t k = 0; k < take; ++k) {
            unsigned char c = (unsigned char)f[k];
            oss << (std::isprint(c) ? (char)c : '.');
          }
          remaining -= take;
          if (remaining == 0) break;
        }
        oss << "]";
      }

      m_printer.append(oss.str());
    }

    for (ulint i = mount->start_idx; i < mount->end_idx; i += kPerLine) {
      ulint line_end = std::min(i + kPerLine, (ulint)mount->end_idx) - 1;

      std::ostringstream next_sim;
      next_sim << indent << "  next_sim[" << i << ".." << line_end << "] ";
      for (ulint j = i; j <= line_end && j < mount->end_idx; ++j) {
        next_sim << m_next_sims[j];
        if (j < line_end && j + 1 < mount->end_idx) {
          next_sim << " ";
        }
      }
      m_printer.append(next_sim);

      std::ostringstream peak_sim;
      peak_sim << indent << "  peak_sim[" << i << ".." << line_end << "] ";
      for (ulint j = i; j <= line_end && j < mount->end_idx; ++j) {
        peak_sim << m_peak_sims[j];
        if (j < line_end && j + 1 < mount->end_idx) {
          peak_sim << " ";
        }
      }
      m_printer.append(peak_sim);
    }
    ++idx;
  }
}

/** Common prefix length between two slices.
 * @param[in] s1 slice 1
 * @param[in] s2 slice 2
 * @return common prefix length
 */
pack_len_t Page_encoder::common_prefix(const Slice *s1, const Slice *s2) const {
  size_t size1;
  size_t size2;
  size_t cmp_size;
  size_t common_size;

  if (page_rec_is_supremum(s2->rec())) {
    return 0;
  }
  SPC_ENCODER_ASSERT(this, !page_rec_is_infimum(s1->rec()) &&
                               !page_rec_is_infimum(s2->rec()) &&
                               !page_rec_is_supremum(s1->rec()) &&
                               !page_rec_is_supremum(s2->rec()));
  /* Compression should not cross the system fields. */
  size1 =
      cal_first_n_field_size(s1, dict_index_get_n_unique_in_tree(s1->index()));
  size2 =
      cal_first_n_field_size(s2, dict_index_get_n_unique_in_tree(s2->index()));
  cmp_size = std::min(size1, size2);

  common_size = 0;
  for (size_t i = 0; i < cmp_size; ++i) {
    if ((*s1)[i] == (*s2)[i]) {
      common_size++;
    } else {
      break;
    }
  }

  return (common_size < PACK_MIN_LEN) ? 0 : common_size;
}

/** Common prefix length between the slice and the dict.
 * @param[in] s slice
 * @param[in] dict_id ID of the dict
 * @return common prefix length
 */
pack_len_t Page_encoder::common_prefix(const Slice *s,
                                       pack_id_t dict_id) const {
  size_t cmp_size;
  size_t common_size;

  const byte *dict;
  size_t dict_len;

  SPC_ENCODER_ASSERT(
      this, !page_rec_is_infimum(s->rec()) && !page_rec_is_supremum(s->rec()));

  dict = dict_get_nth(m_dict_data, m_dict_offs, dict_id, &dict_len);

  /* Compression should not cross the system fields. */
  cmp_size = std::min(
      cal_first_n_field_size(s, dict_index_get_n_unique_in_tree(s->index())),
      (size_t)dict_len);

  common_size = 0;
  for (size_t i = 0; i < cmp_size; ++i) {
    if ((*s)[i] == *(dict + i)) {
      common_size++;
    } else {
      break;
    }
  }

  return (common_size < PACK_MIN_LEN) ? 0 : common_size;
}

/** Climb next mountain, prepare new pack id and Mountain object.
 * @return pair of pack id and mountain object */
std::pair<pack_id_t, Mountain *> Page_encoder::climb_next() {
  if (m_n_mountains == PACK_ID_MAX) {
    return std::make_pair(PACK_ID_NULL, nullptr);
  }

  void *mem = mem_heap_alloc(m_heap, sizeof(Mountain));
  auto *node = new (mem) Mountain();
  UT_LIST_ADD_LAST(m_mountains, node);
  m_n_mountains++;
  return std::make_pair(m_n_mountains, node);
}

/** Build "mountains" of similarity curve between records by traversing them
 * @param[in] page index page
 * @param[in] index record descriptor
 */
void Page_encoder::build_mountains(const page_t *page,
                                   const dict_index_t *index) {
  bool climbing;
  pack_id_t cur_dict_id;
  Mountain *cur_mount;

  Slice_ctx slice_ctx[2];
  Slice *cur;
  Slice *next;
  unsigned int cur_i = 0;

  size_t prev_sim;
  size_t next_sim;

  /** Init first record. */
  std::tie(cur_dict_id, cur_mount) = climb_next();
  cur_mount->build_start(0);
  climbing = true;

  prev_sim = 0;
  cur = slice_ctx[cur_i].get_offsets(
      page_rec_get_next_creg(page_get_infimum_rec(page)), index,
      ULINT_UNDEFINED);

  while (!page_rec_is_supremum(cur->rec()) && cur_dict_id != PACK_ID_NULL) {
    /** Get next slice */
    next = slice_ctx[(cur_i + 1) % 2].get_offsets(
        page_rec_get_next_creg(cur->rec()), index, ULINT_UNDEFINED);

    /** 1. Try to find the next prefix on the public area. */
    next_sim = common_prefix(cur, next);
    m_next_sims[cur_i] = next_sim;
    m_row_sizes[cur_i] = cur->row_size();
    if (climbing) {
      /** Climb the similarity curve until reach the crest. */
      if (prev_sim > next_sim) {
        /** Find the crest of the similarity curve, cur[prev_sim] is
        stored prefix */
        cur_mount->build_peak(cur_i - 1, prev_sim);
        climbing = false;
      }
    } else {
      /** Go down the similarity curve until reach the trough. */
      if (prev_sim < next_sim) {
        /**
          Find the trough of the similarity curve. Climb the next mountain.
        */
        cur_mount->build_end(cur_i - 1, m_next_sims[cur_i - 2]);
        std::tie(cur_dict_id, cur_mount) = climb_next();
        if (cur_dict_id != PACK_ID_NULL) {
          cur_mount->build_start(cur_i - 1);
        }
        climbing = true;
      }
    }
    /** 2. End current record, and go next record. */
    cur = next;
    cur_i++;
    prev_sim = next_sim;
  }

  if (cur_dict_id != PACK_ID_NULL) {
    /* Finish the last uncompleted mountain. */
    SPC_ENCODER_ASSERT(this, page_rec_is_supremum(cur->rec()));
    SPC_ENCODER_ASSERT(this, !cur_mount->completed() && !cur_mount->empty());
    if (cur_mount->peak_height == UINT32_MAX) {
      cur_mount->build_peak(cur_i - 1, prev_sim);
    }
    cur_mount->build_end(cur_i, 0);
  }

  dump_mountains_with_sim("build_mountains end");

  SPC_ENCODER_ASSERT(this, validate_mountains());

  /* Notice there will be some uncomputed records at the end of the page if max
   * dict id has reached PACK_ID_NULL. */
}

/** Caculate benefit between merge and no merge.
 @param[in] cur current mountain
 @param[in] left left mountain
 @return true if merge is beneficial */

/* KNOWN ISSUE, deliberately unfixed: benefit_from_self is unsigned, so
 * subtracting peak_height wraps to a huge value whenever the mountain's
 * own benefit is mathematically negative (e.g. a narrow mountain whose
 * non-peak records save almost nothing). The comparison below then always
 * rejects the merge exactly when merging would be beneficial, leaving a
 * dict entry that costs more than it saves. The produced encoding is
 * still valid, only suboptimal. Fixing the arithmetic changes build()
 * output for pages that were encoded successfully, which would break redo
 * replay determinism (btr_parse_page_encode requires the rebuilt max
 * insert size to match the logged one). Since SPC is already GA, the fix
 * needs a new redo log type and is deferred. */
bool Page_encoder::is_beneficial_from_mount_merge(const Mountain *cur,
                                                  const Mountain *left) const {
  if (left == nullptr) return false;
  SPC_ENCODER_ASSERT(this, m_next_sims[cur->peak_idx] == cur->peak_height);
  /* Calculate benefit from self. */
  uint32_t benefit_from_self = 0;
  for (uint16_t i = cur->start_idx; i <= cur->peak_idx; i++) {
    uint32_t sim = m_next_sims[i];
    SPC_ENCODER_ASSERT(this, sim <= cur->peak_height);
    benefit_from_self += (sim - rec_eval_spc_extra_size(sim));
  }
  for (uint16_t i = cur->peak_idx + 1; i < cur->end_idx; i++) {
    uint32_t sim = m_next_sims[i - 1];
    SPC_ENCODER_ASSERT(this, sim <= cur->peak_height);
    benefit_from_self += (sim - rec_eval_spc_extra_size(sim));
  }
  benefit_from_self -= cur->peak_height;

  /* Calculate benefit from merge. */
  uint32_t benefit_from_merge = 0;
  SPC_ENCODER_ASSERT(this, cur->start_idx == left->end_idx);
  uint32_t max_sim = left->merge_sim_limit;

  for (uint16_t i = cur->start_idx; i < cur->end_idx; i++) {
    uint32_t sim = std::min(m_next_sims[i - 1], max_sim);
    benefit_from_merge += (sim - rec_eval_spc_extra_size(sim));
  }

  return benefit_from_merge > benefit_from_self;
}

/** Merge two mountains if beneficial.
 @param[in] cur current mountain
 @param[in] left left mountain
 @param[in] force_merge force merge
 @return true if merge is done */
bool Page_encoder::try_merge_mount(Mountain *cur, Mountain *left,
                                   bool force_merge) {
  bool do_merge = force_merge || is_beneficial_from_mount_merge(cur, left);

  if (do_merge) {
    uint32_t sim_limit = left->merge_sim_limit;
    for (uint16_t i = cur->start_idx; i < cur->end_idx; i++) {
      sim_limit = std::min(m_next_sims[i - 1], sim_limit);
      m_peak_sims[i] = sim_limit;
    }
    left->end_idx = cur->end_idx;
    left->merge_sim_limit = sim_limit;

  } else {
    for (uint16_t i = cur->start_idx; i <= cur->peak_idx; i++) {
      m_peak_sims[i] = m_next_sims[i];
    }
    for (uint16_t i = cur->peak_idx + 1; i < cur->end_idx; i++) {
      m_peak_sims[i] = m_next_sims[i - 1];
    }
  }
  return do_merge;
}

/** Merge mountains from right to left to maximize storage savings, based on
 * benefit analysis. */
void Page_encoder::merge_mountains() {
  if (m_n_mountains == 0) {
    return;
  }

  ulint pd_size = PAGE_DICT_HDR_SIZE;

  dump_mountains_with_sim("merge_mountains start");

  Mountain *cur_mount = UT_LIST_GET_FIRST(m_mountains);
  Mountain *last_mount = nullptr;

  while (cur_mount != nullptr) {
    SPC_ENCODER_ASSERT(this, cur_mount->completed());
    SPC_ENCODER_ASSERT(this, !last_mount || last_mount->completed());
    bool force_merge = pd_size + cur_mount->peak_height > m_pd_capacity;

    bool merged = try_merge_mount(cur_mount, last_mount, force_merge);
    if (merged) {
      Mountain *next_mount = UT_LIST_GET_NEXT(list, cur_mount);
      UT_LIST_REMOVE(m_mountains, cur_mount);
      cur_mount = next_mount;
      m_n_mountains--;
    } else {
      pd_size += cur_mount->peak_height + PAGE_DICT_DIR_SLOT_SIZE;
      last_mount = cur_mount;
      cur_mount = UT_LIST_GET_NEXT(list, cur_mount);
    }
    SPC_ENCODER_ASSERT(this, validate_mountains());
  }

  dump_mountains_with_sim("merge_mountains end");

  SPC_ENCODER_ASSERT(this, validate_mountains());
}

/** Build one rec pack.
 * @param[in] cur_idx current record index
 * @param[in] new_dict_id new dict id
 * @param[in] new_pack_len new rec pack length
 */
void Page_encoder::build_pack(uint16_t cur_idx, pack_id_t pack_id,
                              pack_len_t pack_len) {
  ut_a(cur_idx == m_n_packs);
  uint16_t rec_size;
  auto &new_pack = m_packs[m_n_packs++];
  if (pack_len > 0) {
    new_pack = {pack_id, pack_len};
  } else {
    new_pack = {PACK_ID_NULL, pack_len};
  }

  SPC_ENCODER_ASSERT(this, m_row_sizes[cur_idx] > 0);
  rec_size = m_row_sizes[cur_idx] - new_pack.len + new_pack.spc_size();
  m_new_max_rec_size = std::max(m_new_max_rec_size, rec_size);
  m_new_all_rec_size += rec_size;
}

/** Build one dict.
 * @param[in] dict_id dict id
 * @param[in] len dict length
 * @param[in] slice the slice that offers dict contents
 */
void Page_encoder::build_dict(pack_id_t dict_id, ulint len,
                              const Slice *slice) {
  pd_data_t *dict;

  ulint offset;

  offset = (dict_id == 0) ? 0 : dict_offs_base(m_dict_offs)[dict_id - 1];

  if ((offset + len) > m_buf_size) {
    /** Realloc heap */
    ut_a(m_dict_data == m_dict_data_);
    SPC_ENCODER_ASSERT(this, m_buf_size == DICT_DATA_NORMAL_SIZE);
    SPC_ENCODER_ASSERT(this, offset + len < UNIV_PAGE_SIZE);

    pd_data_t *new_buf =
        static_cast<pd_data_t *>(mem_heap_zalloc(m_heap, UNIV_PAGE_SIZE));

    memcpy(new_buf, m_dict_data, offset);
    m_dict_data = new_buf;
    m_buf_size = UNIV_PAGE_SIZE;
    /** Make valid. */
    dict_offs_store_position(m_dict_data, m_dict_offs);
  }

  dict = m_dict_data + offset;
  slice->dtuple_copy(dict, len);

  /** Another dict */
  dict_offs_base(m_dict_offs)[dict_id] = offset + len;
  m_dict_offs[0]++;
}

/** Build page dicts and decide rec pack for every records.
 * @param[in] page index page
 * @param[in] index record descriptor
 */
void Page_encoder::build_dicts_and_packs(const page_t *page,
                                         const dict_index_t *index) {
  if (m_n_mountains == 0) {
    return;
  }

  /** Make valid. */
  dict_offs_store_position(m_dict_data, m_dict_offs);

  Slice_ctx sctx;
  Slice *slc;
  creg_t cur_reg = page_rec_get_next_creg(page_get_infimum_rec(page));
  Mountain *cur_mount = UT_LIST_GET_FIRST(m_mountains);
  uint16_t cur_idx = 0;
  pack_id_t cur_dict_id = 0;

  while (!page_rec_is_supremum(cur_reg.rec)) {
    if (cur_mount) {
      SPC_ENCODER_ASSERT(this, cur_idx >= cur_mount->start_idx &&
                                   cur_idx <= cur_mount->end_idx);

      if (cur_idx == cur_mount->peak_idx) {
        SPC_ENCODER_ASSERT(this,
                           cur_mount->peak_height == m_peak_sims[cur_idx]);
        if (cur_mount->peak_height > 0) {
          slc = sctx.get_offsets(cur_reg, index, ULINT_UNDEFINED);
          SPC_ENCODER_ASSERT(this, slc->data_size() + slc->rec_pack().len >=
                                       cur_mount->peak_height);
          build_dict(cur_dict_id, cur_mount->peak_height, slc);
        }
      }

      if (cur_idx == cur_mount->end_idx) {
        cur_mount = UT_LIST_GET_NEXT(list, cur_mount);
        cur_dict_id++;
      }
    }

    /* Write rec pack. */
    if (cur_mount) {
      build_pack(cur_idx, cur_dict_id, m_peak_sims[cur_idx]);
    } else {
      /* Deal with uncomputed records at the end of the page. */
      slc = sctx.get_offsets(cur_reg, index, ULINT_UNDEFINED);
      m_row_sizes[cur_idx] = slc->row_size();
      pack_len_t rec_pack_len = common_prefix(slc, cur_dict_id - 1);
      build_pack(cur_idx, cur_dict_id - 1, rec_pack_len);
    }

    /* End current record, and go next record. */
    cur_idx++;
    cur_reg = page_rec_get_next_creg(cur_reg.rec);
  }

  SPC_ENCODER_ASSERT(this, cur_idx == page_get_n_recs(page));
  SPC_ENCODER_ASSERT(this, cur_idx == m_n_packs);
  SPC_ENCODER_ASSERT(this,
                     m_n_mountains == dict_offs_n_slots(m_dict_offs) ||
                         (m_n_mountains == 1 &&
                          UT_LIST_GET_FIRST(m_mountains)->peak_height == 0));

  SPC_ENCODER_ASSERT(this, validate_packs(page, index));
}

ulint Page_encoder::build(const page_t *page, const dict_index_t *index,
                          ulint n_ins_recs) {
  SPC_ENCODER_ASSERT(this, m_heap == nullptr);

  /* Phase 0: Allocate memories. */
  size_t n_recs = page_get_n_recs(page);
  m_n_recs = n_recs;
  m_heap = mem_heap_create(
      n_recs * (sizeof(rec_pack_t) + 2 * sizeof(uint32_t) + sizeof(uint16_t)) +
          1024 /* Pre-allocate for moutains. */,
      UT_LOCATION_HERE);
  m_packs = static_cast<rec_pack_t *>(
      mem_heap_zalloc(m_heap, n_recs * sizeof(rec_pack_t)));
  m_next_sims = static_cast<uint32_t *>(
      mem_heap_zalloc(m_heap, n_recs * sizeof(uint32_t)));
  m_peak_sims = static_cast<uint32_t *>(
      mem_heap_zalloc(m_heap, n_recs * sizeof(uint32_t)));
  m_row_sizes = static_cast<uint16_t *>(
      mem_heap_zalloc(m_heap, n_recs * sizeof(uint16_t)));
  UT_LIST_INIT(m_mountains);
  m_n_mountains = 0;
  dict_offs_reset(m_dict_offs);

  /* Pre-build per-record raw data for debug dump use. */
  if (m_printer.is_enabled()) {
    m_printer.clear();
    m_printer.reserve_rec_data(n_recs);
    {
      std::ostringstream ctx_oss;
      ctx_oss << "[spc] build: table=" << index->table->name
              << " index=" << index->name
              << " page_no=" << page_get_page_no(page) << " n_recs=" << n_recs;
      m_printer.append(ctx_oss);
    }
    Slice_ctx pre_sctx;
    creg_t pre_reg = page_rec_get_next_creg(page_get_infimum_rec(page));
    while (!page_rec_is_supremum(pre_reg.rec)) {
      Slice *slc = pre_sctx.get_offsets(pre_reg, index, ULINT_UNDEFINED);
      std::vector<std::string> fields;
      ulint n_fields = dict_index_get_n_fields(index);
      fields.reserve(n_fields);
      for (ulint f = 0; f < n_fields; ++f) {
        ulint flen = UNIV_SQL_NULL;
        const byte *fdata = slc->get_nth_field(index, f, &flen);
        if (flen != UNIV_SQL_NULL && fdata != nullptr) {
          fields.emplace_back(reinterpret_cast<const char *>(fdata), flen);
        } else {
          fields.emplace_back();
        }
      }
      m_printer.push_rec_data(std::move(fields));
      pre_reg = page_rec_get_next_creg(pre_reg.rec);
    }
  }

  /* Phase 1: Build mountains. */
  build_mountains(page, index);

  /* A first mountain that alone exceeds the dict capacity has no left
  neighbor to merge into. Give up encoding this page; callers treat a zero
  return as "encoding cannot help" and fall back to reorganize/split. */
  Mountain *first_mount = UT_LIST_GET_FIRST(m_mountains);
  if (first_mount != nullptr &&
      PAGE_DICT_HDR_SIZE + first_mount->peak_height > m_pd_capacity) {
    return 0;
  }

  /* Phase 2: Reduce mountains. */
  merge_mountains();

  /* Phase 3: Build page dicts and rec packs. */
  build_dicts_and_packs(page, index);

  DBUG_EXECUTE_IF("page_encoder_fill_pd_capacity", {
    ulint current_size = dict_offs_size(m_dict_offs);
    if (current_size < m_pd_capacity && m_n_mountains <= PACK_ID_MAX) {
      ulint remaining = m_pd_capacity - current_size;
      pack_id_t dict_id = m_n_mountains;
      ulint offset;
      pd_data_t *dict;

      offset = (dict_id == 0) ? 0 : dict_offs_base(m_dict_offs)[dict_id - 1];
      ulint len = remaining - PAGE_DICT_DIR_SLOT_SIZE;

      if (offset + len > m_buf_size) {
        pd_data_t *new_buf =
            static_cast<pd_data_t *>(mem_heap_zalloc(m_heap, UNIV_PAGE_SIZE));

        memcpy(new_buf, m_dict_data, offset);
        m_dict_data = new_buf;
        m_buf_size = UNIV_PAGE_SIZE;
        /** Make valid. */
        dict_offs_store_position(m_dict_data, m_dict_offs);
      }

      dict = m_dict_data + offset;
      // Fill padding data (zeros) to the end of the last dict
      memset(dict, 0, remaining);

      // Update the dict offset to reflect the new size
      dict_offs_base(m_dict_offs)[dict_id] = offset + len;
      m_dict_offs[0]++;

      SPC_ENCODER_ASSERT(this, dict_offs_size(m_dict_offs) == m_pd_capacity);
    }
  });

  return get_max_insert_size(page, n_ins_recs);
}

/** Differs from page_copy_rec_list_end, because this function does not
 touch the lock table and max trx id on page or compress the page.

 IMPORTANT: The caller will have to update IBUF_BITMAP_FREE
 if new_block is a compressed leaf page in a secondary index.
 This has to be done either within the same mini-transaction,
 or by invoking ibuf_reset_free_bits() before mtr_commit(). */
void Page_encoder::page_copy_rec_list_end_no_locks(
    buf_block_t *new_block, /*!< in: index page to copy to */
    buf_block_t *block,     /*!< in: index page of rec */
    rec_t *rec,             /*!< in: record on page */
    dict_index_t *index,    /*!< in: record descriptor */
    mtr_t *mtr)             /*!< in: mtr */
{
  SPC_ENCODER_ASSERT(this, index->is_spc());
  page_t *new_page = buf_block_get_frame(new_block);

  page_cur_t cur1;
  Slice_ctx cur1_slc_ctx(&m_heap);
  Inheap_slice_ctx inheap_slc_ctx(&m_heap);
  Slice *cur1_slc = nullptr;

  reg_t cur2;

  const rec_pack_t *pack = nullptr;
  byte *rec_buf;

  rec_buf = (byte *)mem_heap_alloc(m_heap, m_new_max_rec_size);
  pack = m_packs;

  page_cur_position(rec, block, &cur1);

  if (page_cur_is_before_first(&cur1)) {
    page_cur_move_to_next(&cur1);
  }

  btr_assert_not_corrupted(new_block, index);
  ut_a(page_is_comp(new_page) == page_rec_is_comp(rec));
  ut_a(mach_read_from_2(new_page + UNIV_PAGE_SIZE - 10) ==
       (ulint)(page_is_comp(new_page) ? PAGE_NEW_INFIMUM : PAGE_OLD_INFIMUM));

  cur2 = page_get_infimum_reg(buf_block_get_frame(new_block));

  /* Copy records from the original page to the new page */
  while (!page_cur_is_after_last(&cur1)) {
    reg_t ins_reg;
    SPC_ENCODER_ASSERT(this, pack < m_packs + m_n_packs);

    cur1_slc = cur1_slc_ctx.get_offsets(page_cur_get_reg(&cur1), index,
                                        ULINT_UNDEFINED);
    /** encode record if need. */
    cur1_slc = rec_encode_to_buf(cur1_slc, *pack, new_page, index,
                                 &inheap_slc_ctx, rec_buf);
    SPC_ENCODER_ASSERT(this, cur1_slc->extra_size() + cur1_slc->data_size() <=
                                 m_new_max_rec_size);

    ins_reg = page_cur_insert_rec_low(cur2, index, cur1_slc, mtr);
    /** TODO: fix it <09-01-26, zanye.zjy> */

    SPC_ENCODER_ASSERT(
        this, ins_reg.is_null() || cur1_slc->is_positioned(ins_reg, index));
    if (UNIV_UNLIKELY(ins_reg.is_null())) {
      ib::fatal(UT_LOCATION_HERE, ER_IB_MSG_862)
          << "Rec offset " << page_offset(rec) << ", cur1 offset "
          << page_offset(page_cur_get_rec(&cur1)) << ", cur2 offset "
          << page_offset(cur2);
    }

    page_cur_move_to_next(&cur1);
    cur2 = ins_reg;

    pack++;
  }
}

const pd_data_t *Page_encoder::dict_data() const { return m_dict_data; }

ulint *Page_encoder::dict_offsets() { return m_dict_offs; }

ulint Page_encoder::get_max_insert_size(const page_t *page,
                                        ulint n_recs) const {
  ulint occupied;
  ulint free_space;

  occupied = m_new_all_rec_size + dict_offs_size(m_dict_offs) +
             page_dir_calc_reserved_space(n_recs + page_get_n_recs(page));

  free_space = page_get_free_space_of_empty(page_is_comp(page));

  if (occupied > free_space) {
    return (0);
  }

  return (free_space - occupied);
}
} /* namespace lizard */
