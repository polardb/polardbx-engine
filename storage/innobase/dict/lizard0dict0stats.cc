/*****************************************************************************

Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file lizard0dict0stats.cc
  SPC dict stats.

 Created 2026-03-06 by Jiyang.zhang
 *******************************************************/
#include "btr0btr.h"
#include "btr0pcur.h"
#include "db0err.h"
#include "dict0mem.h"
#include "mtr0mtr.h"
#include "page0page.h"
#include "srv0srv.h"
#include "storage/perfschema/komodo_pfs_stat.h"
#include "ut0rnd.h"

#include "lizard0dict0dict.h"
#include "lizard0fil0types.h"
#include "lizard0spc0encode.h"
#include "lizard0spc0types.h"

namespace lizard {

static bool dict_stats_analyze_page_spc(dict_index_t *index, btr_pcur_t *pcur,
                                        komodo::PFS_spc_stat *stat,
                                        mtr_t *mtr) {
  Slice_ctx sctx;
  Slice *slc = nullptr;
  uint64_t rec_bytes = 0;
  uint64_t row_bytes = 0;
  uint64_t comp_recs = 0;
  uint64_t uncomp_recs = 0;
  uint64_t dict_slots = 0;
  uint64_t dict_unused_slots = 0;
  uint64_t dict_bytes = 0;
  std::bitset<PACK_ID_MAX + 1> used_slots;

  const page_t *page;
  dberr_t err;

  page = pcur->get_page();

  ut_ad(pcur->is_on_user_rec());
  ut_ad(page_rec_get_next_const(page_get_infimum_rec(page)) == pcur->get_rec());

  for (; pcur->is_on_user_rec(); pcur->move_to_next_on_page()) {
    slc = sctx.get_offsets(pcur->get_creg(), index, ULINT_UNDEFINED);
    rec_bytes += slc->rec_size();
    row_bytes += slc->row_size();

    if (slc->rec_pack().is_spc()) {
      used_slots.set(slc->rec_pack().id);
      comp_recs++;
    } else {
      uncomp_recs++;
    }
  }

  if (fil_page_is_spc(page)) {
    dict_slots = page_dict_get_n_slots(page);
    dict_unused_slots = dict_slots - used_slots.count();
    dict_bytes = page_dict_get_size(page);
  } else {
    dict_slots = dict_unused_slots = dict_bytes = 0;
  }

  stat->aggregate_dict(dict_slots, dict_unused_slots, dict_bytes,
                       (dict_slots > 0) ? 1 : 0);
  stat->aggregate_recs(rec_bytes, row_bytes, comp_recs, uncomp_recs);

  /* Try to encode the page to estimate compression ratio */
  uint16_t pd_capacity = index->pd_capacity;
  if (!index->is_spc()) {
    pd_capacity = dict_index_determine_pd_capacity_low(index, false);
  }
  Page_encoder encoder(pd_capacity);
  encoder.build(page, index, 0);
  uint64_t size_rebuild =
      dict_offs_size(encoder.dict_offsets()) + encoder.get_new_all_rec_size();
  stat->aggregate_rebuild_size(size_rebuild);

  ut_ad(pcur->is_after_last_on_page());
  err = pcur->move_to_next_user_rec(mtr);
  return err == DB_END_OF_INDEX;
}

static void dict_stats_print_spc(const dict_index_t *index,
                                 const komodo::PFS_spc_stat &stat,
                                 ulong sample_pct, std::string *str_out) {
  char line[256];
  const size_t line_cap = sizeof(line);

  static const int NAME_W = 20;
  static const int NUM_W = 14;
  static const char ROW_FMT[] = "%-*s %*s %*s %*s %*s";
  static const char DATA_FMT[] = "%-*s %*llu %*llu %*llu %*llu";
  static const char DATA_NA_FMT[] = "%-*s %*llu %*s %*s %*s";

  auto output_line = [&](const char *content) {
    *str_out += content;
    *str_out += "\n";
  };

  snprintf(line, line_cap, "-------- %s(%s) [sample_rate: %lu%%] --------",
           index->table->name.m_name, index->name(), sample_pct);
  output_line(line);

  if (index->is_spc()) {
    snprintf(line, line_cap, ROW_FMT, NAME_W, "name", NUM_W, "count", NUM_W,
             "min", NUM_W, "max", NUM_W, "sum");
    output_line(line);

    snprintf(line, line_cap, ROW_FMT, NAME_W, "---", NUM_W, "---", NUM_W, "---",
             NUM_W, "---", NUM_W, "---");
    output_line(line);

    auto print_one = [&](const char *name, const PFS_single_stat &s) {
      if (s.m_count == 0 || !s.has_timed_stats()) {
        snprintf(line, line_cap, DATA_NA_FMT, NAME_W, name, NUM_W,
                 static_cast<unsigned long long>(s.m_count), NUM_W, "-", NUM_W,
                 "-", NUM_W, "-");
      } else {
        snprintf(line, line_cap, DATA_FMT, NAME_W, name, NUM_W,
                 static_cast<unsigned long long>(s.m_count), NUM_W,
                 static_cast<unsigned long long>(s.m_min), NUM_W,
                 static_cast<unsigned long long>(s.m_max), NUM_W,
                 static_cast<unsigned long long>(s.m_sum));
      }
      output_line(line);
    };

    print_one("rec_bytes", stat.m_rec_bytes);
    print_one("row_bytes", stat.m_row_bytes);
    print_one("comp_recs", stat.m_comp_recs);
    print_one("uncomp_recs", stat.m_uncomp_recs);
    print_one("dict_bytes", stat.m_dict_bytes);
    print_one("dict_slots", stat.m_dict_slots);
    print_one("dict_unused_slots", stat.m_dict_unused_slots);
    print_one("spc_page", stat.m_spc_page);
  }

  double ratio;
  if (stat.m_rec_bytes.m_sum + stat.m_dict_bytes.m_sum == 0) {
    ratio = 0;
  } else {
    ratio = 1 - (double)stat.m_rebuild_bytes.m_sum /
                    (stat.m_rec_bytes.m_sum + stat.m_dict_bytes.m_sum);
  }
  snprintf(line, line_cap, "compression ratio of rebuilding index: %.6f%%",
           ratio * 100);
  output_line(line);
}

/**
  Analyze SPC statistics for an index.
  @param index    Index to analyze
  @param stat_out Optional output parameter for statistics
  @param str_out  Optional output parameter for formatted string
  @return true if successful
*/
static bool dict_stats_analyze_index_spc(dict_index_t *index,
                                         std::string *str_out) {
  btr_pcur_t pcur;
  mtr_t mtr;

  komodo::PFS_spc_stat spc_stat;

  ulong sample_pct = srv_spc_analyze_sample_percentage;
  if (index->is_clustered() || sample_pct == 0) {
    return true;
  }

  mtr_start(&mtr);

  mtr_sx_lock(dict_index_get_lock(index), &mtr, UT_LOCATION_HERE);

  pcur.open_at_side(true, index, BTR_SEARCH_TREE | BTR_ALREADY_S_LATCHED, true,
                    0, &mtr);
  pcur.move_to_next_on_page();

  mtr_memo_release(&mtr, dict_index_get_lock(index), MTR_MEMO_SX_LOCK);

  while (pcur.is_on_user_rec()) {
    /* Randomly decide whether to sample this page based on percentage */
    bool should_sample = (ut::random_from_interval(0, 99) < sample_pct);

    if (should_sample) {
      if (dict_stats_analyze_page_spc(index, &pcur, &spc_stat, &mtr)) {
        break; /* Reached end of index */
      }
    } else {
      /* Skip this page, move to next page */
      pcur.move_to_last_on_page(&mtr);
      if (pcur.is_after_last_in_tree(&mtr)) {
        break; /* Reached end of index */
      }
      pcur.move_to_next_user_rec(&mtr);
    }
  }

  mtr_commit(&mtr);

  dict_stats_print_spc(index, spc_stat, sample_pct, str_out);

  return true;
}

dberr_t dict_stats_analyze_table_spc(dict_table_t *table,
                                     std::string *str_out) {
  dict_index_t *index;

  index = table->first_index();

  if (index == nullptr || index->is_corrupted() ||
      (index->type | DICT_UNIQUE) != (DICT_CLUSTERED | DICT_UNIQUE)) {
    /* Table definition is corrupt */
    return (DB_CORRUPTION);
  }

  /* Build formatted output string */
  *str_out += "===================================\n";
  *str_out += "SPC INDEX STATUS\n";
  *str_out += "===================================\n";

  for (; index != nullptr; index = index->next()) {
    if (index->type & DICT_FTS || dict_index_is_spatial(index)) {
      continue;
    }

    dict_stats_analyze_index_spc(index, str_out);
  }

  *str_out += "===================================\n";

  return DB_SUCCESS;
}
}  // namespace lizard
