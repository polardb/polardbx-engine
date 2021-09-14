/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2015, Facebook, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */
#pragma once

/* C++ system header files */
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

/* XENGINE header files */
#include "xengine/db.h"

/* MyX header files */
#include "./ha_xengine.h"

namespace myx {

class Xdb_ddl_manager;
class Xdb_key_def;

#if 0 // DEL-SYSSTAT
extern std::atomic<uint64_t> xengine_num_sst_entry_put;
extern std::atomic<uint64_t> xengine_num_sst_entry_delete;
extern std::atomic<uint64_t> xengine_num_sst_entry_singledelete;
extern std::atomic<uint64_t> xengine_num_sst_entry_merge;
extern std::atomic<uint64_t> xengine_num_sst_entry_other;
#endif
extern bool xengine_compaction_sequential_deletes_count_sd;

struct Xdb_compact_params {
  uint64_t m_deletes, m_window, m_file_size;
};

struct Xdb_index_stats {
  enum {
    INDEX_STATS_VERSION_INITIAL = 1,
    INDEX_STATS_VERSION_ENTRY_TYPES = 2,
  };
  GL_INDEX_ID m_gl_index_id;
  int64_t m_data_size, m_rows, m_actual_disk_size;
  int64_t m_entry_deletes, m_entry_single_deletes;
  int64_t m_entry_merges, m_entry_others;
  std::vector<int64_t> m_distinct_keys_per_prefix;
  std::string m_name; // name is not persisted

  static std::string materialize(const std::vector<Xdb_index_stats> &stats,
                                 const float card_adj_extra);
  static int unmaterialize(const std::string &s,
                           std::vector<Xdb_index_stats> *const ret);

  Xdb_index_stats() : Xdb_index_stats({0, 0}) {}
  explicit Xdb_index_stats(GL_INDEX_ID gl_index_id)
      : m_gl_index_id(gl_index_id), m_data_size(0), m_rows(0),
        m_actual_disk_size(0), m_entry_deletes(0), m_entry_single_deletes(0),
        m_entry_merges(0), m_entry_others(0) {}

  void merge(const Xdb_index_stats &s, const bool &increment = true,
             const int64_t &estimated_data_len = 0);
  void update(const Xdb_index_stats &index_stats, const int64_t &estimated_data_len = 0);
  void reset() {
    m_data_size = 0;
    m_rows = 0;
    m_actual_disk_size = 0;
    m_entry_deletes = 0;
    m_entry_single_deletes = 0;
    m_entry_merges = 0;
    m_entry_others = 0;
  }
};

class Xdb_tbl_prop_coll : public xengine::table::TablePropertiesCollector {
public:
  Xdb_tbl_prop_coll(Xdb_ddl_manager *const ddl_manager,
                    const Xdb_compact_params &params, const uint32_t &cf_id,
                    const uint8_t &table_stats_sampling_pct);

  /*
    Override parent class's virtual methods of interest.
  */

  virtual xengine::common::Status AddUserKey(const xengine::common::Slice &key,
                                     const xengine::common::Slice &value,
                                     xengine::table::EntryType type,
                                     xengine::common::SequenceNumber seq,
                                     uint64_t file_size);

  virtual xengine::common::Status
  Finish(xengine::table::UserCollectedProperties *properties) override;

  virtual const char *Name() const override { return "Xdb_tbl_prop_coll"; }

  xengine::table::UserCollectedProperties GetReadableProperties() const override;

  bool NeedCompact() const override;
 
  virtual xengine::common::Status add_extent(bool add_flag,
                                    const xengine::common::Slice &smallest_key,
                                    const xengine::common::Slice &largest_key,
                                    int64_t file_size, int64_t num_entries,
                                    int64_t num_deletes) override;

public:
  uint64_t GetMaxDeletedRows() const { return m_max_deleted_rows; }

  static void read_stats_from_tbl_props(
      const std::shared_ptr<const xengine::table::TableProperties> &table_props,
      std::vector<Xdb_index_stats> *out_stats_vector);

private:
  static std::string GetReadableStats(const Xdb_index_stats &it);

  bool ShouldCollectStats();
  void CollectStatsForRow(const xengine::common::Slice &key,
                          const xengine::common::Slice &value,
                          const xengine::table::EntryType &type,
                          const uint64_t &file_size);
  Xdb_index_stats *AccessStats(const xengine::common::Slice &key);
  void AdjustDeletedRows(xengine::table::EntryType type);

private:
  uint32_t m_cf_id;
  std::shared_ptr<const Xdb_key_def> m_keydef;
  Xdb_ddl_manager *m_ddl_manager;
  std::vector<Xdb_index_stats> m_stats;
  Xdb_index_stats *m_last_stats;
  static const char *INDEXSTATS_KEY;

  // last added key
  std::string m_last_key;

  // floating window to count deleted rows
  std::vector<bool> m_deleted_rows_window;
  uint64_t m_rows, m_window_pos, m_deleted_rows, m_max_deleted_rows;
  uint64_t m_file_size;
  Xdb_compact_params m_params;
  uint8_t m_table_stats_sampling_pct;
  unsigned int m_seed;
  float m_card_adj_extra;
};

class Xdb_tbl_prop_coll_factory
    : public xengine::table::TablePropertiesCollectorFactory {
public:
  Xdb_tbl_prop_coll_factory(const Xdb_tbl_prop_coll_factory &) = delete;
  Xdb_tbl_prop_coll_factory &
  operator=(const Xdb_tbl_prop_coll_factory &) = delete;

  explicit Xdb_tbl_prop_coll_factory(Xdb_ddl_manager *ddl_manager)
      : m_ddl_manager(ddl_manager) {}

  /*
    Override parent class's virtual methods of interest.
  */

  virtual xengine::table::TablePropertiesCollector *CreateTablePropertiesCollector(
      xengine::table::TablePropertiesCollectorFactory::Context context) override {
    return new Xdb_tbl_prop_coll(m_ddl_manager, m_params,
                                 context.column_family_id,
                                 m_table_stats_sampling_pct);
  }

  virtual const char *Name() const override {
    return "Xdb_tbl_prop_coll_factory";
  }

public:
  void SetCompactionParams(const Xdb_compact_params &params) {
    m_params = params;
  }

  void SetTableStatsSamplingPct(const uint8_t &table_stats_sampling_pct) {
    m_table_stats_sampling_pct = table_stats_sampling_pct;
  }

private:
  Xdb_ddl_manager *const m_ddl_manager;
  Xdb_compact_params m_params;
  uint8_t m_table_stats_sampling_pct;
};

} // namespace myx
