/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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


#ifndef POLARX_PROC_CHANGESET_H
#define POLARX_PROC_CHANGESET_H

// #include <my_global.h>
#include <dirent.h>
#include <mysql.h>
#include <mysql/plugin.h>
#include <mysql/service_my_plugin_log.h>

#include "plugin/polarx_rpc/utility/atomicex.h"

#include "../common/component.h"
#include "../consensus_log_manager.h"
#include "../field.h"
#include "../item.h"
#include "../log.h"
#include "../my_decimal.h"
#include "../mysqld.h"
#include "../package/proc.h"
#include "../sql_admin.h"
#include "../sql_base.h"
#include "../sql_class.h"
#include "../sql_plugin.h"
#include "../sql_udf.h"
#include "../table.h"
#include "field_types.h"
#include "my_base.h"
#include "mysqld_error.h"
#include "sql_string.h"
#include "thread_pool.h"

namespace im {

extern LEX_CSTRING POLARX_PROC_SCHEMA;

/**
 * @brief ChangesetResult for fetch data
 *
 */
class ChangesetResult {
 public:
  ChangesetResult(ChangeType type, std::list<Field *> pk_field_list)
      : type(type), pk_field_list(std::move(pk_field_list)) {}

  ~ChangesetResult() {
    for (auto field : pk_field_list) {
      my_free(field->field_ptr());
      // delete field;
    }
  }

  void *operator new(size_t size) {
    return my_malloc(key_memory_CS_RESULT_BUFFER, size,
                     MYF(MY_WME | ME_FATALERROR));
  }

  void operator delete(void *ptr) { my_free(ptr); }

  bool operator< (const ChangesetResult& other) const {
    std::list<Field *>::const_iterator it = pk_field_list.cbegin();
    std::list<Field *>::const_iterator it_other = other.pk_field_list.cbegin();
    for (; it != pk_field_list.cend() && it_other != other.pk_field_list.cend(); ++it, ++it_other) {
      int cmp_res = (*it)->cmp((*it_other)->field_ptr());
      if (cmp_res < 0) {
        return true;
      } else if (cmp_res > 0) {
        return false;
      } else {
        continue;
      }
    }

    return type < other.type;
  } 

  std::string get_op_string() const {
    std::string ret;
    if (type == INSERT) {
      ret.append("INSERT");
    } else if (type == DELETE) {
      ret.append("DELETE");
    }
    return ret;
  }

  ChangeType type;
  std::list<Field *> pk_field_list;
};

/**
 * @brief Changeset describes DMLs during partition migration
 */
class Changeset {
 public:
  Changeset();

  ~Changeset();

  struct Stats {
    uint64_t insert_count;
    uint64_t delete_count;
    uint64_t update_count;
    uint64_t file_count;
    uint64_t memory_size;

    void swap(Stats &rhs) {
      std::swap(insert_count, rhs.insert_count);
      std::swap(delete_count, rhs.delete_count);
      std::swap(update_count, rhs.update_count);
    }
  };

  void *operator new(size_t size) {
    return my_malloc(key_memory_CS_PRIMARY_KEY, size,
                     MYF(MY_WME | ME_FATALERROR));
  }

  void operator delete(void *ptr) { my_free(ptr); }

  /**
   * @brief delete pk
   */
  void pk_map_delete(std::unique_ptr<Change> &change, bool mem_c = false);

  /**
   * @brief insert pk
   */
  void pk_map_insert(std::unique_ptr<Change> &change, bool mem_c = false);

  /**
   * insert delete update of change set
   */
  void add_delete(const std::string &pk);

  void add_insert(const std::string &pk);

  void add_update(const std::string &pk_before, const std::string &pk_after);

  Stats update_stats();

  int init(const DBTableName &full_table_name);

  void close();

  void clear();

  void swap_pk();

  // changeset cache --> changeset
  void append_changeset(std::unique_ptr<ChangeSetCache> &changeset);

  void memory_check_and_flush();

  friend void flush_imm_table(void *changeset);

  const char *get_next_file_name();

  void rm_old_snapshot();

  void rm_all_changeset_files();

  void fetch_pk(bool delete_last_cs,
                std::vector<ChangesetResult *> &res,
                TABLE *table);

  void get_result_list(
      std::unordered_map<std::string, std::unique_ptr<Change>> &pk_map,
      std::vector<ChangesetResult *> &res, TABLE *table);

  void get_result_list(const char *file_name,
                       std::vector<ChangesetResult *> &res,
                       TABLE *table);

  static uint my_get_key_length(KEY_PART_INFO * key_part_info, uchar const *pk);

  std::list<Field *> make_pk_fields(TABLE *table, uchar *pk,
                                    MEM_ROOT *mem_root);

  void set_stop(bool stop) { this->stop = stop; }

  bool is_stop() const { return this->stop; }

  uint64_t get_approximate_memory_size() const;

  // memory table approximate memory size
  std::atomic<uint64_t> memory_size{};

  // imm table approximate memory size
  std::atomic<uint64_t> imm_memory_size{};

  // tmp table approximate memory size
  std::atomic<uint64_t> tmp_memory_size{};

  // pk nums
  std::atomic<uint64_t> primary_key_nums{};
  
  // imm table pk nums
  std::atomic<uint64_t> imm_primary_key_nums{};

  std::atomic<bool> imm_empty;

  // memory limit
  u_int64_t memory_limit;

 private:
  static int open_table(THD *thd, const std::string &db, const std::string &tb,
                        TABLE **output);

  // stats info
  Stats stats_{0, 0, 0, 0, 0};

  // changeset stop
  bool stop;

  // generate file name
  uint32_t file_name_num;

  // tmp file name
  const char *tmp_file_name;

  DBTableName full_table_name;

  mutable Mutex mutex;
  mutable CondVar cv;

  mutable std::unordered_map<std::string, std::unique_ptr<Change>> mem_pk_map;
  mutable std::unordered_map<std::string, std::unique_ptr<Change>> imm_pk_map;
  mutable std::unordered_map<std::string, std::unique_ptr<Change>> tmp_map;
  mutable std::list<std::pair<const char *, bool>> file_list;
};

}  // namespace im

#endif  // POLARX_PROC_CHANGESET_H
