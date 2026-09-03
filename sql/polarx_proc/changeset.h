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

#include <dirent.h>
#include <mysql.h>
#include <mysql/plugin.h>

#include "../common/component.h"
#include "../field.h"
#include "../item.h"
#include "../my_decimal.h"
#include "../mysqld.h"
#include "../package/proc.h"
#include "../sql_base.h"
#include "../sql_class.h"
#include "sql_string.h"
#include "thread_pool.h"
#include "token_bucket.h"

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
    for (auto it = pk_field_list.cbegin(), it_other = other.pk_field_list.cbegin();
      it != pk_field_list.cend() && it_other != other.pk_field_list.cend();
      ++it, ++it_other) {
      int cmp_res = (*it)->cmp((*it_other)->field_ptr());
      if (cmp_res < 0) {
        return true;
      }
      if (cmp_res > 0) {
        return false;
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

    uint64_t current_pk_num;
    std::string bucket_fill_rate;
    std::string bucket_available_tokens;
    std::string backpressure_trigger;

    Stats() : bucket_fill_rate("-"),
        bucket_available_tokens("-"),
        backpressure_trigger("-") {
      insert_count = 0;
      delete_count = 0;
      update_count = 0;
      current_pk_num = 0;
      file_count = 0;
      memory_size = 0;
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
  void pk_map_delete(std::unique_ptr<Change> &change);

  /**
   * @brief insert pk
   */
  void pk_map_insert(std::unique_ptr<Change> &change);

  Stats update_stats();

  int init(const DBTableName &full_table_name);

  void close();

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

  uint64_t get_approximate_memory_size() const;

  // memory table approximate memory size
  std::atomic<uint64_t> memory_size{};

  // imm table approximate memory size
  std::atomic<uint64_t> imm_memory_size{};

  // tmp table approximate memory size
  std::atomic<uint64_t> tmp_memory_size{};

  // num of mem table pks
  std::atomic<uint64_t> primary_key_num{};

  // num of imm table pks
  std::atomic<uint64_t> imm_primary_key_num{};
  
  // current num of all pks
  std::atomic<uint64_t> all_primary_key_num{};

  std::atomic<bool> imm_empty{};

  // memory limit
  u_int64_t memory_limit;

  // backpressure
  bool backpressure;

  // token bucket
  TokenBucket token_bucket;

  // pk num threshold
  uint64_t backpressure_trigger;

  void do_backpressure(double tokens = 1.0);

 private:
  static int open_table(THD *thd, const std::string &db, const std::string &tb,
                        TABLE **output);

  // stats info
  Stats stats_info;

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
  mutable std::list<FileNode> file_list;
};

}  // namespace im

#endif  // POLARX_PROC_CHANGESET_H
