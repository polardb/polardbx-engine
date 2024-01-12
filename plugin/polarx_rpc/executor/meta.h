
#pragma once

#include <list>
#include <memory>
#include <vector>

#include "../global_defines.h"
#ifndef MYSQL8
#define MYSQL_SERVER
#endif
#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/item_sum.h"
#include "sql/my_decimal.h"
#include "sql/table.h"

#include "../coders/protocol_fwd.h"

#include "error.h"
#include "log.h"
#include "parse.h"

namespace rpc_executor {
using KeyPartMap = ::key_part_map;
using KeyRange = ::key_range;
using KeyFlag = enum ha_rkey_function;
using ExprItem = ::Item;
using ExprSendItem = ::Send_field;
using ProjectInfo = std::vector<std::pair<std::string, ExprItem *>>;
using AggrInfo = std::pair<std::string, Item_sum *>;

constexpr KeyPartMap EMPTY_MAP = 0;

class SearchKey;

class FieldRefItem : public ::Item_int {
 public:
  FieldRefItem(FieldIndex field_index)
      : Item_int(static_cast<ulonglong>(field_index)),
        field_index_(field_index) {}

  uint32_t field_index() { return field_index_; }

 private:
  uint32_t field_index_;
};

class ExecKeyMeta {
 public:
  ExecKeyMeta() : init_(false), key_info_(nullptr), key_idx_(-1) {}

  int init(KEY *key_info, uint32_t key_idx) {
    int ret = HA_EXEC_SUCCESS;
    if (!key_info || key_idx >= MAX_INDEXES) {
      ret = HA_ERR_TABLE_CORRUPT;
      log_exec_error(
          "ExecKeyMeta init failed, ret: %d, key_ptr: %p, key_idx: %u", ret,
          key_info, key_idx);
    } else {
      key_info_ = key_info;
      key_idx_ = key_idx;
      init_ = true;
    }
    return ret;
  }

  bool is_init() { return init_; }

  const char *name() { return key_info_->name; }

  uint32_t parts() { return key_info_->user_defined_key_parts; }

  uint32_t length() { return key_info_->key_length; }

  bool is_unique() { return key_info_->flags & HA_NOSAME; }

  uint32_t index() { return key_idx_; }

  KEY *key() { return key_info_; }

 private:
  bool init_;
  KEY *key_info_;
  uint32_t key_idx_;
};

/** structure holds the search field(s) */
class SearchKey {
 public:
  int init(ExecKeyMeta *exec_key,
           const PolarXRPC::ExecPlan::GetExpr &mysqlx_key) {
    int ret = HA_EXEC_SUCCESS;
    used_part_map_ = EMPTY_MAP;
    used_length_ = 0;
    using_full_key_ = false;
    impossible_where_ = false;
    ExecKeyMap expr;
    if ((ret = expr.init(mysqlx_key))) {
      log_exec_error("ExecKeyMap init failed, ret: %d", ret);
    } else if ((ret = store_to_buffer(exec_key, expr))) {
      log_exec_error(
          "store key to table->record[0] failed, "
          "ret: %d, part_map: %lu",
          ret, used_part_map_);
    }
    return ret;
  }

  int store_to_buffer(ExecKeyMeta *exec_key, const ExecKeyMap &expr);

  uchar *key_buffer() const { return key_buffer_.get(); }

  uint64_t used_length() const { return used_length_; }

  KeyPartMap used_part_map() const { return used_part_map_; }

  bool using_full_key() const { return using_full_key_; }

  bool is_impossible_where() const { return impossible_where_; }

 private:
  std::unique_ptr<uchar[]> key_buffer_;
  KeyPartMap used_part_map_;
  uint64_t used_length_;
  bool using_full_key_;
  bool impossible_where_;
};

struct RangeInfo {
  const PolarXRPC::ExecPlan::GetExpr *begin_key_;
  const PolarXRPC::ExecPlan::GetExpr *end_key_;
  bool include_begin_;
  bool include_end_;
};

class RangeSearchKey {
 public:
  int init(ExecKeyMeta *exec_key, const RangeInfo &range_info) {
    int ret = HA_EXEC_SUCCESS;

    // If range_info.begin_key_ is nullptr then it means scan from the first
    // record of table. We do nothing and left begin_range_ nullptr. See
    // handler::read_range_first
    if (range_info.begin_key_) {
      begin_range_.reset(new (std::nothrow) KeyRange());
      if (!begin_range_) {
        ret = HA_ERR_OUT_OF_MEM;
      } else if ((ret = begin_key_.init(exec_key, *(range_info.begin_key_)))) {
        log_exec_error(
            "SearchKey of begin key init failed, "
            "ret: %d, parts: %lu",
            ret, begin_key_.used_part_map());
      } else {
        begin_range_->key = begin_key_.key_buffer();
        begin_range_->length = begin_key_.used_length();
        begin_range_->keypart_map = begin_key_.used_part_map();
        begin_range_->flag =
            range_info.include_begin_ ? HA_READ_KEY_OR_NEXT : HA_READ_AFTER_KEY;
      }
    }

    // If range_info.end_key_ is nullptr then it means scan to the last
    // record of table. We do nothing and left end_range_ nullptr. See
    // handler::read_range_next
    if (!ret && range_info.end_key_) {
      end_range_.reset(new (std::nothrow) KeyRange());
      if (!end_range_) {
        ret = HA_ERR_OUT_OF_MEM;
      } else if ((ret = end_key_.init(exec_key, *(range_info.end_key_)))) {
        log_exec_error(
            "SearchKey of end key init failed, "
            "ret: %d, parts: %lu",
            ret, end_key_.used_part_map());
      } else {
        end_range_->key = end_key_.key_buffer();
        end_range_->length = end_key_.used_length();
        end_range_->keypart_map = end_key_.used_part_map();
        end_range_->flag =
            range_info.include_end_ ? HA_READ_AFTER_KEY : HA_READ_BEFORE_KEY;
      }
    }
    return ret;
  }

  const SearchKey &begin_key() const { return begin_key_; }
  const SearchKey &end_key() const { return end_key_; }
  KeyRange *begin_range() const { return begin_range_.get(); }
  KeyRange *end_range() const { return end_range_.get(); }

 private:
  SearchKey begin_key_;
  std::unique_ptr<KeyRange> begin_range_;
  SearchKey end_key_;
  std::unique_ptr<KeyRange> end_range_;
};

class ExecTable {
 public:
  ExecTable() : init_(false), table_(nullptr) {}

  int init(TABLE *table) {
    int ret = HA_EXEC_SUCCESS;
    if (!table || !(table->file) || !(table->s)) {
      ret = HA_ERR_TABLE_CORRUPT;
      log_exec_error(
          "ExecTable init failed, ret: %d, "
          "table: %p, file: %p, share: %p",
          ret, table, nullptr == table ? nullptr : table->file,
          nullptr == table ? nullptr : table->s);
    } else {
      // table->use_all_columns();
      table_ = table;
      for (uint32_t i = 0; i < table->s->fields; ++i) {
        Field *this_field = table->field[i];
        name_field_map_.emplace(this_field->field_name, this_field);
      }
      for (uint32_t i = 0; i < table->s->keys; ++i) {
        KEY *key_info = &(table->key_info[i]);
        std::unique_ptr<ExecKeyMeta> new_exec_key(new (std::nothrow)
                                                      ExecKeyMeta());
        if (!new_exec_key) {
          ret = HA_ERR_OUT_OF_MEM;
          log_exec_error(
              "ExecTable init failed, new ExecKeyMeta failed, ret: %d", ret);
        } else if ((ret = new_exec_key->init(key_info, i))) {
          log_exec_error("ExecTable init failed, ret: %d", ret);
        } else {
          name_key_map_.emplace(key_info->name, std::move(new_exec_key));
        }
      }
      init_ = true;
    }
    return ret;
  }

  bool is_init() { return init_; }

  int get_field(uint32_t index, Field *&field) {
    int ret = HA_EXEC_SUCCESS;
    if (index > table_->s->fields) {
      ret = HA_ERR_TOO_MANY_FIELDS;
    } else {
      field = table_->field[index];
    }
    return ret;
  }

  int get_field(const std::string &name, Field *&field) {
    int ret = HA_EXEC_SUCCESS;
    auto iter = name_field_map_.find(name);
    if (name_field_map_.end() == iter) {
      ret = HA_ERR_INTERNAL_ERROR;
    } else {
      field = iter->second;
    }
    return ret;
  }

  int get_key(const std::string &name, ExecKeyMeta *&exec_key) {
    int ret = HA_EXEC_SUCCESS;
    auto iter = name_key_map_.find(name);
    if (name_key_map_.end() == iter) {
      ret = HA_ERR_INTERNAL_ERROR;
    } else {
      exec_key = iter->second.get();
    }
    return ret;
  }

  uint32_t field_count() { return table_->s->fields; }

  Field **fields() { return table_->field; }

  TABLE *table() { return table_; }

 private:
  bool init_;
  TABLE *table_;
  std::unordered_map<std::string, Field *> name_field_map_;
  std::unordered_map<std::string, std::unique_ptr<ExecKeyMeta>> name_key_map_;

 public:
#ifdef MYSQL8PLUS
  Table_ref tables;
#else
  TABLE_LIST tables;
#endif
};

class InternalDataSet {
 public:
  ~InternalDataSet() {
    project_exprs_ = nullptr;
    item_free_list_.clear();
#ifdef MYSQL8
    if (THR_MALLOC && (*THR_MALLOC)) {
      (*THR_MALLOC)->ClearForReuse();
    }
#else
    MEM_ROOT **item_mem_root = my_thread_get_THR_MALLOC();
    if (item_mem_root && (*item_mem_root)) {
      free_root(*item_mem_root, MY_MARK_BLOCKS_FREE);
    }
#endif
  }

  int init(int64_t batch_size, ExecTable *table) {
    int ret = HA_EXEC_SUCCESS;
    batch_size_ = batch_size;
    found_ = false;
    no_next_row_ = false;
    table_ = table;
    has_project_ = false;
    project_exprs_ = nullptr;
    is_aggr_ = false;
    init_ = true;
    return ret;
  }

  bool is_init() { return init_; }

  void defer_delete_item(ExprItem *item) { item_free_list_.emplace_back(item); }

  ExecTable *table() { return table_; }

  void set_found() { found_ = true; }

  void reset_found() { found_ = false; }

  bool found() { return found_; }

  void set_no_next_row() { no_next_row_ = true; }

  bool no_next_row() { return no_next_row_; }

  int get_field(uint32_t index, Field *&field) {
    return table_->get_field(index, field);
  }

  int get_field(const std::string *name, Field *&field) {
    return table_->get_field(*name, field);
  }

  // -------------------------------------------------------------------------
  // Project functions
  // -------------------------------------------------------------------------
  int do_project() {
    // Hope to optimize VERY SIMPLE queries like point select.
    //
    // CASE1: If no ProjectNode has applied, a do_project call will
    // only set my_table->read_set (See @project_all). In this case, we should
    // use get_all_fields to send metadata(See
    // @executor::Protocol::write_metadata) and row(See
    // @executor::Protocol::write_row).
    //
    // CASE 2: If some ProjectNode has applied, then project_exprs_ should have
    // been ready and project_field should have been called to set specific bit
    // in my_table->read_set. In this case, we should use get_project_exprs to
    // send metadata and row.
    //
    // CASE 1 and CASE 2 is distinguished by has_project_x
    return has_project_ ? HA_EXEC_SUCCESS : project_all();
  }

  int project_all() {
    TABLE *my_table = table_->table();
    my_table->read_set = &(my_table->s->all_set);
    return HA_EXEC_SUCCESS;
  }

  void get_all_fields(Field **&fields, uint32_t &field_count) {
    fields = table_->fields();
    field_count = table_->field_count();
  }

  int project_field(Field *field) {
#ifdef MYSQL8
    table_->table()->mark_column_used(field, MARK_COLUMNS_READ);
#else
    table_->table()->mark_column_used(current_thd, field, MARK_COLUMNS_READ);
#endif
    return HA_EXEC_SUCCESS;
  }

  int project(ProjectInfo *project_expr) {
    int ret = HA_EXEC_SUCCESS;
    project_exprs_ = project_expr;
    has_project_ = true;
    return ret;
  }

  ProjectInfo &get_project_exprs() { return *project_exprs_; }

  bool has_project() { return has_project_; }

  bool is_aggr() { return is_aggr_; }
  void set_aggr(std::string &aggr_name, Item_sum *aggr_expr) {
    is_aggr_ = true;
    aggr_info_ = make_pair(aggr_name, aggr_expr);
  }

  AggrInfo &get_aggr_info() { return aggr_info_; }

 private:
  bool init_ = false;
  int64_t batch_size_;
  bool found_;
  bool no_next_row_;
  ExecTable *table_;
  ProjectInfo *project_exprs_;
  // TODO: use a more memory-friendly DS.
  std::list<std::unique_ptr<ExprItem>> item_free_list_;
  bool has_project_;
  bool is_aggr_ = false;
  AggrInfo aggr_info_;
};

}  // namespace rpc_executor
