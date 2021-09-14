/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "table_space.h"
namespace xengine
{
using namespace common;
namespace storage
{
TableSpace::TableSpace(util::Env *env, const util::EnvOptions &env_options)
    : is_inited_(false),
      env_(env),
      env_options_(env_options),
      table_space_id_(0),
      table_space_mutex_(),
      db_paths_(),
      extent_space_map_(),
      index_id_set_()
{
}

TableSpace::~TableSpace()
{
  destroy();
}

void TableSpace::destroy()
{
  if (is_inited_) {
    for (auto iter = extent_space_map_.begin(); extent_space_map_.end() != iter; ++iter) {
      MOD_DELETE_OBJECT(ExtentSpace, iter->second);
    }
    extent_space_map_.clear();
    index_id_set_.clear();
    is_inited_ = false;
  }
}

int TableSpace::create(const CreateTableSpaceArgs &args)
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "TableSpace has been inited", K(ret));
  } else if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret));
  } else {
    for (uint32_t i = 0; i < args.db_paths_.size(); ++i) {
      CreateExtentSpaceArgs extent_space_args(args.table_space_id_, i, args.db_paths_.at(i));
      if (IS_NULL(extent_space = MOD_NEW_OBJECT(memory::ModId::kExtentSpaceMgr, ExtentSpace, env_, env_options_))) {
        ret = Status::kMemoryLimit;
        XENGINE_LOG(WARN, "fail to allocate memory for ExtentSpace", K(ret));
      } else if (FAILED(extent_space->create(extent_space_args))) {
        XENGINE_LOG(ERROR, "fail to create extent space", K(ret), K(i), K(extent_space_args));
      } else if (!(extent_space_map_.emplace(i, extent_space).second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(ERROR, "unexpected error, fail to emplace extent_space", K(ret), K(i));
      }
    }
    
    if (SUCCED(ret)) {
      table_space_id_ = args.table_space_id_;
      db_paths_ = args.db_paths_;
      is_inited_ = true;
    }
  }

  return ret;
}

int TableSpace::remove()
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited first", K(ret));
  } else if (!is_free_unsafe()) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, TableSpace is not free, can't been remove", K(ret));
  } else {
    for (auto iter = extent_space_map_.begin(); SUCCED(ret) && extent_space_map_.end() != iter; ++iter) {
      if (FAILED(iter->second->remove())) {
        XENGINE_LOG(WARN, "fail to remove ExtentSpace", K(ret), "extent_space_type", iter->first);
      }
    }
  }

  return ret;
}

int TableSpace::register_subtable(const int64_t index_id)
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace shoule been inited first", K(ret));
  } else if (!(index_id_set_.emplace(index_id).second)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "fail to emplace subtable", K(ret), K(index_id));
  } else {
    XENGINE_LOG(INFO, "success to register subtable", K(index_id));
  }

  return ret;
}

int TableSpace::unregister_subtable(const int64_t index_id)
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited first", K(ret));
  } else if (1 != (index_id_set_.erase(index_id))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to erase from subtable map", K(ret), K(index_id));
  } else {
    XENGINE_LOG(INFO, "success to unregister subtable", K(index_id));
  }

  return ret;
}

int TableSpace::allocate(const int32_t extent_space_type, ExtentIOInfo &io_info)
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited first", K(ret));
  } else if (IS_NULL(extent_space = get_extent_space(extent_space_type))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to get extent space", K(ret), K(extent_space_type));
  } else if (FAILED(extent_space->allocate(io_info))) {
    XENGINE_LOG(WARN, "fail to allocate extent from ExtentSpace", K(ret), K(extent_space_type));
  } else {
    XENGINE_LOG(DEBUG, "success to allocate extent", K(io_info));
  }

  return ret;
}

int TableSpace::recycle(const int32_t extent_space_type, const ExtentId extent_id)
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited firtst", K(ret));
  } else if (IS_NULL(extent_space = get_extent_space(extent_space_type))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to get extent space", K(ret), K(extent_space_type));
  } else if (FAILED(extent_space->recycle(extent_id))) {
    XENGINE_LOG(WARN, "fail to recycle extent", K(ret), K(extent_id));
  } else {
    XENGINE_LOG(DEBUG, "success to recycle extent", K(extent_id));
  }

  return ret;
}

int TableSpace::reference(const int32_t extent_space_type,
                          const ExtentId extent_id,
                          ExtentIOInfo &io_info)
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited firtst", K(ret));
  } else if (IS_NULL(extent_space = get_extent_space(extent_space_type))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to get extent space", K(ret), K(extent_space_type));
  } else if (FAILED(extent_space->reference(extent_id, io_info))) {
    XENGINE_LOG(WARN, "fail to reference extent", K(ret), K(extent_id));
  } else {
    XENGINE_LOG(DEBUG, "success to reference extent", K(extent_id));
  }

  return ret;
}

int TableSpace::move_extens_to_front(const ShrinkInfo &shrink_info,
                                     std::unordered_map<int64_t, ExtentIOInfo> &replace_map)
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited first", K(ret));
  } else if (UNLIKELY(!shrink_info.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(shrink_info));
  } else if (index_id_set_ != shrink_info.index_id_set_) {
    XENGINE_LOG(INFO, "there has ddl happened during shrink, cancle this shrink");
  } else if (IS_NULL(extent_space = get_extent_space(shrink_info.extent_space_type_))){
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, ExtentSpace must not nullptr", K(ret), K(shrink_info));
  //TODO:yuanfeng check the extent count
  } else if (FAILED(extent_space->move_extens_to_front(shrink_info.shrink_extent_count_, replace_map))) {
    XENGINE_LOG(WARN, "fail to move extent to front", K(ret), K(shrink_info));
  } else {
    XENGINE_LOG(INFO, "success to move extent to front", K(shrink_info));
  }

  return ret;
}

int TableSpace::shrink(const ShrinkInfo &shrink_info)
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited first", K(ret));
  } else if (UNLIKELY(!shrink_info.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret));
  } else if (IS_NULL(extent_space = get_extent_space(shrink_info.extent_space_type_))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, ExtentSpace must not nullptr", K(ret), K(shrink_info));
  } else if (FAILED(extent_space->shrink(shrink_info.shrink_extent_count_))) {
    XENGINE_LOG(WARN, "fail to shrink extent space", K(ret), K(shrink_info));
  }

  return ret;
}

int TableSpace::get_shrink_info_if_need(const ShrinkCondition &shrink_condition, std::vector<ShrinkInfo> &shrink_infos)
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;
  bool need_shrink = false;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited first", K(ret));
  } else if (is_dropped_unsafe()) {
    XENGINE_LOG(INFO, "the table space is dropped", K_(table_space_id));
  } else {
    for (auto iter = extent_space_map_.begin(); SUCCED(ret) && extent_space_map_.end() != iter; ++iter) {
      ShrinkInfo shrink_info;
      need_shrink = false;
      if (IS_NULL(extent_space = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, ExentSpace must not nullptr", K(ret), "extent_space_type", iter->first);
      } else if (FAILED(extent_space->get_shrink_info_if_need(shrink_condition, need_shrink, shrink_info))) {
        XENGINE_LOG(WARN, "fail to get shrink info", K(ret), "extent_space_type", iter->first);
      } else {
        if (need_shrink) {
          shrink_info.shrink_condition_ = shrink_condition;
          shrink_info.index_id_set_ = index_id_set_;
          shrink_infos.push_back(shrink_info);
        }
      }
    }
  }

  return ret;
}

int TableSpace::get_shrink_info(const int32_t extent_space_type,
                                const ShrinkCondition &shrink_condition,
                                ShrinkInfo &shrink_info)
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;
  bool need_shrink = false;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited first",
        K(ret), K(extent_space_type), K(shrink_condition));
  } else if (is_dropped_unsafe()) {
    XENGINE_LOG(INFO, "the table space is dropped", K_(table_space_id));
  } else {
    auto iter = extent_space_map_.find(extent_space_type);
    if (extent_space_map_.end() == iter) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, can't find the extent space",
          K(ret), K_(table_space_id), K(extent_space_type));
    } else if (IS_NULL(extent_space = iter->second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, extent space must not nullptr",
          K(ret), K_(table_space_id), K(extent_space_type));
    } else if (FAILED(extent_space->get_shrink_info_if_need(shrink_condition,
                                                            need_shrink,
                                                            shrink_info))) {
      XENGINE_LOG(WARN, "fail to get shrink info", K(ret), K(extent_space_type));
    } else {
      shrink_info.shrink_condition_ = shrink_condition;
      shrink_info.index_id_set_ = index_id_set_;
      XENGINE_LOG(DEBUG, "success to get extent space shrink info", K(shrink_condition), K(shrink_info));
    }
  }

  return ret;
}

bool TableSpace::is_free()
{
  std::lock_guard<std::mutex> guard(table_space_mutex_);
  return is_free_unsafe();
}

bool TableSpace::is_dropped()
{
  std::lock_guard<std::mutex> guard(table_space_mutex_);
  return is_dropped_unsafe();
}

int TableSpace::get_data_file_stats(std::vector<DataFileStatistics> &data_file_stats)
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited first", K(ret));
  } else {
    for (auto iter = extent_space_map_.begin(); SUCCED(ret) && extent_space_map_.end() != iter; ++iter) {
      if (IS_NULL(extent_space = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, ExtentSpace must not nullptr", K(ret), "extent_space_type", iter->first);
      } else if (FAILED(extent_space->get_data_file_stats(data_file_stats))) {
        XENGINE_LOG(WARN, "fail to get data file stats", K(ret));
      }
    }
  }

  return ret;
}

int TableSpace::add_data_file(DataFile *data_file)
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited first", K(ret));
  } else if (IS_NULL(data_file)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(data_file));
  } else if (IS_NULL(extent_space = get_extent_space(data_file->get_extent_space_type()))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to get extent space", K(ret), "extent_space_type", data_file->get_extent_space_type());
  } else if (FAILED(extent_space->add_data_file(data_file))) {
    XENGINE_LOG(WARN, "fail to add datafile to extentspace", K(ret));
  }

  return ret;
}

int TableSpace::rebuild()
{
  int ret = Status::kOk;
  ExtentSpace *extent_space = nullptr;

  std::lock_guard<std::mutex> guard(table_space_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "TableSpace should been inited first", K(ret));
  } else {
    for (auto iter = extent_space_map_.begin(); SUCCED(ret) && extent_space_map_.end() != iter; ++iter) {
      if (IS_NULL(extent_space = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, ExtentSpace must not nullptr", K(ret), "extent_space_type", iter->first);
      } else if (FAILED(extent_space->rebuild())) {
        XENGINE_LOG(WARN, "fail to rebuild extentspace", K(ret), "extent_space_type", iter->first);
      } else {
        XENGINE_LOG(DEBUG, "success to rebuild tablespace", K_(table_space_id));
      }
    }
  }

  return ret;
}

ExtentSpace *TableSpace::get_extent_space(const int32_t extent_space_type)
{
  ExtentSpace *extent_space = nullptr;
  auto iter = extent_space_map_.find(extent_space_type);
  if (extent_space_map_.end() != iter) {
    extent_space = iter->second;
  }
  return extent_space;
}

bool TableSpace::is_free_unsafe()
{
  bool free = true;
  for (auto iter = extent_space_map_.begin(); extent_space_map_.end() != iter; ++iter) {
    if (!(iter->second->is_free())) {
      free = false;
      break;
    }
  }
  return free;

}

bool TableSpace::is_dropped_unsafe()
{
  return index_id_set_.empty();
}

} //namespace storage
} //namespace xengine
