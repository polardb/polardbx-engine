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

#include "util/filename.h"
#include "util/increment_number_allocator.h"
#include "extent_space.h"

namespace xengine
{
using namespace common;
using namespace util;
namespace storage
{
ExtentSpace::ExtentSpace(util::Env *env, const util::EnvOptions &env_options)
    : is_inited_(false),
      env_(env),
      env_options_(env_options),
      table_space_id_(0),
      extent_space_type_(HOT_EXTENT_SPACE),
      extent_space_path_(),
      full_data_file_map_(),
      not_full_data_file_map_(),
      last_alloc_ts_(0)
{
}

ExtentSpace::~ExtentSpace()
{
  destroy();
}

void ExtentSpace::destroy()
{
  if (is_inited_) {
    for (auto iter = all_data_file_map_.begin(); all_data_file_map_.end() != iter; ++iter) {
      MOD_DELETE_OBJECT(DataFile, iter->second);
    }
    all_data_file_map_.clear();
    full_data_file_map_.clear();
    not_full_data_file_map_.clear();
    is_inited_ = false;
  }
}

int ExtentSpace::create(const CreateExtentSpaceArgs &args)
{
  int ret = Status::kOk;
  DataFile *data_file = nullptr;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "ExtentSpace has been inited", K(ret));
  } else if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else {
    table_space_id_ = args.table_space_id_;
    extent_space_type_ = args.extent_space_type_;
    extent_space_path_ = args.db_path_.path;
    update_last_alloc_ts();
    is_inited_ = true;
  }

  return ret;
}

int ExtentSpace::remove()
{
  int ret = Status::kOk;
  bool can_remove = true;
  DataFile *data_file = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (!is_free()) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, ExtentSpace is not free", K(ret));
  } else {
    for (auto iter = all_data_file_map_.begin(); SUCCED(ret) && all_data_file_map_.end() != iter; ++iter) {
      if (FAILED(iter->second->remove())) {
        XENGINE_LOG(WARN, "fail to remove data_file", K(ret), "file_number", iter->first);
      }
    }
  }

  return ret;
}

int ExtentSpace::allocate(ExtentIOInfo& io_info)
{
  int ret = Status::kOk;
  DataFile *data_file = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (FAILED(allocate_extent(io_info))) {
    XENGINE_LOG(WARN, "fail to allocate extent from data file", K(ret));
  } else {
    update_last_alloc_ts();
  }

  return ret;
}

int ExtentSpace::recycle(const ExtentId extent_id)
{
  int ret = Status::kOk;
  DataFile *data_file = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (IS_NULL(data_file = get_data_file(extent_id.file_number))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to get data file", K(ret), K(extent_id));
  } else if (FAILED(recycle_extent_to_data_file(data_file, extent_id))) {
    XENGINE_LOG(WARN, "fail to recycle extent to datafile", K(ret), K(extent_id));
  } else {
    XENGINE_LOG(DEBUG, "success to recycle extent", K(extent_id));
  }

  return ret;
}

int ExtentSpace::reference(const ExtentId extent_id, ExtentIOInfo &io_info)
{
  int ret = Status::kOk;
  DataFile *data_file = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (IS_NULL(data_file = get_data_file(extent_id.file_number))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to get data file", K(ret), K(extent_id));
  } else if (FAILED(data_file->reference(extent_id, io_info))) {
    XENGINE_LOG(WARN, "fail to reference extent to datafile", K(ret), K(extent_id));
  } else {
    XENGINE_LOG(DEBUG, "success to reference extent", K(extent_id));
  }

  return ret;
}

int ExtentSpace::get_shrink_info_if_need(const ShrinkCondition &shrink_condition, bool &need_shrink, ShrinkInfo &shrink_info)
{
  int ret = Status::kOk;
  int64_t total_extent_count = 0;
  int64_t free_extent_count = 0;
  need_shrink = false;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else {
    total_extent_count = get_total_extent_count();
    free_extent_count = get_free_extent_count();
    if (need_shrink_extent_space(shrink_condition, total_extent_count, free_extent_count)){
      calc_shrink_info(shrink_condition, total_extent_count, free_extent_count, shrink_info);
      need_shrink = true;
    }
  }

  return ret;
}

int ExtentSpace::move_extens_to_front(const int64_t move_extent_count, std::unordered_map<int64_t, ExtentIOInfo> &replace_map)
{
  int ret = Status::kOk;
  int64_t free_extent_count = 0;
  ExtentId origin_extent_id;
  ExtentIOInfo new_io_info;
  DataFile *data_file = nullptr;
  int64_t file_number;
  char *extent_buf = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
  } else if (UNLIKELY(move_extent_count < 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(move_extent_count));
  } else if ((free_extent_count = get_free_extent_count()) < move_extent_count) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, no enough space to move", K(ret), K(move_extent_count), K(free_extent_count));
  } else if (IS_NULL(extent_buf = reinterpret_cast<char *>(memory::base_memalign(PAGE_SIZE, MAX_EXTENT_SIZE, memory::ModId::kExtentSpaceMgr)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for extent buf", K(ret));
  } else {
    auto iter = not_full_data_file_map_.rbegin();
    for (int64_t i = 0;
        SUCCED(ret) && not_full_data_file_map_.rend() != iter && i < move_extent_count;
        ++iter) {
      if (IS_NULL(data_file = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, DataFile should not been nullptr", K(ret), "file_number", iter->first);
      } else {
        file_number = data_file->get_file_number();
        for (int32_t offset = (data_file->get_total_extent_count() - 1);
             SUCCED(ret) && offset > 0 && i < move_extent_count; --offset, ++i) {
          origin_extent_id = ExtentId(file_number, offset);
          if (data_file->is_free_extent(origin_extent_id)) {
            //free extent, no need move
            XENGINE_LOG(INFO, "free extent, no need move", K(origin_extent_id));
          } else if (FAILED(move_one_extent_to_front(data_file, origin_extent_id, extent_buf, new_io_info))) {
            XENGINE_LOG(WARN, "fail to move one extent to front", K(ret), K(origin_extent_id));
          } else if (!(replace_map.emplace(origin_extent_id.id(), new_io_info).second)) {
            ret = Status::kErrorUnexpected;
            XENGINE_LOG(WARN, "unexpected error, fail to emplace to replace map", K(ret), K(origin_extent_id), K(new_io_info));
          }
        }
      }
    }
  }

  if (nullptr != extent_buf) {
    memory::base_memalign_free(extent_buf);
    extent_buf = nullptr;
  }

  return ret;
}

int ExtentSpace::shrink(const int64_t shrink_extent_count)
{
  int ret = Status::kOk;
  int64_t free_extent_count = 0;
  int64_t left_shrink_extent_count = 0;
  DataFile *data_file = nullptr;
  int64_t file_number = 0;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (UNLIKELY(shrink_extent_count <= 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(shrink_extent_count));
  } else if (shrink_extent_count > (free_extent_count = calc_tail_continuous_free_extent_count())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, no enough continuous extent to shrink", K(ret), K(shrink_extent_count), K(free_extent_count));
  } else {
    left_shrink_extent_count = shrink_extent_count;
    for (auto iter = not_full_data_file_map_.rbegin();
         SUCCED(ret) && not_full_data_file_map_.rend() != iter && left_shrink_extent_count > 0;) {
      file_number = iter->first;
      if (IS_NULL(data_file = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, datafile must not nullptr", K(ret), K(file_number));
      } else if (FAILED(shrink_data_file(data_file, left_shrink_extent_count))) {
        XENGINE_LOG(WARN, "fail to shrink datafile", K(ret), K(file_number), K(left_shrink_extent_count));
      } else {
        XENGINE_LOG(INFO, "success to shrink data file", K(file_number), K(left_shrink_extent_count));
        iter = not_full_data_file_map_.rbegin();
      }
    }
  }

  return ret;
}

bool ExtentSpace::is_free()
{
  bool free = true;
  for (auto iter = all_data_file_map_.begin(); all_data_file_map_.end() != iter; ++iter) {
    if (!(iter->second->is_free())) {
      free = false;
      break;
    }
  }
  return free;
}

int ExtentSpace::get_data_file_stats(std::vector<DataFileStatistics> &data_file_stats)
{
  int ret = Status::kOk;
  DataFile *data_file = nullptr;
  DataFileStatistics data_file_statistic;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else {
    for (auto iter = all_data_file_map_.begin(); SUCCED(ret) && all_data_file_map_.end() != iter; ++iter) {
      data_file_statistic.reset();
      if (IS_NULL(data_file = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, data file must not nullptr", K(ret), "file_number", iter->first);
      } else if (FAILED(data_file->get_data_file_statistics(data_file_statistic))) {
        XENGINE_LOG(WARN, "fail to get data file statistics", K(ret), "file_number", iter->first);
      } else {
        data_file_stats.push_back(data_file_statistic);
      }
    }
  }

  return ret;
}

int ExtentSpace::add_data_file(DataFile *data_file)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace shoule been inited first", K(ret));
  } else if (IS_NULL(data_file)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(data_file));
  } else if (FAILED(add_new_data_file(data_file))) {
    XENGINE_LOG(WARN, "fail to add new datafile", K(ret));
  }

  return ret;
}

int ExtentSpace::rebuild()
{
  int ret = Status::kOk;
  int64_t file_number;
  DataFile *data_file = nullptr;
  std::vector<DataFile *> full_data_file_vec;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else {
    for (auto iter = not_full_data_file_map_.begin(); SUCCED(ret) && not_full_data_file_map_.end() != iter; ++iter) {
      file_number = iter->first;
      if (IS_NULL(data_file = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, datafile must not nullptr", K(ret), K(file_number));
      } else if (FAILED(data_file->rebuild())) {
        XENGINE_LOG(WARN, "fail to rebuild datafile", K(ret), K(file_number));
      } else {
        XENGINE_LOG(DEBUG, "success to rebuild data file", K_(table_space_id), K_(extent_space_type), K(file_number));
        if (data_file->is_full()) {
          full_data_file_vec.push_back(data_file);
        }
      }
    }

    if (SUCCED(ret)) {
      for (uint32_t i = 0; SUCCED(ret) && i < full_data_file_vec.size(); ++i) {
        if (IS_NULL(data_file = full_data_file_vec.at(i))) {
          ret = Status::kErrorUnexpected;
          XENGINE_LOG(WARN, "unexpected error, datafile should not been nullptr", K(ret), K(i));
        } else if (FAILED(move_to_full_map(data_file))) {
          XENGINE_LOG(WARN, "fail to move data file to full map", K(ret), "file_number", data_file->get_file_number());
        } else {
          XENGINE_LOG(INFO, "success to move data file to full map", "file_number", data_file->get_file_number());
        }
      }
    }
  }

  return ret;
}

int64_t ExtentSpace::get_free_extent_count()
{
  int64_t free_extent_count = 0;
  for (auto iter = not_full_data_file_map_.begin(); not_full_data_file_map_.end() != iter; ++iter) {
    free_extent_count += iter->second->get_free_extent_count();
  }
  return free_extent_count;
}

int64_t ExtentSpace::get_total_extent_count()
{
  int64_t total_extent_count = 0;
  for (auto iter = not_full_data_file_map_.begin(); not_full_data_file_map_.end() != iter; ++iter) {
    total_extent_count += iter->second->get_total_extent_count();
  }
  return total_extent_count;
}

int ExtentSpace::find_allocatable_data_file(DataFile *&allocatable_data_file)
{
  int ret = Status::kOk;
  DataFile *data_file = nullptr;

  if (not_full_data_file_map_.empty() && FAILED(create_data_file())) {
    XENGINE_LOG(WARN, "fail to create data file", K(ret));
  } else {
    for (auto iter = not_full_data_file_map_.begin(); SUCCED(ret) && not_full_data_file_map_.end() != iter; ++iter) {
      if (IS_NULL(data_file = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, data file should not been nullptr", K(ret), "file_number", iter->first);
      } else if (data_file->has_free_extent()) {
        allocatable_data_file = data_file;
        break;
      }
    }

    if (SUCCED(ret)) {
      if (nullptr == allocatable_data_file) {
        //no free extent exist, select the first not full data file to expand
        allocatable_data_file = not_full_data_file_map_.begin()->second;
      }
    }
    assert(nullptr != allocatable_data_file);
  }

  return ret;
}

DataFile *ExtentSpace::get_data_file(const int32_t file_number)
{
  DataFile *data_file = nullptr;
  auto iter = all_data_file_map_.find(file_number);
  if (all_data_file_map_.end() != iter) {
    data_file = iter->second;
  }
  return data_file;
}

int ExtentSpace::create_data_file()
{
  int ret = Status::kOk;
  int64_t file_number = util::FileNumberAllocator::get_instance().alloc();
  DataFile *data_file = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else {
    std::string data_file_path = util::MakeTableFileName(extent_space_path_, file_number);
    CreateDataFileArgs data_file_args(table_space_id_, extent_space_type_, file_number, data_file_path);
    if (IS_NULL(data_file = MOD_NEW_OBJECT(memory::ModId::kExtentSpaceMgr, DataFile, env_, env_options_))) {
      ret = Status::kMemoryLimit;
      XENGINE_LOG(WARN, "fail to allocate memory for DataFile", K(ret));
    } else if (FAILED(data_file->create(data_file_args))) {
      XENGINE_LOG(ERROR, "fail to create data file", K(ret));
    } else if (FAILED(add_new_data_file(data_file))) {
      XENGINE_LOG(ERROR, "fail to add new data file", K(ret), K(data_file_args));
    }
  }

  return ret;
}

int ExtentSpace::add_new_data_file(DataFile *data_file)
{
  int ret = Status::kOk;
  int64_t file_number = 0;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (IS_NULL(data_file)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(data_file));
  } else { 
    file_number = data_file->get_file_number();
    if (!(not_full_data_file_map_.emplace(file_number, data_file).second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(ERROR, "unexpected error, fail to emplace data file", K(ret), K(file_number));
    } else if (!(all_data_file_map_.emplace(file_number, data_file).second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(ERROR, "unexpected error, fail to emplace datfile to all_map", K(ret), K(file_number));
    } else {
      XENGINE_LOG(INFO, "success to add new data file to map", K(file_number));
    }
  }

  return ret;
}

int ExtentSpace::delete_data_file(DataFile *data_file)
{
  int ret = Status::kOk;
  int64_t file_number;
  int64_t erase_count = 0;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (IS_NULL(data_file)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(data_file));
  } else { 
    file_number = data_file->get_file_number();
    if (1 != (erase_count = not_full_data_file_map_.erase(file_number))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to erase data file", K(ret), K(file_number));
    } else if (1 != (erase_count = all_data_file_map_.erase(file_number))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to erase datfile from all_map", K(ret), K(file_number));
    } else if (FAILED(data_file->remove())) {
      XENGINE_LOG(WARN, "fail to close datafile", K(ret), K(file_number));
    } else {
      MOD_DELETE_OBJECT(DataFile, data_file);
      XENGINE_LOG(DEBUG, "success to erase data file to map", K(file_number));
    }
  }

  return ret;

}

int ExtentSpace::allocate_extent(ExtentIOInfo &io_info)
{
  int ret = Status::kOk;
  DataFile *data_file = nullptr;

  if (FAILED(find_allocatable_data_file(data_file))) {
    XENGINE_LOG(WARN, "fail to find datafile to allocate", K(ret));
  } else if (FAILED(alloc_extent_from_data_file(data_file, io_info))) {
    XENGINE_LOG(WARN, "fail to alloc extent from data file", K(ret), "file_number", data_file->get_file_number());
  } else {
    XENGINE_LOG(DEBUG, "success to alloc extent from data file", K(io_info));
  }

  return ret;
}

int ExtentSpace::alloc_extent_from_data_file(DataFile *data_file, ExtentIOInfo &io_info)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (IS_NULL(data_file) || UNLIKELY(data_file->is_full())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(data_file));
  } else if (FAILED(data_file->allocate(io_info))) {
    XENGINE_LOG(WARN, "fail to allocate extent", K(ret));
  } else if (data_file->is_full() && FAILED(move_to_full_map(data_file))) {
    XENGINE_LOG(WARN, "fail to move the datafile to full_map", K(ret), "file_number", data_file->get_file_number());
  }

  return ret;
}

int ExtentSpace::recycle_extent_to_data_file(DataFile *data_file, const ExtentId extent_id)
{
  int ret = Status::kOk;
  bool is_full = false;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (IS_NULL(data_file)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(data_file));
  } else {
    is_full = data_file->is_full();
    if (FAILED(data_file->recycle(extent_id))) {
      XENGINE_LOG(WARN, "fail to recycle extent", K(ret), K(extent_id));
    } else if (is_full && FAILED(move_to_not_full_map(data_file))) {
      XENGINE_LOG(WARN, "fail to move datafile to not_full_map", K(ret), K(extent_id));
    }
  }

  return ret;
}

int ExtentSpace::move_to_full_map(DataFile *data_file)
{
  int ret = Status::kOk;
  int64_t file_number = 0;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (IS_NULL(data_file) || UNLIKELY(!data_file->is_full())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(data_file));
  } else {
    file_number = data_file->get_file_number();
    if (UNLIKELY(not_full_data_file_map_.end() == not_full_data_file_map_.find(file_number))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, the datafile not exist in not_full_map", K(ret), K(file_number));
    } else if (UNLIKELY(full_data_file_map_.end() != full_data_file_map_.find(file_number))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, the datafile has exist in full_map", K(ret), K(file_number));
    } else if (1 != (not_full_data_file_map_.erase(file_number))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to erase from not_full_map", K(ret), K(file_number));
    } else if (!(full_data_file_map_.emplace(file_number, data_file).second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to emplace to full_map", K(ret), K(file_number));
    } else {
      XENGINE_LOG(INFO, "success to move datafile to full_map", K(file_number));
    }
  }

  return ret;
}

int ExtentSpace::move_to_not_full_map(DataFile *data_file)
{
  int ret = Status::kOk;
  int64_t file_number = 0;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (IS_NULL(data_file)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(data_file));
  } else {
    file_number = data_file->get_file_number();
    if (UNLIKELY(full_data_file_map_.end() == full_data_file_map_.find(file_number))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, the datafile not exist in full_map", K(ret), K(file_number));
    } else if (UNLIKELY(not_full_data_file_map_.end() != not_full_data_file_map_.find(file_number))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, the datafile has exist in not_full_map", K(ret), K(file_number));
    } else if (1 != (full_data_file_map_.erase(file_number))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to erase from full_map", K(ret), K(file_number));
    } else if (!(not_full_data_file_map_.emplace(file_number, data_file).second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to emplace to not_full_map", K(ret), K(file_number));
    } else {
     XENGINE_LOG(INFO, "success to move datafile to not_full_map", K(file_number)); 
    }
  }

  return ret;
}

int ExtentSpace::move_one_extent_to_front(DataFile *src_data_file,
                                          const ExtentId origin_extent_id,
                                          char *extent_buf,
                                          ExtentIOInfo &new_io_info)
{
  int ret = Status::kOk;
  ExtentIOInfo origin_io_info;
  RandomAccessExtent origin_extent;
  WritableExtent new_extent;
  Slice extent_slice;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (IS_NULL(src_data_file) || IS_NULL(extent_buf)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(src_data_file), KP(extent_buf));
  } else if (FAILED(src_data_file->get_extent_io_info(origin_extent_id, origin_io_info))){
    XENGINE_LOG(WARN, "fail to get extent io info", K(ret), K(origin_extent_id));
  } else if (FAILED(origin_extent.init(origin_io_info, nullptr))) {
    XENGINE_LOG(WARN, "fail to init origin extent", K(ret), K(origin_io_info));
  } else if (FAILED(allocate_extent(new_io_info))) {
    XENGINE_LOG(WARN, "fail to allocate new extent", K(ret));
  } else if (new_io_info.extent_id_.id() >= origin_io_info.extent_id_.id()) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, the new extent should at the front", K(ret), K(origin_io_info), K(new_io_info));
  } else if (FAILED(new_extent.init(new_io_info))) {
    XENGINE_LOG(WARN, "fail to init new extent", K(ret), K(new_io_info));
  } else if (FAILED(origin_extent.Read(0, origin_io_info.extent_size_, &extent_slice, extent_buf).code())) {
    XENGINE_LOG(WARN, "fail to read origin extent", K(ret), K(origin_extent_id));
  } else if (FAILED(new_extent.Append(extent_slice).code())) {
    XENGINE_LOG(WARN, "fail to write new extent", K(ret), "new_extent_id", new_extent.get_extent_id());
  } else {
    XENGINE_LOG(INFO, "success to move one extent to front", K(origin_extent_id), K(new_io_info));
  }

  return ret;
}

bool ExtentSpace::need_shrink_extent_space(const ShrinkCondition &shrink_condition,
                                           const int64_t total_extent_count,
                                           const int64_t free_extent_count)
{
  double max_free_extent_percent = (shrink_condition.max_free_extent_percent_ * 1.0) / 100;
  int64_t shrink_allocate_interval = shrink_condition.shrink_allocate_interval_ * 1000000; //us
  int64_t max_free_extent_count = total_extent_count * max_free_extent_percent;
  return reach_shrink_allocate_interval(shrink_allocate_interval)
         && free_extent_count > 1
         && free_extent_count > max_free_extent_count;
}

void ExtentSpace::calc_shrink_info(const ShrinkCondition &shrink_condition,
                                   const int64_t total_extent_count,
                                   const int64_t free_extent_count,
                                   ShrinkInfo &shrink_info)
{
  double max_free_extent_percent = (shrink_condition.max_free_extent_percent_ * 1.0) / 100;
  int64_t max_free_extent_count = total_extent_count * max_free_extent_percent;
  shrink_info.table_space_id_ = table_space_id_;
  shrink_info.extent_space_type_ = extent_space_type_;
  shrink_info.total_need_shrink_extent_count_ = free_extent_count - max_free_extent_count;
  shrink_info.shrink_extent_count_ = std::min(shrink_info.total_need_shrink_extent_count_, shrink_condition.max_shrink_extent_count_);
}

int64_t ExtentSpace::calc_tail_continuous_free_extent_count() const
{
  const DataFile *data_file = nullptr;
  int64_t free_extent_count = 0;
  for (auto iter = not_full_data_file_map_.rbegin(); not_full_data_file_map_.rend() != iter; ++iter) {
    data_file = iter->second;
    if (data_file->is_free()) {
      free_extent_count += data_file->get_free_extent_count();
    } else {
      free_extent_count += data_file->calc_tail_continuous_free_extent_count();
      break;
    }
  }
  return free_extent_count;
}

int ExtentSpace::shrink_data_file(DataFile *data_file, int64_t &left_shrink_extent_count)
{
  int ret = Status::kOk;
  int64_t file_number = 0;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpace should been inited first", K(ret));
  } else if (IS_NULL(data_file)
             || UNLIKELY(left_shrink_extent_count <= 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(data_file), K(left_shrink_extent_count));
  } else {
    file_number = data_file->get_file_number();
    if (data_file->is_free()) {
      if (data_file->get_free_extent_count() <= left_shrink_extent_count) {
        left_shrink_extent_count -= data_file->get_free_extent_count();
        if (FAILED(delete_data_file(data_file))) {
          XENGINE_LOG(WARN, "fail to delete data file", K(ret), K(file_number));
        }
      } else if (FAILED(data_file->shrink(left_shrink_extent_count))) {
        XENGINE_LOG(WARN, "fail to shrink datafile", K(ret), K(file_number), K(left_shrink_extent_count));
      } else {
        left_shrink_extent_count = 0;
      }
    } else if (FAILED(data_file->shrink(left_shrink_extent_count))) {
      XENGINE_LOG(WARN, "fail to shrink datafile", K(ret), K(file_number), K(left_shrink_extent_count));
    } else {
      left_shrink_extent_count = 0;
    }
  }

  return ret;
}

} //namespace storage
} //namespace xengine
