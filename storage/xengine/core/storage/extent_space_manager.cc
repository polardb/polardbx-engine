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
#include "extent_meta_manager.h"
#include "extent_space_manager.h"
#include "storage_meta_struct.h"
#include "util/sync_point.h"


namespace xengine
{
using namespace common;
using namespace util;
using namespace memory;
namespace storage
{
ExtentSpaceManager::ExtentSpaceManager(util::Env *env,
                                       const util::EnvOptions &env_options,
                                       const common::DBOptions &db_options)
    : is_inited_(false),
      env_(env),
      env_options_(env_options),
      db_options_(db_options),
      extent_meta_mgr_(nullptr),
      table_space_lock_(),
      table_space_id_(0),
      table_space_map_(),
      wait_remove_table_space_map_(),
      io_info_map_lock_(),
      extent_io_info_map_(),
      garbage_files_()
{
  env_options_.use_direct_reads = true;
  env_options_.use_direct_writes = true;
}

ExtentSpaceManager::~ExtentSpaceManager()
{
  destroy();
}

void ExtentSpaceManager::destroy()
{
  if (is_inited_) {
    for (auto iter = table_space_map_.begin(); table_space_map_.end() != iter; ++iter) {
      MOD_DELETE_OBJECT(TableSpace, iter->second); 
    }
    MOD_DELETE_OBJECT(ExtentMetaManager, extent_meta_mgr_);
    table_space_map_.clear();
    wait_remove_table_space_map_.clear();
    extent_io_info_map_.clear();
    garbage_files_.clear();
    is_inited_ = false;
  }
}

int ExtentSpaceManager::init(StorageLogger *storage_logger)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "ExtentSpaceManager has been inited", K(ret));
  } else if (IS_NULL(storage_logger)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(storage_logger));
  } else if (IS_NULL(extent_meta_mgr_ = MOD_NEW_OBJECT(ModId::kExtentSpaceMgr,
                                                       ExtentMetaManager))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for ExtentMetaManager", K(ret));
  } else if (FAILED(extent_meta_mgr_->init(storage_logger))) {
    XENGINE_LOG(WARN, "fail to init extent_meta_manager", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ExtentSpaceManager::create_table_space(const int64_t table_space_id)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (UNLIKELY(table_space_id < 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(table_space_id));
  } else {
    SpinWLockGuard w_guard(table_space_lock_);
    if (FAILED(internal_create_table_space(table_space_id))) {
      XENGINE_LOG(WARN, "fail to create table space", K(table_space_id));
    } else {
      XENGINE_LOG(INFO, "success to create table space", K(table_space_id));
    }
  }

  return ret;
}

int ExtentSpaceManager::open_table_space(const int64_t table_space_id)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (UNLIKELY(table_space_id < 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(table_space_id));
  } else {
    SpinWLockGuard w_guard(table_space_lock_);
    if (table_space_map_.end() != table_space_map_.find(table_space_id)) {
      XENGINE_LOG(INFO, "table space has opened", K(table_space_id));
    } else if (FAILED(internal_create_table_space(table_space_id))) {
      XENGINE_LOG(WARN, "fail to internal create table space", K(ret), K(table_space_id));
    } else {
      update_max_table_space_id(table_space_id);
      XENGINE_LOG(INFO, "success to open table space", K(table_space_id));
    }
  }
    
  return ret;
}

int ExtentSpaceManager::recycle_dropped_table_space()
{
  int ret = Status::kOk;
  int64_t table_space_id = 0;
  TableSpace *table_space = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else {
    SpinWLockGuard w_guard(table_space_lock_);
    for (auto iter = wait_remove_table_space_map_.begin(); SUCCED(ret) && wait_remove_table_space_map_.end() != iter;) {
      table_space_id = iter->first;
      if (IS_NULL(table_space = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, TableSpace must not nullptr", K(ret), K(table_space_id));
      } else {
        if (table_space->is_free()) {
          if (FAILED(table_space->remove())) {
            XENGINE_LOG(WARN, "fail to remove table space", K(ret), K(table_space_id));
          } else if (1 != table_space_map_.erase(table_space_id)) {
            ret = Status::kErrorUnexpected;
            XENGINE_LOG(WARN, "unexpected error, fail to erase table space", K(ret), K(table_space_id));
          } else {
            MOD_DELETE_OBJECT(TableSpace, table_space);
            iter = wait_remove_table_space_map_.erase(iter);
            XENGINE_LOG(INFO, "success to recycle dropped table space", K(table_space_id));
          }
        } else {
          ++iter;
          XENGINE_LOG(INFO, "TableSpace not free, can't recycle", K(table_space_id));
        }
      }
    }
  }

  return ret;
}

int ExtentSpaceManager::register_subtable(const int64_t table_space_id, const int64_t index_id)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;

  SpinRLockGuard r_guard(table_space_lock_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret), K(table_space_id), K(index_id));
  } else if (UNLIKELY(table_space_id < 0) || UNLIKELY(index_id < 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(table_space_id), K(index_id));
  } else if (IS_NULL(table_space = get_table_space(table_space_id))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, tablespace should not been nullptr", K(ret), K(table_space_id), K(index_id));
  } else if (FAILED(table_space->register_subtable(index_id))) {
    XENGINE_LOG(WARN, "fail to unregister subtable", K(ret), K(table_space_id), K(index_id));
  }

  return ret;
}

int ExtentSpaceManager::unregister_subtable(const int64_t table_space_id, const int64_t index_id)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;

  SpinWLockGuard w_guard(table_space_lock_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret), K(table_space_id), K(index_id));
  } else if (UNLIKELY(table_space_id < 0) || UNLIKELY(index_id < 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalaid argument", K(ret), K(table_space_id), K(index_id));
  } else if (IS_NULL(table_space = get_table_space(table_space_id))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, tablespace should not been nullptr", K(ret), K(table_space_id), K(index_id));
  } else if (FAILED(table_space->unregister_subtable(index_id))) {
    XENGINE_LOG(WARN, "fail to register subtable", K(ret), K(table_space_id), K(index_id));
  } else {
    if (table_space->is_dropped()) {
      if (!(wait_remove_table_space_map_.emplace(table_space_id, table_space).second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, fail to emplace to remove table space map", K(ret), K(table_space_id), K(index_id));
      } else {
        XENGINE_LOG(INFO, "the table space waiting to remove", K(table_space_id), K(index_id));
      }
    }
  }

  return ret;
}

int ExtentSpaceManager::allocate(const int64_t table_space_id,
                                 const int32_t extent_space_type,
                                 WritableExtent &extent)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;
  ExtentIOInfo io_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (UNLIKELY(table_space_id < 0) || UNLIKELY(!is_valid_extent_space_type(extent_space_type))) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(table_space_id), K(extent_space_type));
  } else {
#ifndef NDEBUG
    TEST_SYNC_POINT("ExtentSpaceManager::allocate::inject_allocate_hang");
#endif
    {
      SpinRLockGuard r_guard(table_space_lock_);
      if (IS_NULL(table_space = get_table_space(table_space_id))) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, fail to find tablespace", K(ret), K(table_space_id), K(extent_space_type));
      } else if (FAILED(table_space->allocate(extent_space_type, io_info))) {
        XENGINE_LOG(WARN, "fail to allocate extent", K(ret), K(table_space_id), K(extent_space_type));
      }
    }
    
    if (SUCCED(ret)) {
      SpinWLockGuard w_guard(io_info_map_lock_);
      if (FAILED(extent.init(io_info))) {
        XENGINE_LOG(WARN, "fail to init WritableExtent", K(ret), K(io_info), K(table_space_id), K(extent_space_type));
      } else if (!(extent_io_info_map_.emplace(io_info.extent_id_.id(), io_info).second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, fail to emplace to io_info map", K(ret), K(io_info), K(table_space_id), K(extent_space_type));
      }
      assert(Status::kOk == ret);
    }

    if (SUCCED(ret)) {
      XENGINE_LOG(INFO, "success to allocate extent", K(io_info), K(table_space_id), K(extent_space_type));
    }
  }

  return ret;
}

int ExtentSpaceManager::recycle(const int64_t table_space_id,
                                const int32_t extent_space_type,
                                const ExtentId extent_id,
                                bool has_meta)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (UNLIKELY(table_space_id < 0) || UNLIKELY(!is_valid_extent_space_type(extent_space_type))) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(table_space_id), K(extent_space_type), K(extent_id));
  } else {
    if (SUCCED(ret)) {
#ifndef NDEBUG
      TEST_SYNC_POINT("ExtentSpaceManager::recycle::inject_recycle_io_info_hang");
#endif
      SpinWLockGuard w_guard(io_info_map_lock_);
      if (1 != (extent_io_info_map_.erase(extent_id.id()))) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, fail to erase from io_info map", K(ret), K(table_space_id), K(extent_space_type), K(extent_id));
      }
    }

    if (SUCCED(ret)) {
      if (has_meta && FAILED(extent_meta_mgr_->recycle_meta(extent_id))) {
        XENGINE_LOG(WARN, "fail to recycle extent meta", K(ret), K(table_space_id), K(extent_space_type), K(extent_id));
      } else {
        XENGINE_LOG(INFO, "success to recycle extent", K(table_space_id), K(extent_space_type), K(extent_id));
      }
    }

    /**after recycle from table space, the extent is allocatable. so recycle from table space after recycle
    from extent_io_info_map and extent_meta_mgr_*/
    if (SUCCED(ret)) {
      SpinRLockGuard r_guard(table_space_lock_);
      if (IS_NULL(table_space = get_table_space(table_space_id))) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, tablespace should not nullptr",
                    K(ret), K(table_space_id), K(extent_space_type),
                    K(extent_id));
      } else if (FAILED(table_space->recycle(extent_space_type, extent_id))) {
        XENGINE_LOG(WARN, "fail to recycle extent", K(ret), K(table_space_id),
                    K(extent_space_type), K(extent_id));
      }
    }
  }

  return ret;
}

int ExtentSpaceManager::reference(const int64_t table_space_id,
                                  const int32_t extent_space_type,
                                  const ExtentId extent_id)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;
  ExtentIOInfo io_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (UNLIKELY(table_space_id < 0) || UNLIKELY(!is_valid_extent_space_type(extent_space_type))) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(table_space_id), K(extent_space_type), K(extent_id));
    abort();
  } else if (IS_NULL(table_space = get_table_space(table_space_id))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to find tablespace", K(ret), K(table_space_id), K(extent_space_type), K(extent_id));
  } else if (FAILED(table_space->reference(extent_space_type, extent_id, io_info))) {
    XENGINE_LOG(WARN, "fail to reference extent", K(ret), K(table_space_id), K(extent_space_type), K(extent_id));
  } else if (!(extent_io_info_map_.emplace(extent_id.id(), io_info).second)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "fail to emplace to io_info map", K(ret), K(table_space_id), K(extent_space_type), K(extent_id));
  } else {
    XENGINE_LOG(INFO, "success to reference extent", K(table_space_id), K(extent_space_type), K(extent_id));
  }

  return ret;
}

common::Status ExtentSpaceManager::get_random_access_extent(ExtentId extent_id,
                                                            RandomAccessExtent &random_access_extent)
{
  int ret = Status::kOk;
  ExtentIOInfo io_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else {
    SpinRLockGuard r_guard(io_info_map_lock_);
    auto iter = extent_io_info_map_.find(extent_id.id());
    if (extent_io_info_map_.end() == iter) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, the extent not exist", K(ret), K(extent_id));
    } else if (FAILED(random_access_extent.init(iter->second, this))) {
      XENGINE_LOG(WARN, "fail to init random access extent", K(ret), K(io_info));
    } else {
      XENGINE_LOG(DEBUG, "success to get random access extent", K(extent_id));
    }
  }

  return ret;
}

int ExtentSpaceManager::get_meta(const ExtentId &extent_id, ExtentMeta *&extent_meta)
{
  int ret = Status::kOk;
  if (IS_NULL(extent_meta = extent_meta_mgr_->get_meta(extent_id))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to get extent meta", K(ret), K(extent_id));
  }
  return ret;
}

int ExtentSpaceManager::write_meta(const ExtentMeta &extent_meta, bool write_log)
{
  return extent_meta_mgr_->write_meta(extent_meta, write_log);
}

int ExtentSpaceManager::recycle_meta(const ExtentId extent_id)
{
  return extent_meta_mgr_->recycle_meta(extent_id);
}

int ExtentSpaceManager::do_checkpoint(util::WritableFile *checkpoint_writer, CheckpointHeader *header)
{
  return extent_meta_mgr_->do_checkpoint(checkpoint_writer, header);
}

int ExtentSpaceManager::load_checkpoint(util::RandomAccessFile *checkpoint_reader, CheckpointHeader *header)
{
  return extent_meta_mgr_->load_checkpoint(checkpoint_reader, header);
}

int ExtentSpaceManager::replay(int64_t log_type, char *log_data, int64_t log_len)
{
  return extent_meta_mgr_->replay(log_type, log_data, log_len);
}

int ExtentSpaceManager::get_shrink_infos(const ShrinkCondition &shrink_condition, std::vector<ShrinkInfo> &shrink_infos)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (UNLIKELY(!shrink_condition.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(shrink_condition));
  } else {
    SpinRLockGuard r_guard(table_space_lock_);
    for (auto iter = table_space_map_.begin(); SUCCED(ret) && table_space_map_.end() != iter; ++iter) {
      if (IS_NULL(table_space = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, TableSpace must not nullptr", K(ret), "table_space_id", iter->first);
      } else if (FAILED(table_space->get_shrink_info_if_need(shrink_condition, shrink_infos))) {
        XENGINE_LOG(WARN, "fail to get shrink info", K(ret), "table_space_id", iter->first);
      }
    }
  }

  return ret;
}

int ExtentSpaceManager::get_shrink_infos(const int64_t table_space_id,
                                         const ShrinkCondition &shrink_condition,
                                         std::vector<ShrinkInfo> &shrink_infos)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (UNLIKELY(!shrink_condition.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argment", K(ret), K(shrink_condition));
  } else {
    SpinRLockGuard r_guard(table_space_lock_);
    if (IS_NULL(table_space = get_table_space(table_space_id))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to find tablespace", K(ret));
    } else if (FAILED(table_space->get_shrink_info_if_need(shrink_condition, shrink_infos))) {
      XENGINE_LOG(WARN, "fail to get shrink info", K(ret), K(table_space_id));
    } else {
      XENGINE_LOG(INFO, "success to get shrink info", K(table_space_id), K(shrink_condition));
    }
  }

  return ret;
}

int ExtentSpaceManager::get_shrink_info(const int64_t table_space_id,
                                        const int32_t extent_space_type,
                                        const ShrinkCondition &shrink_condition,
                                        ShrinkInfo &shrink_info)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (UNLIKELY(!shrink_condition.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret),
        K(table_space_id), K(extent_space_type), K(shrink_condition));
  } else {
    SpinRLockGuard r_guard(table_space_lock_);
    if (IS_NULL(table_space = get_table_space(table_space_id))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to find tablespace", K(ret),
          K(table_space_id), K(extent_space_type), K(shrink_condition));
    } else if (FAILED(table_space->get_shrink_info(extent_space_type,
                                                   shrink_condition,
                                                   shrink_info))) {
      XENGINE_LOG(WARN, "fail to get shrink info", K(ret),
          K(table_space_id), K(extent_space_type), K(shrink_condition));
    } else {
      XENGINE_LOG(INFO, "success to get shrink info",
          K(table_space_id), K(extent_space_type), K(shrink_condition));
    }
  }

  return ret;
}

int ExtentSpaceManager::move_extens_to_front(const ShrinkInfo &shrink_info, std::unordered_map<int64_t, ExtentIOInfo> &replace_map)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (UNLIKELY(!shrink_info.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(shrink_info));
  } else {
    {
      SpinRLockGuard r_guard(table_space_lock_);
      if (IS_NULL(table_space = get_table_space(shrink_info.table_space_id_))) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, TableSpace must not nullptr", K(ret), K(shrink_info));
      } else if (FAILED(table_space->move_extens_to_front(shrink_info, replace_map))) {
        XENGINE_LOG(WARN, "fail to move extents to front", K(ret), K(shrink_info));
      }
    }

    if (SUCCED(ret)) {
      SpinWLockGuard w_guard(io_info_map_lock_);
      for (auto iter = replace_map.begin(); SUCCED(ret) && replace_map.end() != iter; ++iter) {
        const ExtentIOInfo &io_info = iter->second;
        if (!(extent_io_info_map_.emplace(io_info.extent_id_.id(), io_info).second)) {
          ret = Status::kErrorUnexpected;
          XENGINE_LOG(WARN, "unexpected error, fail to emplace to io_info map", K(ret), K(io_info));
        }
      }
    }
  }

  return ret;
}

int ExtentSpaceManager::shrink_extent_space(const ShrinkInfo &shrink_info)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;

  SpinRLockGuard r_guard(table_space_lock_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (IS_NULL(table_space = get_table_space(shrink_info.table_space_id_))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "fail to get table space", K(ret), K(shrink_info));
  } else if (FAILED(table_space->shrink(shrink_info))) {
    XENGINE_LOG(WARN, "fail to shrink extent space", K(ret), K(shrink_info));
  } else {
    XENGINE_LOG(INFO, "success to shrink extent space", K(shrink_info));
  }

  return ret;
}

int ExtentSpaceManager::get_data_file_stats(std::vector<DataFileStatistics> &data_file_stats)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "unexpected error, ExtentSpaceManager should been inited first", K(ret));
  } else {
    SpinRLockGuard r_guard(table_space_lock_);
    for (auto iter = table_space_map_.begin(); SUCCED(ret) && table_space_map_.end() != iter; ++iter) {
      if (IS_NULL(table_space = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, TableSpace must not nullptr", K(ret), "table_space_id", iter->first);
      } else if (FAILED(table_space->get_data_file_stats(data_file_stats))) {
        XENGINE_LOG(WARN, "fail to get data file stats", K(ret), "table_space_id", iter->first);
      }
    }
  }

  return ret;
}

int ExtentSpaceManager::get_data_file_stats(const int64_t table_space_id,
                                            std::vector<DataFileStatistics> &data_file_stats)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "unexpected error, ExtentSpaceManager should been inited first", K(ret));
  } else if (UNLIKELY(table_space_id < 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(table_space_id));
  } else {
    SpinRLockGuard r_guard(table_space_lock_);
    auto iter = table_space_map_.find(table_space_id);
    if (table_space_map_.end() == iter) {
      ret = Status::kNotFound;
      XENGINE_LOG(WARN, "the table space not found", K(ret), K(table_space_id));
    } else if (IS_NULL(table_space = iter->second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, TableSpace must not nullptr", K(ret), K(table_space_id));
    } else if (FAILED(table_space->get_data_file_stats(data_file_stats))) {
      XENGINE_LOG(WARN, "fail to get data file stats", K(ret), K(table_space_id));
    } else {
      XENGINE_LOG(DEBUG, "success to get data file stats", K(table_space_id));
    }
  }

  return ret;
}

int ExtentSpaceManager::open_all_data_file()
{
  int ret = Status::kOk;
  std::string dir_path;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < db_options_.db_paths.size(); ++i) {
      dir_path = db_options_.db_paths.at(i).path;
      XENGINE_LOG(INFO, "open datafiles in directory", K(dir_path));
      if (FAILED(open_specific_directory(dir_path))) {
        XENGINE_LOG(WARN, "fail to open specific directory", K(ret), K(dir_path));
      }
    }
  }

  return ret;
}

int ExtentSpaceManager::rebuild()
{
  int ret = Status::kOk;
  int64_t table_space_id = 0;
  TableSpace *table_space = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else {
    SpinRLockGuard r_guard(table_space_lock_);
    for (auto iter = table_space_map_.begin(); SUCCED(ret) && table_space_map_.end() != iter; ++iter) {
      table_space_id = iter->first;
      if (IS_NULL(table_space = iter->second)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, tablespace must not nullptr", K(ret), K(table_space_id));
      } else if (FAILED(table_space->rebuild())) {
        XENGINE_LOG(WARN, "fail to rebuild table space", K(ret), K(table_space_id));
      } else {
        XENGINE_LOG(DEBUG, "success to rebuild tablespace", K(table_space_id));
      }
    }

    if (SUCCED(ret)) {
      if (FAILED(extent_meta_mgr_->clear_free_extent_meta())) {
        XENGINE_LOG(WARN, "fail to clear free extent meta", K(ret));
      } else if (FAILED(clear_garbage_files())) {
        XENGINE_LOG(WARN, "fail to clear garbage files", K(ret));
      }
    }
  }

  return ret;
}

int ExtentSpaceManager::internal_create_table_space(const int64_t table_space_id)
{
  int ret = Status::kOk;
  TableSpace *table_space = nullptr;
  CreateTableSpaceArgs args(table_space_id, db_options_.db_paths);

  if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else if (IS_NULL(table_space = MOD_NEW_OBJECT(ModId::kExtentSpaceMgr,
                                                  TableSpace,
                                                  env_,
                                                  env_options_))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for TableSpace", K(ret));
  } else if (FAILED(table_space->create(args))) {
    XENGINE_LOG(WARN, "fail to create table space", K(ret), K(args));
  } else if (!(table_space_map_.emplace(table_space_id, table_space).second)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, fail to emplace table space", K(ret), K(args));
  }

  return ret;
}

TableSpace *ExtentSpaceManager::get_table_space(int64_t table_space_id)
{
  TableSpace *table_space = nullptr;
  auto iter = table_space_map_.find(table_space_id);
  if (table_space_map_.end() != iter) {
    table_space = iter->second;
  }
  return table_space;
}

int ExtentSpaceManager::open_specific_directory(const std::string &dir_path)
{
  int ret = Status::kOk;
  std::vector<int64_t> data_file_numbers;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret), K(dir_path));
  } else if (FAILED(get_data_file_numbers(dir_path, data_file_numbers))) {
    XENGINE_LOG(WARN, "fail to get datafile numers", K(ret), K(dir_path));
  } else if (FAILED(open_data_files(dir_path, data_file_numbers))) {
    XENGINE_LOG(ERROR, "fail to open datafiles", K(ret), K(dir_path));
  } else {
    XENGINE_LOG(INFO, "success to open specific directory", K(dir_path));
  }

  return ret;
}

int ExtentSpaceManager::get_data_file_numbers(const std::string &dir_path,
                                              std::vector<int64_t> &data_file_numbers)
{
  int ret = Status::kOk;
  std::string file_name;
  std::vector<std::string> data_file_names;
  uint64_t file_number;
  util::FileType file_type;
  Slice dummy_slice;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret));
  } else if (FAILED(env_->GetChildren(dir_path, &data_file_names).code())) {
    XENGINE_LOG(WARN, "fail to get data file names", K(ret), K(dir_path));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < data_file_names.size(); ++i) {
      file_name = data_file_names.at(i);
      if (ParseFileName(file_name, &file_number, dummy_slice, &file_type) && util::kTableFile == file_type) {
        data_file_numbers.push_back(file_number);
        XENGINE_LOG(INFO, "find data file", K(file_number), K(file_name));
      } else {
        XENGINE_LOG(INFO, "other type file", K(file_name));
      }
    }
  }

  return ret;
}

int ExtentSpaceManager::open_data_files(const std::string &dir_path, const std::vector<int64_t> &data_file_numbers)
{
  int ret = Status::kOk;
  std::string file_name;
  int64_t file_number;
  DataFile *data_file = nullptr;
  TableSpace *table_space = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ExtentSpaceManager should been inited first", K(ret), K(dir_path));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < data_file_numbers.size(); ++i) {
      file_number = data_file_numbers.at(i);
      file_name = util::MakeTableFileName(dir_path, file_number);
      if (IS_NULL(data_file = MOD_NEW_OBJECT(ModId::kExtentSpaceMgr,
                                             DataFile,
                                             env_,
                                             env_options_))) {
        ret = Status::kMemoryLimit;
        XENGINE_LOG(WARN, "fail to allocate memory for DataFile", K(ret), K(dir_path), K(file_number), K(file_name));
      } else if (FAILED(data_file->open(file_name))) {
        if (data_file->is_garbage_file()) {
          garbage_files_.push_back(file_name);
          XENGINE_LOG(INFO, "find garbage data file", K(file_name));
          //over write ret
          ret = Status::kOk;
        } else {
          XENGINE_LOG(WARN, "fail to open data file", K(ret), K(dir_path), K(file_number), K(file_name));
        }
      } else if (IS_NULL(table_space = get_table_space(data_file->get_table_space_id()))) {
        XENGINE_LOG(WARN, "the table space may have been removed, ignore it. the datafile can been manual delete", K(ret),
            "table_space_id", data_file->get_table_space_id(), K(file_number), K(file_name));
        MOD_DELETE_OBJECT(DataFile, data_file);
      } else if (FAILED(table_space->add_data_file(data_file))) {
        XENGINE_LOG(ERROR, "fail to add data file to tablespace", K(ret), K(file_name));
      } else {
        XENGINE_LOG(INFO, "success to add datafile", K(file_number), K(file_name));
      }
      update_max_file_number(file_number + 1);
    }
  }

  return ret;
}

void ExtentSpaceManager::update_max_file_number(const int64_t file_number)
{
  if (file_number > FileNumberAllocator::get_instance().get()) {
    FileNumberAllocator::get_instance().set(file_number);
  }
}

void ExtentSpaceManager::update_max_table_space_id(const int64_t table_space_id)
{
  if (table_space_id > table_space_id_.load()) {
    table_space_id_.store(table_space_id);
  }
}

int ExtentSpaceManager::clear_garbage_files()
{
  int ret = Status::kOk;

  for (uint32_t i = 0; SUCCED(ret) && i < garbage_files_.size(); ++i) {
    if (FAILED(env_->DeleteFile(garbage_files_.at(i)).code())) {
      XENGINE_LOG(WARN, "fail to delete garbage file", K(ret), "file_path", garbage_files_.at(i));
    } else {
      XENGINE_LOG(INFO, "success to delete garbage file", "file_path", garbage_files_.at(i));
    }
  }

  garbage_files_.clear();
  return ret;
}

} //namespace storage
} //namespace xengine

