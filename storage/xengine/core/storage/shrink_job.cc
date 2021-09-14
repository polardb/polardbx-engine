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

#include "db/db_impl.h"
#include "db/version_set.h"
#include "shrink_job.h"
#include "storage_logger.h"

namespace xengine
{
using namespace common;
namespace storage
{
ShrinkJob::ShrinkJob()
    : is_inited_(false),
      mutex_(nullptr),
      global_ctx_(nullptr),
      shrink_info_()
{
}

ShrinkJob::~ShrinkJob()
{
}

int ShrinkJob::init(monitor::InstrumentedMutex *mutex,
                    db::GlobalContext *global_ctx,
                    const ShrinkInfo &shrink_info)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "ShrinkJob has been inited", K(ret));
  } else if (IS_NULL(mutex)
             || IS_NULL(global_ctx) 
             || UNLIKELY(!shrink_info.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(mutex), KP(global_ctx), K(shrink_info));
  } else {
    mutex_ = mutex;
    global_ctx_ = global_ctx;
    shrink_info_ = shrink_info;
    is_inited_ = true;
  }

  return ret;
}

int ShrinkJob::run()
{
  int ret = Status::kOk;
  bool can_shrink = true;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "ShrinkJob should been inited first", K(ret));
  } else if (FAILED(before_shrink(can_shrink))) {
    XENGINE_LOG(WARN, "fail to prepare for shrink", K(ret));
  } else {
    if (can_shrink) {
      if (FAILED(do_shrink())) {
        XENGINE_LOG(WARN, "fail to do shrink", K(ret));
      }
      /**if can_shrink is true, after_shrink should execute anyway
       * because unref the subtable and reset pending_shrink must been done*/
      after_shrink();
    } else {
      XENGINE_LOG(INFO, "the shrink job can't run");
    }
  }

  return ret;
}

int ShrinkJob::before_shrink(bool &can_shrink)
{
  int ret = Status::kOk;
  db::SubTable *sub_table = nullptr;

  mutex_->Lock();
  db::AllSubTableGuard all_subtable_guard(global_ctx_);
  const db::SubTableMap &subtable_map = all_subtable_guard.get_subtable_map();
  /**check confict with other task like flush, compaction, recycle...*/
  for (auto iter = shrink_info_.index_id_set_.begin();
       SUCCED(ret) && shrink_info_.index_id_set_.end() != iter;
       ++iter) {
    auto subtable_iter = subtable_map.find(*iter);
    if (subtable_map.end() == subtable_iter) {
      XENGINE_LOG(INFO, "the subtable may been droppen, stop shrink the extent space", "index_id", *iter);
      can_shrink = false;
      break;
    } else if (IS_NULL(sub_table = subtable_iter->second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret), "index_id", *iter);
    } else if (!sub_table->can_shrink()) {
      XENGINE_LOG(INFO, "subtable can't shrink", "index_id", *iter);
      sub_table->print_internal_stat();
      can_shrink = false;
      break;
    }
  }

  /**check ShrinkInfo changed since the ShrinkJob generated durin schedule*/
  if (SUCCED(ret) && can_shrink) {
    if (FAILED(double_check_shrink_info(can_shrink))) {
      XENGINE_LOG(WARN, "fail to double check shrink info.", K(ret));
    }
  }

  /**do precheck success and can_shrink is true, set pending_shrink*/
  if (SUCCED(ret) && can_shrink) {
    for (auto iter = shrink_info_.index_id_set_.begin();
         SUCCED(ret) && shrink_info_.index_id_set_.end() != iter;
         ++iter) {
      auto subtable_iter = subtable_map.find(*iter);
      if (subtable_map.end() == subtable_iter) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpect error, the subtable should exist", K(ret), "index_id", *iter);
      } else { 
        subtable_iter->second->Ref();
        subtable_iter->second->set_pending_shrink(true);
        subtable_map_.emplace(subtable_iter->first, subtable_iter->second);
      }
    }

    /**If failed, rollback the ref and pending_shrink.
    the next rollback operation must not failed*/
    if (FAILED(ret)) {
      for (auto iter = subtable_map_.begin();
           SUCCED(ret) && subtable_map_.end() != iter;
           ++iter) {
        if (IS_NULL(sub_table = iter->second)) {
          ret = Status::kErrorUnexpected;
          XENGINE_LOG(WARN, "unexpected error, the subtable must not nullptr",
              K(ret), "index_id", iter->first);
        } else {
          sub_table->set_pending_shrink(false);
          if (sub_table->Unref()) {
            MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
            XENGINE_LOG(INFO, "delete one subtable in shrink job", "index_id", iter->first);
          }
        }
      }
    }
  }

  mutex_->Unlock();

  return ret;
}

int ShrinkJob::do_shrink()
{
  int ret = Status::kOk;

  if (FAILED(move_extent())) {
    XENGINE_LOG(WARN, "fail to move extent", K(ret));
  } else if (FAILED(install_shrink_result())) {
    XENGINE_LOG(WARN, "fail to install shrink result", K(ret));
  } else if (FAILED(shrink_physical_space())) {
    XENGINE_LOG(WARN, "fail to shrink physical space", K(ret));
  } else {
    XENGINE_LOG(INFO, "success to do shrink", K_(shrink_info));
  }

  return ret;
}

int ShrinkJob::after_shrink()
{
  int ret = Status::kOk;
  mutex_->Lock();
  for (auto iter = subtable_map_.begin(); subtable_map_.end() != iter; ++iter) {
    iter->second->set_pending_shrink(false);
    if (iter->second->Unref()) {
      MOD_DELETE_OBJECT(ColumnFamilyData, iter->second);
    }
  }
  mutex_->Unlock();

  return ret;
}

int ShrinkJob::move_extent()
{
  int ret = Status::kOk;

  if (FAILED(get_extent_infos())) {
    XENGINE_LOG(WARN, "fail to get extent infos", K(ret));
  } else if (FAILED(global_ctx_->extent_space_mgr_->move_extens_to_front(shrink_info_, extent_replace_map_))) {
    XENGINE_LOG(WARN, "fail to move extents to front", K(ret));
  }
  
  return ret;
}

int ShrinkJob::install_shrink_result()
{
  int ret = Status::kOk;
  int64_t dummy_commit_seq = 0;

  if (FAILED(global_ctx_->storage_logger_->begin(XengineEvent::SHRINK_EXTENT_SPACE))) {
    XENGINE_LOG(WARN, "fail to begin shrink trans", K(ret));
  } else if (FAILED(write_extent_metas())) {
    XENGINE_LOG(WARN, "fail to write extent metas", K(ret));
  } else if (FAILED(apply_change_infos())) {
    XENGINE_LOG(WARN, "fail to apply change infos", K(ret));
  } else if (FAILED(global_ctx_->storage_logger_->commit(dummy_commit_seq))) {
    XENGINE_LOG(WARN, "fail to commit shrink trans", K(ret));
  } else if (FAILED(update_super_version())) {
    XENGINE_LOG(WARN, "fail to update super version", K(ret));
  } else {
    XENGINE_LOG(INFO, "success to install shrink result", K(ret));
  }

  return ret;
}

int ShrinkJob::write_extent_metas()
{
  int ret = Status::kOk;
  ExtentId old_extent_id;
  ExtentId new_extent_id;
  ExtentMeta *old_extent_meta = nullptr;

  for (auto extent_iter = extent_replace_map_.begin();
       SUCCED(ret) && extent_replace_map_.end() != extent_iter; ++extent_iter) {
    old_extent_id = extent_iter->first;
    new_extent_id = extent_iter->second.extent_id_;
    auto extent_info_iter = extent_info_map_.find(old_extent_id.id());
    if (extent_info_map_.end() == extent_info_iter) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to find old extent info", K(ret), K(old_extent_id), K(new_extent_id));
    }

    if (SUCCED(ret)) {
      //step1: write new extent meta
      if (FAILED(global_ctx_->extent_space_mgr_->get_meta(old_extent_id, old_extent_meta))) {
        XENGINE_LOG(WARN, "fail to get extent meta", K(ret), K(old_extent_id), K(new_extent_id));
      } else if (IS_NULL(old_extent_meta)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret), K(old_extent_id), K(new_extent_id));
      } else {
        ExtentMeta new_extent_meta(*old_extent_meta);
        new_extent_meta.extent_id_ = new_extent_id;
        if (FAILED(global_ctx_->extent_space_mgr_->write_meta(new_extent_meta, true /*write_log*/))) {
          XENGINE_LOG(WARN, "fail to write meta", K(ret), K(old_extent_id), K(new_extent_id), K(*old_extent_meta));
        }
      }
    }

    if (SUCCED(ret)) {
      //step2: build change info
      const ExtentInfo &extent_info = extent_info_iter->second;
      auto change_info_iter = change_info_map_.find(extent_info.index_id_);
      if (change_info_map_.end() == change_info_iter) {
        if (!(change_info_map_.emplace(extent_info.index_id_, ChangeInfo()).second)) {
          XENGINE_LOG(WARN, "fail to emplace changeinfo", K(ret), K(extent_info));
        } else {
          change_info_iter = change_info_map_.find(extent_info.index_id_);
        }
      }

      if (SUCCED(ret)) {
        if (FAILED(change_info_iter->second.replace_extent(extent_info.layer_position_, old_extent_id, new_extent_id))) {
          XENGINE_LOG(WARN, "fail to replace extent", K(ret), K(extent_info), K(old_extent_id), K(new_extent_id));
        }
      }
    }
  }

  return ret;
}

int ShrinkJob::apply_change_infos()
{
  int ret = Status::kOk;
  db::SubTable *sub_table = nullptr;
  db::SuperVersion *old_version = nullptr;

  for (auto iter = change_info_map_.begin();
       SUCCED(ret) && change_info_map_.end() != iter; ++iter) {
    auto subtable_iter = subtable_map_.find(iter->first);
    if (subtable_map_.end() == subtable_iter) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to find subtable", K(ret));
    } else if (IS_NULL(sub_table = subtable_iter->second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret));
    } else if (FAILED(sub_table->apply_change_info(iter->second, true/*write_log*/))) {
      XENGINE_LOG(WARN, "fail to apply change info", K(ret));
    } else {
      mutex_->Lock();
      old_version = sub_table->InstallSuperVersion(MOD_NEW_OBJECT(memory::ModId::kSuperVersion, db::SuperVersion), mutex_, *(sub_table->GetLatestMutableCFOptions()));
      mutex_->Unlock();
      if (nullptr != old_version) {
        MOD_DELETE_OBJECT(SuperVersion, old_version);
      }
    }
  }

  return ret;
}

int ShrinkJob::update_super_version()
{
  int ret = Status::kOk;
  db::SubTable *sub_table = nullptr;
  db::SuperVersion *old_version = nullptr;

  for (auto iter = change_info_map_.begin();
       SUCCED(ret) && change_info_map_.end() != iter; ++iter) {
    auto subtable_iter = subtable_map_.find(iter->first);
    if (subtable_map_.end() == subtable_iter) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to find subtable", K(ret));
    } else if (IS_NULL(sub_table = subtable_iter->second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret));
    } else {
      mutex_->Lock();
      old_version = sub_table->InstallSuperVersion(MOD_NEW_OBJECT(memory::ModId::kSuperVersion, db::SuperVersion), mutex_, *(sub_table->GetLatestMutableCFOptions()));
      mutex_->Unlock();
      if (nullptr != old_version) {
        MOD_DELETE_OBJECT(SuperVersion, old_version);
      }
    }
  }

  return ret;
}
int ShrinkJob::shrink_physical_space()
{
  int ret = Status::kOk;
  bool can_shrink = can_physical_shrink();
  int32_t wait_times = 2;

  while (!can_shrink && wait_times > 0) {
    util::Env::Default()->SleepForMicroseconds(1000000);
    can_shrink = can_physical_shrink();
    wait_times--;
  }

  if (can_shrink) {
    if (FAILED(global_ctx_->extent_space_mgr_->shrink_extent_space(shrink_info_))) {
      XENGINE_LOG(WARN, "fail to shrink extent space", K(ret));
    } else {
      XENGINE_LOG(INFO, "success to shrink extent space", K_(shrink_info));
    }
  } else {
    XENGINE_LOG(INFO, "cant't do pyhsical shrink", K_(shrink_info));
  }

  return ret;
}

bool ShrinkJob::can_physical_shrink()
{
  bool can_shrink = true;
  for (auto iter = subtable_map_.begin(); can_shrink && subtable_map_.end() != iter; ++iter) {
    mutex_->Lock();
    if (!(iter->second->can_physical_shrink())) {
      can_shrink = false;
    }
    mutex_->Unlock();
  }
  return can_shrink;
}

int ShrinkJob::get_extent_infos()
{
  int ret = Status::kOk;

  for (auto iter = subtable_map_.begin(); SUCCED(ret) && subtable_map_.end() != iter; ++iter) {
    if (FAILED(iter->second->get_extent_infos(extent_info_map_))) {
      XENGINE_LOG(WARN, "fail to get extent infos", K(ret), "index_id", iter->first);
    }
  }

  return ret;
}

int ShrinkJob::double_check_shrink_info(bool &can_shrink)
{
  int ret = Status::kOk;
  ShrinkInfo current_shrink_info;

  if (FAILED(global_ctx_->extent_space_mgr_->get_shrink_info(shrink_info_.table_space_id_,
          shrink_info_.extent_space_type_, shrink_info_.shrink_condition_, current_shrink_info))) {
    XENGINE_LOG(WARN, "fail to get shrink info", K(ret), K_(shrink_info));
  } else {
    if (shrink_info_ == current_shrink_info) {
      //do nothing
    } else {
      can_shrink = false;
      XENGINE_LOG(INFO, "the shrink info has changed, cancel this shrink job", K_(shrink_info),
          K(current_shrink_info));
    }
  }

  return ret;
}

} //namespace storage
} //namespace xengine
