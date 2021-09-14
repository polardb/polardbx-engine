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
#include "chunk_allocator.h"

#include "logger/logger.h"
#include "util/random.h"
#include "xengine/env.h"

namespace xengine
{
namespace memory
{
ChunkAllocator::ChunkAllocator(void (*deleter)(void* value, void *handler), void *handler)
    : chunk_id_(0),
      score_(0),
      refs_(0),
      access_cnt_(0),
      last_obj_(nullptr),
      buf_(nullptr),
      max_free_size_(0),
      usage_(0),
      allocated_(0),
      clear_flag_(false),
      handler_(handler),
      deleter_(deleter),
      in_prep_num_(0),
      prep_mutex_(false),
      prep_cv_(&prep_mutex_){
  memset(class_num_, 0, sizeof(class_num_));
  memset(class_bm_, 0, sizeof(class_bm_));
}

ChunkAllocator::~ChunkAllocator() {
  clear();
}

void ChunkAllocator::init(int64_t chunk_id) {
  chunk_id_ = chunk_id;
  buf_ = base_alloc_chunk(CHUNK_SIZE +AllocMgr::kBlockMeta, ModId::kRowCache);
  ObjNode *init_node = new (buf_) ObjNode();
  init_node->size_ = CHUNK_SIZE;
  set_prev_in_use(init_node);
  set_next_in_use(init_node);
  last_obj_ = init_node;
  max_free_size_ = CHUNK_SIZE;
  int64_t obj_size = FIRST_STEP_SIZE;
  int64_t step = FIRST_STEP_SIZE;
  class_num_[0] = 0;
  int64_t level = 0;
  while (obj_size <= MAX_OBJ_SIZE) {
    ObjClass item;
    item.size_ = obj_size;
    item.num_ = 0;
    objs_class_.push_back(item);
    if (0 == obj_size % LEVEL_SIZE && step < LAST_STEP_SIZE) {
      class_num_[++level] = objs_class_.size();
      step <<= 1;
    }
    obj_size += step;
  }
}

void ChunkAllocator::reset() {
  assert(0 == refs_);
  mutex_.lock();
  if (clear_flag_) {
    clear();
  } else {
    for (int64_t i = 0 ; i < (int64_t)objs_class_.size(); ++i) {
      ObjClass &objs = objs_class_.at(i);
      objs.free_list_.next_ = nullptr;
      objs.free_list_.prev_ = nullptr;
      objs.num_ = 0;
    }
    memset(class_bm_, 0, sizeof(class_bm_));
    memset(buf_, 0, CHUNK_SIZE);
    ObjNode *init_node = new (buf_) ObjNode();
    init_node->size_ = CHUNK_SIZE;
    set_prev_in_use(init_node);
    set_next_in_use(init_node);
    last_obj_ = init_node;
    max_free_size_ = CHUNK_SIZE;
    usage_ = 0;
    allocated_ = 0;
    clear_flag_ = false;
  }
  mutex_.unlock();
}

void ChunkAllocator::clear() {
  score_ = 0;
  last_obj_ = nullptr;
  max_free_size_ = 0;
  // free buf
  base_free_chunk(buf_);
  buf_ = nullptr;
  objs_class_.clear();
}

// use under lock
void ChunkAllocator::evict_and_reuse(const int64_t default_score) {
  std::vector<char *>ptrs;
  mutex_.lock();
  if (-1 == max_free_size_) {
    mutex_.unlock();
    return ;
  }

  score_ = default_score;
  ObjNode *node = (ObjNode *)buf_;
  while (nullptr != node) {
    if (obj_in_use(node)) {
      ptrs.push_back((char *)node + HD_SIZE);
    }
    node = get_next_obj(node);
  }
  max_free_size_ = -1;
  last_obj_ = nullptr;
  ref();
  mutex_.unlock();
  prep_mutex_.Lock();
  while (!all_committed()) {
    prep_cv_.Wait();
  }
  prep_mutex_.Unlock();
  for (size_t i = 0; i < ptrs.size(); ++i) {
    deleter_(ptrs.at(i), handler_);
  }
  unref();
}

void ChunkAllocator::set_in_use(ObjNode *obj) {
  ObjNode *next_obj = get_next_obj(obj);
  if (nullptr != next_obj) {
    set_prev_in_use(next_obj);
  }
  ObjNode *prev_obj = get_prev_obj(obj);
  if (nullptr != prev_obj) {
    set_next_in_use(prev_obj);
  }
  obj->next_ = nullptr;
  obj->prev_ = nullptr;
  set_obj_in_use(obj);
}

ObjNode *ChunkAllocator::pick_obj_from_list(const int64_t idx, ObjClass &cls) {
  void *rtptr = nullptr;
  ObjNode &head = cls.free_list_;
  ObjNode *rnode = head.next_;
  assert(rnode);
  if (IS_NULL(rnode)) {
    return nullptr;
  }
  head.next_ = rnode->next_;
  if (nullptr != head.next_) {
    head.next_->prev_ = &head;
  }
  assert(cls.num_);
  --cls.num_;
  if (0 == cls.num_) {
    clr_bm_bit(idx);
  }
  return rnode;
}

void ChunkAllocator::remove_obj_from_list(ObjNode *obj) {
  if (last_obj_ == obj) {
    last_obj_ = nullptr;
  } else {
    int64_t idx = get_need_class_idx(get_obj_size(obj));
    ObjClass &obj_cls = objs_class_.at(idx);
    if (obj_cls.size_ == get_obj_size(obj)) {
      assert(obj_cls.num_);
      assert(obj->prev_);
      if (nullptr != obj->prev_) {
        obj->prev_->next_ = obj->next_;
      }
      if (nullptr != obj->next_) {
        obj->next_->prev_ = obj->prev_;
      }
      obj->prev_ = nullptr;
      obj->next_ = nullptr;
      --obj_cls.num_;
      if (0 == obj_cls.num_) {
        clr_bm_bit(idx);
      }
    }
  }
}

ObjNode *ChunkAllocator::split_from_large_node(ObjNode *&large_node, const int64_t size) {
  ObjNode *rtnode = nullptr;
  int64_t pre_size = get_obj_size(large_node);
  assert(pre_size >= size);
  int64_t remainder = pre_size - size;
  bool prev_inuse = prev_in_use(large_node);
  bool next_inuse = next_in_use(large_node);
  if (remainder >= MIN_OBJ_SIZE) {
    ObjNode *next_obj = get_next_obj(large_node);
    rtnode = large_node;
    rtnode->size_ = size;
    rtnode->prev_size_ = large_node->prev_size_;
    if (prev_inuse) {
      set_prev_in_use(rtnode);
    }

    large_node = (ObjNode *)((char *)large_node + size);
    large_node->size_ = remainder;
    large_node->prev_size_ = size;
    if (nullptr != next_obj) {
      next_obj->prev_size_ = remainder;
    }
    if (next_inuse) {
      set_next_in_use(large_node);
    }
    assert(get_obj_size(large_node) == remainder);
  } else if (0 == remainder) {
    rtnode = large_node;
    large_node = nullptr;
  }
  return rtnode;
}

ObjNode *ChunkAllocator::try_to_merge_neighbour(ObjNode *obj) {
  int64_t merge_size = get_obj_size(obj);
  const int64_t old_size = merge_size;
  bool next_inuse = next_in_use(obj);
  bool prev_inuse = prev_in_use(obj);
  ObjNode *new_obj = obj;
  if (!next_inuse) {
    ObjNode *phy_next = get_next_obj(obj);
    assert(!obj_in_use(phy_next));
    assert(phy_next);
    if (belong_to_class(merge_size + get_obj_size(phy_next))) {
      remove_obj_from_list(phy_next);
      next_inuse = next_in_use(phy_next);
      merge_size += get_obj_size(phy_next);
    }
  }
  if (!prev_inuse) {
    ObjNode *phy_prev = get_prev_obj(obj);
    assert(phy_prev);
    assert(!obj_in_use(phy_prev));
    if (belong_to_class(merge_size + get_obj_size(phy_prev))) {
      remove_obj_from_list(phy_prev);
      prev_inuse = prev_in_use(phy_prev);
      merge_size += get_obj_size(phy_prev);
      new_obj = phy_prev;
    }
  }
  if (merge_size != old_size) { // need_merge
    new_obj->size_ = merge_size;
    if (next_inuse) {
      set_next_in_use(new_obj);
    }
    if (prev_inuse) {
      set_prev_in_use(new_obj);
    }
    reset_next_presize(new_obj);
  }
  return new_obj;
}

void ChunkAllocator::put_obj_into_free_list(ObjNode *obj) {
  if (nullptr != obj) {
    int64_t idx = get_need_class_idx(get_obj_size(obj));
    ObjClass &obj_cls = objs_class_.at(idx);
    ObjNode &head = obj_cls.free_list_;
    obj->next_ = head.next_;
    if (nullptr != head.next_) {
      head.next_->prev_ = obj;
    }
    obj->prev_ = &head;
    head.next_ = obj;
    ++obj_cls.num_;
    if (1 == obj_cls.num_) {
      set_bm_bit(idx);
    }
  }
}

ObjNode *ChunkAllocator::get_from_large_obj(int64_t size) {
  ObjNode *robj = nullptr;
  int64_t idx = get_need_class_idx(size);
  int64_t next_idx = get_next_valid_idx(idx);
  if (next_idx < 0) {
    assert(last_obj_->size_ > size);
    return robj;
  }
  while (!belong_to_class(objs_class_.at(next_idx).size_- size)) {
    assert(!belong_same_level(idx,next_idx));
    ObjClass &sobj_cls = objs_class_.at(next_idx);
    ObjNode *split_node = pick_obj_from_list(next_idx, sobj_cls);
    ObjNode *rt_obj = split_from_large_node(split_node, sobj_cls.size_ / 2);
    put_obj_into_free_list(split_node);
    put_obj_into_free_list(rt_obj);
    next_idx = get_next_valid_idx(idx);
    assert(next_idx >= 0);
  }
  ObjNode *split_node = pick_obj_from_list(next_idx, objs_class_.at(next_idx));
  robj = split_from_large_node(split_node, size);
  // put remainder into fit list or set last_obj
  if (nullptr == last_obj_) {
    last_obj_ = split_node;
  } else {
    put_obj_into_free_list(split_node);
  }
  return robj;
}

void ChunkAllocator::free(void *ptr, int64_t rsize) {

  if (nullptr != ptr) {
    mutex_.lock();
    ObjNode *obj = reinterpret_cast<ObjNode *>((char *)ptr - HD_SIZE);
    set_free(obj);
    allocated_ -= get_obj_size(obj);
    usage_ -= rsize;
    ObjNode *new_obj = try_to_merge_neighbour(obj);
    assert(get_obj_size(new_obj) <= CHUNK_SIZE);
    put_obj_into_free_list(new_obj);
    update_max_free_size();
    mutex_.unlock();
    unref();
  }
}

void *ChunkAllocator::alloc(const int64_t size) {
  if (size <= 0 || max_free_size_ <= 0) return nullptr;
  void *rtptr = nullptr;
  ObjNode *res_node = nullptr;
  int64_t obj_idx = get_need_class_idx(size + HD_SIZE);
  ObjClass &obj_cls = objs_class_.at(obj_idx);
  int64_t nsize = obj_cls.size_;
  // 1. best fit
  if (obj_cls.num_ > 0) {
    res_node = pick_obj_from_list(obj_idx, obj_cls);
  }
  // 2. cache fit
  if (nullptr == res_node) {
    if (nullptr != last_obj_ && get_obj_size(last_obj_) >= nsize) {
      res_node = split_from_large_node(last_obj_, nsize);
    }
  }
  // 3. latest fit
  if (nullptr == res_node) {
    res_node = get_from_large_obj(nsize);
  }
  if (nullptr != res_node) {
    set_in_use(res_node);
    assert(belong_to_class(get_obj_size(res_node)));
    rtptr = (char *)res_node + HD_SIZE;
    ref();
    allocated_ += get_obj_size(res_node);
    usage_ += size;
  }
  update_max_free_size();
  if (nullptr != rtptr) {
    in_prep_num_.fetch_add(1);
  }
  return rtptr;
}


void ChunkAllocator::print_chunk_stat(std::string &stat_str) const {
  // chunk usage
  int64_t buf_len = 200;
  char *buf = (char *)base_malloc(buf_len);
  int64_t sum = get_total_free_size();
  snprintf(buf, buf_len, "\n CHUNK_INFO: id=%ld, refs=%ld, score=%ld, access_cnt=%ld, last_obj_size=%d, max_free_szie=%ld, sum=%ld",
      chunk_id_, refs_.load(), score_, access_cnt_.load(), nullptr == last_obj_ ? 0 : last_obj_->size_, max_free_size_, sum);
  stat_str.append(buf);
  snprintf(buf, buf_len, "\n          size     ind    allocated   util");
  stat_str.append(buf);
  // size, ind, allocated, util
  for (int64_t i = 0; i < (int64_t)objs_class_.size(); ++i) {
    const ObjClass &obj_class = objs_class_.at(i);
    if (0 != obj_class.num_) {
      int64_t dd = 0;
      snprintf(buf, buf_len, "\n%8ld %8ld %6ld %6ld %2ld",
          obj_class.size_, dd, obj_class.num_, dd, dd);
      stat_str.append(buf);
    }
  }
  stat_str.append("\n");
  base_free(buf);
}

ChunkManager::ChunkManager()
    : chunks_num_(0),
      topn_val_(0),
      avg_score_(0),
      chunks_(nullptr) {
}

ChunkManager::~ChunkManager() {
  for (int64_t i = 0; i < chunks_num_; ++i) {
    chunks_[i].~ChunkAllocator();
  }
  base_free(chunks_);
  chunks_ = nullptr;
}

void ChunkManager::init(const int64_t chunk_num,
                        void (*deleter)(void* value, void *handler),
                        void *handler) {
  chunks_ = static_cast<ChunkAllocator *>(
      base_malloc(chunk_num * sizeof(ChunkAllocator), ModId::kRowCache));
  for (int64_t i = 0 ; i < chunk_num; ++i) {
    new (chunks_ + i) ChunkAllocator(deleter, handler);
    chunks_[i].init(i);
  }
  chunks_num_ = chunk_num;
  topn_val_ = 1;
  topn_val_ = std::max(topn_val_, (chunk_num * EVICT_PERCENT) / 100 );
}

int ChunkManager::evict_one_chunk() {
  int ret = 0;
  if (!mutex_.try_lock()) return ret;
  if (topn_heap_.empty()) {
    int64_t score = 0;
    int64_t sum_score = 0;
    for (int i = 0; i < chunks_num_; i++) {
      int64_t pre_score = chunks_[i].get_score();
      int64_t access_cnt = chunks_[i].get_access_cnt();
      score = pre_score * SCORE_DECAY_RATIO / 100 + access_cnt ;
      sum_score += score;
      chunks_[i].set_score(score);
      chunks_[i].reset_access_cnt();
      topn_heap_.push(&chunks_[i]);
      if ((int64_t)topn_heap_.size() > topn_val_) {
        topn_heap_.pop();
      }
    }
    avg_score_ = sum_score / chunks_num_;
  }
  if (!topn_heap_.empty()) {
    ChunkAllocator *chunk = topn_heap_.top();
    chunk->evict_and_reuse(avg_score_);
    topn_heap_.pop();
  }
  mutex_.unlock();
  return ret;
}

void *ChunkManager::alloc(const int64_t size, ChunkAllocator *&alloc) {
  void *buf = nullptr;
  int64_t ndsize = size + ChunkAllocator::HD_SIZE;
  static int64_t idx;
  int64_t i = idx;
  int64_t num = chunks_num_;
  while(num > 0) {
    i %= chunks_num_;
    if (chunks_[i].get_max_free_size() >= ndsize && chunks_[i].try_lock()) {
      if (chunks_[i].get_max_free_size() >= ndsize) {
        if (nullptr != (buf = chunks_[i].alloc(size))) {
          chunks_[i].hit_add();
          chunks_[i].unlock();
          alloc = &chunks_[i];
          idx = i;
          break;
        }
      }
      chunks_[i].unlock();
    }
    i++;
    num--;
  }
  return buf;
}

void ChunkManager::free(void *value, ChunkAllocator *alloc) {
  if (IS_NULL(alloc)) {
    assert(false);
  } else {
    alloc->free(value);
  }
}

void ChunkManager::async_evict_chunks() {
  // 1. calc chunks' score and update chunks heap
  int64_t score = 0;
  mutex_.lock();
  topn_heap_.clear();
  int64_t sum_score = 0;
  int64_t sum_free_size = 0;
  for (int i = 0; i < chunks_num_; i++) {
    int64_t pre_score = chunks_[i].get_score();
    int64_t access_cnt = chunks_[i].get_access_cnt();
    score = pre_score * SCORE_DECAY_RATIO / 100 + access_cnt;
    sum_score += score;
    sum_free_size += chunks_[i].get_total_free_size();
    chunks_[i].set_score(score);
    chunks_[i].reset_access_cnt();
    topn_heap_.push(&chunks_[i]);
    if ((int64_t)topn_heap_.size() > topn_val_) {
      topn_heap_.pop();
    }
  }
  // 2. evict chunks
  int64_t evict_cnt = 0;
  avg_score_ = sum_score / chunks_num_;
  if (sum_free_size * 100 / (ChunkAllocator::CHUNK_SIZE * chunks_num_) < EVICT_PERCENT) {
    while (!topn_heap_.empty() && evict_cnt < topn_val_) {
      ChunkAllocator *chunk = topn_heap_.top();
      int64_t chunk_id = chunk->get_chunk_id();
      chunk->evict_and_reuse(avg_score_);
      topn_heap_.pop();
      ++evict_cnt;
    }
//    XENGINE_LOG(INFO, "evict time", K(avg_score_), K(sum_free_size), K(chunks_num_), K(evict_cnt));
  }
  mutex_.unlock();
}

void ChunkManager::print_chunks_stat(std::string &stat_str) const {
  int64_t buf_len = 100;
  char *buf = (char *)base_malloc(buf_len);
  snprintf(buf, buf_len, "chunks manager info: chunks_num=%ld, heap_size=%ld",
      chunks_num_, topn_heap_.size());
  stat_str.append(buf);
  for (int64_t i = 0; i < chunks_num_; i++) {
    ChunkAllocator *alloc = &chunks_[i];
    std::string tstr;
    alloc->print_chunk_stat(tstr);
    stat_str.append(tstr);
  }
  base_free(buf);
}

}
}
