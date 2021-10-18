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

#pragma once

#include <stdint.h>
#include <string.h>
#include <cstdio>
#include <memory>

namespace xengine {
namespace util {

// use the wasting one element algorithm to realize the 
// circular buffer, CAUTION it's not thread safe, need
// synchronize outside
template <class T>
class RingBuffer {
public:
  // FIXME: init_size inc negtive
  RingBuffer(int64_t init_size, int32_t inc) : 
             buf_(std::unique_ptr<T[]>(new T[init_size])),
             head_(0), tail_(0), size_(init_size), increment_(inc) {};
  T *dump() {
    T *tmp = new T[size()]; 
    if (nullptr == tmp) {
      fprintf(stderr, "Out of memory");
      return nullptr;
    }
    if (tail_ <= head_) {
      std::copy(buf_.get() + tail_, buf_.get() + head_, tmp);
    } else {
      std::copy(buf_.get() + tail_, buf_.get() + size_, tmp);
      std::copy(buf_.get(), buf_.get() + head_, tmp + size_ - tail_);
    }

    return tmp;
  }

  T *dump(int32_t length) {
    T *tmp = new T[length]; 
    if (nullptr == tmp) {
      fprintf(stderr, "Out of memory");
      return nullptr;
    }
    std::copy(buf_.get() + tail_, buf_.get() + tail_ + length, tmp);

    return tmp;
  }

  int64_t put(T item) {
    if (full()) {
      T *tmp = new T[size_ + increment_];
      if (nullptr == tmp) {
        fprintf(stderr, "Out of memory");
        return -1;
      } 
      // realloc must keep the offset valid of the old ring
      if (tail_ <= head_) {
        std::copy(buf_.get() + tail_, buf_.get() + head_, tmp + tail_); 
      } else {
        std::copy(buf_.get() + tail_, buf_.get() + size_, tmp + tail_);
        if (head_ <= increment_) {
          std::copy(buf_.get(), buf_.get() + head_, tmp + size_);
          head_ += size_;
        } else {
          std::copy(buf_.get(), buf_.get() + increment_, tmp + size_);
          std::copy(buf_.get() + increment_, buf_.get() + head_, tmp); 
          head_ -= increment_;
        }
      }
      buf_.reset(tmp);
      size_ += increment_;
      head_ %= size_;
    }
    buf_[head_] = item;
    head_ = (head_ + 1) % size_;
    // return the obsolute offset
    if (tail_ <= head_) {
      return (head_ - 1);
    } else {
      return (head_ + size_ - 1);
    }
  }

  bool remove(int32_t num = 1) {
    if (empty()) {
      fprintf(stderr, "Delete empty buffer");
      return false;
    }      
    tail_ = (tail_ + num) % size_; 
    return true;
  }

  
  T &get(int64_t index) const {
    // FIXME: invalid offset
    if (index < 0) {
      return buf_[0];
    } 
    return buf_[index % size_];
  }
    
  // if head is equal to tail is empty
  inline bool empty(void) const { return head_ == tail_; }
  // if tail is ahead the head by 1 is full
  inline bool full(void) const { return ((head_ + 1) % size_) == tail_; }

  inline int64_t size() const {
    if (tail_ <= head_) {
      return head_ - tail_;
    } else {
      return (head_ + size_) - tail_;
    }
  }

  inline int64_t head() const { return head_; }
 
  inline int64_t tail() const { return tail_; }

  inline int64_t capacity() const { return size_; }
    
private:
  std::unique_ptr<T[]> buf_;
  int64_t head_;
  int64_t tail_;
  int64_t size_;
  int32_t increment_;
};

} // util
} // xengine
