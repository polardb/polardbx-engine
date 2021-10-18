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

#include "memory/page_arena.h"
#include "memory/mod_info.h"
#include "util/to_string.h"


namespace xengine
{
namespace util
{

enum VectorErrorCode {
  E_OK = 0,
  E_MEMORY_OVERFLOW = -1,
  E_ENTRY_EXIST = -2,
  E_ENTRY_NOT_EXIST = -3,
  E_OUT_OF_BOUND = -4,
  E_DATA_CORRUPT = -5,
};

template <typename T>
class pointer_vector_traits;

template <typename T>
struct pointer_vector_traits<T *> {
public:
  typedef T pointee_type;
  typedef T *value_type;
  typedef const T *const_value_type;
  typedef value_type *iterator;
  typedef const value_type *const_iterator;
  typedef int32_t difference_type;
};

template <typename T>
struct pointer_vector_traits<const T *> {
  typedef T pointee_type;
  typedef const T *value_type;
  typedef const T *const const_value_type;
  typedef value_type *iterator;
  typedef const value_type *const_iterator;
  typedef int32_t difference_type;
};

template <>
struct pointer_vector_traits<int64_t> {
  typedef int64_t &pointee_type;
  typedef int64_t value_type;
  typedef const int64_t const_value_type;
  typedef value_type *iterator;
  typedef const value_type *const_iterator;
  typedef int32_t difference_type;
};

template <typename T, typename Allocator = memory::DefaultPageAllocator >
class PointerVector
{
public:
  typedef typename pointer_vector_traits<T>::pointee_type pointee_type;
  typedef typename pointer_vector_traits<T>::value_type value_type;
  typedef typename pointer_vector_traits<T>::const_value_type const_value_type;
  typedef typename pointer_vector_traits<T>::iterator iterator;
  typedef typename pointer_vector_traits<T>::const_iterator const_iterator;
  typedef typename pointer_vector_traits<T>::difference_type difference_type;

public:
  explicit PointerVector(Allocator *alloc = nullptr);
  explicit PointerVector(int32_t size, Allocator *alloc = nullptr,
                         const int32_t mod_id = memory::ModId::kDefaultMod);
  virtual ~PointerVector();
  PointerVector<T, Allocator> &operator=(
    const PointerVector<T, Allocator> &other);
  explicit PointerVector(const PointerVector<T, Allocator> &other);

public:
  int32_t size() const { return static_cast<int32_t>(mem_end_ - mem_begin_); }
  int32_t capacity() const
  {
    return static_cast<int32_t>(mem_end_of_storage_ - mem_begin_);
  }
  int32_t remain() const
  {
    return static_cast<int32_t>(mem_end_of_storage_ - mem_end_);
  }

public:
  iterator begin() const { return mem_begin_; }
  iterator end() const { return mem_end_; }
  iterator last() const { return mem_end_ - 1; }
  Allocator &get_allocator() const { return *pallocator_; }
  void set_allocator(Allocator &allc) { pallocator_ = &allc; }
  inline bool at(const int32_t index, value_type &v) const
  {
    bool ret = false;
    if (index >= 0 && index < size()) {
      v = *(mem_begin_ + index);
      ret = true;
    }
    return ret;
  }

  inline value_type &at(const int32_t index) const
  {
    assert(index >= 0 && index < size());
    return *(mem_begin_ + index);
  }
  inline value_type &operator[](const int32_t index) const { return at(index); }
public:
  int push_back(const_value_type value);
  int insert(iterator pos, const_value_type value);
  int insert(iterator pos, const_iterator begin, const_iterator end);
  int replace(iterator pos, const_value_type value, value_type &replaced_value);
  int remove(iterator pos);
  int remove(const int32_t index);
  int remove(iterator start_pos, iterator end_pos);
  int remove_if(const_value_type value);
  template <typename ValueType, typename Predicate>
  int remove_if(const ValueType &value, Predicate predicate);
  template <typename ValueType, typename Predicate>
  int remove_if(const ValueType &value, Predicate predicate,
                value_type &removed_value);
  int reserve(int32_t size) { return expand(size); }
  void clear();
  inline void reset() { mem_end_ = mem_begin_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  void destroy();
  int expand(int32_t size);

private:
  iterator alloc_array(const int32_t size);

  static iterator fill(iterator ptr, const_value_type value);
  static iterator copy(iterator dest, const_iterator begin, const_iterator end);
  static iterator move(iterator dest, const_iterator begin, const_iterator end);

protected:
  iterator mem_begin_;
  iterator mem_end_;
  iterator mem_end_of_storage_;
  mutable Allocator default_allocator_;
  Allocator *pallocator_;
};

template <typename T, typename Allocator = memory::DefaultPageAllocator >
class PointerSortedVector
{
public:
  typedef Allocator allocator_type;
  typedef PointerVector<T, Allocator> vector_type;
  typedef typename vector_type::iterator iterator;
  typedef typename vector_type::const_iterator const_iterator;
  typedef typename vector_type::value_type value_type;
  typedef typename vector_type::const_value_type const_value_type;
  typedef typename vector_type::difference_type difference_type;

public:
  PointerSortedVector() {}
  explicit PointerSortedVector(int32_t size, Allocator *alloc = nullptr,
                               const int32_t mod_id = memory::ModId::kDefaultMod)
    : vector_(size, alloc, mod_id)
  {
  }
  virtual ~PointerSortedVector() {}
  PointerSortedVector<T, Allocator> &operator=(
    const PointerSortedVector<T, Allocator> &other)
  {
    if (this != &other) {
      vector_ = other.vector_;
    }
    return *this;
  }
  explicit PointerSortedVector(const PointerSortedVector<T, Allocator> &other)
  {
    vector_ = other.vector_;
  }

public:
  inline int32_t size() const { return vector_.size(); }
  inline int64_t count() const { return vector_.size(); }
  inline int32_t capacity() const { return vector_.capacity(); }
  inline int32_t remain() const { return vector_.remain(); }
  inline int reserve(int32_t size) { return vector_.reserve(size); }
  inline int remove(iterator pos) { return vector_.remove(pos); }
  inline void clear() { return vector_.clear(); }
  inline void reset() { return vector_.reset(); };
  inline int push_back(const_value_type value)
  {
    return vector_.push_back(value);
  }

public:
  inline iterator begin() const { return vector_.begin(); }
  inline iterator end() const { return vector_.end(); }
  inline iterator last() const { return vector_.last(); }
  inline Allocator &get_allocator() const { return vector_.get_allocator(); }
public:
  inline bool at(const int32_t index, value_type &v) const
  {
    return vector_.at(index, v);
  }

  inline value_type &at(const int32_t index) const { return vector_.at(index); }
  inline value_type &operator[](const int64_t index) const
  {
    return at(static_cast<int32_t>(index));
  }

public:
  template <typename Compare>
  int insert(const_value_type value, iterator &insert_pos, Compare compare);
  template <typename Compare>
  int insert(const_iterator begin, const_iterator end, iterator &insert_pos, Compare compare);
  /**
   * WARNING: dangerous function, if entry is existent, overwrite,
   * else insert. we don't free the memory of the entry, be careful.
   */
  template <typename Compare, typename Equal>
  int replace(const_value_type value, iterator &replace_pos, Compare compare,
              Equal equal, value_type &replaced_value);
  template <typename Compare, typename Unique>
  int insert_unique(const_value_type value, iterator &insert_pos,
                    Compare compare, Unique unique);
  template <typename Compare>
  int find(const_value_type value, iterator &pos, Compare compare) const;

  template <typename ValueType, typename Compare>
  iterator lower_bound(const ValueType &value, Compare compare) const;
  template <typename ValueType, typename Compare>
  iterator upper_bound(const ValueType &value, Compare compare) const;

  template <typename ValueType, typename Compare, typename Equal>
  int find(const ValueType &value, iterator &pos, Compare compare,
           Equal equal) const;
  template <typename ValueType, typename Compare, typename Equal>
  int remove_if(const ValueType &value, Compare comapre, Equal equal);
  template <typename ValueType, typename Compare, typename Equal>
  int remove_if(const ValueType &value, Compare comapre, Equal equal,
                value_type &removed_value);

  int remove(iterator start_pos, iterator end_pos);

  template <typename Compare>
  void sort(Compare compare);

  int64_t to_string(char *buf, const int64_t buf_len) const {
    return vector_.to_string(buf, buf_len);
  }
private:
  PointerVector<T, Allocator> vector_;
};

}  // end namespace util
}  // end namespace xengine

#include "pointer_vector.ipp"
