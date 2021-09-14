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

#include <algorithm>

namespace xengine
{
namespace util
{
// --------------------------------------------------------
// class PointerVector<T, Allocator> implements
// --------------------------------------------------------
template <typename T, typename Allocator>
PointerVector<T, Allocator>::PointerVector(Allocator *alloc)
  : mem_begin_(nullptr), mem_end_(nullptr), mem_end_of_storage_(nullptr)
{
  if (nullptr == alloc) {
    pallocator_ = &default_allocator_;
    pallocator_->set_mod_id(memory::ModId::kDefaultMod);
  } else {
    pallocator_ = alloc;
  }
}

template <typename T, typename Allocator>
PointerVector<T, Allocator>::PointerVector(int32_t size, Allocator *alloc,
                                           const int32_t mod_id)
  : mem_begin_(nullptr), mem_end_(nullptr), mem_end_of_storage_(nullptr)
{
  if (nullptr == alloc) {
    pallocator_ = &default_allocator_;
    pallocator_->set_mod_id(mod_id);
  } else {
    pallocator_ = alloc;
  }
  expand(size);
}

template <typename T, typename Allocator>
PointerVector<T, Allocator>::~PointerVector()
{
  destroy();
}

template <typename T, typename Allocator>
PointerVector<T, Allocator>::PointerVector(
  const PointerVector<T, Allocator> &other)
  : mem_begin_(nullptr),
    mem_end_(nullptr),
    mem_end_of_storage_(nullptr),
    pallocator_(&default_allocator_)
{
  pallocator_->set_mod_id(memory::ModId::kDefaultMod);
  *this = other;
}

template <typename T, typename Allocator>
PointerVector<T, Allocator> &PointerVector<T, Allocator>::operator=(
  const PointerVector<T, Allocator> &other)
{
  if (this != &other) {
    destroy();
    if (other.size() > 0) {
      expand(other.size());
      copy(mem_begin_, other.begin(), other.end());
      mem_end_ = mem_begin_ + other.size();
    }
  }

  return *this;
}

template <typename T, typename Allocator>
void PointerVector<T, Allocator>::destroy()
{
  if (mem_begin_) {
    pallocator_->free(mem_begin_);
  }
  mem_begin_ = nullptr;
  mem_end_ = nullptr;
  mem_end_of_storage_ = nullptr;
}

template <typename T, typename Allocator>
int PointerVector<T, Allocator>::push_back(const_value_type value)
{
  return insert(end(), value);
}

template <typename T, typename Allocator>
void PointerVector<T, Allocator>::clear()
{
  pallocator_->free(mem_begin_);
  mem_end_ = mem_begin_ = mem_end_of_storage_ = nullptr;
}

/**
 * expand %size bytes memory when buffer not enough.
 * copy origin memory content to new buffer
 * @return
 * <  0  allocate new memory failed
 * == 0  no need expand
 * == %size  expand succeed.
 */
template <typename T, typename Allocator>
int32_t PointerVector<T, Allocator>::expand(int32_t size)
{
  int32_t old_size = PointerVector<T, Allocator>::size();
  int32_t ret = -1;
  if (old_size < size) {
    iterator new_mem = alloc_array(size);
    if (new_mem) {
      if (old_size) {
        copy(new_mem, mem_begin_, mem_end_);
      }

      // deallocate old memeory
      destroy();

      mem_begin_ = new_mem;
      mem_end_ = mem_begin_ + old_size;
      mem_end_of_storage_ = mem_begin_ + size;
      ret = size;
    }
  } else {
    ret = 0;
  }
  return ret;
}

template <typename T, typename Allocator>
typename PointerVector<T, Allocator>::iterator
PointerVector<T, Allocator>::alloc_array(const int32_t size)
{
  iterator ptr =
    reinterpret_cast<iterator>(pallocator_->alloc(size * sizeof(value_type)));
  return ptr;
}

template <typename T, typename Allocator>
typename PointerVector<T, Allocator>::iterator
PointerVector<T, Allocator>::fill(iterator ptr, const_value_type value)
{
  if (nullptr != ptr) {
    //*ptr = value;
    memcpy(ptr, &value, sizeof(value_type));
  }
  return ptr;
}

/*
 * [dest, x] && [begin, end] can be overlap
 * @param [in] dest: move dest memory
 * @param [in] begin: source memory start pointer
 * @param [in] end: source memory end pointer
 * @return
 */
template <typename T, typename Allocator>
typename PointerVector<T, Allocator>::iterator
PointerVector<T, Allocator>::move(iterator dest, const_iterator begin,
                                  const_iterator end)
{
  assert(dest);
  assert(end >= begin);
  int32_t n = static_cast<int32_t>(end - begin);
  if (n > 0) ::memmove(dest, begin, n * sizeof(value_type));
  return dest + n;
}

/*
 * [dest, x] && [begin, end] cannot be overlap
 */
template <typename T, typename Allocator>
typename PointerVector<T, Allocator>::iterator
PointerVector<T, Allocator>::copy(iterator dest, const_iterator begin,
                                  const_iterator end)
{
  assert(dest);
  assert(end >= begin);
  int32_t n = static_cast<int32_t>(end - begin);
  if (n > 0) ::memcpy(dest, begin, n * sizeof(value_type));
  return dest + n;
}

template <typename T, typename Allocator>
int PointerVector<T, Allocator>::remove(iterator pos)
{
  int ret = 0;
  if (pos < mem_begin_ || pos >= mem_end_) {
    ret = E_OUT_OF_BOUND;
  } else {
    iterator new_end_pos = move(pos, pos + 1, mem_end_);
    assert(mem_end_ == new_end_pos + 1);
    mem_end_ = new_end_pos;
  }
  return ret;
}

template <typename T, typename Allocator>
int PointerVector<T, Allocator>::remove(iterator start_pos, iterator end_pos)
{
  int ret = 0;
  if (start_pos < mem_begin_ || start_pos >= mem_end_ || end_pos < mem_begin_ ||
      end_pos > mem_end_) {
    ret = E_OUT_OF_BOUND;
  } else if (end_pos - start_pos > 0) {
    iterator new_end_pos = move(start_pos, end_pos, mem_end_);
    assert(mem_end_ == new_end_pos + (end_pos - start_pos));
    mem_end_ = new_end_pos;
  }
  return ret;
}

template <typename T, typename Allocator>
int PointerVector<T, Allocator>::remove(const int32_t index)
{
  int ret = 0;
  if (index >= 0 && index < size()) {
    ret = remove(mem_begin_ + index);
  } else {
    ret = E_ENTRY_NOT_EXIST;
  }
  return ret;
}

template <typename T, typename Allocator>
int PointerVector<T, Allocator>::remove_if(const_value_type value)
{
  int ret = 0;
  iterator pos = mem_begin_;
  while (pos != mem_end_) {
    if (*pos == value) break;
    ++pos;
  }

  if (pos >= mem_end_)
    ret = E_ENTRY_NOT_EXIST;
  else
    ret = remove(pos);

  return ret;
}

template <typename T, typename Allocator>
template <typename ValueType, typename Predicate>
int PointerVector<T, Allocator>::remove_if(const ValueType &value,
                                           Predicate predicate)
{
  int ret = 0;
  iterator pos = mem_begin_;
  while (pos != mem_end_) {
    if (predicate(*pos, value)) break;
    ++pos;
  }

  if (pos >= mem_end_)
    ret = E_ENTRY_NOT_EXIST;
  else
    ret = remove(pos);

  return ret;
}

template <typename T, typename Allocator>
template <typename ValueType, typename Predicate>
int PointerVector<T, Allocator>::remove_if(const ValueType &value,
                                           Predicate predicate,
                                           value_type &removed_value)
{
  int ret = 0;
  iterator pos = mem_begin_;
  while (pos != mem_end_) {
    if (predicate(*pos, value)) break;
    ++pos;
  }

  if (pos >= mem_end_) {
    ret = E_ENTRY_NOT_EXIST;
  } else {
    removed_value = *pos;
    ret = remove(pos);
  }

  return ret;
}

template <typename T, typename Allocator>
int PointerVector<T, Allocator>::insert(iterator pos, const_value_type value)
{
  if (remain() >= 1) {
    if (pos == mem_end_) {
      fill(pos, value);
      mem_end_ += 1;
    } else {
      move(pos + 1, pos, mem_end_);
      fill(pos, value);
      mem_end_ += 1;
    }
  } else {
    // need expand
    int32_t old_size = size();
    int32_t new_size = (old_size + 1) << 1;
    iterator new_mem = alloc_array(new_size);
    if (!new_mem) return E_MEMORY_OVERFLOW;
    iterator new_end = new_mem + old_size + 1;
    if (pos == mem_end_) {
      copy(new_mem, mem_begin_, mem_end_);
      fill(new_mem + old_size, value);
    } else {
      // copy head part;
      iterator pivot = copy(new_mem, mem_begin_, pos);
      // copy tail part;
      copy(pivot + 1, pos, mem_end_);
      fill(pivot, value);
    }

    // free old memory and reset pointers..
    destroy();
    mem_begin_ = new_mem;
    mem_end_ = new_end;
    mem_end_of_storage_ = new_mem + new_size;
  }
  return 0;
}

// fill value in range [begin, end) into current vector after position %pos
template <typename T, typename Allocator>
int PointerVector<T, Allocator>::insert(iterator pos, const_iterator begin, const_iterator end)
{
  int32_t add_size = static_cast<int32_t>(end - begin);
  if (remain() >= add_size) {
    if (pos == mem_end_) {
      copy(pos, begin, end);
      mem_end_ += 1;
    } else {
      move(pos + add_size, pos, mem_end_);
      copy(pos, begin, end);
      mem_end_ += add_size;
    }
  } else {
    // need expand
    int32_t old_size = size();
    int32_t new_size = (old_size + add_size) << 1;
    iterator new_mem = alloc_array(new_size);
    if (!new_mem) return E_MEMORY_OVERFLOW;
    iterator new_end = new_mem + old_size + add_size;
    if (pos == mem_end_) {
      copy(new_mem, mem_begin_, mem_end_);
      copy(new_mem + old_size, begin, end);
    } else {
      // copy head part;
      iterator pivot = copy(new_mem, mem_begin_, pos);
      // copy tail part;
      copy(pivot + add_size, pos, mem_end_);
      // fill insert values;
      copy(pivot, begin, end);
    }

    // free old memory and reset pointers..
    destroy();
    mem_begin_ = new_mem;
    mem_end_ = new_end;
    mem_end_of_storage_ = new_mem + new_size;
  }
  return 0;
}

template <typename T, typename Allocator>
int PointerVector<T, Allocator>::replace(iterator pos, const_value_type value,
                                         value_type &replaced_value)
{
  int ret = 0;
  if (pos >= mem_begin_ && pos < mem_end_) {
    replaced_value = *pos;
    fill(pos, value);
  } else {
    ret = E_ENTRY_NOT_EXIST;
  }
  return ret;
}

template <typename T, typename Allocator>
int64_t PointerVector<T, Allocator>::to_string(char *buf,
                                               const int64_t buf_len) const
{
  int64_t pos = 0;
  util::databuff_printf(buf, buf_len, pos, "[");
  iterator it = begin();
  for (; it != end(); ++it) {
    util::databuff_print_obj(buf, buf_len, pos, *it);
    if (it != last()) {
      util::databuff_printf(buf, buf_len, pos, ", ");
    }
  }
  util::databuff_printf(buf, buf_len, pos, "]");
  return pos;
}

// PointerSortedVector
template <typename T, typename Allocator>
template <typename Compare>
int PointerSortedVector<T, Allocator>::insert(const_value_type value,
                                              iterator &insert_pos,
                                              Compare compare)
{
  int ret = 0;
  if (nullptr == value) ret = E_DATA_CORRUPT;
  insert_pos = vector_.end();
  if (0 == ret) {
    iterator find_pos =
      std::lower_bound(vector_.begin(), vector_.end(), value, compare);
    insert_pos = find_pos;

    ret = vector_.insert(insert_pos, value);
  }
  return ret;
}

// insert values between [begin, end) must be sorted.
template <typename T, typename Allocator>
template <typename Compare>
int PointerSortedVector<T, Allocator>::insert(const_iterator begin,
                                              const_iterator end,
                                              iterator &insert_pos,
                                              Compare compare)
{
  int ret = 0;
  if (end < begin) ret = E_DATA_CORRUPT;
  insert_pos = vector_.end();
  if (0 == ret) {
    iterator find_pos =
      std::lower_bound(vector_.begin(), vector_.end(), *begin, compare);
    insert_pos = find_pos;

    if (insert_pos < vector_.last()) {
      if (!compare(*(end - 1), *(insert_pos))) {
        ret = E_DATA_CORRUPT;
      }
    }

    if (0 == ret) ret = vector_.insert(insert_pos, begin, end);
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename Compare, typename Equal>
int PointerSortedVector<T, Allocator>::replace(const_value_type value,
                                               iterator &replace_pos,
                                               Compare compare, Equal equal,
                                               value_type &replaced_value)
{
  int ret = 0;
  replace_pos = vector_.end();
  if (nullptr != value) {
    iterator find_pos =
      std::lower_bound(vector_.begin(), vector_.end(), value, compare);
    if (find_pos != end() && equal(*find_pos, value)) {
      // existent, overwrite
      ret = vector_.replace(find_pos, value, replaced_value);
    } else {
      // non-existent, insert
      ret = vector_.insert(find_pos, value);
    }
    if (0 == ret) {
      replace_pos = find_pos;
    }
  } else {
    ret = E_DATA_CORRUPT;
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename Compare, typename Unique>
int PointerSortedVector<T, Allocator>::insert_unique(const_value_type value,
                                                     iterator &insert_pos,
                                                     Compare compare,
                                                     Unique unique)
{
  int ret = 0;
  if (nullptr == value) ret = E_DATA_CORRUPT;
  iterator begin_iterator = begin();
  iterator end_iterator = end();
  insert_pos = end_iterator;
  if (0 == ret) {
    iterator find_pos =
      std::lower_bound(begin_iterator, end_iterator, value, compare);
    insert_pos = find_pos;
    iterator compare_pos = find_pos;
    iterator prev_pos = end_iterator;

    if (find_pos == end_iterator && size() > 0) compare_pos = last();
    if (find_pos > begin_iterator && find_pos < end_iterator)
      prev_pos = find_pos - 1;

    if (compare_pos >= begin_iterator && compare_pos < end_iterator) {
      if (unique(*compare_pos, value)) {
        insert_pos = compare_pos;
        ret = E_ENTRY_EXIST;
      }
    }

    if (0 == ret && prev_pos >= begin_iterator &&
        prev_pos < end_iterator) {
      if (unique(*prev_pos, value)) {
        insert_pos = prev_pos;
        ret = E_ENTRY_EXIST;
      }
    }

    if (0 == ret) ret = vector_.insert(insert_pos, value);
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename Compare>
int PointerSortedVector<T, Allocator>::find(const_value_type value,
                                            iterator &pos,
                                            Compare compare) const
{
  int ret = E_ENTRY_NOT_EXIST;
  pos = std::lower_bound(begin(), end(), value, compare);
  if (pos != end()) {
    if (!compare(*pos, value) && !compare(value, *pos))
      ret = 0;
    else
      pos = end();
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename ValueType, typename Compare, typename Equal>
int PointerSortedVector<T, Allocator>::find(const ValueType &value,
                                            iterator &pos, Compare compare,
                                            Equal equal) const
{
  int ret = E_ENTRY_NOT_EXIST;
  pos = std::lower_bound(begin(), end(), value, compare);
  if (pos != end()) {
    if (equal(*pos, value))
      ret = 0;
    else
      pos = end();
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename ValueType, typename Compare>
typename PointerSortedVector<T, Allocator>::iterator
PointerSortedVector<T, Allocator>::lower_bound(const ValueType &value,
                                               Compare compare) const
{
  return std::lower_bound(begin(), end(), value, compare);
}

template <typename T, typename Allocator>
template <typename ValueType, typename Compare>
typename PointerSortedVector<T, Allocator>::iterator
PointerSortedVector<T, Allocator>::upper_bound(const ValueType &value,
                                               Compare compare) const
{
  return std::upper_bound(begin(), end(), value, compare);
}

template <typename T, typename Allocator>
template <typename ValueType, typename Compare, typename Equal>
int PointerSortedVector<T, Allocator>::remove_if(const ValueType &value,
                                                 Compare comapre, Equal equal)
{
  iterator pos = end();
  int ret = find(value, pos, comapre, equal);
  if (0 == ret) {
    ret = vector_.remove(pos);
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename ValueType, typename Compare, typename Equal>
int PointerSortedVector<T, Allocator>::remove_if(const ValueType &value,
                                                 Compare comapre, Equal equal,
                                                 value_type &removed_value)
{
  iterator pos = end();
  int ret = find(value, pos, comapre, equal);
  if (0 == ret) {
    removed_value = *pos;
    ret = vector_.remove(pos);
  }
  return ret;
}

template <typename T, typename Allocator>
int PointerSortedVector<T, Allocator>::remove(iterator start_pos,
                                              iterator end_pos)
{
  int ret = 0;
  if (end_pos - start_pos > 0) {
    ret = vector_.remove(start_pos, end_pos);
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename Compare>
void PointerSortedVector<T, Allocator>::sort(Compare compare)
{
  std::sort(begin(), end(), compare);
}

}  // end namespace container
}  // end namespace is
