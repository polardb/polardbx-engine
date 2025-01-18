/*****************************************************************************
Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyEngine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyEngine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/** @file include/lizard0tcn0types.h
  Lizard tcn cache types

 Created 2021-11-05 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0tcn0types_h
#define lizard0tcn0types_h

#include "univ.i"

namespace lizard {

/** Cache interface */
template <typename Key, typename Value>
class Cache_interface {
 public:
  Cache_interface() {}
  virtual ~Cache_interface() {}

  /** Insert value.*/
  virtual bool insert(const Value &value) = 0;
  /** Search value by key. */
  virtual Value search(const Key &key) = 0;
};

/** Linear array. */
template <typename Key, typename Value>
class Linear_array : public Cache_interface<Key, Value> {
 public:
  Linear_array(size_t size, PSI_memory_key psi_mem_key) : m_size(size) {
    m_elements = ut::new_arr_withkey<Value>(
        ut::make_psi_memory_key(psi_mem_key), ut::Count{m_size});
  }

  virtual ~Linear_array() { ut::delete_arr(m_elements); }

  virtual bool do_before_operation(size_t pos) = 0;
  virtual void do_after_operation(bool required, size_t pos) = 0;

  /** Insert value.*/
  virtual bool insert(const Value &value) override {
    size_t pos = value.key() % m_size;

    bool pre_check = do_before_operation(pos);
    if (!pre_check) {
      m_elements[pos] = value;
    }
    do_after_operation(!pre_check, pos);

    return pre_check;
  }

  /** Search value by key. */
  virtual Value search(const Key &key) override {
    size_t pos = key % m_size;
    bool pre_check = do_before_operation(pos);
    Value value;
    if (!pre_check) {
      value = m_elements[pos];
    }
    do_after_operation(!pre_check, pos);
    return value;
  }

 private:
  size_t m_size;
  Value *m_elements;
};

/** Linear array which is protected by atomic operation. */
template <typename Key, typename Value>
class Atomic_linear_array : public Linear_array<Key, Value> {
 private:
  /** Retry max times. */
  constexpr static size_t RETRY_MAX_TIMES = 10;
  /** Atomic array size */
  constexpr static size_t ATOMIC_ARRAY_SIZE = 512 * 64;

 public:
  Atomic_linear_array(size_t size, PSI_memory_key psi)
      : Linear_array<Key, Value>(size, psi) {
    for (size_t i = 0; i < ATOMIC_ARRAY_SIZE; i++) {
      m_used[i].store(false);
    }
  }

  virtual ~Atomic_linear_array() {}

  virtual bool do_before_operation(size_t pos) override {
    uint loop = 0;
    bool expected;
    uint used_index = pos % ATOMIC_ARRAY_SIZE;
  retry:
    if (loop++ > RETRY_MAX_TIMES) return true;

    expected = false;
    if (m_used[used_index].compare_exchange_strong(expected, true)) {
      return false;
    } else {
      goto retry;
    }
  }

  virtual void do_after_operation(bool required, size_t pos) override {
    if (required) {
      ut_ad(m_used[pos % ATOMIC_ARRAY_SIZE].load() == true);
      m_used[pos % ATOMIC_ARRAY_SIZE].store(false);
    }
  }

 private:
  std::atomic<bool> m_used[ATOMIC_ARRAY_SIZE];
};

}  // namespace lizard
#endif
