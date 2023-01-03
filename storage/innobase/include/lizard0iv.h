/*****************************************************************************
Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/** @file include/lizard0iv.h
  Lizard universe tools

 Created 2021-11-05 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0iv_h
#define lizard0iv_h

#include "univ.i"
#include "hash0hash.h"

namespace lizard {

template <typename Element_type, size_t SIZE>
struct iv_hash_t {
 public:
  ulint n_cells;
  hash_cell_t cells[SIZE];

  explicit iv_hash_t() {
    n_cells = SIZE;
    memset(&cells, 0x0, sizeof(cells));
  }

  ulint size() { return n_cells; }

  hash_cell_t *nth_cell(ulint fold) { return cells + fold; }
};

#define HASH_LINK_MAX_SIZE 4

/**
  Insert into hash unqiuely.
  @retval     false     success
  @retval     true      failure
*/
template <typename Element_type, size_t SIZE>
bool iv_hash_insert(iv_hash_t<Element_type, SIZE> *hash, Element_type *elem) {
  hash_cell_t *cell3333;
  Element_type *struct3333;

  ulint i = 0;

  /** Insert last. */
  elem->hash_node() = nullptr;

  cell3333 = hash->nth_cell(ut_hash_ulint(elem->key(), hash->size()));

  if (cell3333->node == nullptr) {
    cell3333->node = elem;
    return false;
  } else {
    struct3333 = (Element_type *)cell3333->node;
    if (struct3333->key() == elem->key()) return true;

    while (struct3333->hash_node() != nullptr) {
      struct3333 = (Element_type *)struct3333->hash_node();

      if (struct3333->key() == elem->key()) return true;

      if (i++ > HASH_LINK_MAX_SIZE) return true;
    }
    struct3333->hash_node() = elem;
    return false;
  }
}

template <typename Element_type, size_t SIZE>
bool iv_hash_delete(iv_hash_t<Element_type, SIZE> *hash, Element_type *elem) {
  hash_cell_t *cell3333;
  Element_type *struct3333;

  cell3333 = hash->nth_cell(ut_hash_ulint(elem->key(), hash->size()));

  if (cell3333->node == elem) {
    cell3333->node = elem->hash_node();
    return false;
  } else {
    struct3333 = (Element_type *)cell3333->node;
    while (struct3333->hash_node() != elem) {
      struct3333 = (Element_type *)struct3333->hash_node();
      ut_a(struct3333);
    }
    struct3333->hash_node() = elem->hash_node();
    return false;
  }
}

template <typename Element_type, typename Key_type, size_t SIZE>
Element_type *iv_hash_search(iv_hash_t<Element_type, SIZE> *hash,
                             Key_type key) {
  hash_cell_t *cell3333;
  cell3333 = hash->nth_cell(ut_hash_ulint(key, hash->size()));
  Element_type *elem = (Element_type *)cell3333->node;

  while (elem != nullptr) {
    if (elem->key() == key) {
      return elem;
    }
    elem = (Element_type *)elem->hash_node();
  }
  return nullptr;
}

template <typename Element_type, typename Key_type, typename Value_type>
class Cache_interface {
 public:
  Cache_interface() {}
  virtual ~Cache_interface() {}

  virtual bool insert(Value_type value) = 0;
  virtual Value_type search(Key_type key) = 0;
};

template <typename Element_type, typename Key_type, typename Value_type,
          size_t Prealloc>
class Random_array
    : public Cache_interface<Element_type, Key_type, Value_type> {
 public:
  Random_array() {}

  virtual ~Random_array() {}

  virtual bool do_before_operation(size_t pos) { return false; }

  virtual void do_after_operation(bool required, size_t pos) {}

  virtual bool insert(Value_type value) {
    size_t pos = ut_hash_ulint(value.key(), Prealloc);
    bool pre_check = do_before_operation(pos);
    if (!pre_check) {
      m_elements[pos] = value;
    }
    do_after_operation(!pre_check, pos);

    return pre_check;
  }

  virtual Value_type search(Key_type key) {
    size_t pos = ut_hash_ulint(key, Prealloc);
    bool pre_check = do_before_operation(pos);
    Value_type value;
    if (!pre_check) {
      value = m_elements[pos];
    }
    do_after_operation(!pre_check, pos);
    return value;
  }

 private:
  Value_type m_elements[Prealloc];
};

#define ATOMIC_RANDOM_ARRAY_RETRY_MAX_TIMES 10

template <typename Element_type, typename Key_type, typename Value_type,
          size_t Prealloc>
class Atomic_random_array
    : public Random_array<Element_type, Key_type, Value_type, Prealloc> {
 public:
  Atomic_random_array() {}

  virtual bool do_before_operation(size_t pos) {
    uint loop = 0;
    bool expected;
    uint used_index = (pos%512) * 64;
  retry:
    if (loop++ > ATOMIC_RANDOM_ARRAY_RETRY_MAX_TIMES) return true;

    expected = false;
    if (m_used[used_index].compare_exchange_strong(expected, true)) {
      return false;
    } else {
      goto retry;
    }
  }

  virtual void do_after_operation(bool required, size_t pos) {
    if (required) {
      m_used[(pos%512) * 64].store(false);
    }
  }

 private:
  std::atomic<bool> m_used[512 * 64];
};

/**
  Lru_list for array of tcn nodes,
  also include:
      1) Fixed array
      2) Free list
      3) LRU list
      4) Hash
*/
template <typename Element_type, typename Key_type, typename Value_type,
          size_t Prealloc>
class Lru_list : public Cache_interface<Element_type, Key_type, Value_type> {
  constexpr static ulint HASH_SIZE = Prealloc / 2;

 public:
  explicit Lru_list();

  virtual ~Lru_list();

  bool insert(Value_type value);

  Value_type search(Key_type key);

  Element_type *get_free_item() {
    Element_type *elem = nullptr;

  retry:
    /** Lock free list mutex */
    elem = UT_LIST_GET_FIRST(m_free_list);

    if (elem != nullptr) {
      UT_LIST_REMOVE(m_free_list, elem);
      return elem;
    } else {
      /** Lock lru list mutex */
      /** Lock hash mutex */
      elem = UT_LIST_GET_FIRST(m_lru_list);
      UT_LIST_REMOVE(m_lru_list, elem);
      iv_hash_delete(&hash, elem);
      put_free_item(elem);

      goto retry;
    }
    return elem;
  }

  void put_free_item(Element_type *elem) {
    /** Lock free list mutex */
    UT_LIST_ADD_LAST(m_free_list, elem);
  }

 private:
  Element_type m_elements[Prealloc];
  UT_LIST_BASE_NODE_T(Element_type) m_free_list;
  UT_LIST_BASE_NODE_T(Element_type) m_lru_list;
  iv_hash_t<Element_type, HASH_SIZE> hash;
};

}  // namespace lizard
#endif
