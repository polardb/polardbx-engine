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
/** @file include/lizard0iv.cc
  Lizard universe tools

 Created 2021-11-05 by Jianwei.zhao
 *******************************************************/

#include "lizard0tcn.h"
#include "ut0new.h"

namespace lizard {

/** Constructor */
template <typename Element_type, typename Key_type, typename Value_type,
          size_t Prealloc>
Lru_list<Element_type, Key_type, Value_type, Prealloc>::Lru_list() : hash() {
  /** Unneccessary to memset m_elements if add into free list. */
  UT_LIST_INIT(m_free_list);
  UT_LIST_INIT(m_lru_list);

  for (size_t i = 0; i < Prealloc; i++) {
    // new (&m_elements[i]) Element_type();
    UT_LIST_ADD_LAST(m_free_list, m_elements + i);
  }

  ut_ad(UT_LIST_GET_LEN(m_free_list) == Prealloc);
}

/** Destructor */
template <typename Element_type, typename Key_type, typename Value_type,
          size_t Prealloc>
Lru_list<Element_type, Key_type, Value_type, Prealloc>::~Lru_list() {
  /** 1.LOCK lru mutex */
  /** 2.LOCK hash mutex */
  /**
  Element_type *elem = UT_LIST_GET_FIRST(m_lru_list);
  while (elem != nullptr) {
    iv_hash_delete(&hash, elem);
    UT_LIST_REMOVE(m_lru_list, elem);
    elem = UT_LIST_GET_FIRST(m_lru_list);
  }
  */

  /** 3.LOCK free mutex*/
  /*
  elem = UT_LIST_GET_FIRST(m_free_list);
  while (elem != nullptr) {
    UT_LIST_REMOVE(m_free_list, elem);
    elem = UT_LIST_GET_FIRST(m_free_list);
  }


  for (size_t i = 0; i < Prealloc; i++) {
    m_elements[i].~Element_type();
  }
  */
}

template <typename Element_type, typename Key_type, typename Value_type,
          size_t Prealloc>
bool Lru_list<Element_type, Key_type, Value_type, Prealloc>::insert(
    Value_type value) {
  Element_type *elem = nullptr;
  bool result = false;

  /** Step 1. */
  /** Lock free mutex to find available slot */
  elem = get_free_item();
  /** Release free mutex */

  if (elem) {
    elem->assign(value);
    /** Lock LRU mutex */
    /** Lock hash mutex */
    result = iv_hash_insert(&hash, elem);
    if (!result) {
      UT_LIST_ADD_LAST(m_lru_list, elem);
    } else {
      put_free_item(elem);
    }
    return result;
  } else {
    return true;
  }
}

/**
  Search committed transaction information by trx_id.
*/
template <typename Element_type, typename Key_type, typename Value_type,
          size_t Prealloc>
Value_type Lru_list<Element_type, Key_type, Value_type, Prealloc>::search(
    Key_type key) {
  Element_type *elem;
  Value_type value;
  elem = iv_hash_search(&hash, key);
  if (elem) {
    elem->copy_to(value);
  }
  return value;
}

/** Template */
template tcn_t Lru_list<tcn_node_t, trx_id_t, tcn_t, SESSION_TCN_SIZE>::search(
    trx_id_t id);
template bool Lru_list<tcn_node_t, trx_id_t, tcn_t, SESSION_TCN_SIZE>::insert(
    tcn_t tcn);
template Lru_list<tcn_node_t, trx_id_t, tcn_t, SESSION_TCN_SIZE>::Lru_list();
template Lru_list<tcn_node_t, trx_id_t, tcn_t, SESSION_TCN_SIZE>::~Lru_list();

template tcn_t Lru_list<tcn_node_t, trx_id_t, tcn_t, LRU_TCN_SIZE>::search(
    trx_id_t id);
template bool Lru_list<tcn_node_t, trx_id_t, tcn_t, LRU_TCN_SIZE>::insert(
    tcn_t tcn);
template Lru_list<tcn_node_t, trx_id_t, tcn_t, LRU_TCN_SIZE>::Lru_list();
template Lru_list<tcn_node_t, trx_id_t, tcn_t, LRU_TCN_SIZE>::~Lru_list();

}  // namespace lizard
