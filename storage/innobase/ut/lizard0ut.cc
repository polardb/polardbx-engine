/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/lizard0ut.h
 Lizard utility

 Created 2020-06-04 by Jianwei.zhao
 *******************************************************/

#include "lizard0ut.h"
#include "univ.i"

#include "lizard0cleanout.h"

namespace lizard {

template <typename Element_type, typename Object_type, std::size_t Prealloc>
Partition<Element_type, Object_type, Prealloc>::Partition()
    : m_size(Prealloc), m_counter(0), m_parts() {
  Element_type *elem;
  for (std::size_t i = 0; i < m_size; i++) {
    elem = static_cast<Element_type *>(ut::zalloc(sizeof(Element_type)));
    new (elem) Element_type();

    m_parts.push_back(elem);
  }
}

template <typename Element_type, typename Object_type, std::size_t Prealloc>
Partition<Element_type, Object_type, Prealloc>::~Partition() {
  Element_type *elem;
  for (std::size_t i = 0; i < m_size; i++) {
    elem = m_parts.at(i);
    elem->~Element_type();
    ut::free(elem);
  }

  m_parts.clear();
  m_size = 0;
  m_counter = 0;
}

template <typename Element_type, typename Object_type, std::size_t Prealloc>
bool Partition<Element_type, Object_type, Prealloc>::insert(
    Object_type object) {
  Element_type *elem;
  bool res = false;
  std::size_t i;
  for (i = 0; i < m_size; i++) {
    elem = m_parts.at(i);
    res = elem->insert(object);
    if (res == false) break;
  }

  ut_ad(i == 0 || i == m_size);

  return res;
}

template <typename Element_type, typename Object_type, std::size_t Prealloc>
bool Partition<Element_type, Object_type, Prealloc>::exist(Object_type object) {
  Element_type *elem;
  m_counter++;
  elem = m_parts.at(m_counter.load() % m_size);
  return elem->exist(object);
}

template Partition<Undo_logs, Undo_hdr, TXN_UNDO_HASH_PARTITIONS>::Partition();
template Partition<Undo_logs, Undo_hdr, TXN_UNDO_HASH_PARTITIONS>::~Partition();
template bool Partition<Undo_logs, Undo_hdr, TXN_UNDO_HASH_PARTITIONS>::insert(
    Undo_hdr object);
template bool Partition<Undo_logs, Undo_hdr, TXN_UNDO_HASH_PARTITIONS>::exist(
    Undo_hdr object);

}  // namespace lizard
