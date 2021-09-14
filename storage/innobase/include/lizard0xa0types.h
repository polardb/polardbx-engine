/*****************************************************************************

Copyright (c) 2013, 2021, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/lizard0xa0types.h
  Lizard XA transaction structure.

 Created 2021-08-10 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0xa0types_h
#define lizard0xa0types_h

#include <string>
#include <unordered_set>

#include "trx0types.h"

typedef std::unordered_set<trx_id_t> trx_group_ids_t;

/**
  Structure used by Vision
*/
struct trx_group_ids {
 public:
  trx_group_ids_t m_ids;
  ulint m_size;

  explicit trx_group_ids() : m_ids(), m_size(0) {}

  virtual ~trx_group_ids() { clear(); }

  void push(trx_id_t id) {
    if (m_ids.insert(id).second) m_size++;
  }

  void clear() {
    if (m_size > 0) {
      m_ids.clear();
      m_size = 0;
    }
  }

  ulint size() { return m_size; }

  bool has(const trx_id_t id) const {
    if (m_size > 0 && find(id)) return true;
    return false;
  }

 private:
  bool find(const trx_id_t id) const {
    auto it = m_ids.find(id);
    if (it != m_ids.end()) return true;
    return false;
  }
};

#endif
