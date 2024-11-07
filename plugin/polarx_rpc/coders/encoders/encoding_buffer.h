/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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


/*
 * Copyright (c) 2019, 2022, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#pragma once

#include <assert.h>
#include <cassert>
#include <cstdint>
#include <vector>

// NOLINT(build/include_subdir)

#include "encoding_pool.h"

namespace polarx_rpc {
namespace protocol {

class Encoding_buffer final {
 public:
  constexpr static uint32_t k_page_size = 4096;

 public:
  explicit Encoding_buffer(Encoding_pool *local_pool)
      : m_local_pool(local_pool) {
    assert(k_page_size == local_pool->get_page_size());
    m_front = m_current = m_local_pool->alloc_page();
  }
  ~Encoding_buffer() {
    remove_page_list(m_front);
    m_front = m_current = nullptr;
  }

  Page *get_next_page() {
    auto page = m_local_pool->alloc_page();

    m_current->m_next_page = page;
    m_current = page;

    return m_current;
  }

  void remove_page_list(Page *page) {
    while (page) {
      auto page_to_delete = page;
      page = page_to_delete->m_next_page;

      m_local_pool->release_page(page_to_delete);
    }
  }

  template <uint32_t size>
  void ensure_buffer_size() {
    static_assert(size < k_page_size,
                  "Page size might be too small to put those data in.");

    if (!m_current->is_at_least(size)) get_next_page();
  }

  bool ensure_buffer_size(const uint32_t size) {
    if (!m_current->is_at_least(size)) {
      get_next_page();

      // Page size limits the number of bytes that
      // user can acquire in single call
      assert(m_current->is_at_least(size));
      return true;
    }

    return false;
  }

  inline bool is_empty() const {
    auto page = m_front;
    while (page) {
      if (page->get_used_bytes() > 0) return false;
      page = page->m_next_page;
    }
    return true;
  }

  Page *m_front;
  Page *m_current;

 private:
  Encoding_pool *const m_local_pool;
};

}  // namespace protocol
}  // namespace polarx_rpc
