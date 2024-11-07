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
 * Copyright (c) 2018, 2022, Oracle and/or its affiliates.
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

#include <cassert>
#include <cstdint>
#include <vector>

namespace polarx_rpc {
namespace protocol {

class Page {
 public:
  Page(const uint32_t size, char *data_ptr)
      : m_begin_data(reinterpret_cast<uint8_t *>(data_ptr)),
        m_end_data(m_begin_data + size),
        m_current_data(reinterpret_cast<uint8_t *>(data_ptr)) {}

  ~Page() {
    m_current_data = nullptr;
    m_next_page = nullptr;
  }

  void reset() {
    m_current_data = m_begin_data;
    m_next_page = nullptr;
  }

  bool is_at_least(const uint32_t needed_size) const {
    return m_end_data > m_current_data + needed_size;
  }
  bool is_full() const { return m_end_data <= m_current_data; }
  bool is_empty() const { return m_begin_data == m_current_data; }

  uint32_t get_used_bytes() const { return m_current_data - m_begin_data; }
  uint32_t get_free_bytes() const { return m_end_data - m_current_data; }

  uint8_t *const m_begin_data;
  uint8_t *const m_end_data;
  uint8_t *m_current_data;

  Page *m_next_page = nullptr;
};

class Encoding_pool {
 public:
  explicit Encoding_pool(const uint32_t local_cache, const uint32_t page_size)
      : m_local_cache(local_cache), m_page_size(page_size) {}

  ~Encoding_pool() {
    while (m_empty_pages) {
      auto page = m_empty_pages;
      m_empty_pages = m_empty_pages->m_next_page;
      --m_pages;
      /// destruct and free
      page->~Page();
      delete[] reinterpret_cast<char *>(page);
    }
    assert(0 == m_pages);
    m_empty_pages = nullptr;
  }

  Page *alloc_page() {
    if (m_empty_pages) {
      auto result = m_empty_pages;
      m_empty_pages = m_empty_pages->m_next_page;

      result->reset();
      return result;
    }

    ++m_pages;
    auto page = new char[m_page_size];
    return new (page) Page(m_page_size - sizeof(Page), page + sizeof(Page));
  }

  void release_page(Page *page) {
    if (m_pages < m_local_cache) {
      page->m_next_page = m_empty_pages;
      m_empty_pages = page;

      return;
    }

    --m_pages;
    /// destruct and free
    page->~Page();
    delete[] reinterpret_cast<char *>(page);
  }

  uint32_t get_page_size() const { return m_page_size; }

 private:
  const uint32_t m_local_cache = 0;
  const uint32_t m_page_size = 0;
  Page *m_empty_pages = nullptr;
  uint32_t m_pages = 0;
};

}  // namespace protocol
}  // namespace polarx_rpc
