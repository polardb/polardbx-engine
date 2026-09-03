/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/consensus/komodo0mem0pool.cc
  Memory pool for consensus cache implement

 Created 2025-05-16 by Jiyang.zhang
 *******************************************************/

#include "sql/consensus/komodo0fifo0cache.h"
#include "sql/consensus/komodo0mem0pool.h"
#include "sql/komodo0mon.h"

namespace komodo {

Mem_pool_fifo_type opt_fifo_mem_pool_type = MEM_POOL_REDUNDANT;

void Flexible_mem_pool::create(PSI_memory_key key_memory,
                               const size_t preallocated_size_arg) {
  m_key_memory = key_memory;

  m_buffer_pool_size = align_up(preallocated_size_arg, 4096);

  m_buffer_pool =
      (uchar *)my_malloc(m_key_memory, m_buffer_pool_size, MYF(MY_FAE));
  m_front = m_back = m_buffer_pool;

  m_malloc_threshold = (m_buffer_pool_size * MEM_POOL_MALLOC_THRESHOLD) / 100;
}

void Flexible_mem_pool::destroy() {
  if (m_buffer_pool) {
    my_free(m_buffer_pool);
    m_buffer_pool = nullptr;
  }
}

Redundant_mem_pool::Buffer_t *Redundant_mem_pool::allocate_from_system(
    size_t bytes, Buffer_t *buffer) {
  buffer->m_data = (uchar *)my_malloc(m_key_memory, bytes, MYF(MY_FAE));
  buffer->m_by_malloc = true;
  buffer->m_alloc_size = buffer->m_linear_size = bytes;
  buffer->m_offset = 0;
  fifo_cache_status.malloc_total++;

  return buffer;
}

}
