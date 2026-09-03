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

#include "komodo0buffer.h"
#include "komodo0fifo0cache.h"
#include "sql/komodo0assert.h"

namespace komodo {

void Buffer_array::add_buffer(Buffer *buffer, uint64_t consensus_index) {
  m_consensus_indices.push_back(consensus_index);
  m_buffers.push_back(buffer);
}

void Buffer_array::append(const uchar *data, std::size_t bytes) {
  kut_a(m_consensus_indices.size() == m_buffers.size());

  while (bytes > 0) {
    kut_a(!m_buffers.empty());

    std::size_t free_space = m_buffers.front()->free_space();
    if (free_space >= bytes) {
      m_buffers.front()->append(data, bytes);
      data += bytes;
      bytes = 0;
    } else {
      m_buffers.front()->append(data, free_space);
      data += free_space;
      bytes -= free_space;
      m_fifo_cache->close_buffer(m_consensus_indices.front(), false);
      m_buffers.pop_front();
      m_consensus_indices.pop_front();
    }
  }
}

void Buffer_array::close_buffers(bool error) {
  while (!m_consensus_indices.empty()) {
    m_fifo_cache->close_buffer(m_consensus_indices.front(), error);
    m_consensus_indices.pop_front();
    m_buffers.pop_front();
  }
}

Buffer_array::~Buffer_array() {
  /* make sure buffers are closed before destroying */
  kut_a(m_consensus_indices.empty() && m_buffers.empty());
}

}  // namespace komodo
