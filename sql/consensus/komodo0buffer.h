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

/** @file sql/consensus/komodo0buffer.h
  Komodo Buffer Abstract.

 Created 2025-05-16 by Jiyang.zhang
 *******************************************************/

#ifndef komodo0buffer_h
#define komodo0buffer_h

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include "my_inttypes.h"
#include <deque>

namespace komodo {

class Fifo_cache;

/**
  Abstract base class for objects that can append data.
*/
class Appendable {
 public:
  virtual ~Appendable() = default;
  virtual void append(const uchar *data, std::size_t bytes) = 0;
};

/**
  Abstract Buffer.

  Function style like std::string.
*/
class Buffer : public Appendable {
 public:
  virtual void reset() = 0;
  virtual std::size_t size() const = 0;
  virtual std::size_t capacity() const = 0;
  virtual std::size_t free_space() const = 0;
  virtual void deep_copy(std::string &to) const = 0;
  virtual void append(const uchar *data, std::size_t bytes) override = 0;
};

class Buffer_array : public Appendable {
 public:
  Buffer_array(Fifo_cache *fifo_cache) : m_fifo_cache(fifo_cache) {}
  ~Buffer_array();

  Buffer_array(const Buffer_array &) = delete;
  Buffer_array &operator=(const Buffer_array &) = delete;
  Buffer_array(Buffer_array &&other) = delete;
  Buffer_array &operator=(Buffer_array &&other) = delete;

  void append(const uchar *data, std::size_t bytes) override;
  void add_buffer(Buffer *buffer, uint64_t consensus_index);
  void close_buffers(bool error);

 private:
  std::deque<Buffer *> m_buffers;
  std::deque<uint64_t> m_consensus_indices;
  Fifo_cache *m_fifo_cache;
};

}  // namespace komodo

#endif
