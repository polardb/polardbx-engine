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

/** @file sql/consensus/komodo0mem0pool.h
  Memory pool for consensus cache implement

 Created 2025-05-16 by Jiyang.zhang
 *******************************************************/

#ifndef komodo0mem0pool_h
#define komodo0mem0pool_h

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "komodo0buffer.h"
#include "my_inttypes.h"
#include "my_sys.h"
#include "mysql/components/services/bits/psi_memory_bits.h"  // PSI_memory_key
#include "mysql/service_mysql_alloc.h"
#include "sql/komodo0assert.h"

namespace komodo {

inline size_t align_up(size_t val, size_t alignment) {
  kut_a(alignment != 0);
  return (val + alignment - 1) & ~(alignment - 1);
}

inline size_t div_ceil(size_t bytes, size_t block_size) {
  return (bytes + block_size - 1) / block_size;
}

enum Mem_pool_fifo_type {
  MEM_POOL_REDUNDANT,
  // MEM_POOL_COMPACT,
  // MEM_POOL_BLOCK,
  // MEM_POOL_ALL,
};

extern Mem_pool_fifo_type opt_fifo_mem_pool_type;

/**
  @Flexible_mem_pool
    The memory pool is arranged as a ring buffer.

    Ability to allocate buffers of variable length, that is why it is named
    **Flexible**.

    Always allocate from the backward of the ring buffer.

    The buffer can only be deallocated in order from the backend or frontend of
    the allocated space.

  @Compact_mem_pool, subclass of @Flexible_mem_pool (Removed)
    The allocated buffer is compact, no space is wasted.

    But the allocated buffer is not guaranteed to be continuous. In fact, the
    buffer may consist of two continuous spaces.

  @Redundant_mem_pool, subclass of @Flexible_mem_pool
    The allocated buffer is redundant. If there are no continuous available
    spaces, some tailer spaces will be wasted.

    But the allocated buffer is guaranteed to be continuous.

  @Block_mem_pool (Removed)
    The memory pool is arranged as list of Fixed-Length blocks.

    Allocate at least one block each time. This may result in a waste of space.

    Allocated buffer can be deallocated arbitrarily.
*/

class Flexible_mem_pool {
 public:
  /** The safe margin for check if buffer pool is full. */
  static constexpr size_t MEM_POOL_SAFE_MARGIN = 1;

  /** Maxmum percentage of preallocated memory for a large payload. */
  static constexpr size_t MEM_POOL_MALLOC_THRESHOLD = 25;

 public:
  Flexible_mem_pool(PSI_memory_key key_memory_arg,
                    const size_t preallocated_size_arg)
      : m_key_memory(),
        m_buffer_pool_size(0),
        m_buffer_pool(nullptr),
        m_front(nullptr),
        m_back(nullptr),
        m_malloc_threshold(0) {
    create(key_memory_arg, preallocated_size_arg);
  }

  ~Flexible_mem_pool() { destroy(); }

  bool almost_full(unsigned int percentage) const {
    return free_buffer() * 100 <= m_buffer_pool_size * percentage;
  }

  size_t get_buffer_pool_size() const { return m_buffer_pool_size; }

  size_t used_buffer() const {
    if (m_front <= m_back) {
      return (m_back - m_front);
    } else {
      return m_buffer_pool_size - (m_front - m_back);
    }
  }

 protected:
  void create(PSI_memory_key key_memory_arg,
              const size_t preallocated_size_arg);

  void destroy();

  size_t free_buffer() const {
    return m_buffer_pool_size - used_buffer() - MEM_POOL_SAFE_MARGIN;
  }

  uchar *cal_pos_by_relative_pos(const uchar *p, int64_t relative_pos) {
    return m_buffer_pool +
           (p + m_buffer_pool_size + relative_pos - m_buffer_pool) %
               m_buffer_pool_size;
  }

 protected:
  PSI_memory_key m_key_memory;
  size_t m_buffer_pool_size;
  uchar *m_buffer_pool;
  uchar *m_front;
  uchar *m_back;
  size_t m_malloc_threshold;
};

/**
  @brief Represents a writable, contiguous memory segment from a ring buffer.

  The allocation strategy is to ALWAYS provide a single, contiguous block of
  memory. If the space at the end of the ring buffer is insufficient for a
  request, that space is skipped (wasted), and the allocation is attempted
  from the beginning of the buffer.

  This comment explains the relationship between the key member variables.


  ### Scenario 1: Allocation without wrap-around

  A request for 100 bytes is satisfied from a contiguous free block.

  Ring Buffer State:
  |...used...|-------------- Free Space --------------|...used...|
                   ^
                   | A 100-byte block is allocated here
                   v
              |...[ A 100-byte allocated block ]...|

  In this case:
  - m_alloc_size:  100. The total space consumed from the ring buffer.
  - m_linear_size: 100. The usable, contiguous space given to the user.
  Here, `m_alloc_size` == `m_linear_size`.


  ### Scenario 2: Allocation with wrap-around

  A request for 100 bytes arrives when only 30 bytes are free at the end,
  but ample space is available at the beginning of the buffer.

  Ring Buffer State:
  |---Free (Start)---|...used data...|---Free (End, 30B)---|

  Allocation process for a 100-byte request:
  1. The allocator finds the 30 bytes at the end is insufficient.
  2. To ensure contiguity, it *skips* this space, creating a gap.
  3. It wraps around and allocates 100 bytes from the start of the buffer.

  Resulting Ring Buffer Layout:
  |..[Allocated 100B]..|...|...used...|---[WASTED 30B]---|
   ^
   | m_data points here

  In this wrap-around scenario:
  - m_alloc_size:  130. Total space consumed (30 wasted + 100 used). This
                     is how far the ring buffer's internal pointers advance.
  - m_linear_size: 100. The size of the actual, usable, contiguous block
                     that was returned to the user.

  The difference, `(m_alloc_size - m_linear_size)`, represents the internal
  fragmentation that was sacrificed to guarantee a contiguous block.
*/
class Redundant_mem_pool : public Flexible_mem_pool {
 private:
  class Buffer_impl : public Buffer {
   public:
    size_t m_alloc_size;
    size_t m_linear_size;
    size_t m_offset;
    bool m_by_malloc;
    uchar *m_data;

    Buffer_impl() { reset(); }

    Buffer_impl &operator=(Buffer_impl &&rhs) noexcept {
      m_alloc_size = rhs.m_alloc_size;
      m_linear_size = rhs.m_linear_size;
      m_offset = rhs.m_offset;
      m_by_malloc = rhs.m_by_malloc;
      m_data = rhs.m_data;
      return *this;
    }

    virtual void reset() override {
      m_alloc_size = 0;
      m_linear_size = 0;
      m_offset = 0;
      m_by_malloc = false;
      m_data = nullptr;
    }

    virtual size_t size() const override { return m_offset; }

    virtual size_t capacity() const override { return m_linear_size; }

    virtual size_t free_space() const override {
      return m_linear_size - m_offset;
    }

    virtual void deep_copy(std::string &to) const override {
      kut_a(m_offset == m_linear_size);
      to.append((char *)m_data, m_offset);
    }

    virtual void append(const uchar *data, size_t bytes) override {
      kut_a(m_linear_size - m_offset >= bytes);
      memcpy(m_data + m_offset, data, bytes);
      m_offset += bytes;
    }
  };

 public:
  typedef Buffer_impl BUFFER_TYPE;
  typedef Buffer_impl Buffer_t;

 public:
  Redundant_mem_pool(PSI_memory_key key_memory_arg,
                     const size_t preallocated_size_arg)
      : Flexible_mem_pool(key_memory_arg, preallocated_size_arg) {}

  ~Redundant_mem_pool() { destroy(); }

  Buffer_t *allocate(size_t bytes, Buffer_t *buf,
                     bool malloc_if_not_enough = false) {
    if (bytes > m_malloc_threshold ||
        (malloc_if_not_enough && bytes > free_space())) {
      return allocate_from_system(bytes, buf);
    } else {
      return allocate_from_pool(bytes, buf);
    }
  }

  void deallocate(Buffer_t *buffer, bool error = false) {
    if (buffer->m_by_malloc) {
      deallocate_to_system(buffer);
    } else {
      deallocate_to_pool(buffer, error);
    }
  }

 private:
  size_t free_space() const {
    if (m_front <= m_back) {
      return m_buffer_pool_size - (m_back - m_buffer_pool) -
             MEM_POOL_SAFE_MARGIN;
    } else {
      return m_front - m_back - MEM_POOL_SAFE_MARGIN;
    }
  }

  /**
    Try to rewind the ring-buffer. If rewind, then the tail buffer will be
    wasted, and also its space will be taken over by the next allocated buffer.
    m_back will be also adjusted if successfully.

    @param[in]  needed_bytes    How much space is needed.
    @param[out] alloc_bytes     The wasted bytes

    @return     true if there is enough space after rewind buffer.
                false if there is not enough space.
  */
  bool try_rewind(size_t needed_bytes, size_t *alloc_bytes) {
    size_t available_bytes;

    available_bytes = m_front < (m_buffer_pool + MEM_POOL_SAFE_MARGIN)
                          ? 0
                          : m_front - m_buffer_pool - MEM_POOL_SAFE_MARGIN;
    if (available_bytes >= needed_bytes) {
      *alloc_bytes += (m_buffer_pool + m_buffer_pool_size - m_back);
      m_back = cal_pos_by_relative_pos(m_back, *alloc_bytes);
      assert(m_back == m_buffer_pool);
      assert(m_back <= m_front - MEM_POOL_SAFE_MARGIN);
      return true;
    }

    return false;
  }

  Buffer_t *allocate_from_system(size_t bytes, Buffer_t *buffer);

  Buffer_t *allocate_from_pool(size_t bytes, Buffer_t *buffer) {
    size_t available_bytes;
    size_t alloc_bytes = 0;
    if (m_front <= m_back) {
      available_bytes = free_space();

      if (available_bytes < bytes) {
        /** Will adjust m_back if try successfully. */
        if (!try_rewind(bytes, &alloc_bytes)) {
          return nullptr;
        }
      }
    } else {
      /** m_back < m_front */
      available_bytes = free_space();
      if (available_bytes < bytes) {
        return nullptr;
      }
    }

    alloc_bytes += bytes;
    buffer->m_alloc_size = alloc_bytes;
    buffer->m_linear_size = bytes;
    buffer->m_offset = 0;
    buffer->m_data = m_back;
    buffer->m_by_malloc = false;

    m_back = cal_pos_by_relative_pos(m_back, bytes);

    return buffer;
  }

  void deallocate_to_pool(Buffer_t *buffer, bool error) {
    bool is_front;
    size_t dealloc_bytes = buffer->m_alloc_size;
    kut_a(error || buffer->m_offset == buffer->m_linear_size);
    kut_a(used_buffer() >= dealloc_bytes);

    if (m_front ==
        cal_pos_by_relative_pos(buffer->m_data,
                                buffer->m_linear_size - buffer->m_alloc_size)) {
      is_front = true;
    } else {
      kut_a(m_back ==
            cal_pos_by_relative_pos(buffer->m_data, buffer->m_linear_size));
      is_front = false;
    }

    if (likely(is_front)) {
      m_front = cal_pos_by_relative_pos(m_front, dealloc_bytes);
      assert(m_front ==
             cal_pos_by_relative_pos(buffer->m_data, buffer->m_linear_size));
    } else {
      /**
        Revision:
        (-dealloc_bytes) --> (m_buffer_pool_size - dealloc_bytes)
      */
      m_back =
          cal_pos_by_relative_pos(m_back, m_buffer_pool_size - dealloc_bytes);
      assert(m_back ==
             cal_pos_by_relative_pos(
                 buffer->m_data, buffer->m_linear_size - buffer->m_alloc_size));
    }

    buffer->reset();
  }

  void deallocate_to_system(Buffer_t *buffer) {
    my_free(buffer->m_data);
    buffer->reset();
  }
};

}  // namespace komodo

#endif
