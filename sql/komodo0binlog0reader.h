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

/** @file sql/komodo0binlog0reader.h
  Komodo binlog reader.

 Created 2025-09-11 by Jiyang.zhang
 *******************************************************/
#ifndef komodo0binlog0reader_h
#define komodo0binlog0reader_h

#include "sql/binlog_reader.h"

namespace komodo {

class Prealloc_binlog_event_data_allocator {
 public:
  Prealloc_binlog_event_data_allocator(ulonglong prealloc_size);
  ~Prealloc_binlog_event_data_allocator();

  enum { DELEGATE_MEMORY_TO_EVENT_OBJECT = false };
  unsigned char *allocate(ulonglong);
  void deallocate(unsigned char *) { /* do nothing */
  }

  void clear() { m_offset = 0; }

  unsigned char *data() { return m_buffer; }
  ulonglong size() const { return m_offset; }
  ulonglong free() const { return m_buffer_size - m_offset; }

 protected:
  unsigned char *m_buffer;
  ulonglong m_buffer_size;
  ulonglong m_offset;
};

class Peekable_binlog_event_data_istream {
  class NON_SEEKABLE_IO_CACHE_istream : public Basic_istream {
   public:
    NON_SEEKABLE_IO_CACHE_istream(IO_CACHE *io_cache) : m_io_cache(io_cache) {}

    ssize_t read(unsigned char *buffer, size_t length) override {
      DBUG_ENTER("IO_CACHE_istream::read");
      if (my_b_read(m_io_cache, buffer, length) ||
          DBUG_EVALUATE_IF("simulate_magic_header_io_failure", 1, 0))
        DBUG_RETURN(m_io_cache->error);
      DBUG_RETURN(static_cast<longlong>(length));
    }

   private:
    IO_CACHE *m_io_cache;
  };

 public:
  Peekable_binlog_event_data_istream(Binlog_read_error *error,
                                     IO_CACHE *io_cache,
                                     ulonglong max_event_size);
  Peekable_binlog_event_data_istream() = delete;
  Peekable_binlog_event_data_istream(const Binlog_event_data_istream &) =
      delete;
  Peekable_binlog_event_data_istream &operator=(
      const Peekable_binlog_event_data_istream &) = delete;
  virtual ~Peekable_binlog_event_data_istream() = default;

  bool peek_next_event_length(ulonglong *event_length) {
    if (!m_event_length) {
      if (read_event_header() || check_event_header()) {
        return true;
      }
    }

    *event_length = m_event_length;
    return false;
  }

  /**
     Read an event data from the stream and verify its checksum if
     verify_checksum is true.

     @param[out] data The pointer of the event data
     @param[out] length The length of the event data
     @param[in] allocator It is used to allocate memory for the event data.
     @retval false Succeed
     @retval true Error
  */
  template <class ALLOCATOR>
  bool read_event_data(unsigned char **data, ulonglong *length,
                       ALLOCATOR *allocator) {
    assert(m_event_length > 0);
    unsigned char *event_data = allocator->allocate(m_event_length);
    if (event_data == nullptr)
      return m_error->set_type(Binlog_read_error::MEM_ALLOCATE);

    if (fill_event_data(event_data)) {
      allocator->deallocate(event_data);
      return true;
    }

    *data = event_data;
    *length = m_event_length;

    m_event_length = 0;

    return false;
  }

  Binlog_read_error get_read_error() const { return *m_error; }

 protected:
  unsigned char m_header[LOG_EVENT_MINIMAL_HEADER_LEN];
  /**
     Read the event header from the Basic_istream

     @retval false Succeed
     @retval true Error
  */
  virtual bool read_event_header();
  /**
     Check if it is a valid event header

     @retval false Succeed
     @retval true Error
  */
  bool check_event_header();
  /**
     Read fixed length of data from Basic_istream. It sets error to
     - Binlog_read_error::SYTEM_IO if Basic_istream returns error.
     - Binlog_read_error::TRUNC_EVENT if less than length is read.
     - ERROR_TYPE if zero byte is read. ERROR_TYPE is either TRUNC_EVENT or
       READ_EOF.

     @param[in] data    The buffer where the data stored.
     @param[in] length  Bytes of the data to be read.
     @retval false Succeed
     @retval true Error
  */
  template <Binlog_read_error::Error_type ERROR_TYPE>
  bool read_fixed_length(unsigned char *data, ulonglong length) {
    DBUG_TRACE;
    if (length == 0) return false;

    longlong ret = m_istream.read(data, length);
    switch (ret) {
      case -1:
        return m_error->set_type(Binlog_read_error::SYSTEM_IO);
      case 0:
        return m_error->set_type(ERROR_TYPE);
      default:
        if (static_cast<ulonglong>(ret) == length) {
          return false;
        } else {
          return m_error->set_type(Binlog_read_error::TRUNC_EVENT);
        }
    }
  }

  /**
     It is convenient for caller to share a Binlog_read_error object between
     streams. So Binlog_read_error pointer is defined here. It should be
     initialized in constructor by caller.
  */
  Binlog_read_error *m_error;

 private:
  NON_SEEKABLE_IO_CACHE_istream m_istream;
  ulonglong m_max_event_size;
  ulonglong m_event_length = 0;
  ulonglong m_event_type = 0;

  /**
     Fill the event data into the given buffer and verify checksum if
     'verify_checksum' is true.

     @param[in] event_data The buffer where the event data will be stored.
     @param[in] verify_checksum Verify the event data's checksum if it is true.
     @param[in] checksum_alg Checksum algorithm for verifying the event data. It
                             is used only when verify_checksum is true.
     @retval false Succeed
     @retval true Error
  */
  bool fill_event_data(unsigned char *event_data);
};
}  // namespace komodo

#endif
