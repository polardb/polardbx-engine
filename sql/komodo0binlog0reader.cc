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

/** @file sql/komodo0binlog0reader.cc
  Komodo binlog reader.

 Created 2025-09-11 by Jiyang.zhang
 *******************************************************/
#include "sql/komodo0binlog0reader.h"
#include "mysql/service_mysql_alloc.h"
#include "sql/psi_memory_key.h"

namespace komodo {
PSI_memory_key key_memory_prealloc_binlog_event_data_allocator;

Prealloc_binlog_event_data_allocator::Prealloc_binlog_event_data_allocator(
    ulonglong prealloc_size) {
  m_buffer = static_cast<unsigned char *>(
      my_malloc(key_memory_log_event, prealloc_size, MYF(MY_WME)));
  m_buffer_size = prealloc_size;
  m_offset = 0;
}

Prealloc_binlog_event_data_allocator::~Prealloc_binlog_event_data_allocator() {
  my_free(m_buffer);
}

unsigned char *Prealloc_binlog_event_data_allocator::allocate(ulonglong bytes) {
  unsigned char *ptr;

  if (free() < bytes) {
    return nullptr;
  }

  ptr = m_buffer + m_offset;
  m_offset += bytes;
  return ptr;
}

Peekable_binlog_event_data_istream::Peekable_binlog_event_data_istream(
    Binlog_read_error *error, IO_CACHE *io_cache, ulonglong max_event_size)
    : m_error(error),
      m_istream(io_cache),
      m_max_event_size(max_event_size),
      m_event_length(0),
      m_event_type(0) {}

bool Peekable_binlog_event_data_istream::read_event_header() {
  return read_fixed_length<Binlog_read_error::READ_EOF>(
      m_header, LOG_EVENT_MINIMAL_HEADER_LEN);
}

bool Peekable_binlog_event_data_istream::check_event_header() {
  m_event_type = m_header[EVENT_TYPE_OFFSET];
  m_event_length = uint4korr(m_header + EVENT_LEN_OFFSET);

  if (m_event_length < LOG_EVENT_MINIMAL_HEADER_LEN)
    return m_error->set_type(Binlog_read_error::BOGUS);
  if (m_event_length > m_max_event_size)
    return m_error->set_type(Binlog_read_error::EVENT_TOO_LARGE);

  return false;
}

bool Peekable_binlog_event_data_istream::fill_event_data(
    unsigned char *event_data) {
  memcpy(event_data, m_header, LOG_EVENT_MINIMAL_HEADER_LEN);
  if (read_fixed_length<Binlog_read_error::TRUNC_EVENT>(
          event_data + LOG_EVENT_MINIMAL_HEADER_LEN,
          m_event_length - LOG_EVENT_MINIMAL_HEADER_LEN))
    return true;

  return false;
}

}  // namespace komodo
