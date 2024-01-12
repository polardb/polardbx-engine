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

#include <google/protobuf/wire_format_lite.h>
#include <cassert>
#include <cstdint>
#include <string>

#include "my_dbug.h"

#include "encoding_protobuf.h"

namespace polarx_rpc {
namespace protocol {

/**
  This class is wraps protobuf payload with X Protocol header

  This class generates X Protocol headers for protobuf messages
  and for compressed messages.
  Additionally it supplies sub-field protobuf functionality,
  because similar mechanism was used for generation for protobuf
  fields and X headers.
*/
class PolarX_Protocol_encoder : public Protobuf_encoder {
 private:
  constexpr static uint32_t k_header_size = 8 + 4 + 1;
  uint64_t m_sid;

 public:
  explicit PolarX_Protocol_encoder(const uint64_t &sid, Encoding_buffer *buffer)
      : Protobuf_encoder(buffer), m_sid(sid) {
    ensure_buffer_size<1>();
  }

  inline const uint64_t &sid() const { return m_sid; }

  inline void reset_sid(const uint64_t &sid) { m_sid = sid; }

  struct Position {
    Page *m_page;
    uint8_t *m_position;

    uint8_t *get_position() const { return m_position; }

    uint32_t bytes_until_page(Page *current_page) const {
      uint32_t size = m_page->m_current_data - m_position;

      if (current_page == m_page) {
        return size;
      }

      Page *i = m_page->m_next_page;
      for (;;) {
        assert(nullptr != i);
        size += i->get_used_bytes();

        if (i == current_page) {
          assert(nullptr == i->m_next_page);
          break;
        }

        i = i->m_next_page;
      }

      return size;
    }
  };

  template <uint32_t delimiter_length>
  struct Field_delimiter : Position {};

  template <uint32_t id>
  void empty_xmessage() {
    ensure_buffer_size<k_header_size>();

    primitives::base::Fixint_length<8>::encode_value(m_page->m_current_data,
                                                     m_sid);
    primitives::base::Fixint_length<4>::encode<1>(m_page->m_current_data);
    primitives::base::Fixint_length<1>::encode<id>(m_page->m_current_data);
  }

  template <uint32_t id, uint32_t needed_buffer_size>
  Position begin_xmessage() {
    Position result;

    begin_xmessage<id, needed_buffer_size>(&result);

    return result;
  }

  template <uint32_t needed_buffer_size>
  Position begin_xmessage(const uint32_t id) {
    Position result;

    ensure_buffer_size<needed_buffer_size + k_header_size>();

    auto &ptr = m_page->m_current_data;
    result.m_page = m_page;
    result.m_position = ptr;

    primitives::base::Fixint_length<8>::encode_value(ptr, m_sid);
    ptr += 4;
    primitives::base::Fixint_length<1>::encode_value(ptr, id);
    return result;
  }

  template <uint32_t id, uint32_t needed_buffer_size>
  void begin_xmessage(Position *position) {
    ensure_buffer_size<needed_buffer_size + k_header_size>();

    auto &ptr = m_page->m_current_data;
    position->m_page = m_page;
    position->m_position = ptr;

    primitives::base::Fixint_length<8>::encode_value(ptr, m_sid);
    ptr += 4;
    primitives::base::Fixint_length<1>::encode<id>(ptr);
  }

  void end_xmessage(const Position &position) {
    auto ptr = position.get_position() + 8;
    primitives::base::Fixint_length<4>::encode_value(
        ptr, position.bytes_until_page(m_page) - 8 - 4);
  }

  void abort_xmessage(const Position &position) {
    auto page = position.m_page->m_next_page;

    m_buffer->remove_page_list(page);  /// may null but can deal it

    /// restore encoder ptr and buffer ptr
    m_page = position.m_page;
    m_page->m_current_data = position.m_position;
    m_page->m_next_page = nullptr;  /// Important: unlink to prevent double free
    m_buffer->m_current = m_page;
  }

  template <uint32_t id, uint32_t delimiter_length = 1>
  Field_delimiter<delimiter_length> begin_delimited_field() {
    Field_delimiter<delimiter_length> result;

    begin_delimited_field<id>(&result);

    return result;
  }

  template <uint32_t id, uint32_t delimiter_length = 1>
  void begin_delimited_field(Field_delimiter<delimiter_length> *position) {
    encode_field_delimited_header<id>();
    position->m_position = m_page->m_current_data;
    position->m_page = m_page;

    m_page->m_current_data += delimiter_length;
  }

  template <uint32_t delimiter_length>
  void end_delimited_field(const Field_delimiter<delimiter_length> &position) {
    auto ptr = position.get_position();
    primitives::base::Varint_length<delimiter_length>::encode(
        ptr, position.bytes_until_page(m_page) - delimiter_length);
  }
};

}  // namespace protocol
}  // namespace polarx_rpc
