/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef PLUGIN_X_PROTOCOL_ENCODERS_ENCODING_XPROTOCOL_H_
#define PLUGIN_X_PROTOCOL_ENCODERS_ENCODING_XPROTOCOL_H_

#include <google/protobuf/wire_format_lite.h>
#include <cassert>
#include <cstdint>
#include <string>

#include "my_dbug.h"

#include "plugin/x/ngs/include/ngs/galaxy_protocol.h"
#include "plugin/x/ngs/include/ngs/galaxy_session.h"
#include "plugin/x/src/xpl_log.h"

#include "plugin/x/protocol/encoders/encoding_protobuf.h"

namespace protocol {

#define GALAXY_XMESSAGE_DEBUG 0

namespace tags {

enum Raw_payload_ids {
  COMPRESSION_SINGLE = 19,
  COMPRESSION_MULTIPLE = 20,
  COMPRESSION_GROUP = 21,
};

}  // namespace tags

enum class Compression_type { k_single, k_multiple, k_group };

class Compression_buffer_interface {
 public:
  virtual ~Compression_buffer_interface() = default;

  virtual void reset_counters() = 0;
  virtual bool process(Encoding_buffer *output_buffer,
                       const Encoding_buffer *input_buffer) = 0;

  virtual void get_processed_data(uint32_t *out_uncompressed,
                                  uint32_t *out_compressed) = 0;
};

/**
  This class is wraps protobuf payload with X Protocol header

  This class generates X Protocol headers for protobuf messages
  and for compressed messages.
  Additionally it supplies sub-field protobuf functionality,
  because similar mechanism was used for generation for protobuf
  fields and X headers.
*/
class GProtocol_encoder : public Protobuf_encoder {
 private:
  /** Galaxy X-protocol */

  static gx::GHeader gheader;
  gx::GHeader *m_hdr = &gheader;

  constexpr static uint32_t k_xmessage_header_length = 5 + gx::GREQUEST_SIZE;

  enum class Header_configuration { k_full, k_size_only, k_none };

  Header_configuration m_header_configuration = Header_configuration::k_full;
  uint32_t m_header_size = header_size(m_header_configuration, m_hdr->ptype);

  void set_header_config(const Header_configuration config) {
    m_header_configuration = config;
    m_header_size = header_size(m_header_configuration, m_hdr->ptype);
  }

  static uint32_t header_size(const Header_configuration config,
                              gx::Protocol_type ptype) {
    switch (config) {
      case Header_configuration::k_full:
        return 5 + gx::header_size(ptype);
      case Header_configuration::k_none:
        return 0;
      case Header_configuration::k_size_only:
        return 4 + gx::header_size(ptype);
      default:
        assert(false && "Not allowed value");
        return 0;
    }
  }

 public:
  explicit GProtocol_encoder(Encoding_buffer *buffer, gx::GHeader *hdr)
      : Protobuf_encoder(buffer), m_hdr(hdr) {
    ensure_buffer_size<1>();
    set_header_config(m_header_configuration);
  }

  void rebuild_header() { set_header_config(m_header_configuration); }

#if GALAXY_XMESSAGE_DEBUG
  static uint32_t crc32(const void *data, size_t size,
                        uint32_t crc = 0xffffffff) {
    static const uint32_t table[] = {
        0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
        0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
        0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
        0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
        0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
        0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
        0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
        0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
        0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
        0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
        0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
        0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
        0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
        0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
        0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
        0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
        0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
        0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
        0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
        0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
        0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
        0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
        0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
        0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
        0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
        0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
        0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
        0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
        0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
        0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
        0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
        0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
        0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
        0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
        0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
        0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
        0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
        0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
        0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
        0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
        0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
        0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
        0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d,
    };

    crc ^= 0xffffffff;  // Restore last state.
    auto ptr = reinterpret_cast<const uint8_t *>(data);

    for (size_t i = 0; i < size; ++i)
      crc = table[(crc ^ ptr[i]) & 0xffu] ^ (crc >> 8u);
    crc = crc ^ 0xffffffff;

    return crc;
  }
#endif

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

#if GALAXY_XMESSAGE_DEBUG
    uint32 crc32_until_page(Page *current_page) const {
      // Init to 0.
      auto crc = crc32(m_position, m_page->m_current_data - m_position, 0);

      if (current_page == m_page) {
        return crc;
      }

      Page *i = m_page->m_next_page;
      for (;;) {
        assert(nullptr != i);
        crc = crc32(i->m_begin_data, i->get_used_bytes(), crc);

        if (i == current_page) {
          assert(nullptr == i->m_next_page);
          break;
        }

        i = i->m_next_page;
      }

      return crc;
    }
#endif
  };

  template <uint32_t delimiter_length>
  struct Field_delimiter : Position {};

  struct Compression_position : Position {
    Encoding_buffer *m_compressed_buffer;
    Compression_type m_compression_type;
    uint32_t m_compressed_data_size;
    uint32_t m_uncompressed_data_size;
    uint8_t m_msg_id;
  };

  template <uint32_t id>
  void empty_xmessage() {
    ensure_buffer_size<k_xmessage_header_length>();

    if (Header_configuration::k_full == m_header_configuration) {
      DBUG_LOG("debug", "empty_msg_full_header");
      /** Galaxy X-protocol */
      if (m_hdr->is_galaxy()) {
        primitives::base::Fixint_length<8>::encode_value(m_page->m_current_data,
                                                         m_hdr->sid);
        primitives::base::Fixint_length<1>::encode_value(m_page->m_current_data,
                                                         m_hdr->version);
      }
      primitives::base::Fixint_length<4>::encode<1>(m_page->m_current_data);
      primitives::base::Fixint_length<1>::encode<id>(m_page->m_current_data);
#if GALAXY_XMESSAGE_DEBUG
      auto start = m_page->m_current_data;
      LogPluginErrMsg(WARNING_LEVEL, ER_XPLUGIN_ERROR_MSG,
                      "GP: msg type:%u sid:%lu len:%u crc:%u", id, m_hdr->sid,
                      1, crc32(start, m_page->m_current_data - start, 0));
#endif
    } else if (Header_configuration::k_size_only == m_header_configuration) {
      DBUG_LOG("debug", "empty_msg_size_only");
      /** Galaxy X-protocol */
      if (m_hdr->is_galaxy()) {
        primitives::base::Fixint_length<8>::encode_value(m_page->m_current_data,
                                                         m_hdr->sid);
        primitives::base::Fixint_length<1>::encode_value(m_page->m_current_data,
                                                         m_hdr->version);
      }
      primitives::base::Fixint_length<4>::encode<0>(m_page->m_current_data);
    }
  }

  Compression_position begin_compression(const uint8_t msg_id,
                                         const Compression_type type,
                                         Encoding_buffer *to_compress) {
    Compression_position result;

    DBUG_ASSERT(!m_hdr->is_galaxy());

    switch (type) {
      case Compression_type::k_single:
        begin_xmessage<tags::Raw_payload_ids::COMPRESSION_SINGLE, 100>(&result);
        set_header_config(Header_configuration::k_none);
        m_buffer->m_current->m_current_data += 5;
        break;
      case Compression_type::k_multiple:
        begin_xmessage<tags::Raw_payload_ids::COMPRESSION_MULTIPLE, 100>(
            &result);
        set_header_config(Header_configuration::k_size_only);
        m_buffer->m_current->m_current_data += 5;
        break;
      case Compression_type::k_group:
        begin_xmessage<tags::Raw_payload_ids::COMPRESSION_GROUP, 100>(&result);
        set_header_config(Header_configuration::k_full);
        m_buffer->m_current->m_current_data += 4;
        break;
    }

    DBUG_ASSERT(to_compress->m_current == to_compress->m_front);
    DBUG_ASSERT(to_compress->m_current->m_begin_data ==
                to_compress->m_current->m_current_data);
    result.m_compressed_buffer = m_buffer;
    result.m_compression_type = type;
    result.m_msg_id = msg_id;
    // Reset buffer, and initialize the 'handy' data hold inside
    // 'Encoder_primitives'
    buffer_set(to_compress);

    return result;
  }

  bool end_compression(const Compression_position &position,
                       Compression_buffer_interface *compress) {
    Position before_compression{m_buffer->m_front,
                                m_buffer->m_front->m_begin_data};
    const auto before_compression_size =
        before_compression.bytes_until_page(m_page);

    if (!compress->process(position.m_compressed_buffer, m_buffer))
      return false;

    auto ptr = position.m_position;
    const auto message_size =
        position.bytes_until_page(position.m_compressed_buffer->m_current);

    switch (position.m_compression_type) {
      case Compression_type::k_single:
        primitives::base::Fixint_length<4>::encode_value(ptr, message_size - 4);
        primitives::base::Fixint_length<1>::encode_value(
            ptr, tags::Raw_payload_ids::COMPRESSION_SINGLE);
        primitives::base::Fixint_length<1>::encode_value(ptr,
                                                         position.m_msg_id);
        primitives::base::Fixint_length<4>::encode_value(
            ptr, before_compression_size);
        break;
      case Compression_type::k_multiple:
        primitives::base::Fixint_length<4>::encode_value(ptr, message_size - 4);
        primitives::base::Fixint_length<1>::encode_value(
            ptr, tags::Raw_payload_ids::COMPRESSION_MULTIPLE);
        primitives::base::Fixint_length<1>::encode_value(ptr,
                                                         position.m_msg_id);
        primitives::base::Fixint_length<4>::encode_value(
            ptr, before_compression_size);
        break;
      case Compression_type::k_group:
        primitives::base::Fixint_length<4>::encode_value(ptr, message_size - 4);
        primitives::base::Fixint_length<1>::encode_value(
            ptr, tags::Raw_payload_ids::COMPRESSION_GROUP);
        primitives::base::Fixint_length<4>::encode_value(
            ptr, before_compression_size);
        break;
    }
    // Lets discard data inside new/compression buffer
    // in case when 'compress' call didn't do that.
    m_buffer->reset();

    // and now we restore original buffer
    buffer_set(position.m_compressed_buffer);

    set_header_config(Header_configuration::k_full);

    return true;
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

    ensure_buffer_size<needed_buffer_size + k_xmessage_header_length>();

    auto xmsg_start = m_page->m_current_data;
    if (Header_configuration::k_full == m_header_configuration) {
      if (m_hdr->is_galaxy()) {
        auto xmsg_hdr = xmsg_start;
        primitives::base::Fixint_length<8>::encode_value(xmsg_hdr, m_hdr->sid);
        primitives::base::Fixint_length<1>::encode_value(xmsg_hdr,
                                                         m_hdr->version);
      }
      auto xmsg_type = xmsg_start + 4 + m_hdr->size();
      primitives::base::Fixint_length<1>::encode_value(xmsg_type, id);
    }
    result.m_page = m_page;
    result.m_position = xmsg_start;

    m_page->m_current_data += m_header_size;

    return result;
  }

  template <uint32_t id, uint32_t needed_buffer_size>
  void begin_xmessage(Position *position) {
    ensure_buffer_size<needed_buffer_size + k_xmessage_header_length>();

    auto xmsg_start = m_page->m_current_data;
    if (Header_configuration::k_full == m_header_configuration) {
      if (m_hdr->is_galaxy()) {
        auto xmsg_hdr = xmsg_start;
        primitives::base::Fixint_length<8>::encode_value(xmsg_hdr, m_hdr->sid);
        primitives::base::Fixint_length<1>::encode_value(xmsg_hdr,
                                                         m_hdr->version);
      }
      auto xmsg_type = xmsg_start + 4 + m_hdr->size();
      primitives::base::Fixint_length<1>::encode<id>(xmsg_type);
    }
    position->m_page = m_page;
    position->m_position = xmsg_start;

    m_page->m_current_data += m_header_size;
  }

  void end_xmessage(const Position &position) {
    auto ptr = position.get_position() + m_hdr->size();

    if (Header_configuration::k_none != m_header_configuration) {
      primitives::base::Fixint_length<4>::encode_value(
          ptr, position.bytes_until_page(m_page) - 4 - m_hdr->size());
#if GALAXY_XMESSAGE_DEBUG
      LogPluginErrMsg(WARNING_LEVEL, ER_XPLUGIN_ERROR_MSG,
                      "GP: msg type:%u sid:%lu len:%u crc:%u",
                      *(uint8_t *)(position.get_position() + 13),
                      *(uint64_t *)position.get_position(),
                      *(uint32_t *)(position.get_position() + 9),
                      position.crc32_until_page(m_page));
#endif
    }
  }

  void abort_xmessage(const Position &position) {
    auto page = position.m_page->m_next_page;

    m_buffer->remove_page_list(page);

    m_page = position.m_page;
    m_page->m_current_data = position.m_position;
  }

  void abort_compression(const Compression_position &position) {
    // Lets discard data inside new/compression buffer
    // in case when 'compress' call didn't do that.
    m_buffer->reset();

    // and now we restore original buffer
    buffer_set(position.m_compressed_buffer);

    set_header_config(Header_configuration::k_full);

    abort_xmessage(position);
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

#endif  // PLUGIN_X_PROTOCOL_ENCODERS_ENCODING_XPROTOCOL_H_
