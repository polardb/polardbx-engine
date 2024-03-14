//
// Created by zzy on 2023/1/3.
//

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include "../../global_defines.h"
#ifdef MYSQL8
#include "my_time.h"
#endif

#include "google/protobuf/io/zero_copy_stream_impl_lite.h"

#include "../../common_define.h"

#include "../xdecimal.h"
#include "encoding_polarx_messages.h"

namespace polarx_rpc {
namespace protocol {

const int k_block_size = 1024 * 4;
const int k_bitmap_size = k_block_size / 8 + 1;

static const int kMaxVarintBytes = 10;
static const int kMaxVarint32Bytes = 5;

static const int kReservedVariableSizeData = 32;

struct Block {
  NO_COPY_MOVE(Block)

 public:
  google::protobuf::io::ArrayOutputStream *array;
  google::protobuf::io::CodedOutputStream *coder;
  uint8_t *buf;
  size_t len;
  std::string extra;
  bool fixed_size;

  Block(uint8_t *buf, size_t len) : buf(buf), len(len), fixed_size(true) {
    array =
        new google::protobuf::io::ArrayOutputStream(buf, static_cast<int>(len));
    coder = new google::protobuf::io::CodedOutputStream(array);
  }

  ~Block() {
    assert(coder != nullptr);
    delete coder;
    assert(array != nullptr);
    delete array;
  }

  inline void reset() {
    extra.clear();
    assert(coder != nullptr);
    delete coder;
    assert(array != nullptr);
    delete array;
    array =
        new google::protobuf::io::ArrayOutputStream(buf, static_cast<int>(len));
    coder = new google::protobuf::io::CodedOutputStream(array);
  }

  inline int64_t written() const { return coder->ByteCount(); }
};

struct Chunk {
  NO_COPY_MOVE(Chunk)

 public:
  size_t field_num;
  size_t row_count;
  bool is_full;
  uint8_t *buffer;
  uint8_t *null_bitmap;
  Block **blocks;

  Chunk()
      : field_num(0),
        row_count(0),
        is_full(false),
        buffer(nullptr),
        null_bitmap(nullptr),
        blocks(nullptr) {}

  ~Chunk() { clear(); }

  inline void init(size_t fields) {
    assert(fields > 0);
    field_num = fields;
    row_count = 0;
    is_full = false;
    assert(nullptr == buffer && nullptr == null_bitmap && nullptr == blocks);
    buffer = new uint8_t[field_num * k_block_size];
    null_bitmap = new uint8_t[field_num * k_bitmap_size];
    ::memset(null_bitmap, 0, field_num * k_bitmap_size);

    blocks = new Block *[field_num];
    for (size_t i = 0; i < field_num; ++i)
      blocks[i] = new Block(buffer + i * k_block_size, k_block_size);
  }

  inline void clear() {
    if (blocks != nullptr) {
      for (size_t i = 0; i < field_num; ++i) {
        if (blocks[i] != nullptr) delete blocks[i];
        blocks[i] = nullptr;
      }
    }
    delete[] blocks;
    blocks = nullptr;
    delete[] buffer;
    buffer = nullptr;
    delete[] null_bitmap;
    null_bitmap = nullptr;
    field_num = 0;
    row_count = 0;
    is_full = false;
  }
};

template <typename Encoder_type>
class XChunk_encoder_base {
 private:
  using Position = typename Encoder_type::Position;
  Encoder_type *m_encoder = nullptr;
  Position m_row_begin;

  // result will be saved in buffer of Chunk
  Chunk m_chunk;

  // which field will be stored in the buffer for the row being currently
  // processed
  size_t m_num_fields;
  // true if currently the chunk is being built
  bool m_chunk_in_use;
  bool m_chunk_processing;
  bool m_row_processing;

 public:
  explicit XChunk_encoder_base(Encoder_type *encoder)
      : m_encoder(encoder), m_chunk_in_use(false), m_chunk_processing(false) {}

  ~XChunk_encoder_base() { abort_chunk(); }

  inline bool chunk_empty() const { return 0 == m_chunk.row_count; }

  inline void chunk_init(size_t field_num) {
    assert(!m_chunk_in_use);
    m_chunk.init(field_num);
    m_chunk_in_use = true;
  }

  inline void abort_chunk() {
    if (m_chunk_in_use) {
      m_chunk.clear();
      m_chunk_processing = false;
      m_chunk_in_use = false;
    }
  }

  inline void reset_block(size_t i) const {
    ::memset(m_chunk.null_bitmap + i * k_bitmap_size, 0, k_bitmap_size);
    m_chunk.blocks[i]->reset();
  }

  inline void start_chunk(size_t start, size_t end) {
    if (m_chunk_processing) {
      m_encoder->abort_xmessage(m_row_begin);
      m_chunk_processing = false;
    }

    m_encoder->template begin_xmessage<tags::Chunk::server_id, 100>(
        &m_row_begin);

    m_chunk_processing = true;
    add_to_chunk(start, end);
  }

  inline void end_chunk() {
    if (m_chunk_processing) {
      m_encoder->end_xmessage(m_row_begin);
      m_chunk_processing = false;
    }
  }

  void add_to_block(size_t field_num, bool has_bitmap) {
    /*  write null_bitmap
     *  type bytes -> WIRETYPE_LENGTH_DELIMITED
     *  WIRETYPE_LENGTH_DELIMITED -> length | value
     */
    if (has_bitmap)
      m_encoder->template encode_field_delimited_raw<tags::Column::null_bitmap>(
          m_chunk.null_bitmap + field_num * k_bitmap_size,
          (m_chunk.row_count + 7) / 8);

    auto written = m_chunk.blocks[field_num]->written();
    assert(written <= k_block_size);
    size_t length = written + m_chunk.blocks[field_num]->extra.size();
    size_t encoded_length =
        1 +  /// tag
        google::protobuf::io::CodedOutputStream::VarintSize32(
            static_cast<google::protobuf::uint32>(length)) +  /// len
        length;                                               /// data
    /// tag + len + tag + len
    m_encoder
        ->template ensure_buffer_size<kMaxVarintBytes + kMaxVarint32Bytes +
                                      kMaxVarintBytes + kMaxVarint32Bytes>();
    if (m_chunk.blocks[field_num]->fixed_size)
      m_encoder->template encode_field_delimited_header<
          tags::Column::fixed_size_column>();
    else
      m_encoder->template encode_field_delimited_header<
          tags::Column::variable_size_column>();
    m_encoder->encode_var_uint32(encoded_length);
    m_encoder
        ->template encode_field_delimited_header<tags::ColumnData::value>();
    m_encoder->encode_var_uint32(length);
#if GOOGLE_PROTOBUF_VERSION / 1000000 >= 3
    // Caution: trim before use the underlying buffer
    // (only in new version of protobuf)
    m_chunk.blocks[field_num]->coder->Trim();
#endif
    m_encoder->encode_raw(m_chunk.blocks[field_num]->buf, written);
    if (!m_chunk.blocks[field_num]->extra.empty())
      m_encoder->encode_raw(reinterpret_cast<const uint8_t *>(
                                m_chunk.blocks[field_num]->extra.data()),
                            m_chunk.blocks[field_num]->extra.size());
  }

  void add_to_chunk(size_t start, size_t end) {
    // write row_count
    assert(m_chunk.row_count > 0);
    m_encoder->template encode_field_var_uint32<tags::Chunk::row_count>(
        m_chunk.row_count);

    for (size_t i = start; i < end; ++i) {
      size_t length =
          m_chunk.blocks[i]->written() + m_chunk.blocks[i]->extra.size();
      size_t encoded_length =
          1 +  /// tag
          google::protobuf::io::CodedOutputStream::VarintSize32(
              static_cast<google::protobuf::uint32>(length)) +
          length;
      auto max_bitmap_len = (m_chunk.row_count + 7) / 8;
      assert(max_bitmap_len <= k_bitmap_size);
      size_t bitmap_length = 0;
      /// check all zero(no null in this block)
      while (bitmap_length < max_bitmap_len) {
        if (*(uint8_t *)(m_chunk.null_bitmap + i * k_bitmap_size +
                         bitmap_length) > 0)
          break;
        bitmap_length++;
      }
      size_t column_encoded_length =
          (bitmap_length >= max_bitmap_len)
              ? (1 +
                 google::protobuf::io::CodedOutputStream::VarintSize32(
                     static_cast<google::protobuf::uint32>(encoded_length)) +
                 encoded_length)
              : (1 +
                 google::protobuf::io::CodedOutputStream::VarintSize32(
                     static_cast<google::protobuf::uint32>(max_bitmap_len)) +
                 max_bitmap_len + 1 +
                 google::protobuf::io::CodedOutputStream::VarintSize32(
                     static_cast<google::protobuf::uint32>(encoded_length)) +
                 encoded_length);

      m_encoder
          ->template ensure_buffer_size<kMaxVarintBytes + kMaxVarint32Bytes>();
      m_encoder->template encode_field_delimited_header<tags::Chunk::columns>();
      m_encoder->encode_var_uint32(column_encoded_length);
      add_to_block(i, bitmap_length < max_bitmap_len);
      reset_block(i);
    }
  }

  void send_chunk() {
    size_t chunk_num;
    for (chunk_num = 0; chunk_num + 10 < m_num_fields; chunk_num += 10) {
      start_chunk(chunk_num, chunk_num + 10);
      end_chunk();
    }

    start_chunk(chunk_num, m_num_fields);
    end_chunk();
    m_chunk.row_count = 0;
    m_chunk.is_full = false;
  }

  void begin_row() {
    m_num_fields = 0;
    m_row_processing = true;
  }

  void end_row() {
    if (m_row_processing) {
      ++m_chunk.row_count;
      /// send it before null bitmap full
      if (m_chunk.row_count >= k_block_size) m_chunk.is_full = true;
      if (m_chunk.is_full) {
        send_chunk();
      }
      m_row_processing = false;
    }
  }

  void abort_row() {
    if (m_row_processing) {
      // clear all data in chunk because they may be incorrect
      for (auto i = 0; i < static_cast<int>(m_chunk.field_num); ++i)
        reset_block(i);

      m_row_processing = false;
    }
  }

  uint32_t get_num_fields() const { return m_num_fields; }

  void field_null() {
    /// caution: null bitmap overflow check!
    assert(m_chunk.row_count / 8 < k_bitmap_size);
    *(m_chunk.null_bitmap + m_num_fields * k_bitmap_size +
      m_chunk.row_count / 8) |= 1 << (7 - (m_chunk.row_count & 7));
    ++m_num_fields;
  }

  void field_signed_longlong(const longlong value) {
    auto &block = m_chunk.blocks[m_num_fields];
    assert(k_block_size - block->written() >= kMaxVarintBytes);
    auto encoded =
        google::protobuf::internal::WireFormatLite::ZigZagEncode64(value);
    block->coder->WriteVarint64(encoded);
    if (block->written() + kMaxVarintBytes > k_block_size)
      m_chunk.is_full = true;
    ++m_num_fields;
  }

  void field_unsigned_longlong(const ulonglong value) {
    auto &block = m_chunk.blocks[m_num_fields];
    assert(k_block_size - block->written() >= kMaxVarintBytes);
    block->coder->WriteVarint64(value);
    if (block->written() + kMaxVarintBytes > k_block_size)
      m_chunk.is_full = true;
    ++m_num_fields;
  }

  void field_bit(const char *const value, size_t length) {
    assert(length <= 8);

    uint64_t binary_value = 0;
    for (size_t i = 0; i < length; ++i) {
      binary_value +=
          ((static_cast<uint64_t>(value[i]) & 0xff) << ((length - i - 1) * 8));
    }

    auto &block = m_chunk.blocks[m_num_fields];
    assert(k_block_size - block->written() >= kMaxVarintBytes);
    block->coder->WriteVarint64(binary_value);
    if (block->written() + kMaxVarintBytes > k_block_size)
      m_chunk.is_full = true;
    ++m_num_fields;
  }

  void field_set(const char *const value, size_t length) {
    auto &block = m_chunk.blocks[m_num_fields];
    if (block->fixed_size) block->fixed_size = false;
    // special case: empty SET
    if (0 == length) {
      assert(k_block_size - block->written() >= 2);
      block->coder->WriteVarint32(1);
      block->coder->WriteVarint64(0);
      if (block->written() + 2 + kReservedVariableSizeData > k_block_size)
        m_chunk.is_full = true;
      ++m_num_fields;
      return;
    }

    // TODO can optimize this to prevent copy
    std::vector<std::string> set_vals;
    const char *comma, *p_value = value;
    unsigned int elem_len;
    do {
      comma = std::strchr(p_value, ',');
      if (comma != nullptr) {
        elem_len = static_cast<unsigned int>(comma - p_value);
        set_vals.emplace_back(p_value, elem_len);
        p_value = comma + 1;
      }
    } while (comma != nullptr);

    // still sth left to store
    if ((size_t)(p_value - value) < length) {
      elem_len = static_cast<unsigned int>(length - (p_value - value));
      set_vals.emplace_back(p_value, elem_len);
    }

    // calculate size needed for all lengths and values
    google::protobuf::uint32 size = 0;
    for (const auto &val : set_vals) {
      size +=
          google::protobuf::io::CodedOutputStream::VarintSize64(val.length());
      size += static_cast<google::protobuf::uint32>(val.length());
    }

    if (block->written() +
            google::protobuf::io::CodedOutputStream::VarintSize32(size) + size >
        k_block_size) {
      google::protobuf::io::StringOutputStream string_stream(&block->extra);
      google::protobuf::io::CodedOutputStream stream(&string_stream);
      m_chunk.is_full = true;
      // write total size to the buffer
      stream.WriteVarint32(size);

      // write all lengths and values to the buffer
      for (const auto &val : set_vals) {
        stream.WriteVarint64(val.length());
        stream.WriteString(val);
      }
    } else {
      // write total size to the buffer
      block->coder->WriteVarint32(size);
      // write all lengths and values to the buffer
      for (const auto &val : set_vals) {
        block->coder->WriteVarint64(val.length());
        block->coder->WriteString(val);
      }
      // make sure for enough space for empty set
      if (block->written() + 2 + kReservedVariableSizeData > k_block_size)
        m_chunk.is_full = true;
    }
    ++m_num_fields;
  }

  void field_string(const char *value, const size_t length) {
    auto &block = m_chunk.blocks[m_num_fields];
    if (block->fixed_size) block->fixed_size = false;

    char zero = '\0';
    if (block->written() + kMaxVarint32Bytes + length + 1 > k_block_size) {
      google::protobuf::io::StringOutputStream string_stream(&block->extra);
      google::protobuf::io::CodedOutputStream stream(&string_stream);
      m_chunk.is_full = true;
      stream.WriteVarint32(static_cast<google::protobuf::uint32>(
          length + 1));  // 1 byte for trailing '\0'
      stream.WriteRaw(value, static_cast<int>(length));
      stream.WriteRaw(&zero, 1);
    } else {
      block->coder->WriteVarint32(
          static_cast<google::protobuf::uint32>(length + 1));
      block->coder->WriteRaw(value, static_cast<int>(length));
      block->coder->WriteRaw(&zero, 1);
      if (block->written() + kMaxVarint32Bytes + kReservedVariableSizeData +
              1 >=
          k_block_size)
        m_chunk.is_full = true;
    }
    ++m_num_fields;
  }

  void field_datetime(const MYSQL_TIME *value) {
    auto &block = m_chunk.blocks[m_num_fields];
    assert(k_block_size - block->written() >= kMaxVarintBytes);
#ifdef MYSQL8
    block->coder->WriteVarint64(TIME_to_longlong_datetime_packed(*value));
#else
    block->coder->WriteVarint64(TIME_to_longlong_datetime_packed(value));
#endif
    if (block->written() + kMaxVarintBytes > k_block_size)
      m_chunk.is_full = true;
    ++m_num_fields;
  }

  void field_time(const MYSQL_TIME *value) {
    auto &block = m_chunk.blocks[m_num_fields];
    assert(k_block_size - block->written() >= kMaxVarintBytes);
#ifdef MYSQL8
    block->coder->WriteVarint64(TIME_to_longlong_time_packed(*value));
#else
    block->coder->WriteVarint64(TIME_to_longlong_time_packed(value));
#endif
    if (block->written() + kMaxVarintBytes > k_block_size)
      m_chunk.is_full = true;
    ++m_num_fields;
  }

  void field_date(const MYSQL_TIME *value) {
    auto &block = m_chunk.blocks[m_num_fields];
    assert(k_block_size - block->written() >= kMaxVarintBytes);
#ifdef MYSQL8
    block->coder->WriteVarint64(TIME_to_longlong_date_packed(*value));
#else
    block->coder->WriteVarint64(TIME_to_longlong_date_packed(value));
#endif
    if (block->written() + kMaxVarintBytes > k_block_size)
      m_chunk.is_full = true;
    ++m_num_fields;
  }

  void field_float(const float value) {
    auto &block = m_chunk.blocks[m_num_fields];
    assert(k_block_size - block->written() >= 4);
    block->coder->WriteLittleEndian32(
        google::protobuf::internal::WireFormatLite::EncodeFloat(value));
    if (block->written() + 4 > k_block_size) m_chunk.is_full = true;
    ++m_num_fields;
  }

  void field_double(const double value) {
    auto &block = m_chunk.blocks[m_num_fields];
    assert(k_block_size - block->written() >= 8);
    block->coder->WriteLittleEndian64(
        google::protobuf::internal::WireFormatLite::EncodeDouble(value));
    if (block->written() + 8 > k_block_size) m_chunk.is_full = true;
    ++m_num_fields;
  }

  void field_decimal(const char *value, const size_t length) {
    std::string dec_str(value, length);
    polarx_rpc::Decimal dec(dec_str);
    std::string dec_bytes = dec.to_bytes();

    auto &block = m_chunk.blocks[m_num_fields];
    if (block->fixed_size) block->fixed_size = false;

    if (block->written() + kMaxVarint32Bytes + dec_bytes.length() >
        k_block_size) {
      google::protobuf::io::StringOutputStream string_stream(&block->extra);
      google::protobuf::io::CodedOutputStream stream(&string_stream);
      m_chunk.is_full = true;
      stream.WriteVarint32(
          static_cast<google::protobuf::uint32>(dec_bytes.length()));
      stream.WriteString(dec_bytes);
    } else {
      block->coder->WriteVarint32(
          static_cast<google::protobuf::uint32>(dec_bytes.length()));
      block->coder->WriteString(dec_bytes);
      if (block->written() + kMaxVarint32Bytes + kReservedVariableSizeData >
          k_block_size)
        m_chunk.is_full = true;
    }
    ++m_num_fields;
  }

  void field_decimal(const decimal_t *value) {
    // TODO: inefficient, refactor to skip the string conversion
    std::string str_buf;
    int str_len = 200;
    str_buf.resize(str_len);
#ifdef MYSQL8PLUS
    decimal2string(value, &(str_buf)[0], &str_len);
#else
    decimal2string(value, &(str_buf)[0], &str_len, 0, 0, 0);
#endif
    str_buf.resize(str_len);

    polarx_rpc::Decimal dec(str_buf);
    std::string dec_bytes = dec.to_bytes();

    auto &block = m_chunk.blocks[m_num_fields];
    if (block->fixed_size) block->fixed_size = false;

    if (block->written() + kMaxVarint32Bytes + dec_bytes.length() >
        k_block_size) {
      google::protobuf::io::StringOutputStream string_stream(&block->extra);
      google::protobuf::io::CodedOutputStream stream(&string_stream);
      m_chunk.is_full = true;
      stream.WriteVarint32(
          static_cast<google::protobuf::uint32>(dec_bytes.length()));
      stream.WriteString(dec_bytes);
    } else {
      block->coder->WriteVarint32(
          static_cast<google::protobuf::uint32>(dec_bytes.length()));
      block->coder->WriteString(dec_bytes);
      if (block->written() + kMaxVarint32Bytes + kReservedVariableSizeData >
          k_block_size)
        m_chunk.is_full = true;
    }
    ++m_num_fields;
  }
};

class PolarX_Chunk_encoder
    : public XChunk_encoder_base<PolarX_Protocol_encoder> {
 public:
  using Base = XChunk_encoder_base<PolarX_Protocol_encoder>;
  using Base::Base;
};

}  // namespace protocol
}  // namespace polarx_rpc
