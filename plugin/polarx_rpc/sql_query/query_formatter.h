/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "m_ctype.h"

#include "../helper/to_string.h"
#include "identifier.h"
#include "raw_binary.h"
#include "raw_string.h"

namespace polarx_rpc {

class Query_formatter {
 public:
  Query_formatter(std::string &query);

  inline void set_charset(const CHARSET_INFO *charset) {
    m_charset = charset;
    assert(m_charset != nullptr);
  }

  template <typename Value_type>
  class No_escape {
   public:
    explicit No_escape(const Value_type &value) : m_value(value) {}

    const Value_type &m_value;
  };

  Query_formatter &operator%(const char *value);
  Query_formatter &operator%(const No_escape<const char *> &value);
  Query_formatter &operator%(const std::string &value);
  Query_formatter &operator%(const No_escape<std::string> &value);
  Query_formatter &operator%(const Identifier &identifier);
  Query_formatter &operator%(const RawBinary &raw_binary);
  Query_formatter &operator%(const RawString &raw_string);

  template <typename Value_type>
  Query_formatter &operator%(const Value_type &value) {
    return put(value);
  }

  std::size_t count_tags() const;

 private:
  template <typename Value_type>
  Query_formatter &put(const Value_type &value) {
    validate_next_tag();
    std::string string_value = to_string(value);
    put_value(string_value.c_str(), string_value.length());

    return *this;
  }

  Query_formatter &put(const bool value) {
    validate_next_tag();
    std::string string_value = value ? "true" : "false";
    put_value(string_value.c_str(), string_value.length());

    return *this;
  }

  template <typename Value_type>
  Query_formatter &put_fp(const Value_type &value) {
    std::stringstream stream;
    validate_next_tag();
    stream << std::setprecision(std::numeric_limits<Value_type>::max_digits10)
           << value;
    std::string string_value = stream.str();
    put_value(string_value.c_str(), string_value.length());

    return *this;
  }

  void put_value(const char *value, std::size_t length);
  void put_value_and_escape(const char *value, std::size_t length);
  void validate_next_tag();

  void put_ident_and_escape(const char *value, std::size_t length);

  std::string &m_query;
  const CHARSET_INFO *m_charset;  /// target sql charset
  std::size_t m_last_tag_position;
  std::size_t m_prev_start_position{0};

  /** Fast query string builder buffer. */
  std::vector<std::unique_ptr<std::string>> m_bufs;
  static constexpr size_t BLOCK_BUF_SIZE = 0x10000;  // 64KB

  inline std::string &reserve_buf(size_t size) {
    if (m_bufs.empty()) {
      std::unique_ptr<std::string> block(new std::string);
      block->reserve(size > BLOCK_BUF_SIZE ? size : BLOCK_BUF_SIZE);
      m_bufs.emplace_back(std::move(block));
      return *m_bufs.back();
    }
    if (size <= BLOCK_BUF_SIZE) {
      // Allocate new if now not enough.
      auto &now = *m_bufs.back();
      if (now.size() + size <= BLOCK_BUF_SIZE) return now;
      // Allocate new one.
      std::unique_ptr<std::string> block(new std::string);
      block->reserve(BLOCK_BUF_SIZE);
      m_bufs.emplace_back(std::move(block));
      return *m_bufs.back();
    }
    // Too big for single block. Just allocate big block.
    std::unique_ptr<std::string> block(new std::string);
    block->reserve(size);
    m_bufs.emplace_back(std::move(block));
    return *m_bufs.back();
  }

 public:
  inline void finalize() {
    // Concat last one.
    if (m_prev_start_position < m_query.size())
      reserve_buf(0).append(m_query.data() + m_prev_start_position,
                            m_query.size() - m_prev_start_position);
    // Form full stmt.
    size_t sz = 0;
    for (const auto &block : m_bufs) sz += block->size();
    m_query.resize(sz);
    auto ptr = const_cast<char *>(m_query.data());
    for (const auto &block : m_bufs) {
      ::memcpy(ptr, block->data(), block->size());
      ptr += block->size();
    }
    m_bufs.clear();
  }
};

template <>
inline Query_formatter &Query_formatter::operator%
    <double>(const double &value) {
  return put_fp(value);
}

template <>
inline Query_formatter &Query_formatter::operator% <float>(const float &value) {
  return put_fp(value);
}

template <>
inline Query_formatter &Query_formatter::put<bool>(const bool &value) {
  validate_next_tag();
  const std::string string_value = value ? "true" : "false";
  put_value(string_value.c_str(), string_value.length());
  return *this;
}

}  // namespace polarx_rpc
