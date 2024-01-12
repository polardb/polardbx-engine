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
#include <cstring>
#include <memory>
#include <mutex>
#include <string>

#include "m_ctype.h"

#include "query_formatter.h"

namespace polarx_rpc {

class Query_string_builder {
 public:
  Query_string_builder(size_t reserve = 256);

  inline void set_charset(const CHARSET_INFO *charset) {
    m_charset = charset;
    assert(m_charset != nullptr);
    format().set_charset(charset);
  }

  Query_string_builder &bquote() {
    m_str.push_back('\'');
    m_in_quoted = true;
    return *this;
  }

  Query_string_builder &equote() {
    m_str.push_back('\'');
    m_in_quoted = false;
    return *this;
  }

  Query_string_builder &bident() {
    m_str.push_back('`');
    m_in_identifier = true;
    return *this;
  }

  Query_string_builder &eident() {
    m_str.push_back('`');
    m_in_identifier = false;
    return *this;
  }

  Query_string_builder &quote_identifier_if_needed(const char *s,
                                                   size_t length);
  Query_string_builder &quote_identifier(const char *s, size_t length);
  Query_string_builder &quote_string(const char *s, size_t length);

  Query_string_builder &quote_identifier_if_needed(const std::string &s) {
    return quote_identifier_if_needed(s.data(), s.length());
  }

  Query_string_builder &quote_identifier(const std::string &s) {
    return quote_identifier(s.data(), s.length());
  }

  Query_string_builder &quote_string(const std::string &s) {
    return quote_string(s.data(), s.length());
  }

  Query_string_builder &escape_identifier(const char *s, size_t length);
  Query_string_builder &escape_string(const char *s, size_t length);

  Query_string_builder &dot() { return put(".", 1); }

  Query_string_builder &put(const int64_t i) { return put(to_string(i)); }
  Query_string_builder &put(const uint64_t u) { return put(to_string(u)); }
  Query_string_builder &put(const int32_t i) { return put(to_string(i)); }
  Query_string_builder &put(const uint32_t u) { return put(to_string(u)); }
  Query_string_builder &put(const float f) { return put(to_string(f)); }
  Query_string_builder &put(const double d) { return put(to_string(d)); }

  Query_string_builder &put(const char *s, size_t length);

  // Note: format_finalize should work with format.
  //       (Only Arg_inserter use this now)
  Query_formatter &format();

  inline void format_finalize() {
    if (m_formatter) {
      m_formatter->finalize();
      m_formatter.reset();  // Free it.
    }
  }

  Query_string_builder &put(const char *s) { return put(s, strlen(s)); }

  Query_string_builder &put(const std::string &s) {
    return put(s.data(), s.length());
  }

  template <typename I>
  Query_string_builder &put_list(I begin, I end, const std::string &sep = ",") {
    if (std::distance(begin, end) == 0) return *this;
    put(*begin);
    for (++begin; begin != end; ++begin) {
      put(sep);
      put(*begin);
    }
    return *this;
  }

  template <typename I, typename P>
  Query_string_builder &put_list(I begin, I end, P push,
                                 const std::string &sep = ",") {
    if (std::distance(begin, end) == 0) return *this;
    push(*begin, this);
    for (++begin; begin != end; ++begin) {
      put(sep);
      push(*begin, this);
    }
    return *this;
  }

  void clear() { m_str.clear(); }

  void reserve(size_t bytes) { m_str.reserve(bytes); }

  const std::string &get() const { return m_str; }

 private:
  std::string m_str;
  bool m_in_quoted;
  bool m_in_identifier;
  const CHARSET_INFO *m_charset;

  /** Try reuse the formatter. */
  std::unique_ptr<Query_formatter> m_formatter;
};

}  // namespace polarx_rpc
