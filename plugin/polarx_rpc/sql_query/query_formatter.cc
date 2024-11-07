/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

#include <algorithm>

#include "my_sys.h"  // escape_string_for_mysql

#include "../utility/error.h"

#include "query_formatter.h"

namespace polarx_rpc {

enum Block_enum {
  Block_none,
  Block_string_quoted,
  Block_string_double_quoted,
  Block_identifier,
  Block_comment,
  Block_line_comment
};

class Sql_search_tags {
 public:
  Sql_search_tags()
      : m_state(Block_none),
        m_matching_chars_comment(0),
        m_matching_chars_line_comment1(0),
        m_matching_chars_line_comment2(0),
        m_escape_chars(0) {}

  bool should_ignore_block(const char character, const Block_enum try_block,
                           const char character_begin, const char character_end,
                           bool escape = false) {
    if (m_state != try_block && m_state != Block_none) return false;

    if (m_state == Block_none) {
      if (character_begin == character) {
        m_escape_chars = 0;
        m_state = try_block;

        return true;
      }
    } else {
      if (escape) {
        if (0 != m_escape_chars) {
          --m_escape_chars;
          return true;
        } else if ('\\' == character) {
          ++m_escape_chars;
          return true;
        }
      }

      if (character_end == character) {
        m_state = Block_none;
      }

      return true;
    }

    return false;
  }

  bool if_matching_switch_state(const char character,
                                const Block_enum try_block,
                                uint8_t &matching_chars, const char *match,
                                const std::size_t match_length) {
    bool repeat = true;

    while (repeat) {
      if (character == match[matching_chars]) {
        ++matching_chars;
        break;
      }

      repeat = matching_chars != 0;

      matching_chars = 0;
    }

    if (matching_chars == match_length - 1) {
      m_state = try_block;
      matching_chars = 0;

      return true;
    }

    return false;
  }

  template <std::size_t block_begin_length, std::size_t block_end_length>
  bool should_ignore_block_multichar(
      const char character, const Block_enum try_block_state,
      uint8_t &matching_chars, const char (&block_begin)[block_begin_length],
      const char (&block_end)[block_end_length]) {
    if (m_state != try_block_state && m_state != Block_none) return false;

    if (m_state == Block_none) {
      return if_matching_switch_state(character, try_block_state,
                                      matching_chars, block_begin,
                                      block_begin_length);
    } else {
      if_matching_switch_state(character, Block_none, matching_chars, block_end,
                               block_end_length);

      return true;
    }
  }

  bool should_be_ignored(const char character) {
    const bool escape_sequence = true;

    if (should_ignore_block(character, Block_string_quoted, '\'', '\'',
                            escape_sequence))
      return true;

    if (should_ignore_block(character, Block_string_double_quoted, '"', '"',
                            escape_sequence))
      return true;

    if (should_ignore_block(character, Block_identifier, '`', '`')) return true;

    if (should_ignore_block_multichar(character, Block_comment,
                                      m_matching_chars_comment, "/*", "*/"))
      return true;

    if (should_ignore_block_multichar(character, Block_line_comment,
                                      m_matching_chars_line_comment1, "#",
                                      "\n"))
      return true;

    if (should_ignore_block_multichar(character, Block_line_comment,
                                      m_matching_chars_line_comment2, "-- ",
                                      "\n"))
      return true;

    return false;
  }

  bool operator()(const char query_character) {
    if (should_be_ignored(query_character)) return false;

    return query_character == '?';
  }

 private:
  Block_enum m_state;
  uint8_t m_matching_chars_comment;
  uint8_t m_matching_chars_line_comment1;
  uint8_t m_matching_chars_line_comment2;
  uint8_t m_escape_chars;
};

Query_formatter::Query_formatter(std::string &query)
    : m_query(query),
      m_charset(&my_charset_utf8mb4_general_ci),
      m_last_tag_position(0) {}

Query_formatter &Query_formatter::operator%(const char *value) {
  validate_next_tag();

  put_value_and_escape(value, strlen(value));

  return *this;
}

Query_formatter &Query_formatter::operator%(
    const No_escape<const char *> &value) {
  validate_next_tag();

  put_value(value.m_value, strlen(value.m_value));

  return *this;
}

Query_formatter &Query_formatter::operator%(const std::string &value) {
  validate_next_tag();

  put_value_and_escape(value.c_str(), value.length());

  return *this;
}

Query_formatter &Query_formatter::operator%(
    const No_escape<std::string> &value) {
  validate_next_tag();

  put_value(value.m_value.c_str(), value.m_value.length());

  return *this;
}

void Query_formatter::validate_next_tag() {
  auto i = std::find_if(m_query.begin() + m_last_tag_position, m_query.end(),
                        Sql_search_tags());

  if (m_query.end() == i)
    throw err_t(ER_POLARX_RPC_ERROR_MSG, "Too many arguments");

  m_last_tag_position = std::distance(m_query.begin(), i);
}

void Query_formatter::put_value_and_escape(const char *value,
                                           std::size_t length) {
  const std::size_t length_maximum = 2 * length + 1 + 2;
  std::string value_escaped(length_maximum, '\0');

  std::size_t length_escaped = escape_string_for_mysql(
      m_charset, &value_escaped[1], length_maximum, value, length);
  value_escaped[0] = value_escaped[1 + length_escaped] = '\'';

  value_escaped.resize(length_escaped + 2);

  put_value(value_escaped.c_str(), value_escaped.length());
}

void Query_formatter::put_value(const char *value, std::size_t length) {
  // Concat previous first.
  auto len = m_last_tag_position - m_prev_start_position;
  reserve_buf(len).append(m_query.data() + m_prev_start_position, len);

  // Concat value.
  reserve_buf(length).append(value, length);
  m_prev_start_position = ++m_last_tag_position;  // Jump to next.
}

std::size_t Query_formatter::count_tags() const {
  return std::count_if(m_query.begin(), m_query.end(), Sql_search_tags());
}

/** Galaxy X-protocol */
void Query_formatter::put_ident_and_escape(const char *value,
                                           std::size_t length) {
  const std::size_t length_maximum = 2 * length + 1 + 2;
  std::string value_escaped(length_maximum, '\0');

  std::size_t length_escaped = escape_string_for_mysql(
      m_charset, &value_escaped[1], length_maximum, value, length);
  value_escaped[0] = value_escaped[1 + length_escaped] = '`';

  value_escaped.resize(length_escaped + 2);

  put_value(value_escaped.c_str(), value_escaped.length());
}

Query_formatter &Query_formatter::operator%(const Identifier &identifier) {
  validate_next_tag();

  put_ident_and_escape(identifier.identifier_.c_str(),
                       identifier.identifier_.length());

  return *this;
}

Query_formatter &Query_formatter::operator%(const RawBinary &raw_binary) {
  validate_next_tag();

  auto hex(raw_binary.to_hex_string());
  put_value(hex.c_str(), hex.length());

  return *this;
}

Query_formatter &Query_formatter::operator%(const RawString &raw_string) {
  validate_next_tag();

  put_value(raw_string.get_value().c_str(), raw_string.get_value().length());

  return *this;
}

}  // namespace polarx_rpc
