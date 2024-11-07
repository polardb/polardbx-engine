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


//
// Created by 0xCC on 2023/6/29.
//

#pragma once

#include <cstddef>
#include <memory>

#include "m_ctype.h"
#include "my_sys.h"

namespace polarx_rpc {

inline bool is_value_charset_valid(const CHARSET_INFO *resultset_cs,
                                   const CHARSET_INFO *value_cs) {
  return !resultset_cs || !value_cs ||
         my_charset_same(resultset_cs, value_cs) ||
         (resultset_cs == &my_charset_bin) || (value_cs == &my_charset_bin);
}

inline uint get_valid_charset_collation(const CHARSET_INFO *resultset_cs,
                                        const CHARSET_INFO *value_cs) {
  const CHARSET_INFO *cs =
      is_value_charset_valid(resultset_cs, value_cs) ? value_cs : resultset_cs;
  return cs ? cs->number : 0;
}

class CconvertIfNecessary {
 public:
  CconvertIfNecessary(const CHARSET_INFO *resultset_cs, const char *value,
                      const size_t value_length, const CHARSET_INFO *value_cs) {
    if (is_value_charset_valid(resultset_cs, value_cs)) {
      m_ptr = value;
      m_len = value_length;
      return;
    }
    size_t result_length =
        resultset_cs->mbmaxlen * value_length / value_cs->mbminlen + 1;
    m_buff.reset(new char[result_length]());
    uint errors = 0;
    result_length = my_convert(m_buff.get(), result_length, resultset_cs, value,
                               value_length, value_cs, &errors);
    if (errors) {
      m_ptr = value;
      m_len = value_length;
    } else {
      m_ptr = m_buff.get();
      m_len = result_length;
      m_buff[m_len] = 0;
    }
  }
  const char *get_ptr() const { return m_ptr; }
  size_t get_length() const { return m_len; }

 private:
  const char *m_ptr;
  size_t m_len;
  std::unique_ptr<char[]> m_buff;
};

}  // namespace polarx_rpc
