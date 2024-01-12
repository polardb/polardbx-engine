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
