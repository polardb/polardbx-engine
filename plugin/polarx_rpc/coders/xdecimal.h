//
// Created by zzy on 2022/7/29.
//

#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

namespace polarx_rpc {

class Decimal {
 public:
  Decimal() = default;

  explicit Decimal(const std::string &s) {
    std::size_t scale = 0;
    std::size_t dot_pos = s.find('.');
    bool dot_skipped = false;
    if (dot_pos != std::string::npos) {
      scale = s.length() - dot_pos - 1;
    }
    m_buffer.push_back(static_cast<char>(scale));

    std::string::const_iterator c = s.begin();
    if (c != s.end()) {
      int sign = (*c == '-') ? 0xd : (*c == '+' ? 0xc : 0);
      if (sign == 0)
        sign = 0xc;
      else
        ++c;

      while (c != s.end()) {
        int c1 = *(c++);
        if (c1 == '.') {
          if (dot_skipped) {
            /*more than one dot*/
            m_buffer = "";
            return;
          }
          dot_skipped = true;
          continue;
        }
        if (c1 < '0' || c1 > '9') {
          m_buffer = "";
          return;
        }
        if (c == s.end()) {
          m_buffer.push_back((c1 - '0') << 4 | sign);
          sign = 0;
          break;
        }
        int c2 = *(c++);
        if (c2 == '.') {
          if (dot_skipped) {
            /*more than one dot*/
            m_buffer = "";
            return;
          }
          dot_skipped = true;
          if (c == s.end()) {
            m_buffer.push_back((c1 - '0') << 4 | sign);
            sign = 0;
            break;
          } else {
            c2 = *(c++);
          }
        }
        if (c2 < '0' || c2 > '9') {
          m_buffer = "";
          return;
        }

        m_buffer.push_back((c1 - '0') << 4 | (c2 - '0'));
      }
      if (m_buffer.length() <= 1) {
        m_buffer = "";
        return;
      }
      if (sign) m_buffer.push_back(sign << 4);
    }
  }

  std::string to_string() const {
    std::string result;
    str(&result);

    return result;
  }

  bool str(std::string *value_str) const {
    if (m_buffer.length() < 1) return false;
    size_t scale = m_buffer[0];

    for (std::string::const_iterator d = m_buffer.begin() + 1;
         d != m_buffer.end(); ++d) {
      uint32_t n1 = ((uint32_t)*d & 0xf0) >> 4;
      uint32_t n2 = (uint32_t)*d & 0xf;

      if (n1 > 9) {
        if (n1 == 0xb || n1 == 0xd) (*value_str) = "-" + (*value_str);
        break;
      } else {
        (*value_str).push_back('0' + n1);
      }
      if (n2 > 9) {
        if (n2 == 0xb || n2 == 0xd) (*value_str) = "-" + (*value_str);
        break;
      } else {
        (*value_str).push_back('0' + n2);
      }
    }

    if (scale > (*value_str).length()) return false;

    if (scale > 0) {
      (*value_str).insert((*value_str).length() - scale, 1, '.');
    }

    return true;
  }

  std::string to_bytes() const { return m_buffer; }

  bool is_valid() const { return 0 != m_buffer.size(); }

  static Decimal from_str(const std::string &s) { return Decimal(s); }

  explicit operator std::string() const {
    std::string result;
    auto error = str(&result);

    if (error) return "";

    return result;
  }

  static Decimal from_bytes(const std::string &buffer) {
    Decimal dec;
    dec.m_buffer = buffer;
    return dec;
  }

 private:
  /* first byte stores the scale (number of digits after '.') */
  /* then all digits in BCD */
  std::string m_buffer;
};
}  // namespace polarx_rpc
