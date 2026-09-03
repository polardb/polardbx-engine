/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2023-10-29
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <cstring>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>
#include "bytes.h"
#include "logger.h"
#include "mysql_utils.h"

namespace encdb {
using std::ptrdiff_t;
/**
 * @class LEncInt
 *
 * @c LEncInt is a thin wrapper around a MySQL length encoded integer
 * that makes it simple to extract length encoded integers from packets.
 */
class LEncInt {
 public:
  /**
   * Constructor
   *
   * @param pData  Pointer to the beginning of an length encoded integer.
   */
  LEncInt(uint8_t *pData) { m_value = mxs_leint_value(pData); }

  /**
   * Constructor
   *
   * @param pData  Pointer to a pointer to the beginning of an length
   *               encoded integer. After the call, the pointer will be advanced
   *               to point at the byte following the length encoded integer.
   */
  LEncInt(uint8_t **ppData) {
    size_t nBytes = mxs_leint_bytes(*ppData);
    m_value = mxs_leint_value(*ppData);
    *ppData += nBytes;
  }

  /**
   * @return The value of the length encoded integer.
   */
  uint64_t value() const { return m_value; }

  /**
   * @return The value of the length encoded integer.
   */
  operator uint64_t() const { return value(); }

  /**
   * Write the integer to an @c std::ostream.
   *
   * @param out  The stream.
   *
   * @return The stream provided as argument.
   */
  std::ostream &print(std::ostream &out) const {
    out << m_value;
    return out;
  }

 private:
  uint64_t m_value;
};

/**
 * Stream the integer to an @c std::ostream.
 *
 * @param out  A stream.
 * @param i    A length encoded integer.
 *
 * @return The stream provided as argument.
 */
inline std::ostream &operator<<(std::ostream &out, const LEncInt &i) {
  return i.print(out);
}

/**
 * @class LEncString
 *
 * @c LEncString is a thin wrapper around a MySQL length encoded string
 * that makes it simpler to use length encoded strings in conjunction with
 * @c char* and @c std::string strings.
 */
class LEncString {
 public:
  /**
   * @class iterator
   *
   * A _random access iterator_ to a @c LEncString.
   */
  class iterator : public std::iterator<std::random_access_iterator_tag, char,
                                        std::ptrdiff_t, char *, char &> {
   public:
    iterator(char *pS) : m_pS(pS) {}

    iterator &operator++() {
      mxb_assert(m_pS);
      ++m_pS;
      return *this;
    }

    iterator operator++(int) {
      iterator rv(*this);
      ++(*this);
      return rv;
    }

    iterator operator+(ptrdiff_t n) {
      mxb_assert(m_pS);
      iterator rv = m_pS;
      rv += n;
      return rv;
    }

    iterator &operator+=(ptrdiff_t n) {
      mxb_assert(m_pS);
      m_pS += n;
      return *this;
    }

    iterator &operator-=(ptrdiff_t n) {
      mxb_assert(m_pS);
      m_pS -= n;
      return *this;
    }

    ptrdiff_t operator-(const iterator &rhs) const {
      mxb_assert(m_pS);
      mxb_assert(rhs.m_pS);
      return m_pS - rhs.m_pS;
    }

    bool operator==(const iterator &rhs) const { return m_pS == rhs.m_pS; }

    bool operator!=(const iterator &rhs) const { return !(*this == rhs); }

    bool operator<(const iterator &rhs) const { return m_pS < rhs.m_pS; }

    bool operator<=(const iterator &rhs) const { return m_pS < rhs.m_pS; }

    bool operator>(const iterator &rhs) const { return m_pS > rhs.m_pS; }

    bool operator>=(const iterator &rhs) const { return m_pS > rhs.m_pS; }

    reference operator*() {
      mxb_assert(m_pS);
      return *m_pS;
    }

    reference operator[](ptrdiff_t i) {
      mxb_assert(m_pS);
      return m_pS[i];
    }

   private:
    char *m_pS;
  };

  /**
   * Constructor
   *
   * @param pData  Pointer to the beginning of a length encoded string
   */
  LEncString(uint8_t *pData) {
    // NULL is sent as 0xfb. See
    // https://dev.mysql.com/doc/internals/en/com-query-response.html
    if (*pData != 0xfb) {
      m_pString = mxs_lestr_consume(&pData, &m_length);
    } else {
      m_pString = NULL;
      m_length = 0;
    }
  }

  /**
   * Constructor
   *
   * @param ppData  Pointer to a pointer to the beginning of a length
   *                encoded string. After the call, the pointer will point
   *                one past the end of the length encoded string.
   */
  LEncString(uint8_t **ppData) {
    // NULL is sent as 0xfb. See
    // https://dev.mysql.com/doc/internals/en/com-query-response.html
    if (**ppData != 0xfb) {
      m_pString = mxs_lestr_consume(ppData, &m_length);
    } else {
      m_pString = NULL;
      m_length = 0;
      ++(*ppData);
    }
  }

  /**
   * Returns an iterator to the beginning of the string.
   *
   * @return A random access iterator.
   */
  iterator begin() { return iterator(m_pString); }

  /**
   * Returns an iterator one past the end of the string.
   *
   * @return A random access iterator.
   */
  iterator end() { return iterator(m_pString + m_length); }

  /**
   * @return The length of the string.
   */
  size_t length() const { return m_length; }

  /**
   * @return True if the string is empty, false otherwise.
   */
  bool empty() const { return m_length == 0; }

  /**
   * Compare for equality.
   *
   * @param s  The string to compare with.
   *
   * @return True, if the strings are equal.
   */
  bool eq(const LEncString &s) const {
    return m_length == s.m_length
               ? (memcmp(m_pString, s.m_pString, m_length) == 0)
               : false;
  }

  /**
   * Compare for equality.
   *
   * @param s  The string to compare with.
   *
   * @return True, if the strings are equal.
   */
  bool eq(const char *zString) const {
    size_t length = strlen(zString);

    return m_length == length ? (memcmp(m_pString, zString, m_length) == 0)
                              : false;
  }

  /**
   * Compare for equality.
   *
   * @param s  The string to compare with.
   *
   * @return True, if the strings are equal.
   */
  bool eq(const std::string &s) const {
    return m_length == s.length() ? (memcmp(m_pString, s.data(), m_length) == 0)
                                  : false;
  }

  /**
   * Convert a @c LEncString to the equivalent @c std::string.
   *
   * @return An @c std::string
   */
  std::string to_string() const {
    if (m_pString) {
      return std::string(m_pString, m_length);
    } else {
      return std::string("NULL");
    }
  }

  /**
   * Print the string to a @c ostream.
   *
   * @param o  The @c ostream to print the string to.
   *
   * @return The stream provided as parameter.
   */
  std::ostream &print(std::ostream &o) const {
    o.write(m_pString, m_length);
    return o;
  }

  /**
   * Is NULL
   *
   * @return True, if the string represents a NULL value.
   */
  bool is_null() const { return m_pString == NULL; }

  char *get_data() { return m_pString; }

 private:
  char *m_pString; /*<! Pointer to beginning of string, NOT zero-terminated. */
  size_t m_length; /*<! Length of string. */
};

/**
 * Compare two strings for equality.
 *
 * @param lhs  A string.
 * @param rhs  Another string.
 *
 * @return True, if the strings are equal.
 */
inline bool operator==(const LEncString &lhs, const LEncString &rhs) {
  return lhs.eq(rhs);
}

/**
 * Compare two strings for equality.
 *
 * @param lhs  A string.
 * @param rhs  Another string.
 *
 * @return True, if the strings are equal.
 */
inline bool operator==(const std::string &lhs, const LEncString &rhs) {
  return rhs.eq(lhs);
}

/**
 * Compare two strings for equality.
 *
 * @param lhs  A string.
 * @param rhs  Another string.
 *
 * @return True, if the strings are equal.
 */
inline bool operator==(const LEncString &lhs, const std::string &rhs) {
  return lhs.eq(rhs);
}

/**
 * Compare two strings for equality.
 *
 * @param lhs  A string.
 * @param rhs  Another string.
 *
 * @return True, if the strings are equal.
 */
inline bool operator==(const LEncString &lhs, const char *zRhs) {
  return lhs.eq(zRhs);
}

/**
 * Stream a @c LEncString to an @c ostream.
 *
 * @param out  The @c ostream to stream to.
 * @param x    The string.
 *
 * @return The @c ostream provided as argument.
 */
inline std::ostream &operator<<(std::ostream &out, const LEncString &s) {
  return s.print(out);
}

/**
 * Create an iterator placed at a particular position relative to another
 * iterator.
 *
 * @param it  An iterator.
 * @param n   Steps to move, either negative or positive.
 *
 * @return An iterator referring to the new position. The result is undefined
 *         if @c n causes the iterator to conceptually move before the beginning
 *         of the string or beyond the end.
 */
inline LEncString::iterator operator+(const LEncString::iterator &it,
                                      ptrdiff_t n) {
  LEncString::iterator rv(it);
  rv += n;
  return it;
}

/**
 * Create an iterator placed at a particular position relative to another
 * iterator.
 *
 * @param it  An iterator.
 * @param n   Steps to move, either negative or positive.
 *
 * @return An iterator referring to the new position. The result is undefined
 *         if @c n causes the iterator to conceptually move before the beginning
 *         of the string or beyond the end.
 */
inline LEncString::iterator operator+(ptrdiff_t n,
                                      const LEncString::iterator &it) {
  return it + n;
}

/**
 * Create an iterator placed at a particular position relative to another
 * iterator.
 *
 * @param it  An iterator.
 * @param n   Steps to move, either negative or positive.
 *
 * @return An iterator referring to the new position. The result is undefined
 *         if @c n causes the iterator to conceptually move before the beginning
 *         of the string or beyond the end.
 */
inline LEncString::iterator operator-(const LEncString::iterator &it,
                                      ptrdiff_t n) {
  LEncString::iterator rv(it);
  rv -= n;
  return it;
}

/**
 * @class CQRResultsetValue
 *
 * An instance of this class represents a value in a resultset row. As this
 * currently is for the purpose of the masking filter, it effectively is useful
 * for accessing NULL and string values.
 *
 * @attention The name should not be used as such, but instead either
 *            @c ComQueryResponse::TextResultsetRow::Value or
 *            @c ComQueryResponse::TextResultsetRow::Value.
 */
class CQRResultsetValue {
 public:
  CQRResultsetValue()
      : m_is_binary_result(true),
        m_type(MYSQL_TYPE_NULL),
        m_flag(0),
        m_pData(NULL) {}

  CQRResultsetValue(enum_field_types type, uint16_t flag, uint8_t *pData)
      : m_is_binary_result(true), m_type(type), m_flag(flag), m_pData(pData) {}

  bool is_unsigned() const { return m_flag & 0x0020; }

  LEncString as_string() {
    mxb_assert(!m_is_binary_result || is_string() || is_fixed_decimal());
    return LEncString(m_pData);
  }

  uint8_t *get_data() { return m_pData; }

  enum_field_types get_type() { return m_type; }

  bool is_null() const { return m_type == MYSQL_TYPE_NULL; }

  bool is_string() const { return is_string(m_type); }

  static bool is_string(enum_field_types type) {
    switch (type) {
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING:
      /* These are masked by string masking function before
       * any specific masking function is provided for them. */
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_BIT:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_GEOMETRY:
      case MYSQL_TYPE_JSON:
        return true;

        // These, although returned as length-encoded strings, also in the case
        // of a binary resultset row, are not are not considered to be strings
        // from the perspective of masking.
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        return false;

      default:
        // Nothing else is considered to be strings even though, in the case of
        // a textual resultset, that's what they all are.
        return false;
    }
  }

  bool is_integer() const { return is_integer(m_type); }

  static bool is_integer(enum_field_types type) {
    switch (type) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_YEAR:
        return true;

      default:
        // Nothing else is considered to be integer even though.
        return false;
    }
  }

  bool is_decimal() const { return is_decimal(m_type); }

  static bool is_decimal(enum_field_types type) {
    switch (type) {
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        return true;

      default:
        // Nothing else is considered to be decimal even though.
        return false;
    }
  }

  bool is_fixed_decimal() const { return is_fixed_decimal(m_type); }

  static bool is_fixed_decimal(enum_field_types type) {
    switch (type) {
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        return true;

      default:
        // Nothing else is considered to be fixed decimal even though.
        return false;
    }
  }

  bool is_time() { return is_time(m_type); }

  static bool is_time(enum_field_types type) {
    return is_mysql_datetime(type) || is_mysql_timestamp(type) ||
           is_mysql_time(type);
  }

  bool is_mysql_datetime() { return is_mysql_datetime(m_type); }

  static bool is_mysql_datetime(enum_field_types type) {
    return type == MYSQL_TYPE_DATE || type == MYSQL_TYPE_DATETIME;
  }

  bool is_mysql_timestamp() { return is_mysql_timestamp(m_type); }

  static bool is_mysql_timestamp(enum_field_types type) {
    return type == MYSQL_TYPE_TIMESTAMP;
  }

  bool is_mysql_time() { return is_mysql_time(m_type); }

  static bool is_mysql_time(enum_field_types type) {
    return type == MYSQL_TYPE_TIME;
  }

  bool is_binary_result() const { return m_is_binary_result; }
  uint16_t get_flag() { return m_flag; }

  bytes_t to_bytes() const {
    if (!m_is_binary_result) {
      // return LEncString bytes
      LEncString lstr = LEncString(m_pData);
      return bytes_t((uint8_t *)lstr.get_data(), lstr.length());
    } else {
      switch (m_type) {
        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_GEOMETRY:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING: {
          LEncString s(m_pData);
          return bytes_t((uint8_t *)s.get_data(), s.length());
        }

        case MYSQL_TYPE_LONGLONG:
        case MYSQL_TYPE_DOUBLE:
          return bytes_t((uint8_t *)m_pData, 8);

        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_FLOAT:
          return bytes_t((uint8_t *)m_pData, 4);

        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_YEAR:
          return bytes_t((uint8_t *)m_pData, 2);

        case MYSQL_TYPE_TINY:
          return bytes_t((uint8_t *)m_pData, 1);

        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIME: {
          // A byte specifying the length, followed by that many bytes.
          // Either 0, 4, 7 or 11.
          uint8_t len = *m_pData;
          return bytes_t((uint8_t *)m_pData + 1, len);
        }
        default:
          return bytes_t(0);
      }
    }
  }

 protected:
  bool m_is_binary_result;
  enum_field_types m_type;
  uint16_t m_flag;
  uint8_t *m_pData;
};

/**
 * @class CQRTextResultsetValue
 *
 * An instance of this class represents a value in a textual resultset row.
 *
 * @attention The name should not be used as such, but always using the
 *            typedef @c ComQueryResponse::TextResultsetRow::Value.
 */
class CQRTextResultsetValue : public CQRResultsetValue {
 public:
  CQRTextResultsetValue(enum_field_types type, uint16_t flag, uint8_t *pData)
      : CQRResultsetValue(type, flag, pData) {
    m_is_binary_result = false;
    if (*pData == 0xfb) {
      m_type = MYSQL_TYPE_NULL;
    }
  }
};

/**
 * @class CQRBinaryResultsetValue
 *
 * An instance of this class represents a value in a binary resultset row.
 *
 * @attention The name should not be used as such, but always using the
 *            typedef @c ComQueryResponse::BinaryResultsetRow::Value.
 */
typedef CQRResultsetValue CQRBinaryResultsetValue;

}  // namespace encdb
