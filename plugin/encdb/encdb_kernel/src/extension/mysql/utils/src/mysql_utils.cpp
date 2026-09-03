#include "mysql_utils.h"
#include <string.h>
#include "logger.h"

namespace encdb {

size_t mxs_set_leint_bytes(uint8_t *ptr, uint64_t num) {
  if (num < (uint64_t)251LL) {
    *ptr = num;
    return 1;
  }

  /* 251 is reserved for NULL */
  if (num < (uint64_t)65536LL) {
    *ptr++ = 252;
    gw_mysql_set_byte2(ptr, num);
    return 3;
  }

  if (num < (uint64_t)16777216LL) {
    *ptr++ = 253;
    gw_mysql_set_byte3(ptr, num);
    return 4;
  }

  *ptr++ = 254;
  gw_mysql_set_byte8(ptr, num);
  return 9;
}

/**
 * @brief Calculate the length of a length-encoded integer in bytes
 *
 * @param ptr Start of the length encoded value
 * @return Number of bytes before the actual value
 */
size_t mxs_leint_bytes(const uint8_t *ptr) {
  uint8_t val = *ptr;
  if (val < 0xfb) {
    return 1;
  } else if (val == 0xfc) {
    return 3;
  } else if (val == 0xfd) {
    return 4;
  } else {
    return 9;
  }
}

/**
 * @brief Converts a length-encoded integer to @c uint64_t
 *
 * @see https://dev.mysql.com/doc/internals/en/integer.html
 * @param c Pointer to the first byte of a length-encoded integer
 * @return The value converted to a standard unsigned integer
 */
uint64_t mxs_leint_value(const uint8_t *c) {
  uint64_t sz = 0;

  if (*c < 0xfb) {
    sz = *c;
  } else if (*c == 0xfc) {
    memcpy(&sz, c + 1, 2);
  } else if (*c == 0xfd) {
    memcpy(&sz, c + 1, 3);
  } else if (*c == 0xfe) {
    memcpy(&sz, c + 1, 8);
  } else {
    mxb_assert(*c == 0xff);
    ENCDB_LOG(LL_ERROR,
              "Unexpected length encoding '%x' encountered when reading "
              "length-encoded integer.",
              *c);
  }

  return sz;
}

/**
 * Converts a length-encoded integer into a standard unsigned integer
 * and advances the pointer to the next unrelated byte.
 *
 * @param c Pointer to the first byte of a length-encoded integer
 */
uint64_t mxs_leint_consume(uint8_t **c) {
  uint64_t rval = mxs_leint_value(*c);
  *c += mxs_leint_bytes(*c);
  return rval;
}

/**
 * @brief Consume a length-encoded string
 *
 * Converts length-encoded strings to character strings and advanced
 * the pointer to the next unrelated byte.
 * @param c Pointer to the start of the length-encoded string
 * @param size Pointer to a variable where the size of the string is stored
 * @return Pointer to the start of the string
 */
char *mxs_lestr_consume(uint8_t **c, size_t *size) {
  uint64_t slen = mxs_leint_consume(c);
  *size = slen;
  char *start = (char *)*c;
  *c += slen;
  return start;
}

}  // namespace encdb
