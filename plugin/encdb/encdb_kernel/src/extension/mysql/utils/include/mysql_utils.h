#pragma once
#include <stdint.h>
#include <stdlib.h>
/** Protocol packing macros. */
#define gw_mysql_set_byte2(__buffer, __int)           \
  do {                                                \
    (__buffer)[0] = (uint8_t)((__int)&0xFF);          \
    (__buffer)[1] = (uint8_t)(((__int) >> 8) & 0xFF); \
  } while (0)
#define gw_mysql_set_byte3(__buffer, __int)            \
  do {                                                 \
    (__buffer)[0] = (uint8_t)((__int)&0xFF);           \
    (__buffer)[1] = (uint8_t)(((__int) >> 8) & 0xFF);  \
    (__buffer)[2] = (uint8_t)(((__int) >> 16) & 0xFF); \
  } while (0)
#define gw_mysql_set_byte4(__buffer, __int)            \
  do {                                                 \
    (__buffer)[0] = (uint8_t)((__int)&0xFF);           \
    (__buffer)[1] = (uint8_t)(((__int) >> 8) & 0xFF);  \
    (__buffer)[2] = (uint8_t)(((__int) >> 16) & 0xFF); \
    (__buffer)[3] = (uint8_t)(((__int) >> 24) & 0xFF); \
  } while (0)
#define gw_mysql_set_byte8(__buffer, __int)            \
  do {                                                 \
    (__buffer)[0] = (uint8_t)((__int)&0xFF);           \
    (__buffer)[1] = (uint8_t)(((__int) >> 8) & 0xFF);  \
    (__buffer)[2] = (uint8_t)(((__int) >> 16) & 0xFF); \
    (__buffer)[3] = (uint8_t)(((__int) >> 24) & 0xFF); \
    (__buffer)[4] = (uint8_t)(((__int) >> 32) & 0xFF); \
    (__buffer)[5] = (uint8_t)(((__int) >> 40) & 0xFF); \
    (__buffer)[6] = (uint8_t)(((__int) >> 48) & 0xFF); \
    (__buffer)[7] = (uint8_t)(((__int) >> 56) & 0xFF); \
  } while (0)
#define mxb_assert(exp)
#define mxb_assert_message(exp, message)
#define MXS_ERROR(format, ...) ENCDB_LOG(LL_ERROR, format, ##__VA_ARGS__)

namespace encdb {

typedef enum enum_field_types {
  MYSQL_TYPE_DECIMAL,
  MYSQL_TYPE_TINY,
  MYSQL_TYPE_SHORT,
  MYSQL_TYPE_LONG,
  MYSQL_TYPE_FLOAT,
  MYSQL_TYPE_DOUBLE,
  MYSQL_TYPE_NULL,
  MYSQL_TYPE_TIMESTAMP,
  MYSQL_TYPE_LONGLONG,
  MYSQL_TYPE_INT24,
  MYSQL_TYPE_DATE,
  MYSQL_TYPE_TIME,
  MYSQL_TYPE_DATETIME,
  MYSQL_TYPE_YEAR,
  MYSQL_TYPE_NEWDATE,
  MYSQL_TYPE_VARCHAR,
  MYSQL_TYPE_BIT,
  MYSQL_TYPE_TIMESTAMP2,
  MYSQL_TYPE_DATETIME2,
  MYSQL_TYPE_TIME2,
  MYSQL_TYPE_JSON = 245,
  MYSQL_TYPE_NEWDECIMAL = 246,
  MYSQL_TYPE_ENUM = 247,
  MYSQL_TYPE_SET = 248,
  MYSQL_TYPE_TINY_BLOB = 249,
  MYSQL_TYPE_MEDIUM_BLOB = 250,
  MYSQL_TYPE_LONG_BLOB = 251,
  MYSQL_TYPE_BLOB = 252,
  MYSQL_TYPE_VAR_STRING = 253,
  MYSQL_TYPE_STRING = 254,
  MYSQL_TYPE_GEOMETRY = 255
} enum_field_types;

size_t mxs_set_leint_bytes(uint8_t *ptr, uint64_t num);
size_t mxs_leint_bytes(const uint8_t *ptr);
uint64_t mxs_leint_value(const uint8_t *c);
uint64_t mxs_leint_consume(uint8_t **c);
char *mxs_lestr_consume(uint8_t **c, size_t *size);
}  // namespace encdb
