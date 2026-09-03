#pragma once
#include "encdb_encode.h"
#include "mysql.hh"
#include "mysql_utils.h"
#define UINT24_MAX 16777215
#define INT24_MAX 8388607
#define INT24_MIN -8388608

namespace encdb {

class ReplaceEncode : public Encode {
 public:
  ReplaceEncode(const std::string &name, std::string &params);
  virtual ~ReplaceEncode();
  bool is_variadic_encoding() { return false; }
  bool need_encrypting() { return false; }
  encode_ret encode(ENCODE_VALUE &v, const ENCODE_PARA &para);

 private:
  ReplaceEncode(const ReplaceEncode &);
  ReplaceEncode &operator=(const ReplaceEncode &);
  std::string m_fill;
};

class IntegerRandomEncode : public Encode {
 public:
  typedef struct {
    bool is_positive;
    uint64_t positive_num;
    int64_t negative_num;
  } integer_val;

  /**
   * Constructor of IntegerRandomEncode
   * @param max         The maximum random number after masking
   * @param min         The minimum random number after masking
   */
  IntegerRandomEncode(const std::string &name, integer_val *max,
                      integer_val *min);

  virtual ~IntegerRandomEncode();
  bool is_variadic_encoding() { return false; }
  bool need_encrypting() { return false; }
  encode_ret encode(ENCODE_VALUE &v, const ENCODE_PARA &para);

  /**
   * Mask the column value content by specific rule
   *
   * @param v    The current value to be rewritten.
   */
  encode_ret rewrite_value(CQRResultsetValue &v) const;

  const integer_val *get_max_by_digits_len(bool is_positive,
                                           uint32_t len) const {
    if (!len || (is_positive && len > 20) || (!is_positive && len > 19)) {
      MXS_ERROR("Invalid len of digits, is_positive: %s, len: %u",
                is_positive ? "true" : "false", len);
      len = is_positive ? 20 : 19;
    }
    return is_positive ? &positive_fixed_digits_max[len - 1]
                       : &negative_fixed_digits_max[len - 1];
  }
  const integer_val *get_min_by_digits_len(bool is_positive,
                                           uint32_t len) const {
    if (!len || (is_positive && len > 20) || (!is_positive && len > 19)) {
      MXS_ERROR("Invalid len of digits, is_positive: %s, len: %u",
                is_positive ? "true" : "false", len);
      len = is_positive ? 20 : 19;
    }
    return is_positive ? &positive_fixed_digits_min[len - 1]
                       : &negative_fixed_digits_min[len - 1];
  }

  static void set_integer_val(integer_val *v, bool is_positive,
                              uint64_t uint64_t_v, int64_t int64_t_v) {
    v->is_positive = is_positive;
    v->positive_num = uint64_t_v;
    v->negative_num = int64_t_v;
  }

  static int cmp_value(const integer_val *a, const integer_val *b);

  static void get_intersection(const integer_val *max, const integer_val *min,
                               const integer_val **target_max,
                               const integer_val **target_min);

  uint64_t get_rand_with_range(const integer_val *max,
                               const integer_val *min) const;

 private:
  void get_range(enum_field_types type, bool is_unsigned,
                 const integer_val **max, const integer_val **min) const;
  IntegerRandomEncode(const IntegerRandomEncode &);
  IntegerRandomEncode &operator=(const IntegerRandomEncode &);

  integer_val m_max; /* User defined maximum num. */
  integer_val m_min; /* User defined minimum num. */

  integer_val positive_fixed_digits_max[20]; /* Positive maximums of different
                                              * digit numbers, i.e. 9, 99, 999,
                                              * ... 18446744073709551615 */
  integer_val positive_fixed_digits_min[20]; /* Positive minimums of different
                                              * digit numbers, i.e. 0, 10, 100,
                                              * ... 10000000000000000000 */

  integer_val
      negative_fixed_digits_max[19]; /* Negative maximums of different digit
                                      * numbers, i.e. 0, -10, -100, ...
                                      * -1000000000000000000 */
  integer_val
      negative_fixed_digits_min[19]; /* Negative minimums of different digit
                                      * numbers, i.e. -9, -99, -999, ...
                                      * -9223372036854775808 */

  integer_val uint8_max; /* The theoretical maximum of uint8_t represented by
                            uint64_t. */
  integer_val uint8_min; /* The theoretical minimum of uint8_t represented by
                            uint64_t. */

  integer_val
      int8_max; /* The theoretical maximum of int8_t represented by int64_t. */
  integer_val
      int8_min; /* The theoretical minimum of int8_t represented by int64_t. */

  integer_val uint16_max; /* The theoretical maximum of uint16_t represented by
                             uint64_t. */
  integer_val uint16_min; /* The theoretical minimum of uint16_t represented by
                             uint64_t. */

  integer_val int16_max; /* The theoretical maximum of int16_t represented by
                            int64_t. */
  integer_val int16_min; /* The theoretical minimum of int16_t represented by
                            int64_t. */

  integer_val uint24_max; /* The theoretical maximum of uint24_t represented by
                             uint64_t. */
  integer_val uint24_min; /* The theoretical minimum of uint24_t represented by
                             uint64_t. */

  integer_val int24_max; /* The theoretical maximum of int24_t represented by
                            int64_t. */
  integer_val int24_min; /* The theoretical minimum of int24_t represented by
                            int64_t. */

  integer_val uint32_max; /* The theoretical maximum of uint32_t represented by
                             uint64_t. */
  integer_val uint32_min; /* The theoretical minimum of uint32_t represented by
                             uint64_t. */

  integer_val int32_max; /* The theoretical maximum of int32_t represented by
                            int64_t. */
  integer_val int32_min; /* The theoretical minimum of int32_t represented by
                            int64_t. */

  integer_val uint64_max; /* The theoretical maximum of uint16_t represented by
                             uint64_t. */
  integer_val uint64_min; /* The theoretical minimum of uint16_t represented by
                             uint64_t. */

  integer_val int64_max; /* The theoretical maximum of int64_t represented by
                            int64_t. */
  integer_val int64_min; /* The theoretical minimum of int64_t represented by
                            int64_t. */

  integer_val year_max; /* The maximum of MYSQL_TYPE_YEAR. */
  integer_val year_min; /* The minimum of MYSQL_TYPE_YEAR. */
};

class DecimalRandomEncode : public Encode {
 public:
  typedef double decimal_val;

  /**
   * Constructor of DecimalRandomRule
   * @param max         The maximum random number after masking
   * @param min         The minimum random number after masking
   */
  DecimalRandomEncode(const std::string &name, decimal_val *max,
                      decimal_val *min);

  ~DecimalRandomEncode();
  bool is_variadic_encoding() { return false; }
  bool need_encrypting() { return false; }
  encode_ret encode(ENCODE_VALUE &v, const ENCODE_PARA &para);
  /**
   * Encode the column value content by specific rule
   *
   * @param v    The current value to be rewritten.
   */
  encode_ret rewrite_value(CQRResultsetValue &v) const;

  static void get_intersection(const decimal_val *max, const decimal_val *min,
                               const decimal_val **target_max,
                               const decimal_val **target_min);

  static void safely_dtos(char *target, uint32_t max_bytes, double d,
                          uint16_t mantissa, bool is_scientific) {
    char format[16] = "%.";
    strcat(format, std::to_string(mantissa).c_str());
    strcat(format, is_scientific ? "e" : "f");
    std::snprintf(target, max_bytes, format, d);
  }

  double get_rand_with_range(const decimal_val *max,
                             const decimal_val *min) const;

 private:
  bool get_range(enum_field_types type, bool is_unsigned, bool is_positive,
                 int32_t index, const decimal_val **max,
                 const decimal_val **min) const;
  DecimalRandomEncode(const DecimalRandomEncode &);
  DecimalRandomEncode &operator=(const DecimalRandomEncode &);

  /* The maximum of double is 1.79769e+308, and the minimum 2.22507e-308.
   * So the total range of orders of magnitude is 308 + 1 + 308 = 617.
   * P.S. The first order of magnitude is 1e-308, the 309th is 1e0, and the
   * final one(617th) is 1e308. */
  decimal_val
      positive_fixed_digits_max[617]; /* Positive maximums of different orders
                                       * of magnitude, i.e. ..., 0.09...,
                                       * 0.9..., 9.9..., 99.9..., ... */
  decimal_val positive_fixed_digits_min[617]; /* Positive minimums of different
                                               * orders of magnitude, i.e. ...,
                                               * 0.01, 0.1, 1, 10, ... */

  decimal_val negative_fixed_digits_max[617]; /* Negative maximums of different
                                               * orders of magnitude, i.e. ...,
                                               * -0.01, -0.1, -1, -10, ... */
  decimal_val
      negative_fixed_digits_min[617]; /* Negative minimums of different orders
                                       * of magnitude, i.e. ..., -0.09...,
                                       * -0.9..., -9.9..., -99.9..., ... */
  decimal_val float_max;
  decimal_val ufloat_min;
  decimal_val float_min;

  decimal_val double_max;
  decimal_val udouble_min;
  decimal_val double_min;

  decimal_val m_max;
  decimal_val m_min;
};
class TimeRandomEncode : public Encode {
 public:
  typedef int64_t time_val;
  typedef struct {
    bool is_positive;
    uint16_t year;
    uint8_t month;
    uint32_t day; /* To be compatible with MYSQL_TYPE_TIME, we use uint32_t
                     instead of uint8_t. */
    uint32_t hour;
    uint8_t minute;
    uint8_t second;
    uint32_t micro_second;
  } mxs_datetime;

  /**
   * Constructor of TimeRandomEncode
   *
   * @param max         The maximum random time after masking
   * @param min         The minimum random time after masking
   */
  TimeRandomEncode(const std::string &name, time_val *max, time_val *min);

  ~TimeRandomEncode();
  bool is_variadic_encoding() { return false; }
  bool need_encrypting() { return false; }
  encode_ret encode(ENCODE_VALUE &v, const ENCODE_PARA &para);
  /**
   * Encode the column value content by specific rule
   *
   * @param v    The current value to be rewritten.
   */
  encode_ret rewrite_value(CQRResultsetValue &v) const;

  static void set_time_val(time_val *v, bool is_positive, uint16_t year,
                           uint8_t mon, uint32_t day, uint32_t hour,
                           uint8_t min, uint8_t sec, uint32_t micro_sec) {
    mxs_datetime t;
    t.is_positive = is_positive;
    t.year = year;
    t.month = mon;
    t.day = day;
    t.hour = hour;
    t.minute = min;
    t.second = sec;
    t.micro_second = micro_sec;
    *v = micro_seconds_from_datetime(&t);
  }

  static uint32_t get_max_day(uint32_t year, uint32_t month) {
    switch (month) {
      case 1:
      case 3:
      case 5:
      case 7:
      case 8:
      case 10:
      case 12:
        return 31;
      case 4:
      case 6:
      case 9:
      case 11:
        return 30;
      case 2:
        return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) ? 29 : 28;
      default:
        MXS_ERROR("Unexpected month: %u", month);
        return 1;
    }
  }

  static bool check_mxs_datetime(mxs_datetime *t) {
    if (t->year < 0 || t->year > 9999 || t->month < 0 || t->month > 12 ||
        (t->year > 0 && t->month == 0) || (t->year == 0 && t->month > 0) ||
        (t->year > 0 && t->month > 0 &&
         t->day > get_max_day(t->year, t->month)) ||
        (t->year > 0 && t->month > 0 && t->hour > 24) || t->minute > 59 ||
        t->second > 59 || t->micro_second > 999999) {
      return false;
    }
    return true;
  }

  /* Reference from
   * http://howardhinnant.github.io/date_algorithms.html#days_from_civil */
  static int64_t days_from_date(int32_t y, uint32_t m, uint32_t d) noexcept {
    y -= m <= 2;
    const int32_t era = (y >= 0 ? y : y - 399) / 400;
    const uint32_t yoe = static_cast<uint32_t>(y - era * 400);  // [0, 399]
    const uint32_t doy =
        (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;          // [0, 365]
    const uint32_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;  // [0, 146096]
    return era * 146097 + static_cast<int64_t>(doe);
  }

  /* Reference from
   * http://howardhinnant.github.io/date_algorithms.html#civil_from_days */
  static std::tuple<int32_t, uint32_t, uint32_t> date_from_days(
      int64_t z) noexcept {
    const int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    const uint32_t doe =
        static_cast<uint32_t>(z - era * 146097);  // [0, 146096]
    const uint32_t yoe =
        (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;  // [0, 399]
    const int32_t y = static_cast<int32_t>(yoe) + era * 400;
    const uint32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);  // [0, 365]
    const uint32_t mp = (5 * doy + 2) / 153;                       // [0, 11]
    const uint32_t d = doy - (153 * mp + 2) / 5 + 1;               // [1, 31]
    const uint32_t m = mp + (mp < 10 ? 3 : -9);                    // [1, 12]
    return std::tuple<int32_t, uint32_t, uint32_t>(y + (m <= 2), m, d);
  }

  static int64_t micro_seconds_from_datetime(const mxs_datetime *t) {
    mxb_assert_message(
        (t->year == 0 && t->month == 0) || (t->year > 0 && t->month > 0),
        "Invalid year and month");
    if (t->year > 0 && t->month > 0) {
      return (t->is_positive ? 1 : -1) *
             ((((days_from_date(t->year, t->month, t->day) * 24 + t->hour) *
                    60 +
                t->minute) *
                   60 +
               t->second) *
                  1000000 +
              t->micro_second);
    } else {
      return (t->is_positive ? 1 : -1) *
             ((((int64_t)t->hour * 60 + t->minute) * 60 + t->second) * 1000000 +
              t->micro_second);
    }
  }

  /**
   * For MYSQL_TYPE_DATE and MYSQL_TYPE_DATETIME:
   * 31551811200000000 represents: 1000-01-01 00:00:00.000000
   * 31551811199999999 represents: 0000-00-00 00:00:00.000000
   *
   * For MYSQL_TYPE_TIMESTAMP:
   * 62162035201000000 represents: 1970-01-01 00:00:01.000000
   * 62162035200999999 represents: 0000-00-00 00:00:00.000000
   *
   * For MYSQL_TYPE_TIME:
   * Any value less than 31551811199999999
   */
  static mxs_datetime datetime_from_micro_seconds(int64_t total_ms,
                                                  bool is_timestamp = false) {
    mxs_datetime t = {0};
    t.is_positive = total_ms >= 0;
    total_ms *= total_ms >= 0 ? 1 : -1;
    /* 1000-01-01 00:00:00.000000 */
    /* 1970-01-01 00:00:01.000000 */
    if ((!is_timestamp && total_ms >= 31551811200000000) ||
        (is_timestamp && total_ms >= 62162035201000000)) {
      auto d = date_from_days(total_ms / 86400000000);
      t.year = std::get<0>(d);
      t.month = std::get<1>(d);
      t.day = std::get<2>(d);
      t.hour = (total_ms % 86400000000) / 3600000000;
      t.minute = (total_ms % 3600000000) / 60000000;
      t.second = (total_ms % 60000000) / 1000000;
      t.micro_second = total_ms % 1000000;
    }
    /* Represents MYSQL_TIME (+|-)xxx:xx:xx.xxxxxx */
    else if (total_ms < 31551811199999999) {
      t.hour = total_ms / 3600000000;
      t.minute = (total_ms % 3600000000) / 60000000;
      t.second = (total_ms % 60000000) / 1000000;
      t.micro_second = total_ms % 1000000;
    }
    return t;
  }

  static void get_intersection(const time_val *max, const time_val *min,
                               const time_val **target_max,
                               const time_val **target_min);

  time_val get_rand_with_range(const time_val *max, const time_val *min) const;

 private:
  bool get_range(enum_field_types type, const time_val **max,
                 const time_val **min) const;
  TimeRandomEncode(const TimeRandomEncode &);
  TimeRandomEncode &operator=(const TimeRandomEncode &);

  time_val m_max;
  time_val m_min;

  time_val date_max;
  time_val date_min;

  time_val datetime_max;
  time_val datetime_min;

  time_val timestamp_max;
  time_val timestamp_min;

  time_val time_max;
  time_val time_min;

  time_val
      positive_time_max[3]; /* hour of MYSQL_TYPE_TIME range from 0 ~ 838 */
  time_val positive_time_min[3];

  time_val negative_time_max[3];
  time_val negative_time_min[3];
};

class AutoEncode : public Encode {
 public:
  AutoEncode(const std::string &name, const std::string &params);
  virtual ~AutoEncode();
  bool is_variadic_encoding() { return false; }
  bool need_encrypting() { return false; }
  encode_ret encode(ENCODE_VALUE &v, const ENCODE_PARA &para);

 private:
  AutoEncode(const ReplaceEncode &);
  AutoEncode &operator=(const ReplaceEncode &);
  SEncode m_default_replace_encode;
  SEncode m_default_integer_random_encode;
  SEncode m_default_decimal_random_encode;
  SEncode m_default_time_random_encode;
};

}  // namespace encdb
