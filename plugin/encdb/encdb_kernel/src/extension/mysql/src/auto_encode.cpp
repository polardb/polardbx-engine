#include "auto_encode.h"
#include <limits.h>
#include <math.h>
#include <cinttypes>
#include <limits>
#include "masking_util.h"
#include "mysql.hh"

namespace encdb {

ReplaceEncode::ReplaceEncode(const std::string &name, std::string &params)
    : Encode(name) {
  m_fill = "*";
}
ReplaceEncode::~ReplaceEncode() {}
template <class FillIter, class OutIter>
inline void fill_buffer(FillIter f_first, FillIter f_last, OutIter o_first,
                        OutIter o_last) {
  FillIter pFill = f_first;
  while (o_first != o_last) {
    *o_first++ = *pFill++;
    if (pFill == f_last) {
      pFill = f_first;
    }
  }
}
void get_info_of_decimal_num(LEncString &s, int32_t *orders_of_magnitude,
                             uint16_t *mantissa, bool *is_scientific) {
  std::string s_str = s.to_string();
  bool has_sign =
      !s_str.empty() && (s_str.front() < '0' || s_str.front() > '9');
  const char *ss = has_sign ? s_str.c_str() + 1 : s_str.c_str();
  const char *ss_end = s_str.c_str() + s_str.size();
  const char *e_pos = strchr(ss, 'e');
  e_pos = e_pos ? e_pos : strchr(ss, 'E');
  const char *dp_pos = strchr(ss, '.');
  if (e_pos) {
    *orders_of_magnitude = std::stol(e_pos + 1);
    mxb_assert(!dp_pos || dp_pos < e_pos);
    *mantissa = dp_pos ? e_pos - dp_pos - 1 : 0;
    *is_scientific = true;
  } else {
    const char *non_zero = ss;
    while (*non_zero == '0' || *non_zero == '.') non_zero++;
    if (non_zero == ss_end) {
      non_zero = ss;
    }
    *orders_of_magnitude = !dp_pos             ? ss_end - non_zero - 1
                           : dp_pos > non_zero ? dp_pos - non_zero - 1
                                               : dp_pos - non_zero;
    *mantissa = dp_pos ? ss_end - dp_pos - 1 : 0;
    *is_scientific = false;
  }
}

encode_ret ReplaceEncode::encode(ENCODE_VALUE &v, const ENCODE_PARA &para) {
  if (v.is_binary_result &&
      !CQRResultsetValue::is_string((enum_field_types)v.type)) {
    return ENCODE_TYPE_NOT_SUPPORTED;
  }
  auto s = LEncString(v.pData);
  if (!m_fill.empty()) {
    // Copy the fill string
    fill_buffer(m_fill.begin(), m_fill.end(), s.begin(), s.end());
    return ENCODE_SUCC;
  } else {
    MXS_ERROR("fill value is empty in replace encode!");
    return ENCODE_FAIL;
  }
}

IntegerRandomEncode::IntegerRandomEncode(const std::string &name,
                                         integer_val *max, integer_val *min)
    : Encode(name) {
  set_integer_val(&m_max, max->is_positive, max->positive_num,
                  max->negative_num);
  set_integer_val(&m_min, min->is_positive, min->positive_num,
                  min->negative_num);

  for (uint64_t i = 0, j = 9; i < 20; i++) {
    positive_fixed_digits_max[i].is_positive = true;
    positive_fixed_digits_max[i].positive_num = j;

    j = i < 18 ? j * 10 + 9 : ULONG_LONG_MAX;

    positive_fixed_digits_min[i].is_positive = true;
    positive_fixed_digits_min[i].positive_num = i ? pow(10, i) : 0;
  }

  for (int64_t i = 0, j = -9; i < 19; i++) {
    negative_fixed_digits_max[i].is_positive = false;
    negative_fixed_digits_max[i].negative_num = i ? pow(10, i) * -1 : 0;

    negative_fixed_digits_min[i].is_positive = false;
    negative_fixed_digits_min[i].negative_num = j;

    j = i < 17 ? j * 10 - 9 : LONG_LONG_MIN;
  }

  set_integer_val(&uint8_max, true, UCHAR_MAX, 0);
  set_integer_val(&uint8_min, true, 0, 0);
  set_integer_val(&int8_max, true, CHAR_MAX, 0);
  set_integer_val(&int8_min, false, 0, CHAR_MIN);

  set_integer_val(&uint16_max, true, USHRT_MAX, 0);
  set_integer_val(&uint16_min, true, 0, 0);
  set_integer_val(&int16_max, true, SHRT_MAX, 0);
  set_integer_val(&int16_min, false, 0, SHRT_MIN);

  set_integer_val(&uint24_max, true, UINT24_MAX, 0);
  set_integer_val(&uint24_min, true, 0, 0);
  set_integer_val(&int24_max, true, INT24_MAX, 0);
  set_integer_val(&int24_min, false, 0, INT24_MIN);

  set_integer_val(&uint32_max, true, UINT_MAX, 0);
  set_integer_val(&uint32_min, true, 0, 0);
  set_integer_val(&int32_max, true, INT_MAX, 0);
  set_integer_val(&int32_min, false, 0, INT_MIN);

  set_integer_val(&uint64_max, true, ULONG_LONG_MAX, 0);
  set_integer_val(&uint64_min, true, 0, 0);
  set_integer_val(&int64_max, true, LONG_LONG_MAX, 0);
  set_integer_val(&int64_min, false, 0, LONG_LONG_MIN);

  /* Range of MYSQL_TYPE_YEAR is [1901, 2155].
   * https://dev.mysql.com/doc/refman/8.0/en/year.html */
  set_integer_val(&year_max, true, 2155, 0);
  set_integer_val(&year_min, true, 1901, 0);
}
IntegerRandomEncode::~IntegerRandomEncode() {}
encode_ret IntegerRandomEncode::encode(ENCODE_VALUE &v,
                                       const ENCODE_PARA &para) {
  CQRResultsetValue value = encode_value_convert(v);
  return rewrite_value(value);
}
encode_ret IntegerRandomEncode::rewrite_value(CQRResultsetValue &v) const {
  if (v.is_binary_result()) {
    if (v.is_integer()) {
      const integer_val *max = &uint64_max;
      const integer_val *min = &int64_min;
      get_range(v.get_type(), v.is_unsigned(), &max, &min);
      uint64_t rand_num = get_rand_with_range(max, min);

      uint8_t *pData = v.get_data();
      encode_ret ret = ENCODE_SUCC;
      switch (v.get_type()) {
        case MYSQL_TYPE_TINY:
          *pData = rand_num & 0xFF;
          break;
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_YEAR:
          memcpy(pData, &rand_num, 2);
          break;
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
          memcpy(pData, &rand_num, 4);
          break;
        case MYSQL_TYPE_LONGLONG:
          memcpy(pData, &rand_num, 8);
          break;
        default:
          MXS_ERROR("Field type is not supported: %d", v.get_type());
          ret = ENCODE_TYPE_NOT_SUPPORTED;
          break;
      }
      return ret;
    } else {
      MXS_ERROR("Field type is not integer: %d", v.get_type());
      return ENCODE_TYPE_NOT_SUPPORTED;
    }
  } else {
    if (v.is_integer()) {
      LEncString s = v.as_string();
      LEncString::iterator v_it = s.begin();
      size_t v_len = s.length();
      bool has_sign = v_len != 0 && (*v_it < '0' || *v_it > '9');
      bool is_positive = true;
      if (has_sign) {
        is_positive = *v_it != '-';
        v_it++;
        v_len--;
      }
      if (!v_len) {
        ENCDB_LOG(LL_WARNING,
                  "Empty string for IntegerRandom masking, len: %lu", v_len);
        return ENCODE_FAIL;
      }

      const integer_val *max = get_max_by_digits_len(is_positive, v_len);
      const integer_val *min = get_min_by_digits_len(is_positive, v_len);

      get_range(v.get_type(), v.is_unsigned(), &max, &min);
      uint64_t rand_num = get_rand_with_range(max, min);
      std::string r_str;

      encode_ret ret = ENCODE_SUCC;
      switch (v.get_type()) {
        case MYSQL_TYPE_TINY:
          r_str = v.is_unsigned() ? std::to_string((uint8_t)(rand_num & 0xFF))
                                  : std::to_string((int8_t)(rand_num & 0xFF));
          break;
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_YEAR:
          r_str = v.is_unsigned()
                      ? std::to_string((uint16_t)(rand_num & 0xFFFF))
                      : std::to_string((int16_t)(rand_num & 0xFFFF));
          break;
        case MYSQL_TYPE_INT24:
          r_str = v.is_unsigned()
                      ? std::to_string((uint32_t)(rand_num & 0xFFFFFF))
                      : std::to_string((int32_t)(rand_num & 0xFFFFFF));
        case MYSQL_TYPE_LONG:
          r_str = v.is_unsigned()
                      ? std::to_string((uint32_t)(rand_num & 0xFFFFFFFF))
                      : std::to_string((int32_t)(rand_num & 0xFFFFFFFF));
          break;
        case MYSQL_TYPE_LONGLONG:
          r_str = v.is_unsigned() ? std::to_string((uint64_t)(rand_num))
                                  : std::to_string((int64_t)(rand_num));
          break;
        default:
          MXS_ERROR("Field type is not supported: %d", v.get_type());
          ret = ENCODE_TYPE_NOT_SUPPORTED;
          break;
      }
      auto r_it = r_str.begin();
      if (r_it != r_str.end() && (*r_it < '0' || *r_it > '9')) {
        mxb_assert(r_str.size() - 1 == v_len);
        r_it++;
      } else {
        mxb_assert(r_str.size() == v_len);
      }
      while (r_it != r_str.end() && v_it != s.end()) {
        *v_it++ = *r_it++;
      }
      return ret;
    } else {
      MXS_ERROR("Field type is not integer: %d", v.get_type());
      return ENCODE_TYPE_NOT_SUPPORTED;
    }
  }
}

int IntegerRandomEncode::cmp_value(const integer_val *a, const integer_val *b) {
  if (a->is_positive && !b->is_positive) {
    return a->positive_num == 0 && b->negative_num == 0 ? 0 : 1;
  } else if (!a->is_positive && b->is_positive) {
    return a->negative_num == 0 && b->positive_num == 0 ? 0 : -1;
  } else if (a->is_positive && b->is_positive) {
    return a->positive_num == b->positive_num  ? 0
           : a->positive_num < b->positive_num ? -1
                                               : 1;
  } else {
    return a->negative_num == b->negative_num  ? 0
           : a->negative_num < b->negative_num ? -1
                                               : 1;
  }
}
void IntegerRandomEncode::get_intersection(const integer_val *max,
                                           const integer_val *min,
                                           const integer_val **target_max,
                                           const integer_val **target_min) {
  const integer_val *max_t =
      cmp_value(max, *target_max) < 0 ? max : *target_max;
  const integer_val *min_t =
      cmp_value(min, *target_min) > 0 ? min : *target_min;
  if (cmp_value(max_t, min_t) < 0) {
    max_t = *target_max;
    min_t = *target_min;
  }
  *target_max = max_t;
  *target_min = min_t;
}
uint64_t IntegerRandomEncode::get_rand_with_range(
    const integer_val *max, const integer_val *min) const {
  uint64_t rand_num = (uint64_t)rand() << 32 | (uint64_t)rand();
  uint64_t range;
  if (max->is_positive && min->is_positive) {
    range = max->positive_num - min->positive_num;
  } else if (max->is_positive && !min->is_positive) {
    range = max->positive_num + (uint64_t)(min->negative_num * -1);
  } else if (!max->is_positive && !min->is_positive) {
    range = (uint64_t)(max->negative_num - min->negative_num);
  } else {
    MXS_ERROR("Invalid range, max %" PRId64 " is less than min %" PRIu64,
              max->negative_num, min->positive_num);
    mxb_assert(false);
    range = 0;
  }

  if (range != ULONG_LONG_MAX) {
    rand_num = rand_num % (range + 1);
  }

  return rand_num + (min->is_positive ? min->positive_num : min->negative_num);
}
void IntegerRandomEncode::get_range(enum_field_types type, bool is_unsigned,
                                    const integer_val **max,
                                    const integer_val **min) const {
  if (type == MYSQL_TYPE_TINY) {
    get_intersection(is_unsigned ? &uint8_max : &int8_max,
                     is_unsigned ? &uint8_min : &int8_min, max, min);
  } else if (type == MYSQL_TYPE_SHORT) {
    get_intersection(is_unsigned ? &uint16_max : &int16_max,
                     is_unsigned ? &uint16_min : &int16_min, max, min);
  } else if (type == MYSQL_TYPE_INT24) {
    get_intersection(is_unsigned ? &uint24_max : &int24_max,
                     is_unsigned ? &uint24_min : &int24_min, max, min);
  } else if (type == MYSQL_TYPE_LONG) {
    get_intersection(is_unsigned ? &uint32_max : &int32_max,
                     is_unsigned ? &uint32_min : &int32_min, max, min);
  } else if (type == MYSQL_TYPE_LONGLONG) {
    get_intersection(is_unsigned ? &uint64_max : &int64_max,
                     is_unsigned ? &uint64_min : &int64_min, max, min);
  } else if (type == MYSQL_TYPE_YEAR) {
    get_intersection(&year_max, &year_min, max, min);
  } else {
    ENCDB_LOG(LL_WARNING,
              "Unknown data type in IntegerRandomRule::get_range: %d", type);
  }
  get_intersection(&m_max, &m_min, max, min);
}
DecimalRandomEncode::DecimalRandomEncode(const std::string &name,
                                         decimal_val *max, decimal_val *min)
    : Encode(name) {
  m_max = *max;
  m_min = *min;

  float_max = std::numeric_limits<float>::max();
  ufloat_min = 0;
  float_min = -std::numeric_limits<float>::max();

  double_max = std::numeric_limits<double>::max();
  udouble_min = 0;
  double_min = -std::numeric_limits<double>::max();

  /* s is 00(309 zeros)00.00(306 zeros)001 to 100(308 zeros)00.00(307 zeros)00
   */
  char s[618];
  memset(s, '0', 617);
  s[309] = '.';
  s[617] = 0;
  for (int i = 0, j = 616; i<616; i++, j -= j> 0 && s[j - 1] == '.' ? 2 : 1) {
    s[j] = '1';
    positive_fixed_digits_max[i] = std::stod(s);
    positive_fixed_digits_min[i + 1] = std::stod(s);
    negative_fixed_digits_max[i + 1] = -std::stod(s);
    negative_fixed_digits_min[i] = -std::stod(s);
    s[j] = '0';
  }
  positive_fixed_digits_max[616] = std::numeric_limits<double>::max();
  positive_fixed_digits_min[0] = std::numeric_limits<double>::min();
  negative_fixed_digits_max[0] = -std::numeric_limits<double>::min();
  negative_fixed_digits_min[616] = -std::numeric_limits<double>::max();
}

DecimalRandomEncode::~DecimalRandomEncode() {}
encode_ret DecimalRandomEncode::encode(ENCODE_VALUE &v,
                                       const ENCODE_PARA &para) {
  CQRResultsetValue value = encode_value_convert(v);
  return rewrite_value(value);
}
encode_ret DecimalRandomEncode::rewrite_value(CQRResultsetValue &v) const {
  if (!v.is_decimal()) {
    MXS_ERROR("Field type is not decimal: %d", v.get_type());
    return ENCODE_TYPE_NOT_SUPPORTED;
  }

  /* The DECIMAL and NEWDECIMAL type always represents the fraction part with
   * fixed length. And the value in binary mode is still a length encoded
   * string. So we mask the value by the way as same as the non-binary mode. */
  if (v.is_binary_result() && !v.is_fixed_decimal()) {
    const decimal_val *max;
    const decimal_val *min;
    if (!get_range(v.get_type(), v.is_unsigned(), true, -1, &max, &min)) {
      return ENCODE_FAIL;
    }
    double rand_num = get_rand_with_range(max, min);

    uint8_t *pData = v.get_data();
    encode_ret ret = ENCODE_SUCC;
    switch (v.get_type()) {
      case MYSQL_TYPE_FLOAT: {
        float rand_num_f = rand_num;
        memcpy(pData, &rand_num_f, 4);
        break;
      }
      case MYSQL_TYPE_DOUBLE: {
        memcpy(pData, &rand_num, 8);
        break;
      }
      default: {
        MXS_ERROR("Field type is not supported: %d", v.get_type());
        ret = ENCODE_TYPE_NOT_SUPPORTED;
        break;
      }
    }
    return ret;
  } else {
    LEncString s = v.as_string();
    LEncString::iterator v_it = s.begin();
    LEncString::iterator v_end = s.end();
    size_t v_len = s.length();

    /* Skip the first byte if it is a sign character. */
    bool is_positive = true;
    bool has_sign = v_len != 0 && (*v_it < '0' || *v_it > '9');
    if (has_sign) {
      is_positive = *v_it != '-';
      v_it++;
      v_len--;
    }
    if (!v_len) {
      ENCDB_LOG(LL_WARNING, "Empty string for DecimalRandom masking, len: %lu",
                v_len);
      return ENCODE_FAIL;
    }

    /* Try to get the format of this decimal number. */
    int32_t orders_of_magnitude;
    uint16_t mantissa;
    bool is_scientific;
    get_info_of_decimal_num(s, &orders_of_magnitude, &mantissa, &is_scientific);

    /* The range of double type is 2.225074e-308 to 1.797693e+308. */
    if (orders_of_magnitude < -308 || orders_of_magnitude > 308) {
      MXS_ERROR("orders_of_magnitude out of range: %d, src: %s",
                orders_of_magnitude, s.to_string().c_str());
      return ENCODE_FAIL;
    }

    const decimal_val *max;
    const decimal_val *min;

    /* The first order of magnitude is 1e-308, the 309th is 1e0, and the final
     * one(617th) is 1e308. */
    if (!get_range(v.get_type(), v.is_unsigned(), is_positive,
                   orders_of_magnitude + 308, &max, &min)) {
      return ENCODE_FAIL;
    }
    double rand_num = get_rand_with_range(max, min);
    char r_str[1024];
    safely_dtos(r_str, sizeof(r_str) - 1, rand_num, mantissa, is_scientific);
    char *r_str_it = *r_str < '0' || *r_str > '9' ? r_str + 1 : r_str;

    while (*r_str_it && v_it != v_end && *r_str_it != 'e' && *r_str_it != 'E') {
      *v_it++ = *r_str_it++;
    }

    return ENCODE_SUCC;
  }
}

void DecimalRandomEncode::get_intersection(const decimal_val *max,
                                           const decimal_val *min,
                                           const decimal_val **target_max,
                                           const decimal_val **target_min) {
  mxb_assert(*min <= *max && **target_min <= **target_max);
  const decimal_val *max_t = *max < **target_max ? max : *target_max;
  const decimal_val *min_t = *min > **target_min ? min : *target_min;
  if (*min_t > *max_t) {
    max_t = *target_max;
    min_t = *target_min;
  }
  *target_max = max_t;
  *target_min = min_t;
}
double DecimalRandomEncode::get_rand_with_range(const decimal_val *max,
                                                const decimal_val *min) const {
  uint64_t rand_num = (uint64_t)rand() << 32 | (uint64_t)rand();
  /* We get the middle number of min and max, and then random a delta number
   * between mid and max. Finally, make mid adds/subtracts delta will
   * get the random number between min and max.
   *
   * N.B. (max - min) may exceed the maximum number double type can represents.
   */
  decimal_val mid = {*max / 2 + *min / 2};
  double delta = rand_num / (ULONG_LONG_MAX / (*max - mid));
  return rand_num % 2 ? mid + delta : mid - delta;
}
bool DecimalRandomEncode::get_range(enum_field_types type, bool is_unsigned,
                                    bool is_positive, int32_t index,
                                    const decimal_val **max,
                                    const decimal_val **min) const {
  if (index >= 0) {
    if (index >= 617) {
      MXS_ERROR("index out of range: %d.", index);
      return false;
    }
    *max = is_positive ? &positive_fixed_digits_max[index]
                       : &negative_fixed_digits_max[index];
    *min = is_positive ? &positive_fixed_digits_min[index]
                       : &negative_fixed_digits_min[index];
  } else if (type == MYSQL_TYPE_FLOAT) {
    *max = &float_max;
    *min = is_unsigned ? &ufloat_min : &float_min;
  } else if (type == MYSQL_TYPE_DOUBLE) {
    *max = &double_max;
    *min = is_unsigned ? &udouble_min : &double_min;
  } else {
    ENCDB_LOG(LL_WARNING,
              "Unknown data type in DecimalRandomRule::get_range: %d", type);
    return false;
  }

  get_intersection(&m_max, &m_min, max, min);
  return true;
}
TimeRandomEncode::TimeRandomEncode(const std::string &name, time_val *max,
                                   time_val *min)
    : Encode(name) {
  m_max = *max;
  m_min = *min;

  /* Ranges of MYSQL_TYPE_DATE and MYSQL_TYPE_DATETIME are
   * [1000-01-01 00:00:00.000000, 9999-12-31 23:59:59.999999].
   *
   * While MYSQL_TYPE_TIMESTAMP ranges from 1970-01-01 00:00:01.000000 to
   * 2038-01-19 03:14:07.999999.
   *
   * https://dev.mysql.com/doc/refman/8.0/en/datetime.html
   *
   * P.S. Both '999-12-31 23:59:59.999999' and '1970-01-01 00:00:00.999999'
   * represent zero. The function 'datetime_from_micro_seconds' will transfer
   * the input which represents one of them to '0000-00-00 00:00:00.000000'.
   * */
  set_time_val(&date_max, true, 9999, 12, 31, 23, 59, 59, 999999);
  set_time_val(&date_min, true, 999, 12, 31, 23, 59, 59, 999999);

  set_time_val(&datetime_max, true, 9999, 12, 31, 23, 59, 59, 999999);
  set_time_val(&datetime_min, true, 999, 12, 31, 23, 59, 59, 999999);

  set_time_val(&timestamp_max, true, 2038, 01, 19, 03, 14, 07, 999999);
  set_time_val(&timestamp_min, true, 1970, 01, 01, 00, 00, 00, 999999);

  /* Range of MYSQL_TYPE_TIME is -838:59:59.000000 to 838:59:59.000000.
   *
   * https://dev.mysql.com/doc/refman/8.0/en/time.html */
  set_time_val(&time_max, true, 0, 0, 0, 838, 59, 59, 000000);
  set_time_val(&time_min, false, 0, 0, 0, 838, 59, 59, 000000);

  /* Ranges of -838~-100, -99~-10, -9~-0, 0~9, 10~99, 100~838 hours for type
   * MYSQL_TYPE_TIME. */
  set_time_val(&positive_time_max[0], true, 0, 0, 0, 9, 59, 59, 999999);
  set_time_val(&positive_time_max[1], true, 0, 0, 0, 99, 59, 59, 999999);
  set_time_val(&positive_time_max[2], true, 0, 0, 0, 838, 59, 59, 0);

  set_time_val(&positive_time_min[0], true, 0, 0, 0, 0, 0, 0, 0);
  set_time_val(&positive_time_min[1], true, 0, 0, 0, 10, 0, 0, 0);
  set_time_val(&positive_time_min[2], true, 0, 0, 0, 100, 0, 0, 0);

  set_time_val(&negative_time_max[0], false, 0, 0, 0, 0, 0, 0, 0);
  set_time_val(&negative_time_max[1], false, 0, 0, 0, 10, 0, 0, 0);
  set_time_val(&negative_time_max[2], false, 0, 0, 0, 100, 0, 0, 0);

  set_time_val(&negative_time_min[0], false, 0, 0, 0, 9, 59, 59, 999999);
  set_time_val(&negative_time_min[1], false, 0, 0, 0, 99, 59, 59, 999999);
  set_time_val(&negative_time_min[2], false, 0, 0, 0, 838, 59, 59, 0);
}
TimeRandomEncode::~TimeRandomEncode() {}
encode_ret TimeRandomEncode::encode(ENCODE_VALUE &v, const ENCODE_PARA &para) {
  CQRResultsetValue value = encode_value_convert(v);
  return rewrite_value(value);
}
encode_ret TimeRandomEncode::rewrite_value(CQRResultsetValue &v) const {
  if (!v.is_time()) {
    MXS_ERROR("Field type is not time: %d", v.get_type());
    return ENCODE_TYPE_NOT_SUPPORTED;
  }

  if (v.is_binary_result()) {
    uint8_t *pData = v.get_data();
    uint8_t len = *pData++;
    encode_ret ret = ENCODE_SUCC;
    const time_val *max = &datetime_max;
    const time_val *min = &time_min;
    get_range(v.get_type(), &max, &min);
    time_val rand_t = get_rand_with_range(max, min);
    mxs_datetime rand_time =
        datetime_from_micro_seconds(rand_t, v.is_mysql_timestamp());
    if (v.is_mysql_datetime() || v.is_mysql_timestamp()) {
      switch (len) {
        case 11:
          memcpy(pData + 7, &rand_time.micro_second, 4);
        case 7: {
          *(pData + 4) = rand_time.hour;
          *(pData + 5) = rand_time.minute;
          *(pData + 6) = rand_time.second;
        }
        case 4: {
          memcpy(pData, &rand_time.year, 2);
          *(pData + 2) = rand_time.month;
          *(pData + 3) = rand_time.day;
        }
        case 0:
          break;
        default:
          MXS_ERROR(
              "Invalid len for mysql date time types in masking filter: %u",
              len);
          ret = ENCODE_FAIL;
      }
    } else if (v.is_mysql_time()) {
      switch (len) {
        case 12:
          memcpy(pData + 7, &rand_time.micro_second, 4);
        case 8: {
          *pData = rand_time.is_positive ? 0 : 1;
          uint32_t day = rand_time.hour / 24;
          memcpy(pData + 1, &day, 4);
          *(pData + 5) = rand_time.hour % 24;
          *(pData + 6) = rand_time.minute;
          *(pData + 7) = rand_time.second;
        }
        case 0:
          break;
        default:
          MXS_ERROR("Invalid len for mysql time types in masking filter: %u",
                    len);
          ret = ENCODE_FAIL;
      }
    } else {
      MXS_ERROR("Unsupported mysql type for time filter. %u", v.get_type());
      ret = ENCODE_TYPE_NOT_SUPPORTED;
    }
    return ret;
  } else {
    LEncString s = v.as_string();
    char *s_src = s.get_data();
    size_t s_len = s.length();

    /* Skip the first byte if it is a sign character. */
    bool is_positive = true;
    bool has_sign = s_len != 0 && (*s_src == '+' || *s_src == '-');
    if (has_sign) {
      is_positive = *s_src != '-';
      s_src++;
      s_len--;
    }
    if (!s_len) {
      ENCDB_LOG(LL_WARNING, "Empty string for TimeRandom masking, len: %lu",
                s_len);
      return ENCODE_FAIL;
    }

    const time_val *max = &datetime_max;
    const time_val *min = &time_min;
    /* The format of MYSQL_TYPE_TIME is hh:mm:ss or hhh:mm:ss.
     * https://dev.mysql.com/doc/refman/8.0/en/time.html */
    if (v.is_mysql_time()) {
      if (*(s_src + 3) == ':') {
        max = is_positive ? &positive_time_max[2] : &negative_time_max[2];
        min = is_positive ? &positive_time_min[2] : &negative_time_min[2];
      } else {
        max = is_positive ? &positive_time_max[1] : &negative_time_max[0];
        min = is_positive ? &positive_time_min[0] : &negative_time_min[1];
      }
    }
    get_range(v.get_type(), &max, &min);
    time_val rand_t = get_rand_with_range(max, min);
    mxs_datetime rand_time =
        datetime_from_micro_seconds(rand_t, v.is_mysql_timestamp());

    /* snprintf always adds a trailing null-terminator at the end of string,
     * so we write the string to a temporary buffer and then copy to Lenc string
     * buffer. */
    char tmp[64] = {0};
    if (v.is_mysql_time()) {
      std::snprintf(tmp, 64, "%02u:%02u:%02u.%06u", rand_time.hour,
                    rand_time.minute, rand_time.second, rand_time.micro_second);
    } else {
      std::snprintf(tmp, 64, "%04u-%02u-%02u %02u:%02u:%02u.%06u",
                    rand_time.year, rand_time.month, rand_time.day,
                    rand_time.hour, rand_time.minute, rand_time.second,
                    rand_time.micro_second);
    }
    memcpy(s_src, tmp, s_len);
    return ENCODE_SUCC;
  }
}
void TimeRandomEncode::get_intersection(const time_val *max,
                                        const time_val *min,
                                        const time_val **target_max,
                                        const time_val **target_min) {
  mxb_assert(*min <= *max && **target_min <= **target_max);
  const time_val *max_t = *max < **target_max ? max : *target_max;
  const time_val *min_t = *min > **target_min ? min : *target_min;
  if (*min_t <= *max_t) {
    *target_max = max_t;
    *target_min = min_t;
  }
}

TimeRandomEncode::time_val TimeRandomEncode::get_rand_with_range(
    const time_val *max, const time_val *min) const {
  uint64_t rand_num = (uint64_t)rand() << 32 | (uint64_t)rand();
  return *min + (int64_t)(rand_num % ((uint64_t)(*max - *min) + 1));
}
bool TimeRandomEncode::get_range(enum_field_types type, const time_val **max,
                                 const time_val **min) const {
  if (CQRResultsetValue::is_mysql_datetime(type)) {
    get_intersection(&datetime_max, &datetime_min, max, min);
  } else if (CQRResultsetValue::is_mysql_timestamp(type)) {
    get_intersection(&timestamp_max, &timestamp_min, max, min);
  } else if (CQRResultsetValue::is_mysql_time(type)) {
    get_intersection(&time_max, &time_min, max, min);
  } else {
    ENCDB_LOG(LL_WARNING, "Unknown data type in TimeRandomRule::get_range: %d",
              type);
    return false;
  }

  get_intersection(&m_max, &m_min, max, min);
  return true;
}
AutoEncode::AutoEncode(const std::string &name, const std::string &params)
    : Encode(name) {
  std::string fill = "*";
  m_default_replace_encode = std::make_shared<ReplaceEncode>("replace", fill);
  IntegerRandomEncode::integer_val int_max = {true, ULONG_LONG_MAX, 0};
  IntegerRandomEncode::integer_val int_min = {false, 0, LONG_LONG_MIN};
  m_default_integer_random_encode = std::make_shared<IntegerRandomEncode>(
      "integerRandom", &int_max, &int_min);

  DecimalRandomEncode::decimal_val double_max = {
      std::numeric_limits<double>::max()};
  DecimalRandomEncode::decimal_val double_min = {
      -std::numeric_limits<double>::max()};
  m_default_decimal_random_encode = std::make_shared<DecimalRandomEncode>(
      "decimalRandom", &double_max, &double_min);

  TimeRandomEncode::time_val max;
  TimeRandomEncode::time_val min;
  TimeRandomEncode::set_time_val(&max, true, 9999, 12, 31, 23, 59, 59, 999999);
  TimeRandomEncode::set_time_val(&min, false, 0, 0, 0, 838, 59, 59, 000000);
  m_default_time_random_encode =
      std::make_shared<TimeRandomEncode>("timeRandom", &max, &min);
}
AutoEncode::~AutoEncode() {}

encode_ret AutoEncode::encode(ENCODE_VALUE &v, const ENCODE_PARA &para) {
  if (CQRResultsetValue::is_integer((enum_field_types)v.type)) {
    return m_default_integer_random_encode->encode(v, para);
  } else if (CQRResultsetValue::is_decimal((enum_field_types)v.type)) {
    return m_default_decimal_random_encode->encode(v, para);
  } else if (CQRResultsetValue::is_time((enum_field_types)v.type)) {
    return m_default_time_random_encode->encode(v, para);
  }
  /* N.B. The last default rule must be string rule.
   *      If types above are all miss under text protocol,
   *      the default string rule will still mask this column. */
  else if (CQRResultsetValue::is_string((enum_field_types)v.type) ||
           !v.is_binary_result) {
    return m_default_replace_encode->encode(v, para);
  } else if ((enum_field_types)v.type == MYSQL_TYPE_NULL) {
    return ENCODE_SUCC;
  } else {
    ENCDB_LOG(LL_WARNING,
              "Not supported type(%d) for this column which "
              "needs to be encoded. is_binary_protocol: %s",
              v.type, v.is_binary_result ? "true" : "false");
    return ENCODE_TYPE_NOT_SUPPORTED;
  }
}

}  // namespace encdb
