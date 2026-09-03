/* Copyright (c) 2025, 2025, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "vidx/vidx_text_parser.h"

#include <cerrno>
#include <cmath>  // std::isinf, std::isnan
#include <cstring>

#include "fast_float/fast_float.h"  // fast_float::from_chars

#include "m_ctype.h"       // CHARSET_INFO, my_charset_is_ascii_based, my_convert
#include "sql_string.h"    // String, check_if_only_end_space

namespace vidx {

/* Controlled by the vidx.use_fast_float sysvar; defined in vidx_index.cc. */
extern bool use_fast_float;

static inline bool is_ascii_ws(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

/*
  Fallback float parser used when use_fast_float is OFF.
  strtof() scans until a non-numeric character (comma/bracket/whitespace),
  so a separate end-of-input bound is unnecessary.
  Returns input (unchanged) on failure.
  Subnormal underflow (ERANGE but finite result) is allowed through,
  matching fast_float behavior and pgvector semantics.
*/
static inline const char *parse_float_fallback(const char *input, float *val) {
  char *endptr;
  errno = 0;
  *val = strtof(input, &endptr);
  if (endptr == input || (errno == ERANGE && std::isinf(*val))) return input;
  return endptr;
}

/*
  ASCII fast path: no charset virtual calls, fast_float inlined, direct
  pointer-increment writes.  Returns true on parse error.
*/
static bool from_string_to_vector_ascii_fast(const char *input,
                                             uint32_t input_len,
                                             char *const output,
                                             uint32_t *max_output_dims) {
  const char *const input_start = input;
  const char *const end = input + input_len;

  /* Skip leading ASCII whitespace. */
  while (input < end && is_ascii_ws(*input)) input++;

  /* Must start with '['. */
  if (input >= end || *input != '[') {
    *max_output_dims = 0;
    return true;
  }

  /* Use an out-of-place buffer when output overlaps input. */
  const size_t output_len = sizeof(float) * (*max_output_dims);
  String temp_output(output, output_len, nullptr);
  if (output + output_len >= input_start &&
      input_start + input_len >= output) {
    temp_output = String(output_len);
    if (temp_output.ptr() == nullptr) {
      *max_output_dims = 0;
      return true;
    }
  }

  input++;  /* skip '[' */

  float *out_ptr = reinterpret_cast<float *>(temp_output.ptr());
  const float *const out_limit = out_ptr + *max_output_dims;
  bool bracket_closed = false;

  while (out_ptr < out_limit) {
    while (input < end && is_ascii_ws(*input)) input++;
    if (input >= end) break;

    float val;
    if (use_fast_float) {
      /* fast_float::from_chars handles '-' but not '+'; skip '+' manually. */
      const char *num_start = input;
      if (*input == '+') input++;
      auto answer = fast_float::from_chars(input, end, val);
      /*
        fast_float reports result_out_of_range when the value rounds to a
        finite 0.0 (underflow) or to inf (overflow); val is written in both
        cases.  Underflow to 0.0f is accepted -- matching the strtof fallback
        and pgvector -- while overflow to inf is rejected by the isnan/isinf
        check below.  Any other ec is a genuine parse failure.
      */
      if (answer.ec != std::errc() &&
          answer.ec != std::errc::result_out_of_range) {
        input = num_start;
        break;
      }
      input = answer.ptr;
    } else {
      const char *ptr = parse_float_fallback(input, &val);
      if (ptr == input) break;
      input = ptr;
    }

    if (std::isnan(val) || std::isinf(val)) break;

    *out_ptr++ = val;

    while (input < end && is_ascii_ws(*input)) input++;
    if (input >= end) break;

    if (*input == ',') {
      input++;
    } else if (*input == ']') {
      input++;
      bracket_closed = true;
      break;
    } else {
      break;
    }
  }

  /* Only ASCII whitespace allowed after ']'. */
  if (bracket_closed) {
    while (input < end && is_ascii_ws(*input)) input++;
    if (input != end) {
      *max_output_dims = 0;
      return true;
    }
  }

  const uint32_t dim = static_cast<uint32_t>(
      out_ptr - reinterpret_cast<float *>(temp_output.ptr()));

  if (temp_output.ptr() != output) {
    memcpy(output, temp_output.ptr(), dim * sizeof(float));
  }

  *max_output_dims = dim;
  return !bracket_closed;
}

/*
  Generic path: charset-aware whitespace scan, with strtof fallback when
  use_fast_float is OFF.
*/
static bool from_string_to_vector_generic(const CHARSET_INFO *cs,
                                          const char *input,
                                          uint32_t input_len,
                                          char *const output,
                                          uint32_t *max_output_dims) {
  /* Convert non-ASCII-based input to UTF-8 for uniform processing. */
  const CHARSET_INFO *process_cs = cs;
  String temp_input;
  if (!my_charset_is_ascii_based(cs)) {
    process_cs = &my_charset_utf8mb4_bin;
    if (temp_input.alloc(input_len)) {
      *max_output_dims = 0;
      return true;
    }
    uint errors = 0;
    input_len = my_convert(temp_input.ptr(), input_len, process_cs, input,
                           input_len, cs, &errors);
    if (errors != 0) {
      *max_output_dims = 0;
      return true;
    }
    input = temp_input.ptr();
  }

  /* Skip leading whitespace using charset-aware scan. */
  auto len = process_cs->cset->scan(process_cs, input, input + input_len,
                                    MY_SEQ_SPACES);
  if (len >= input_len) {
    *max_output_dims = 0;
    return true;
  }
  input_len -= len;
  input += len;

  /* Must start with '['. */
  if (input_len == 0 || input[0] != '[') {
    *max_output_dims = 0;
    return true;
  }

  /* Use an out-of-place buffer when output overlaps input. */
  size_t const output_len = sizeof(float) * (*max_output_dims);
  String temp_output(output, output_len, nullptr);
  if (output + output_len >= input && input + input_len >= output) {
    temp_output = String(output_len);
    if (temp_output.ptr() == nullptr) {
      *max_output_dims = 0;
      return true;
    }
  }

  const char *const input_end = input + input_len - 1;
  input = input + 1;  /* skip '[' */
  float *out_ptr = reinterpret_cast<float *>(temp_output.ptr());
  const float *const out_limit = out_ptr + *max_output_dims;
  bool bracket_closed = false;

  while (out_ptr < out_limit) {
    input += process_cs->cset->scan(process_cs, input, input_end + 1,
                                    MY_SEQ_SPACES);
    if (input > input_end) break;

    float fnum;
    if (use_fast_float) {
      const char *num_start = input;
      if (*input == '+') input++;
      auto answer = fast_float::from_chars(input, input_end + 1, fnum);
      /* See from_string_to_vector_ascii_fast: underflow (result_out_of_range
         with a finite 0.0) is accepted; overflow (inf) is caught by the
         isnan/isinf check below. */
      if (answer.ec != std::errc() &&
          answer.ec != std::errc::result_out_of_range) {
        input = num_start;
        break;
      }
      input = answer.ptr;
    } else {
      const char *ptr = parse_float_fallback(input, &fnum);
      if (ptr == input) break;
      input = ptr;
    }

    if (std::isnan(fnum) || std::isinf(fnum)) break;
    *out_ptr++ = fnum;

    input += process_cs->cset->scan(process_cs, input, input_end + 1,
                                    MY_SEQ_SPACES);
    if (input > input_end) break;

    if (*input == ',') {
      input++;
    } else if (*input == ']') {
      input++;
      bracket_closed = true;
      break;
    } else {
      break;
    }
  }

  if (bracket_closed &&
      !check_if_only_end_space(process_cs, input, input_end + 1)) {
    *max_output_dims = 0;
    return true;
  }

  const uint32_t dim = static_cast<uint32_t>(
      out_ptr - reinterpret_cast<float *>(temp_output.ptr()));

  if (temp_output.ptr() != output) {
    memcpy(output, temp_output.ptr(), dim * sizeof(float));
  }

  *max_output_dims = dim;
  return !bracket_closed;
}

bool from_string_to_vector(const CHARSET_INFO *cs, const char *input,
                           uint32_t input_len, char *const output,
                           uint32_t *max_output_dims) {
  if (input == nullptr || input_len == 0) {
    *max_output_dims = 0;
    return true;
  }

  if (my_charset_is_ascii_based(cs)) {
    return from_string_to_vector_ascii_fast(input, input_len, output,
                                            max_output_dims);
  }

  return from_string_to_vector_generic(cs, input, input_len, output,
                                       max_output_dims);
}

}  // namespace vidx
