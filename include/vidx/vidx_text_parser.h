#ifndef VIDX_TEXT_PARSER_INCLUDED
#define VIDX_TEXT_PARSER_INCLUDED

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

#include <cstdint>

struct CHARSET_INFO;

namespace vidx {

/*
  Parse a textual vector "[v1,v2,...,vn]" into a float[] buffer.

  Dispatches to an ASCII fast path when the input charset is ASCII-based
  and the use_fast_float sysvar is ON; otherwise falls back to a charset-
  aware generic path (which can still use fast_float when enabled).

  @param[in]     cs                Input charset.
  @param[in]     input             Input string.
  @param[in]     input_len         Byte length of input.
  @param[out]    output            Pre-allocated buffer of at least
                                   *max_output_dims * sizeof(float) bytes.
  @param[in,out] max_output_dims   In: capacity in float elements.
                                   Out: actual number of parsed elements,
                                        or 0 on error.

  @retval false on success
  @retval true  on parse error or OOM
*/
bool from_string_to_vector(const CHARSET_INFO *cs, const char *input,
                           uint32_t input_len, char *output,
                           uint32_t *max_output_dims);

}  // namespace vidx

#endif /* VIDX_TEXT_PARSER_INCLUDED */
