#ifndef VIDX_COMMON_INCLUDED
#define VIDX_COMMON_INCLUDED

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

#include <assert.h>
#include <cstdio>
#include <string>

#include "my_inttypes.h"

class String;
struct CHARSET_INFO;

namespace vidx {
static constexpr uint32_t MAX_DIMENSIONS = 16383;
static constexpr uint32_t VECTOR_PRECISION = sizeof(float);

static const char *distance_names[] = {"EUCLIDEAN", "COSINE", "INNER_PRODUCT",
                                       nullptr};
static constexpr uint METRIC_DEF = 0;
static constexpr uint METRIC_MAX =
    sizeof(distance_names) / sizeof(distance_names[0]) - 1;

namespace hnsw {
static constexpr uint M_DEF = 6;
static constexpr uint M_MAX = 127;
static constexpr uint M_MIN = 3;

static inline bool validate_index_option_m(const uint option) {
  return option <= M_MAX && option >= M_MIN;
}

static constexpr uint EF_CONSTRUCTION_DEF = 10;
static constexpr uint EF_CONSTRUCTION_MAX = 1000;
static constexpr uint EF_CONSTRUCTION_MIN = 5;

static inline bool validate_index_option_ef_construction(const uint option) {
  return option >= EF_CONSTRUCTION_MIN && option <= EF_CONSTRUCTION_MAX;
}

/*
  Cold-start defaults of the two online-learned search-quality params.
  Single source of truth for three coupled initializers that must stay
  identical:
    - MHNSW_Share::ef_power / diameter        (vidx_hnsw_ctx.h)
    - TABLE_SHARE::hlindex_learned_*          (sql/table.h, survives resets)
    - Vidx_stats::last_*_init                 (vidx_stats.h, observability)
*/
static constexpr double EF_POWER_DEF = 0.6;
static constexpr float DIAMETER_DEF = 0.0f;
}  // namespace hnsw

/* Strict '<': METRIC_MAX is the nullptr sentinel, not a valid metric. */
static inline bool validate_index_option_distance(const uint option) {
  return option < METRIC_MAX;
}

static inline uint32_t get_dimensions_low(const uint32_t length,
                                          const uint32_t precision) {
  if (length % precision > 0) {
    return UINT_MAX32;
  }
  return length / precision;
}

/**
  Convert a VECTOR binary representation (packed float32 values, little-endian
  by storage layout) into a textual JSON-array form like "[v1,v2,...]".

  Implementation lives in sql/vidx/vidx_func.cc; declared here so non-vidx
  modules (e.g. AI functions) can recognize VECTOR column data and convert
  it to the parseable text form.

  @param input      Binary vector buffer (input->length() must be a multiple
                    of @c precision).
  @param precision  Bytes per dimension (== VECTOR_PRECISION).
  @param cs         Target charset for the produced text.
  @param output     Out: textual JSON-array vector.
  @retval false     Success.
  @retval true      Input length is not a multiple of @c precision.
*/
bool from_vector_to_string(String *input, const uint32_t precision,
                           CHARSET_INFO *cs, String *output);
}  // namespace vidx

#endif /* VIDX_COMMON_INCLUDED */
