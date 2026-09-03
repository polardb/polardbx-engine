/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file trx/lizard0gcs0cache.cc
  Global Commit System cache.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/

#include "lizard0gcs0cache.h"
#include "lizard0cmmt0cache.h"

namespace lizard {

/** Global variables for gcs cache. */
ulong srv_tcn_cache_level = GLOBAL_LEVEL;
ulong srv_tcn_block_cache_type = BLOCK_LRU;
bool srv_tcn_cache_replace_after_commit = true;
longlong srv_tcn_cache_size = 0;
const longlong srv_tcn_cache_def_size = 0;
const longlong srv_tcn_cache_max_size = LLONG_MAX;
/** Percentage of tcn cache entries used as the tid window for guessing. */
ulint srv_tcn_cache_guess_ratio = 95;
/** Number of entries of the global tcn cache, it's derived from
[srv_buf_pool_size, srv_tcn_cache_size] within innodb_init_params(). */
ulint srv_tcn_cache_entries = 0;

/** Calculate the number of entries of the global tcn cache.
 *
 * @param[in]		buf_pool_size	buffer pool size in bytes
 * @param[in]		tcn_cache_size	tcn cache size in bytes, zero indicates
 *				to use the mapping policy of the buffer pool size
 *
 * @retval		tcn entry number. */
ulint gcs_cache_calc_entries(longlong buf_pool_size, longlong tcn_cache_size) {
  if (srv_tcn_cache_level == NONE_LEVEL) return 0;

  /** Mapping rules between buffer pool size and the number of entries in global
   * tcn cache*/
  static std::map<longlong, ulong> buf_2_tcn = {
      {4LL * 1024 * 1024 * 1024, 1 * 1024 * 1024},
      {8LL * 1024 * 1024 * 1024, 2 * 1024 * 1024},
      {16LL * 1024 * 1024 * 1024, 4 * 1024 * 1024},
      {32LL * 1024 * 1024 * 1024, 8 * 1024 * 1024},
      {64LL * 1024 * 1024 * 1024, 16 * 1024 * 1024},
      {LLONG_MAX, 32 * 1024 * 1024}};

  if (tcn_cache_size == 0) {
    return buf_2_tcn.upper_bound(buf_pool_size)->second;
  } else {
    return tcn_cache_size / sizeof(tcn_t);
  }
}

}  // namespace lizard
