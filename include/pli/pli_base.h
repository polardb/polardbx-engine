/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

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
/** @file include/pli/pli_base.h

  Base Definitions of PolarDB-X License Interface.

  Created 2025-04-02 by Zefeng.Liu
 *******************************************************/

#ifndef PLI_PLI_BASE_H_INCLUDED
#define PLI_PLI_BASE_H_INCLUDED
#include "pli/pli_common.h"

namespace xlicense {

/**
 * Verify license conns constrains
 * @param[in] current_conn current connections
 * @return true if pass
 */
typedef bool (*verify_license_conns_constrains_func)(uint current_conn);

/**
 * Verify license BP constrains
 * @param[in/out] srv_buf_pool_size server buffer pool size
 * @return true if pass, false the srv_buf_pool_size will be adjusted
 */
typedef bool (*verify_license_BP_constrains_func)(long long &srv_buf_pool_size);

/**
 * Search current license show info
 * @param[out] license_show_info license show info
 * @return true if valid
 */
typedef bool (*search_license_show_info_func)(
    License_show_info &license_show_info);

/**
 * Check specified license and return the license show info
 * @param[in] file_path license file path
 * @param[out] show_info license show info
 * @param[in] ignore_error ignore error
 * @return true if valid
 */
typedef bool (*check_specified_license_func)(const std::string &file_path,
                                             License_show_info &show_info,
                                             bool ignore_error);

struct PLI_service_t {
  verify_license_conns_constrains_func verify_license_conns_constrains;
  verify_license_BP_constrains_func verify_license_BP_constrains;
  search_license_show_info_func search_license_show_info;
  check_specified_license_func check_specified_license;
};

extern PLI_service_t *PLI_service;

}  // namespace xlicense
#endif