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
/** @file sql/pli/pli_noop.cc

  Definition of PolarDB-X License Interface noop functions

  Created 2025-04-02 by Zefeng.Liu
 *******************************************************/
#include "pli/pli.h"

namespace xlicense {

static bool verify_license_conns_constrains_noop(uint) { return true; }

static bool verify_license_BP_constrains_noop(long long &) { return true; }

static bool search_license_show_info_noop(License_show_info &) { return false; }

static bool check_specified_license_noop(const std::string &,
                                         License_show_info &, bool) {
  return false;
}

static PLI_service_t PLI_service_noop = {
    .verify_license_conns_constrains = verify_license_conns_constrains_noop,
    .verify_license_BP_constrains = verify_license_BP_constrains_noop,
    .search_license_show_info = search_license_show_info_noop,
    .check_specified_license = check_specified_license_noop,
};

PLI_service_t *PLI_service = &PLI_service_noop;

}  // namespace xlicense