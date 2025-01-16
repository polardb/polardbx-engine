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
/** @file include/pli/pli_common.h

  Definition of PolarDB-X License errors and show information.

  Created 2025-04-02 by Zefeng.Liu
 *******************************************************/

#ifndef PLI_PLI_COMMON_H_INCLUDED
#define PLI_PLI_COMMON_H_INCLUDED

#include <sys/types.h>
#include <string>
#include "my_time.h"

namespace xlicense {

enum class License_error_t {
  LICENSE_OK, /** No error */

  LICENSE_FILE_NOT_EXIST, /** License file not exist */
  LICENSE_FILE_CORRUPTED, /** License file corrupted */

  LICENSE_FORMAT_UNRECOGNIZED,  /** The format of license is not supported */
  LICENSE_TYPE_UNSUPPORT,       /** The type of license is not supported */
  LICENSE_EXPIRED,              /** The license has been expired */
  LICENSE_UNAUTHORIZED_MACHINE, /** The server is unauthorized to use */

  LICENSE_TOO_MUCH_CONNS, /** Exceed the limit of conns for current license */
  LICNESE_UNPERMITTED_BP_SIZE, /** Exceed the limit of BP for current license */
};

enum class License_type_t {
  LICENSE_TYPE_STANDARD = 0, /** License for normal server */
  LICENSE_TYPE_UNSUPPORT = 9999,
};

/** License information to show to user. */
struct License_show_info {
  License_show_info()
      : license_file_path(),
        apply_time(),
        expire_time(),
        license_type(License_type_t::LICENSE_TYPE_UNSUPPORT),
        error(License_error_t::LICENSE_FILE_NOT_EXIST),
        conns_constrains(0),
        BP_constrains(0) {}

  std::string license_file_path;
  MYSQL_TIME apply_time;
  MYSQL_TIME expire_time;
  License_type_t license_type;
  License_error_t error; /** Tell the status of license file */

  uint64_t conns_constrains;  // max supported connections
  uint64_t BP_constrains;     // max supported buffer pool size in bytes
};

}  // namespace xlicense

#endif