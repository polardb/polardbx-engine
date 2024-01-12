/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file fil/lizard0fil.cc
 Special lizard tablespace file implementation.

 Created 2020-03-20 by Jianwei.zhao
 *******************************************************/

#include "fil0fil.h"
#include "lizard0dict.h"
#include "lizard0fsp.h"

/** Zeus tablespace */
fil_space_t *fil_space_t::s_lizard_space = nullptr;

/** Check if the name is an lizard tablespace name.
@param[in]	name		Tablespace name
@return true if it is an lizard tablespace name */
bool Fil_path::is_lizard_tablespace_name(const std::string &name) {
  if (name.empty()) return false;

  std::string filename(name);
  std::size_t lizard_length =
      strlen(lizard::dict_lizard::s_lizard_space_file_name);

  auto pos = filename.find_last_of(SEPARATOR);
  if (pos != std::string::npos) {
    return filename.compare(pos + 1, lizard_length,
                            lizard::dict_lizard::s_lizard_space_file_name) == 0
               ? true
               : false;
  }

  return false;
}

namespace lizard {

/** Get the lizard tablespace directly */
fil_space_t *fil_space_get_lizard_space() {
  ut_ad(fil_space_t::s_lizard_space != nullptr);
  return fil_space_t::s_lizard_space;
}

} /* namespace lizard */
