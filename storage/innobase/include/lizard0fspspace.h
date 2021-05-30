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

/** @file include/lizard0fspspace.h
 Special Zeus tablespace implementation.

 Created 2020-03-20 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0fspspace_h
#define lizard0fspspace_h

#include "api0api.h"

namespace lizard {

/** Judge wether it's lizard tablespace according to space id
@param[in]      space id
@return         true        yes */
extern bool fsp_is_lizard_tablespace(space_id_t space_id);

/** Init the lizard tablespace header when install db
@param[in]      size        tablespace inited size
@return         true        sucesss */
extern bool fsp_header_init_for_lizard(page_no_t size);

/** Get the size of lizard tablespace from header */
extern page_no_t fsp_header_get_lizard_tablespace_size(void);

}  // namespace lizard
#endif
