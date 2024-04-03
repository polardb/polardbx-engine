/*****************************************************************************

Copyright (c) 1995, 2019, Alibaba and/or its affiliates. All Rights Reserved.

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
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#ifndef os_key_h_
#define os_key_h_

#include "univ.i"

/** If early-load-plugin is keyring_rds.so */
extern bool is_keyring_rds;

/** Fetch current master key id from keyring_rds plugin.
@param[in]	lock_master_key_id	if protect s_master_key_id
@return true if success */
extern bool rds_get_master_key_id(bool lock_master_key_id);

/** Create new master key for key rotation by keyring_rds.
@param[in,out]	master_key	master key */
extern void rds_create_master_key(byte **master_key);

#endif
