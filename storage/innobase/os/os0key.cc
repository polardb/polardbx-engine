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

#include "os0key.h"
#include <stdlib.h>
#include <sstream>
#include "mysql/service_mysql_keyring.h"
#include "os0enc.h"

#include "ut0mutex.h"

/** If early-load-plugin is keyring_rds.so */
bool is_keyring_rds = false;

/** Fetch current master key id from keyring_rds plugin.
@param[in]    lock_master_key_id      if protect s_master_key_id
@return true if success */
bool rds_get_master_key_id(bool lock_master_key_id) {
#ifndef UNIV_HOTBACKUP
  int ret;
  size_t key_len;
  char *key_type = nullptr;
  char *master_key_id = nullptr;
  extern ib_mutex_t master_key_id_mutex;
  bool result = true;

  if (lock_master_key_id) {
    mutex_enter(&master_key_id_mutex);
  }

  /* We call key ring API to get master key id here. */
  ret = my_key_fetch(nullptr, &key_type, nullptr,
                     reinterpret_cast<void **>(&master_key_id), &key_len);

  if (ret != 0 || master_key_id == nullptr) {
    ib::error(ER_TDE_GET_MASTER_KEY_ID) << "can't get master key id, please "
                                        << "generate one first.";
    result = false;
    goto END;
  }

#ifdef UNIV_ENCRYPT_DEBUG
  {
    std::ostringstream msg;
    ut_print_buf(msg, *master_key_id, key_len);
    ib::info(ER_TDE_FETCH_MASTER_KEY_ID)
        << "Fetched new master key id: {" << msg.str() << "}";
  }
#endif /* UNIV_ENCRYPT_DEBUG */

  /* Fill uuid and seq */
  {
    static const size_t prefix_len = strlen(Encryption::MASTER_KEY_PREFIX);
    const char *uuid_p = master_key_id, *seq_p, *tmp;
    ulint seq;

    /* Skip prefix INNODBKey- */
    if (memcmp(uuid_p, Encryption::MASTER_KEY_PREFIX, prefix_len) == 0 &&
        uuid_p[prefix_len] == '-') {
      uuid_p += (prefix_len + 1);
    }

    /* Check uuid */
    seq_p = strrchr(uuid_p, '-');
    if (seq_p == nullptr ||
        (uint)(seq_p - uuid_p) > Encryption::SERVER_UUID_LEN) {
      ib::error(ER_TDE_FETCH_MASTER_KEY_ID)
          << "Fetched invalid new master key id: {" << master_key_id << "}";
      result = false;
      goto END;
    }

    *(char *)seq_p++ = 0;

    /* Check sequence, must be a number */
    tmp = seq_p;
    while (my_isdigit(system_charset_info, *tmp)) tmp++;
    if (*tmp || (seq = static_cast<ulint>(atoll(seq_p))) == 0) {
      ib::error(ER_TDE_FETCH_MASTER_KEY_ID)
          << "Fetched invalid new master key id: {" << master_key_id << "}";
      result = false;
      goto END;
    }

    /* Checking OK, got uuid and sequence */
    strcpy(Encryption::s_uuid, uuid_p);
    Encryption::s_master_key_id = seq;
  }

END:
  if (lock_master_key_id) {
    mutex_exit(&master_key_id_mutex);
  }

  if (key_type != nullptr) {
    my_free(key_type);
  }

  if (master_key_id != nullptr) {
    my_free(master_key_id);
  }

  return result;
#else
  return false;
#endif /* !UNIV_HOTBACKUP */
}

/** Create new master key for key rotation by keyring_rds.
@param[in,out]	master_key	master key */
void rds_create_master_key(byte **master_key) {
  if (rds_get_master_key_id(false)) {
    uint32_t unused;
    Encryption::get_master_key(&unused, master_key);
  } else {
    *master_key = nullptr;
  }
}
