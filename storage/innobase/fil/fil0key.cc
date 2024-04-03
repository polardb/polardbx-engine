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

#include "fil0key.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "page0page.h"

/* Block concurrent rotate */
extern ib_mutex_t master_key_id_mutex;

/** Get master key id and uuid from the space header.
@param[in]      space_id        tablespace id
@param[out]     tbl_keys        map to store the master keys */
static void get_master_key(space_id_t space_id, Tbl_keys &tbl_keys) {
  fil_space_t *space = fil_space_acquire_silent(space_id);
  if (space == nullptr) return;

  ulint space_flags = space->flags;
  const page_size_t page_size(space_flags);
  buf_block_t *block;
  ulint offset;
  page_t *page, *ptr;
  char uuid[Encryption::SERVER_UUID_LEN + 1];
  uint32_t key_id;
  mtr_t mtr;

  mtr_start(&mtr);

  /* Save the encryption info to the page 0. */
  block = buf_page_get(page_id_t(space_id, 0), page_size, RW_S_LATCH,
                       UT_LOCATION_HERE, &mtr);
  if (block == nullptr) {
    mtr_commit(&mtr);
    fil_space_release(space);
    return;
  }

  buf_block_dbg_add_level(block, SYNC_FSP_PAGE);
  ut_ad(space_id == page_get_space_id(buf_block_get_frame(block)));

  offset = fsp_header_get_encryption_offset(page_size);
  ut_ad(offset != 0 && offset < UNIV_PAGE_SIZE);

  page = buf_block_get_frame(block);

  ptr = page + offset;

  /* Get master key id */
  key_id = mach_read_from_4(ptr + Encryption::MAGIC_SIZE);

  /* Get uuid */
  if (!memcmp(ptr, Encryption::KEY_MAGIC_V1, Encryption::MAGIC_SIZE)) {
    uuid[0] = 0;
  } else if (!memcmp(ptr, Encryption::KEY_MAGIC_V2, Encryption::MAGIC_SIZE)) {
    ptr += Encryption::MAGIC_SIZE;
    ptr += sizeof(uint32);
    if (mach_read_from_4(ptr) == 0) {
      ptr += sizeof(uint32);
    }
    memcpy(uuid, ptr, Encryption::SERVER_UUID_LEN);
    uuid[Encryption::SERVER_UUID_LEN] = 0;
  } else if (!memcmp(ptr, Encryption::KEY_MAGIC_V3, Encryption::MAGIC_SIZE)) {
    ptr += Encryption::MAGIC_SIZE;
    ptr += sizeof(uint32);
    memcpy(uuid, ptr, Encryption::SERVER_UUID_LEN);
    uuid[Encryption::SERVER_UUID_LEN] = 0;
  } else {
    mtr_commit(&mtr);
    fil_space_release(space);
    ut_ad(0);
    return;
  }

  mtr_commit(&mtr);
  fil_space_release(space);

  char key_name[Encryption::MASTER_KEY_NAME_MAX_LEN];
  snprintf(key_name, sizeof(key_name), "%s-%s-%u",
           Encryption::MASTER_KEY_PREFIX, uuid, key_id);
  tbl_keys.emplace(space->name, key_name);
}

/** Collect encrypted table space id.
@param[in]      space        table space
@param[out]     para         array to store id */
static void get_encrypted_tablespace(fil_space_t *space, void *para) {
  ut_ad(space != nullptr);
  ut_ad(para != nullptr);

  /* Skipped tables spaces */
  if (!space->can_encrypt() || fsp_is_system_or_temp_tablespace(space->id) ||
      fsp_is_undo_tablespace(space->id))
    return;

  (static_cast<std::vector<space_id_t> *>(para))->push_back(space->id);
}

/** Get all the master key id of encrypted tablespaces.
@param[out]   tbl_keys   Encryt tablespace & keys */
void fil_encryption_all_key(Tbl_keys &tbl_keys) {
  mutex_enter(&master_key_id_mutex);

  std::vector<space_id_t> array;
  Space_iterator::Function func = get_encrypted_tablespace;
  Space_iterator::for_each_space(&array, func);

  for (auto id : array) get_master_key(id, tbl_keys);

  mutex_exit(&master_key_id_mutex);
}
