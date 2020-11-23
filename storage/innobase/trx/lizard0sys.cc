/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file trx/lizard0sys.cc
 Lizard system implementation.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/

#include "fsp0fsp.h"
#include "mtr0mtr.h"

#include "lizard0dict.h"
#include "lizard0fil.h"
#include "lizard0fsp.h"
#include "lizard0scn.h"
#include "lizard0sys.h"

namespace lizard {

/** The global lizard system memory structure */
lizard_sys_t *lizard_sys = nullptr;

/** Create lizard system structure. */
void lizard_sys_create() {
  ut_ad(lizard_sys == nullptr);

  lizard_sys = static_cast<lizard_sys_t *>(ut::zalloc(sizeof(*lizard_sys)));

  /** Placement new SCN object  */
  new (&(lizard_sys->scn)) SCN();

  /** Attention: it's monitor metrics, didn't promise accuration */
  lizard_sys->txn_undo_log_free_list_len = 0;
  lizard_sys->txn_undo_log_cached = 0;

  return;
}

/** Close lizard system structure. */
void lizard_sys_close() {
  if (lizard_sys != nullptr) {
    lizard_sys->scn.~SCN();
    ut::free(lizard_sys);
    lizard_sys = nullptr;
  }
}

/** Get the address of lizard system header */
lizard_sysf_t *lizard_sysf_get(mtr_t *mtr) {
  buf_block_t *block = nullptr;
  lizard_sysf_t *hdr;
  ut_ad(mtr);

  block = buf_page_get(
      page_id_t(dict_lizard::s_lizard_space_id, LIZARD_SYS_PAGE_NO),
      univ_page_size, RW_X_LATCH, UT_LOCATION_HERE, mtr);

  hdr = LIZARD_SYS + buf_block_get_frame(block);
  return hdr;
}

/** Init the elements of lizard system */
void lizard_sys_init() {
  ut_ad(lizard_sys);
  lizard_sys->scn.init();
}

/** Create a new file segment special for lizard system
@param[in]      mtr */
static void lizard_create_sysf(mtr_t *mtr) {
  lizard_sysf_t *lzd_hdr;
  buf_block_t *block;
  page_t *page;
  byte *ptr;
  ut_ad(mtr);

  mtr_x_lock_space(fil_space_get_lizard_space(), mtr);

  block = fseg_create(dict_lizard::s_lizard_space_id, 0,
                      LIZARD_SYS + LIZARD_SYS_FSEG_HEADER, mtr);

  ut_a(block->page.id.page_no() == LIZARD_SYS_PAGE_NO);
  page = buf_block_get_frame(block);

  lzd_hdr = lizard_sysf_get(mtr);

  /* The scan number is counting from 1 */
  mach_write_to_8(lzd_hdr + LIZARD_SYS_SCN, 1);

  ptr = lzd_hdr + LIZARD_SYS_NOT_USED;

  memset(ptr, 0, UNIV_PAGE_SIZE - FIL_PAGE_DATA_END + page - ptr);

  mlog_log_string(lzd_hdr, UNIV_PAGE_SIZE - FIL_PAGE_DATA_END + page - lzd_hdr,
                  mtr);
  ib::info(ER_LIZARD) << "Initialize lizard system";
}

/** Create lizard system pages within system tablespace */
void lizard_create_sys_pages() {
  mtr_t mtr;

  mtr_start(&mtr);

  /* Create a new file segment special for lizard system */
  lizard_create_sysf(&mtr);

  mtr_commit(&mtr);
}

}  // namespace lizard
