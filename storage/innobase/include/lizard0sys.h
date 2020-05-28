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

/** @file include/lizard0sys.h
 Lizard system implementation.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0sys_h
#define lizard0sys_h

#include "fsp0types.h"
#include "lizard0scn.h"
#include "trx0types.h"

struct mtr_t;

/** Lizard system file segment header */
typedef byte lizard_sys_fseg_t;

/** Lizard system header:
    it begins from LIZARD_SYS, and includes file segment header. */
typedef byte lizard_sysf_t;

/** Lizard system header */
/**-----------------------------------------------------------------------*/

/** The offset of lizard system header on the file segment page */
#define LIZARD_SYS FSEG_PAGE_DATA

/** The scn number which is stored here, it occupied 8 bytes */
#define LIZARD_SYS_SCN 0

/** The purge scn number which is stored here, it occupied 8 bytes */
#define LIZARD_SYS_PURGE_SCN (LIZARD_SYS_SCN + 8)

/** The offset of file segment header */
#define LIZARD_SYS_FSEG_HEADER (LIZARD_SYS_PURGE_SCN + 8)

/** The start of not used */
#define LIZARD_SYS_NOT_USED (LIZARD_SYS_FSEG_HEADER + FSEG_HEADER_SIZE)

/** The page number of lizard system header in lizard tablespace */
#define LIZARD_SYS_PAGE_NO 3

namespace lizard {

/** The memory structure of lizard system */
struct lizard_sys_t {
  /** The global scn number which is total order. */
  SCN scn;
};

/** Create lizard system structure. */
void lizard_sys_create();

/** Close lizard system structure. */
void lizard_sys_close();

/** Init the elements of lizard system */
void lizard_sys_init();

/** Get the address of lizard system header */
extern lizard_sysf_t *lizard_sysf_get(mtr_t *mtr);

/** Create lizard system pages within lizard tablespace */
extern void lizard_create_sys_pages();

/** GLobal lizard system */
extern lizard_sys_t *lizard_sys;

}  // namespace lizard

#endif  // lizard0sys_h define
