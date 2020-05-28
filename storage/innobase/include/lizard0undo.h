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

/** @file include/lizard0undo.h
  Lizard transaction undo and purge types.

 Created 2020-04-02 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0undo_h
#define lizard0undo_h

#include "trx0types.h"
#include "trx0undo.h"

#include "lizard0undo0types.h"

#include <atomic>
#include <queue>
#include <set>
#include <vector>

struct trx_rseg_t;
struct trx_undo_t;

/**
  Lizard transaction system undo format:

  At the end of undo log header history node:

  8 bytes     SCN number
  8 bytes     UTC time

  Those two option will be included into all INSERT/UPDATE/TXN undo
  log header.


  Start from undo log old header, txn_undo will be different with trx_undo:

  1) txn undo : flag + reserved space

  2) trx undo : XA + GTID

  As the optional info, those will be controlled by TRX_UNDO_FLAGS.

     0x01 TRX_UNDO_FLAG_XID
     0x02 TRX_UNDO_FLAG_GTID
     0x80 TRX_UNDO_FLAG_TXN
*/

/** Those will only exist in txn undo log header*/
/*-------------------------------------------------------------*/
/* Random magic number */
#define TXN_UNDO_LOG_EXT_MAGIC (TRX_UNDO_LOG_OLD_HDR_SIZE)
/* Flag how to use reserved space */
#define TXN_UNDO_LOG_EXT_FLAG (TXN_UNDO_LOG_EXT_MAGIC + 4)
/* Unused space */
#define TXN_UNDO_LOG_EXT_RESERVED (TXN_UNDO_LOG_EXT_FLAG + 1)
/* Unused space size */
#define TXN_UNDO_LOG_EXT_RESERVED_LEN 200
/* txn undo log header size */
#define TXN_UNDO_LOG_EXT_HDR_SIZE \
  (TXN_UNDO_LOG_EXT_RESERVED + TXN_UNDO_LOG_EXT_RESERVED_LEN)
/*-------------------------------------------------------------*/

static_assert(TXN_UNDO_LOG_EXT_HDR_SIZE == TRX_UNDO_LOG_GTID_HDR_SIZE,
              "txn and trx undo log header size must be equal!");

/** Pls reuse the reserved space */
static_assert(TXN_UNDO_LOG_EXT_HDR_SIZE == 267,
              "txn undo log header size cann't change!");

/** txn magic number */
#define TXN_MAGIC_N 91118498

namespace lizard {

/** Initial value of undo ptr  */
constexpr undo_ptr_t UNDO_PTR_NULL = std::numeric_limits<undo_ptr_t>::min();

/** Fake UBA for internal table */
constexpr undo_ptr_t UNDO_PTR_FAKE = (undo_ptr_t)1 << 55;

/* Lizard transaction undo header operation */
/*-----------------------------------------------------------------------------*/
#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/**
  Validate the page is undo page

  @param[in]      page      undo page
  @return         true      it's undo page
*/
bool trx_undo_page_validation(const page_t *page);

#endif  // UNIV_DEBUG || LIZARD_DEBUG

/**
  Initial the NULL value on SCN and UTC when create undo log header.
  include all kinds of undo log header type.
  The redo log logic is included in "MLOG_UNDO_HDR_CREATE";

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/

extern void trx_undo_hdr_init_scn(trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Write the scn and utc when commit.
  Include the redo log

  @param[in]      log_hdr       undo log header
  @param[in]      commit_scn    commit scn number
  @param[in]      mtr           current mtr context
*/
extern void trx_undo_hdr_write_scn(trx_ulogf_t *log_hdr,
                                   std::pair<scn_t, utc_t> &cmmt_scn,
                                   mtr_t *mtr);

/**
  Read the scn and utc.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
extern std::pair<scn_t, utc_t> trx_undo_hdr_read_scn(trx_ulogf_t *log_hdr,
                                                     mtr_t *mtr);

/**
  Add the space for the txn especially.

  @param[in]      undo_page     undo log header page
  @param[in]      log_hdr       undo log hdr
  @param[in]      mtr
*/
extern void trx_undo_hdr_add_space_for_txn(page_t *undo_page,
                                           trx_ulogf_t *log_hdr, mtr_t *mtr);
/**
  Init the txn extension information.

  @param[in]      undo_page     undo log header page
  @param[in]      log_hdr       undo log hdr
  @param[in]      mtr
*/
void trx_undo_hdr_init_for_txn(page_t *undo_page, trx_ulogf_t *log_hdr,
                               mtr_t *mtr);

/*-----------------------------------------------------------------------------*/

}  // namespace lizard


#define TXN_DESC_NULL \
  { lizard::UNDO_PTR_NULL, lizard::SCN_NULL }


#if defined UNIV_DEBUG || defined LIZARD_DEBUG

#define lizard_trx_undo_page_validation(page)     \
  do {                                            \
    ut_a(lizard::trx_undo_page_validation(page)); \
  } while (0)

#else

#define lizard_trx_undo_page_validation(page)

#endif

#endif  // lizard0undo_h
