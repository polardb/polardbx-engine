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

/** @file include/lizard0undo0types.h
  Lizard transaction undo and purge types.

 Created 2020-04-02 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0undo0types_h
#define lizard0undo0types_h

#include "trx0types.h"

#include "lizard0scn0types.h"

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


/** Those will exist all kinds of undo log header*/
/*-------------------------------------------------------------*/
/** Size of scn within undo log header */
#define TRX_UNDO_SCN_LEN 8

/** Size of UTC within undo log header */
#define TRX_UNDO_UTC_LEN 8
/*-------------------------------------------------------------*/

/** Undo block address (UBA) */
struct undo_addr_t {
  /* undo tablespace id */
  space_id_t space_id;
  /* undo log header page */
  page_no_t page_no;
  /* offset of undo log header */
  ulint offset;
  /* Commit scn number */
  scn_t scn;
  /* Active or Commit state */
  bool state;
};

typedef struct undo_addr_t undo_addr_t;

/**
  New record format will include SCN and UBA:

  1) Format of scn in record:

   64 bit     scn number (8 bytes);

  2) Format of undo log address in record:

   1  bit     active/commit state (0:active 1:commit)
   7  bit     undo space number (1-127)
   32 bit     page no (4 bytes)
   16 bit     Offset of undo log header (2 bytes)
*/

/** Undo log header address in record */
typedef ib_id_t undo_ptr_t;

/** Scn in record */
typedef scn_t scn_id_t;

/**
  The transaction description:

  It will be inited when allocate the first txn undo log
  header, and never change until transaction commit or rollback.
*/
struct txn_desc_t {
  /** undo log header address */
  undo_ptr_t undo_ptr;
  /** scn number */
  scn_id_t scn;
};

/**
  Lizard transaction attributes in record (used by Vision)
   1) trx_id
   2) scn
   3) undo_ptr
*/

struct txn_rec_t {
  /* trx id */
  trx_id_t trx_id;
  /** scn number */
  scn_id_t scn;
  /** undo log header address */
  undo_ptr_t undo_ptr;
};

/** The struct of transaction undo for UBA */
struct txn_undo_ptr_t {
  /** Rollback segment in txn space */
  trx_rseg_t *rseg;
  /* transaction undo log segment */
  trx_undo_t *txn_undo;
};

#endif  // lizard0undo0types_h
