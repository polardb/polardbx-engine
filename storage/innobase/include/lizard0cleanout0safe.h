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

/** @file include/lizard0cleanout0safe.h
 Lizard cleanout safe mode

 Created 2020-04-15 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0cleanout0safe_h
#define lizard0cleanout0safe_h

#include "univ.i"
#include "trx0types.h"

#include "lizard0ut.h"

/**
   Lizard Cleanout Safe Mode

   When setting on safe mode, innodb will maintain the allocated txn undo
   log segment hash table, then before cleanout, it will search the hash
   table by [space_id + page_no] which is interpreted through UBA, if hit,
   then will continue, other than give up and assign a fake SCN number.
*/

struct mtr_t;
struct dict_index_t;
struct page_zip_des_t;
struct txn_rec_t;
struct btr_pcur_t;
struct trx_undo_t;
struct trx_rseg_t;

#ifdef UNIV_PFS_MUTEX
/* lizard undo hdr hash mutex PFS key */
extern mysql_pfs_key_t undo_hdr_hash_mutex_key;
#endif

/** Undo log header page as key */
using Undo_hdr_key = std::pair<space_id_t, page_no_t>;

struct Undo_hdr {
  space_id_t space_id;
  page_no_t page_no;
};

namespace std {
template <>
struct hash<Undo_hdr_key> {
 public:
  typedef Undo_hdr_key argument_type;
  typedef size_t result_type;
  size_t operator()(const Undo_hdr_key &p) const;
};
}  // namespace std

/** Compare function */
class Undo_hdr_equal {
 public:
  bool operator()(const Undo_hdr_key &lhs, const Undo_hdr_key &rhs) const;
};

/** Hash table of undo log header pages */
typedef std::unordered_map<Undo_hdr_key, bool, std::hash<Undo_hdr_key>,
                           Undo_hdr_equal,
                           ut::allocator<std::pair<const Undo_hdr_key, bool>>>
    Undo_hdr_hash;


namespace lizard {

/** Whether do the safe cleanout */
extern bool opt_cleanout_safe_mode;

/**------------- Page Cleanout is deprecated. --------------*/
// extern ulong cleanout_mode;
// enum cleanout_mode_enum { CLEANOUT_BY_CURSOR, CLEANOUT_BY_PAGE };
/** Lizard max scan record count once cleanout one page.*/
// extern ulint cleanout_max_scans_on_page;
/** Lizard max clean record count once cleanout one page.*/
// extern ulint cleanout_max_cleans_on_page;
/**------------- Page Cleanout is deprecated. --------------*/

/**
  Put txn undo into hash table.

  @param[in]      undo      txn undo memory structure.
*/
void txn_undo_hash_insert(trx_undo_t *undo);

/**
  Put all the undo log segment into hash table include active undo,
  cached undo, history list, free list.

  @param[in]      space_id      rollback segment space
  @param[in]      rseg_hdr      rollback segment header page
  @param[in]      rseg          rollback segment memory object
  @param[in]      mtr           mtr that hold the rseg hdr page
*/
void trx_rseg_init_undo_hdr_hash(space_id_t space_id, trx_rsegf_t *rseg_hdr,
                                 trx_rseg_t *rseg, mtr_t *mtr);

/** Undo log segments */
class Undo_logs {
 public:
  Undo_logs();
  virtual ~Undo_logs();

  bool insert(Undo_hdr hdr);
  bool exist(Undo_hdr hdr);

 private:
  ib_mutex_t m_mutex;
  Undo_hdr_hash m_hash;
};

#define TXN_UNDO_HASH_PARTITIONS 64

/** Global txn undo logs container */
extern Partition<Undo_logs, Undo_hdr, TXN_UNDO_HASH_PARTITIONS> *txn_undo_logs;

extern void txn_undo_hash_init();

extern void txn_undo_hash_close();

}  // namespace lizard
#endif
