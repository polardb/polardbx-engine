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

/** @file include/lizard0cleanout.h
 Lizard cleanout operation

 Created 2020-04-15 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0cleanout_h
#define lizard0cleanout_h

#include "trx0types.h"
#include "ut0mutex.h"
#include "rem0types.h"
#include "page0types.h"

#include "lizard0ut.h"

struct mtr_t;
struct dict_index_t;
struct page_zip_des_t;
struct txn_rec_t;

#ifdef UNIV_PFS_MUTEX
/* lizard undo hdr hash mutex PFS key */
extern mysql_pfs_key_t lizard_undo_hdr_hash_mutex_key;
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

/*----------------------------------------------------------------*/
namespace lizard {

/** Whether do the safe cleanout */
extern bool opt_cleanout_safe_mode;

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

void txn_undo_hash_init();

void txn_undo_hash_close();

/**
  Write redo log when updating scn and uba fileds in physical records.
  @param[in]      rec        physical record
  @param[in]      index      dict that interprets the row record
  @param[in]      txn_rec    txn info from the record
  @param[in]      mtr        mtr
*/
void btr_cur_upd_lizard_fields_clust_rec_log(const rec_t *rec,
                                             const dict_index_t *index,
                                             const txn_rec_t* txn_rec,
                                             mtr_t *mtr);

/**
  Parse the txn info from redo log record, and apply it if necessary.
  @param[in]      ptr        buffer
  @param[in]      end        buffer end
  @param[in]      page       page (NULL if it's just get the length)
  @param[in]      page_zip   compressed page, or NULL
  @param[in]      index      index corresponding to page

  @return         return the end of log record or NULL
*/
byte *btr_cur_parse_lizard_fields_upd_clust_rec(byte *ptr, byte *end_ptr,
                                                page_t *page,
                                                page_zip_des_t *page_zip,
                                                const dict_index_t *index);

}  // namespace lizard

#endif
