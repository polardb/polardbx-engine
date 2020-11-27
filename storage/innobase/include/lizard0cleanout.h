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
#include "buf0types.h"

#include "lizard0undo0types.h"
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

/*----------------------------------------------------------------*/
/* Lizard cleanout structure and function. */
/*----------------------------------------------------------------*/

/** Whether disable the delayed cleanout when read */
extern bool opt_cleanout_disable;

/** Lizard max scan record count once cleanout one page.*/
extern ulint cleanout_max_scans_on_page;
/** Lizard max clean record count once cleanout one page.*/
extern ulint cleanout_max_cleans_on_page;

class Page {
 public:
  Page() : m_page_id(0, 0), m_index(nullptr) {}

  Page(const page_id_t &page_id, const dict_index_t *index);

  Page(const Page &);

  Page(const Page &&) = delete;

  Page &operator=(const Page &page);

  bool operator==(const Page &page) {
    if (m_index != nullptr && page.m_index != nullptr &&
        m_index == page.m_index && m_page_id == page.m_page_id)
      return true;

    return false;
  }

  const page_id_t &page() const { return m_page_id; }

  const dict_index_t *index() const { return m_index; }
 private:
  page_id_t m_page_id;
  const dict_index_t *m_index;
};

/** All the pages need to cleanout within pcur */
typedef std::vector<Page, ut::allocator<Page>> Pages;

/** Committed txn container */
typedef std::unordered_map<
    trx_id_t, txn_commit_t, std::hash<trx_id_t>, std::equal_to<trx_id_t>,
    ut::allocator<std::pair<const trx_id_t, txn_commit_t>>>
    Txn_commits;

/**
  Collected pages that will be cleanout soon.
 */
class Cleanout_pages {
 public:
  explicit Cleanout_pages()
      : m_pages(), m_txns(), m_page_num(0), m_txn_num(0), m_last_page() {}

  virtual ~Cleanout_pages();

  void init();

  bool is_empty();

  const Txn_commits *txns() const { return &m_txns; }

  ulint page_count() const { return m_page_num; }

  ulint txn_count() const { return m_txn_num; }

  template <typename Func>
  ulint iterate_page(Func &functor) {
    ulint cleaned = 0;
    for (auto it : m_pages) {
      cleaned += functor(it);
    }
    return cleaned;
  }

  /**
    Add the committed trx into hash map.
    @param[in]    trx_id
    @param[in]    trx_commit

    @retval       true        Add success
    @retval       false       Add failure
  */
  bool push_trx(trx_id_t trx_id, txn_commit_t txn_commit);
  /**
    Add the page which needed to cleanout into vector.

    @param[in]      page_id
    @param[in]      index
  */
  void push_page(const page_id_t &page_id, const dict_index_t *index);

 private:
  Pages m_pages;
  Txn_commits m_txns;
  ulint m_page_num;
  ulint m_txn_num;
  Page m_last_page;
};

}  // namespace lizard

#endif
