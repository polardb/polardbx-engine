/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0txn0slot.h
  Lizard transactional slot management.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn0slot_h
#define lizard0txn0slot_h

#include "trx0types.h"
#include "sql/xa.h"
#include "buf0buf.h"

#include "lizard0txn0rec0types.h"
#include "lizard0txn0slot0types.h"

/** Transaction slot structure. */
struct txn_slot_t {
 public:
  /** txn magic number */
  static constexpr ulint MAGIC_N = 91118498;

  /* States of an txn undo log header */
  static constexpr ulint ACTIVE = 1;
  static constexpr ulint COMMITED = 2;
  static constexpr ulint PURGED = 3;

  /* 2PC Purge done state */
  static constexpr ulint ERASED = 4;

 public:
  explicit txn_slot_t()
      : image(),
        slot_ptr(0),
        trx_id(0),
        magic_n(0),
        prev_image(),
        state(0),
        xes_storage(0),
        tags(0),
        is_2pp(false),
        pmmt(),
        branch(),
        maddr() {}

  explicit txn_slot_t(commit_mark_t image_arg, slot_ptr_t slot_ptr_arg,
                      trx_id_t trx_id_arg, ulint magic_n_arg,
                      commit_mark_t prev_image_arg, ulint state_arg,
                      uint8_t xes_storage_arg, ulint tags_arg, bool is_2pp_arg,
                      proposal_mark_t pmmt_arg, xa_branch_t branch_arg,
                      xa_addr_t addr_arg)
      : image(image_arg),
        slot_ptr(slot_ptr_arg),
        trx_id(trx_id_arg),
        magic_n(magic_n_arg),
        prev_image(prev_image_arg),
        state(state_arg),
        xes_storage(xes_storage_arg),
        tags(tags_arg),
        is_2pp(is_2pp_arg),
        pmmt(pmmt_arg),
        branch(branch_arg),
        maddr(addr_arg) {}

  /** commit image in txn undo header */
  commit_mark_t image;
  /** slot address */
  slot_ptr_t slot_ptr;
  /** current trx who own the txn header */
  trx_id_t trx_id;
  /** A magic number, check if the page is corrupt */
  ulint magic_n;
  /* Previous scn/utc of the trx who used the same TXN */
  commit_mark_t prev_image;
  /** txn undo header state: txn_slot_t::ACTIVE, txn_slot_t::COMMITED,
  or txn_slot_t::PURGED */
  ulint state;
  /** A flag determining how to explain the txn extension */
  uint8_t xes_storage;
  /** flags of the TXN. For example: 0x01 means rollback. */
  ulint tags;
  /** true if the TXN is two phase purge. */
  bool is_2pp;
  /** AC XA PMMT info */
  proposal_mark_t pmmt;
  /** XA branch count info */
  xa_branch_t branch;
  /** XA master branch info */
  xa_addr_t maddr;
  /** Return true if the transaction was eventually rolled back. */
  bool is_rollback() const;
  /** Return true if the txn has new_flags. */
  bool tags_allocated() const;
  bool ac_prepare_allocated() const;
  bool ac_commit_allocated() const;

  bool is_validate() const;
};

/** Transaction slot or tcn cache lookup result structure. */
struct txn_lookup_t {
 public:
  /** State of the looked up TXN. See txn_status_t in lizard0txn0slot0types.h
  for the meaning of each state and the TXN header life cycle. */
  using Status = txn_status_t;

 public:
  txn_lookup_t()
      : txn_slot(),
        real_image(),
        real_status(Status::ACTIVE),
        block(nullptr),
        m_do_ref_count(false) {}

  txn_lookup_t(bool do_ref_count)
      : txn_slot(),
        real_image(),
        real_status(Status::ACTIVE),
        block(nullptr),
        m_do_ref_count(do_ref_count) {}

  /** Initialize elements after lookup. */
  void init(const txn_slot_t &txn_slot_arg, const commit_mark_t &real_image_arg,
            const Status &real_status_arg) {
    txn_slot = txn_slot_arg;
    real_image = real_image_arg;
    real_status = real_status_arg;
#if defined UNIV_DEBUG
    if (real_status == Status::ACTIVE || real_status == Status::COMMITTED ||
        real_status == Status::PURGED || real_status == Status::ERASED) {
      if (real_status != Status::ACTIVE) {
        /** TXN reuse, current scn should be larger than prev scn */
        ut_a(txn_slot.image.scn > txn_slot.prev_image.scn);
      }
      ut_a(real_image == txn_slot.image);
    } else if (real_status == Status::REUSE) {
      ut_a(real_image == txn_slot.prev_image);
    }
#endif /* UNIV_DEBUG */
  }

  /** Judge record rollptr is valid through query type and status. */
  bool undo_missing(bool flashback_area) {
    bool valid;
    if (flashback_area && txn_slot.is_2pp) {
      valid = (real_status < Status::ERASED);
    } else {
      valid = (real_status < Status::PURGED);
    }
    return !valid;
  }

  /** Judge uba have been missing. */
  bool txn_missing() {
    if (real_status == Status::REUSE || real_status == Status::UNDO_INVALID)
      return true;

    return false;
  }

  bool was_slot_fixed() const { return block != nullptr; }

  bool is_do_ref_count() const { return m_do_ref_count; }

  void fix_slot_when_active(buf_block_t *block_arg) {
    /** Must be active status. */
    ut_ad(block_arg && real_status == Status::ACTIVE);
    if (m_do_ref_count) {
      buf_block_fix(block_arg);
      block = block_arg;
    }
  }

 public:
  /** The raw data in txn slot. */
  txn_slot_t txn_slot;
  /**
    If the txn is still existing:
      * real_state: [Status::ACTIVE, Status::COMMITTED, Status::PURGED]
      * real_image == txn_slot.image

    If the txn is non-existing:
      * real_state: [Status::REUSE]
      * real_image == txn_slot.prev_image

    If the txn is unexpectedly lost:
      * real_state: [Status::UNDO_INVALID]
      * real_image == {SCN_UNDO_INVALID, US_UNDO_INVALID}
  */
  commit_mark_t real_image;
  Status real_status;

  /** Fixed txn slot block. */
  buf_block_t *block;

 private:
  /** Whether fix txn slot buffer block. */
  bool m_do_ref_count;
};

namespace lizard {

/** Check txn slot interface. */
class Txn_slot_reuse_checker {
 public:
  virtual bool operator()(const trx_ulogf_t *log_hdr) const = 0;
  virtual ~Txn_slot_reuse_checker() = default;
};

/** Check txn slot through transaction id. */
class Txn_slot_reuse_by_tid_checker : public Txn_slot_reuse_checker {
 public:

  Txn_slot_reuse_by_tid_checker(const trx_id_t trx_id) : m_trx_id(trx_id) {}

  virtual bool operator()(const trx_ulogf_t *log_hdr) const override;

 private:
  const trx_id_t m_trx_id;
};

/** Check txn slot through xid. */
class Txn_slot_reuse_by_xid_checker : public Txn_slot_reuse_checker {
 public:

  Txn_slot_reuse_by_xid_checker(const XID *xid) : m_xid(xid) {}

  virtual bool operator()(const trx_ulogf_t *log_hdr) const override;

 private:
  const XID *m_xid;
};

/**
  Try to lookup the real scn of given record only once,

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care
  @param[in]      hint          Cache hint

  @return         pair          first: whether corresponding trx is active.
                                second: txn slot real status.
*/
std::pair<bool, txn_status_t> txn_slot_read_once(txn_rec_t *txn_rec,
                                                 txn_lookup_t *txn_lookup,
                                                 const Cache_hint hint);
/**
  Try to lookup the real scn of given record again with fixed block,

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care
  @param[in]      hint          Cache hint

  @return         pair          first: whether corresponding trx is active.
                                second: txn slot real status.
*/
std::pair<bool, txn_status_t> txn_slot_read_again(txn_rec_t *txn_rec,
                                                  txn_lookup_t *txn_lookup,
                                                  buf_block_t *block,
                                                  const Cache_hint hint);

/**
  Try to lookup the real scn of given record with related mtr,

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care
  @param[in]      hint          Cache hint

  @return         pair          first: whether corresponding trx is active.
                                second: txn slot real status.
*/
std::pair<bool, txn_status_t> txn_slot_read_couple(txn_rec_t *txn_rec,
                                                   txn_lookup_t *txn_lookup,
                                                   const Cache_hint hint,
                                                   mtr_t *mtr);

/**
  Check if the TXN is purged or erased. The latch of the TXN page will be held
  if precheck failed.

  @param[in/out]  txn_rec         txn_info of record
  @param[in]      flashback_area  true if it's a flashback area query
  @param[in]      txn_mtr         txn mtr

  @retval         true if txn has been purged (non flashback area) or
                  erased (flashback area)
*/
extern bool txn_slot_is_missing_history(txn_rec_t *txn_rec, bool flashback_area,
                                        mtr_t *txn_mtr);

/**
  Try to read TXN by only TXN slot address. The TXN slot might not be found.

  @param[in]      slot_ptr      TXN Slot address
  @param[in]      hint          Cache hint
  @param[in]      reuse_checker Check if the TXN slot is reused.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care

  @return   true if the expected TXN is found.
*/
extern bool txn_slot_read_guess(const slot_ptr_t slot_ptr, Cache_hint hint,
                                const Txn_slot_reuse_checker &reuse_checker,
                                txn_lookup_t *txn_lookup);

}  // namespace lizard

#endif
