/*****************************************************************************

Copyright (c) 2013, 2021, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0xa.h
  Lizard XA transaction structure.

 Created 2021-08-10 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0xa_h
#define lizard0xa_h

#include <string>
#include <unordered_set>
#include "ut0guarded.h"
#include "ut0new.h"

#include "lizard0ut.h"

#include "trx0undo.h"


namespace lizard {
class Vision;
}

struct SYS_VAR;
struct Gtid_desc;
struct trx_t;

namespace lizard {

/**
 * Release the reference of the Xa Group for a given trx. Remove
 * it from trx_sys->xa_group_shards when the reference count is 0.
 * do nothing if xa_desc.is_group_null(), cause that the xa group might
 * have been released or even not exist(i.e. disabled).
 * @param[in]  trx  innodb transaction
 */
extern void trx_release_xa_group_if_need(trx_t *trx);

/**
 * Adds the transaction to Xa group if need. If transaction group is
 * disabled, trx->xa_desc.group() would be nullptr and this trx should not
 * be added to any xa group.
 * @param[in]  trx   The transaction assumed to not be in the xa_group yet
 */
extern void trx_add_to_xa_group_if_need(trx_t *trx);

/** Design transaction strategy for recovering if has xa specification. */
class XA_specification_strategy {
 public:
  XA_specification_strategy(const trx_t *trx);

  virtual ~XA_specification_strategy() {}

  /**
   * Judge if has gtid when recovery trx.
   *
   * @retval	true
   * @retval	false
   */
  bool has_gtid() const;

  /**
   * Judge storage way for gtid according to gtid source.
   */
  trx_undo_t::Gtid_storage decide_gtid_storage();

  /**
   * Overwrite gtid storage type of trx_undo_t when recovery.
   */
  void overwrite_gtid_storage(trx_t *trx);

  /** Fill gtid info from xa spec. */
  void get_gtid_info(Gtid_desc *gtid_desc);

  /**
   * Judge if has gcn when recovering or commiting detached xa trxs.
   *
   * @return true if has gcn, false otherwise
   */
  bool has_gcn() const;

  /**
   * Overwite GCN info in trx. There could be two cases:
   * 1. we are recovering the xa transaction
   * 2. we are commiting the detached xa transaction
   */
  void overwrite_xa(trx_t *trx) const;

 private:
  const trx_t *m_trx;
  XA_specification *m_xa_spec;
};

class Guard_xa_specification {
 public:
  Guard_xa_specification(trx_t *trx, XA_specification *xa_spec, bool prepare);

  virtual ~Guard_xa_specification();

 private:
  trx_t *m_trx;
  XA_specification *m_xa_spec;
};

/**
  Hash the XID to an integer.

  @params[in] in_xid   in_xid key

  @retval hash value.
*/
uint64_t hash_xid(const XID *in_xid);

/** Check validity of the XID status of the trx.
@param[in]      trx   innodb transaction
@return true if check successfully. */
extern bool trx_slot_check_validity(const trx_t *trx);

/** Get XID of an external xa from THD.
@param[in]      THD   thd
@return nullptr if no external xa. */
extern const XID *get_external_xid_from_thd(THD *thd);

/**
  Search detached prepare XA transaction info by XID. NOTES:
  Assume holding xid_state lock, can't happen parallel rollback or commit.

  @param[in]  XID   xid
  @param[out] info  XA trx info

  @return true if found.
          false if not found.
*/
extern bool trx_search_detach_prepare_by_xid(const XID *xid, MyXAInfo *info);

/**
  Search rollbacking trx in background by XID. If found, such a transaction is
  considered as ATTACHED.

  @param[in]  XID   xid
  @param[out] info  XA trx info

  @return true if found.
          false if not found.
*/
extern bool trx_search_rollback_background_by_xid(const XID *xid,
                                                  MyXAInfo *info);

/**
  Find transactions in the finalized state by XID.

  @params[in]   xid               XID
  @params[out]  info              Corresponding transaction info

  @retval     true if the corresponding transaction is found, false otherwise.
*/
extern bool trx_search_history_by_xid(const XID *xid, MyXAInfo *info,
                                      const slot_ptr_t slot_ptr_hint);

}  // namespace lizard

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

#define assert_xa_desc_state_initial(trx) \
  do {                                    \
    ut_a(trx->xa_desc.is_null() == true); \
  } while (0)

#else

#define assert_xa_desc_state_initial(trx)

#endif  // defined UNIV_DEBUG || defined LIZARD_DEBUG


#endif
