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

#include "lizard0ut.h"
#include "lizard0xa0types.h"
#include "trx0types.h"
#include "trx0xa.h"

namespace lizard {
class Vision;
}

struct SYS_VAR;

/**
  The description of XA transaction within trx_t
*/
typedef struct xa_desc_t {
 public:
  xa_desc_t() : xid(), group() {}
  virtual ~xa_desc_t() { reset(); }

  void reset() { xid.null(); }

  bool is_null() const { return xid.is_null(); }

  const XID *my_xid() const { return &xid; }

  void build_group() { return; }

 private:
  /** Whether valid is determined by xid.is_null() */
  XID xid;

  /**
    For future:
      Building group in advance can improve the performance of collecting
    group_ids when open new vision;
  */
  std::string group;

} XAD;

/**
  Loop all the rw trx to find the same group transaction and
  push trx_id into group container.

  @param[in]    trx       current trx handler
  @param[in]    vision    current query view
*/
extern void vision_collect_trx_group_ids(const trx_t *my_trx,
                                         lizard::Vision *vision);

extern bool thd_get_transaction_group(THD *thd);

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

#define assert_xad_state_initial(trx) \
  do {                                \
    ut_a(trx->xad.is_null() == true); \
  } while (0)

#else

#define assert_xad_state_initial(trx)

#endif  // defined UNIV_DEBUG || defined LIZARD_DEBUG

namespace lizard {
namespace xa {
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
extern const XID *trx_slot_get_xa_xid_from_thd(THD *thd);

/**
  Find transactions in the finalized state by XID.

  @params[in] xid               XID
  @param[out] Transaction_info  Corresponding transaction info

  @retval     true if the corresponding transaction is found, false otherwise.
*/
bool trx_search_by_xid(const XID *xid, Transaction_info *info);
}  // namespace xa
}  // namespace lizard

#endif
