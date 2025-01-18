/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/lizard0handler.h

  Transaction coordinator recovery xa specification.

  Created 2023-06-14 by Jianwei.zhao
 *******************************************************/

#ifndef SQL_LIZARD0HANDLER_H
#define SQL_LIZARD0HANDLER_H

#include "sql/mem_root_allocator.h"
#include "sql/xa.h"
#include "sql/xa/lizard_xa_trx.h"

#include "sql/xa_specification.h"
#include "sql_string.h"

#include "sql/package/gpp_stat.h"
#include "sql/lizard/lizard_service.h"

class THD;

namespace im {
struct Undo_purge_show_result;
}  // namespace im

class XA_spec_list {
 public:
  /** Internal XA spec structure. */
  using Commit_pair = std::pair<const my_xid, XA_specification *>;
  using Commit_map = std::map<my_xid, XA_specification *, std::less<my_xid>,
                              Mem_root_allocator<Commit_pair>>;

  /** External XA spec structure. */
  using State_pair = std::pair<const XID, XA_specification *>;
  using State_map = std::map<XID, XA_specification *, std::less<XID>,
                             Mem_root_allocator<State_pair>>;

 public:
  explicit XA_spec_list(MEM_ROOT *mem_root)
      : m_mem_root(mem_root),
        m_commit_alloc(m_mem_root),
        m_commit_map(m_commit_alloc),
        m_state_alloc(m_mem_root),
        m_state_map(m_state_alloc) {}

  void add(const my_xid xid, XA_specification *spec) {
    m_commit_map[xid] = spec;
  }
  void add(const XID &xid, XA_specification *spec) { m_state_map[xid] = spec; }

  XA_specification *find(const my_xid xid) {
    auto found = m_commit_map.find(xid);
    if (found != m_commit_map.end()) {
      return found->second;
    }
    return nullptr;
  }

  XA_specification *find(const XID &xid) {
    auto found = m_state_map.find(xid);
    if (found != m_state_map.end()) {
      return found->second;
    }
    return nullptr;
  }

  Commit_map *commit_map() { return &m_commit_map; }
  State_map *state_map() { return &m_state_map; }

  void clear() {
    m_commit_map.clear();
    m_state_map.clear();
  }

 private:
  MEM_ROOT *m_mem_root;

  Mem_root_allocator<Commit_pair> m_commit_alloc;
  Commit_map m_commit_map;

  Mem_root_allocator<State_pair> m_state_alloc;
  State_map m_state_map;
};

/**------------------------------------------------------------*/
/** Extension interface of handler singleton. */
/**------------------------------------------------------------*/

typedef void (*register_xa_attributes_t)(THD *thd);

typedef bool (*register_xa_group_t)(THD *thd);

typedef gcn_t (*load_gcn_t)();
typedef scn_t (*load_scn_t)();

typedef bool (*snapshot_scn_too_old_t)(scn_t scn, bool flashback_area);
typedef bool (*snapshot_gcn_too_old_t)(gcn_t gcn, bool flashback_area);
typedef void (*set_gcn_if_bigger_t)(gcn_t gcn);

typedef bool (*start_trx_for_xa_t)(handlerton *hton, THD *thd, bool rw);
typedef bool (*start_trx_for_gu_t)(handlerton *hton, THD *thd);
typedef bool (*assign_trans_slot_t)(THD *thd, slot_ptr_t *slot_ptr,
                                    trx_id_t *trx_id);

typedef bool (*search_trx_by_xid_t)(const XID *xid, MyXAInfo *info);

typedef bool (*search_trx_by_xid_with_hint_t)(const XID *xid, MyXAInfo *info,
                                              const slot_ptr_t slot_ptr_hint);

typedef int (*convert_timestamp_to_scn_t)(THD *thd, utc_t utc,
                                          scn_t *scn);

typedef void (*trunc_status_t)(std::vector<lizard::trunc_status_t> &array);
typedef void (*purge_status_t)(lizard::purge_status_t &status);

typedef void (*flush_gpp_stat_t)();

typedef void (*flush_cleanout_stat_t)();

typedef bool (*trx_slot_check_retention_t)();

typedef bool (*has_started_mysql_trx_t)();

template <typename T>
using search_up_limit_tid_t = trx_id_t (*)(const T *lhs);

namespace lizard {
class Snapshot_scn_vision;
class Snapshot_gcn_vision;
}  // namespace lizard

/** Extension structure of handlerton */
struct handlerton_ext {
  register_xa_attributes_t register_xa_attributes;
  register_xa_group_t register_xa_group;
  load_gcn_t load_gcn;
  load_scn_t load_scn;
  snapshot_scn_too_old_t snapshot_scn_too_old;
  snapshot_gcn_too_old_t snapshot_assigned_gcn_too_old;
  snapshot_gcn_too_old_t snapshot_automatic_gcn_too_old;
  set_gcn_if_bigger_t set_gcn_if_bigger;
  start_trx_for_xa_t start_trx_for_xa;
  start_trx_for_gu_t start_trx_for_gu;
  assign_trans_slot_t assign_trans_slot;
  search_trx_by_xid_t search_detach_prepare_trx_by_xid;
  search_trx_by_xid_t search_rollback_background_trx_by_xid;
  search_trx_by_xid_with_hint_t search_history_trx_by_xid;
  convert_timestamp_to_scn_t convert_timestamp_to_scn;
  search_up_limit_tid_t<lizard::Snapshot_scn_vision>
      search_up_limit_tid_for_scn;
  search_up_limit_tid_t<lizard::Snapshot_gcn_vision>
      search_up_limit_tid_for_gcn;
  trunc_status_t trunc_status;
  purge_status_t purge_status;
  flush_gpp_stat_t flush_gpp_stat;
  flush_cleanout_stat_t flush_cleanout_stat;
  trx_slot_check_retention_t trx_slot_check_retention;
  has_started_mysql_trx_t has_started_mysql_trx;
};
#endif
