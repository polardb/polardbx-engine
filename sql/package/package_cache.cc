/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "my_macros.h"
#include "mysql/components/services/bits/psi_bits.h"
#include "mysql/psi/mysql_memory.h"

#include "sql/ccl/ccl_proc.h"
#include "sql/keyring/keyring_current_key_id.h"
#include "sql/keyring/keyring_generate_key.h"
#include "sql/outline/outline_proc.h"
#include "sql/package/package.h"
#include "sql/package/package_common.h"
#include "sql/package/package_parse.h"
#include "sql/package/proc.h"
#include "sql/recycle_bin/recycle_proc.h"
#include "sql/sp_head.h"
#include "sql/trans_proc/implicit_savepoint.h"
#include "sql/trans_proc/returning.h"
#include "sql/tso/tso_proc.h"

#include "sql/consensus/consensus_proc.h"

#ifndef DBUG_OFF
#include "sql/package/proc_dummy.h"
#endif

#include "sql/package/show_native_procedure.h"

#include "sql/package/proc_undo_purge.h"
#include "sql/xa/lizard_xa_proc.h"
#include "sql/xrpc/xrpc_proc.h"

#include "sql/polarx_proc/changeset_proc.h"

#ifdef RDS_HAVE_JEMALLOC
#include "sql/sql_jemalloc.h"
#endif

namespace im {

/* All package memory usage aggregation point */
PSI_memory_key key_memory_package;

const char *PACKAGE_SCHEMA = "mysql";

static bool package_inited = false;

#ifdef HAVE_PSI_INTERFACE
static PSI_memory_info package_memory[] = {
    {&key_memory_package, "im::package", 0, 0, PSI_DOCUMENT_ME}};

static void init_package_psi_key() {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(package_memory));
  mysql_memory_register(category, package_memory, count);
}
#endif

/* Register all the native package element */
template <typename K, typename T>
static void register_package(const LEX_CSTRING &schema) {
  if (package_inited) {
    Package::instance()->register_element<K>(
        std::string(schema.str), T::instance()->str(), T::instance());
  }
}

/* Template of search package element */
template <typename T>
static const T *find_package_element(const std::string &schema_name,
                                     const std::string &element_name) {
  return Package::instance()->lookup_element<T>(schema_name, element_name);
}
/* Template instantiation */
template const Proc *find_package_element(const std::string &schema_name,
                                          const std::string &element_name);

/**
  whether exist native proc by schema_name and proc_name

  @retval       true              Exist
  @retval       false             Not exist
*/
bool exist_native_proc(const char *db, const char *name) {
  return find_package_element<Proc>(std::string(db), std::string(name)) ? true
                                                                        : false;
}

/**
  Find the native proc and evoke the parse tree root

  @param[in]    THD               Thread context
  @param[in]    sp_name           Proc name
  @param[in]    pt_expr_list      Parameters

  @retval       parse_tree_root   Parser structure
*/
Parse_tree_root *find_native_proc_and_evoke(THD *thd, sp_name *sp_name,
                                            PT_item_list *pt_expr_list) {
  const Proc *proc = find_package_element<Proc>(
      std::string(sp_name->m_db.str), std::string(sp_name->m_name.str));

  return proc ? proc->PT_evoke(thd, pt_expr_list, proc) : nullptr;
}

/**
  Initialize Package context.
*/
void package_context_init() {
#ifdef HAVE_PSI_INTERFACE
  init_package_psi_key();
#endif

  /* The schema of dummy and dummy_2 proc */
  LEX_CSTRING PROC_DUMMY_SCHEMA = {C_STRING_WITH_LEN("mysql")};

  package_inited = true;

#ifndef DBUG_OFF
  register_package<Proc, Proc_dummy>(PROC_DUMMY_SCHEMA);
  register_package<Proc, Proc_dummy_2>(PROC_DUMMY_SCHEMA);
#endif

  /* dbms_tso.get_timestamp() */
  register_package<Proc, Proc_get_timestamp>(TSO_PROC_SCHEMA);

  /* dbms_xa.find_by_xid("$gtrid", "$bqual", "$formatID") */
  register_package<Proc, Xa_proc_find_by_xid>(XA_PROC_SCHEMA);

  /* dbms_xa.prepare_with_trx_slot */
  register_package<Proc, Xa_proc_prepare_with_trx_slot>(XA_PROC_SCHEMA);

  /* dbms_xa.send_heartbeat() */
  register_package<Proc, Xa_proc_send_heartbeat>(XA_PROC_SCHEMA);

  /* dbms_xa.Xa_proc_advance_gcn_no_flush() */
  register_package<Proc, Xa_proc_advance_gcn_no_flush>(XA_PROC_SCHEMA);

  /* dbms_trans.returning() */
  register_package<Proc, Trans_proc_returning>(TRANS_PROC_SCHEMA);

  /* dbms_undo.purge_status() */
  register_package<Proc, Proc_purge_status>(PROC_UNDO_SCHEMA);

  /* dbms_ccl.add_ccl_rule(...) */
  register_package<Proc, Ccl_proc_add>(CCL_PROC_SCHEMA);
  /* dbms_ccl.flush_ccl_rule(...) */
  register_package<Proc, Ccl_proc_flush>(CCL_PROC_SCHEMA);
  /* dbms_ccl.del_ccl_rule(...) */
  register_package<Proc, Ccl_proc_del>(CCL_PROC_SCHEMA);
  /* dbms_ccl.show_ccl_rule(...) */
  register_package<Proc, Ccl_proc_show>(CCL_PROC_SCHEMA);

  /* dbms_ccl.flush_ccl_queue(...) */
  register_package<Proc, Ccl_proc_flush_queue>(CCL_PROC_SCHEMA);
  /* dbms_ccl.show_ccl_queue(...) */
  register_package<Proc, Ccl_proc_show_queue>(CCL_PROC_SCHEMA);

  /* dbms_recycle.show_tables() */
  register_package<Proc, im::recycle_bin::Recycle_proc_show>(
      im::recycle_bin::RECYCLE_BIN_PROC_SCHEMA);
  /* dbms_recycle.purge_table() */
  register_package<Proc, im::recycle_bin::Recycle_proc_purge>(
      im::recycle_bin::RECYCLE_BIN_PROC_SCHEMA);
  /* dbms_recycle.restore_table(...) */
  register_package<Proc, im::recycle_bin::Recycle_proc_restore>(
      im::recycle_bin::RECYCLE_BIN_PROC_SCHEMA);

  /* dbms_admin.show_native_procedure() */
  register_package<Proc, im::Show_native_procedure_proc>(im::ADMIN_PROC_SCHEMA);

  /* dbms_outln.add_optimizer_outline(...) */
  register_package<Proc, Outline_optimizer_proc_add>(OUTLINE_PROC_SCHEMA);
  /* dbms_outln.add_index_outline(...) */
  register_package<Proc, Outline_index_proc_add>(OUTLINE_PROC_SCHEMA);
  /* dbms_outln.del_outline(...) */
  register_package<Proc, Outline_proc_del>(OUTLINE_PROC_SCHEMA);
  /* dbms_outln.flush_outline() */
  register_package<Proc, Outline_proc_flush>(OUTLINE_PROC_SCHEMA);
  /* dbms_outln.show_outline() */
  register_package<Proc, Outline_proc_show>(OUTLINE_PROC_SCHEMA);
  /* dbms_outln.preview_outline() */
  register_package<Proc, Outline_proc_preview>(OUTLINE_PROC_SCHEMA);

  /* dbms_trans.rollback_to_implicit_savepoint() */
  register_package<Proc, Trans_proc_implicit_savepoint>(TRANS_PROC_SCHEMA);

  /* dbms_consensus */
  register_package<Proc, Consensus_proc_change_leader>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_add_learner>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_add_follower>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_drop_learner>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_upgrade_learner>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_downgrade_follower>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_refresh_learner_meta>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_configure_follower>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_configure_learner>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_force_single_mode>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_fix_cluster_id>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_fix_matchindex>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_show_global>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_show_local>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_show_logs>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_purge_log>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_local_purge_log>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_force_purge_log>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_drop_prefetch_channel>(
      CONSENSUS_PROC_SCHEMA);

  /** xrpc.perf_hist() */
  register_package<Proc, Proc_perf_hist>(XRPC_PROC_SCHEMA);

  /* procedures: polarx.changeset_* */
  register_package<Proc, Changeset_proc_start>(POLARX_PROC_SCHEMA);
  register_package<Proc, Changeset_proc_stats>(POLARX_PROC_SCHEMA);
  register_package<Proc, Changeset_proc_fetch>(POLARX_PROC_SCHEMA);
  register_package<Proc, Changeset_proc_stop>(POLARX_PROC_SCHEMA);
  register_package<Proc, Changeset_proc_finish>(POLARX_PROC_SCHEMA);
  register_package<Proc, Changeset_proc_times>(POLARX_PROC_SCHEMA);

#ifdef RDS_HAVE_JEMALLOC
  register_package<Proc, im::Jemalloc_profile_proc>(im::JEMALLOC_PROC_SCHEMA);
#endif

  /* dbms_keyring.generate_key(...) */
  register_package<Proc, Proc_generate_key>(KEYRING_PROC_SCHEMA);
  /* dbms_keyring.current_key_id(...) */
  register_package<Proc, Proc_current_key_id>(KEYRING_PROC_SCHEMA);
}

} /* namespace im */
