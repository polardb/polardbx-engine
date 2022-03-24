/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyEngine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyEngine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "my_sys.h"
#include "sql/item.h"
#include "sql/auth/auth_acls.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/consensus/consensus_proc.h"
#include "sql/consensus/consensus_common.h"

/**
  Consensus procedures (dbms_consensus package)

  TODO: use a specific PSI_memory_key
*/
namespace im {

/* The uniform schema name for consensus package */
LEX_CSTRING CONSENSUS_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_consensus")};

bool Sql_cmd_consensus_proc::check_parameter() {
  if (Sql_cmd_proc::check_parameter())
    return true;
  /* consensus package only accepts ip:port format for a string parameter */
  if (m_proc->get_parameters()->size()) {
    std::size_t i = 0;
    List_iterator_fast<Item> it(*m_list);
    Item *item;
    while ((item = it++)) {
      if (item->data_type() == MYSQL_TYPE_VARCHAR) {
        Item_string *ip_port = dynamic_cast<Item_string *>(item);
        if (check_addr_format(ip_port->val_str(NULL)->ptr())) {
          my_error(ER_CONSENSUS_IP_PORT_FORMAT, MYF(0));
          return true;
        }
      }
      i++;
    }
  }
  return false;
}

bool Sql_cmd_consensus_proc::check_access(THD *thd) {
  if (Sql_cmd_proc::check_access(thd))
    return true;
  if (opt_consensus_force_recovery) {
    my_error(ER_CONSENSUS_SERVER_NOT_READY, MYF(0));
    return true;
  }
  return false;
}

bool Sql_cmd_consensus_proc::check_addr_format(const char *node_addr) {
  if (!node_addr) return true;
  int a, b, c, d, p;
  if (std::sscanf(node_addr,"%d.%d.%d.%d:%d", &a, &b, &c, &d, &p) != 5)
    return true;
  if (a >= 0 && a <= 255 &&
      b >= 0 && b <= 255 &&
      c >= 0 && c <= 255 &&
      d >= 0 && d <= 255 &&
      p >= 0 && p <= 65535)
    return false;
  else
    return true;
}

bool Sql_cmd_consensus_option_last_proc::check_parameter() {
  std::size_t actual_size = (m_list == NULL ? 0 : m_list->elements);
  std::size_t define_size = m_proc->get_parameters()->size();

  /* last param is option */
  if (actual_size != define_size && (actual_size + 1) != define_size) {
    my_error(ER_SP_WRONG_NO_OF_ARGS, MYF(0), "PROCEDURE",
             m_proc->qname().c_str(), define_size, actual_size);
    return true;
  }

  if (actual_size > 0) {
    std::size_t i = 0;
    List_iterator_fast<Item> it(*m_list);
    Item *item;
    while ((item = it++)) {
      if (item->data_type() != m_proc->get_parameters()->at(i)) {
        my_error(ER_NATIVE_PROC_PARAMETER_MISMATCH, MYF(0), i + 1,
                 m_proc->qname().c_str());
        return true;
      }
      if (item->data_type() == MYSQL_TYPE_VARCHAR) {
        Item_string *ip_port = dynamic_cast<Item_string *>(item);
        if (check_addr_format(ip_port->val_str(NULL)->ptr())) {
          my_error(ER_CONSENSUS_IP_PORT_FORMAT, MYF(0));
          return true;
        }
      }
      i++;
    }
  }
  return false;
}

bool Sql_cmd_consensus_no_logger_proc::check_access(THD *thd) {
  if (Sql_cmd_consensus_proc::check_access(thd))
    return true;
  if (opt_cluster_log_type_instance && thd->variables.opt_force_revise == FALSE) {
    my_error(ER_CONSENSUS_LOG_TYPE_NODE, MYF(0));
    return true;
  }
  return false;
}
bool Sql_cmd_consensus_option_last_no_logger_proc::check_access(THD *thd) {
  if (Sql_cmd_consensus_option_last_proc::check_access(thd))
    return true;
  if (opt_cluster_log_type_instance && thd->variables.opt_force_revise == FALSE) {
    my_error(ER_CONSENSUS_LOG_TYPE_NODE, MYF(0));
    return true;
  }
  return false;
}

/**
  dbms_consensus.change_leader(...)
*/
Proc *Consensus_proc_change_leader::instance() {
  static Proc *proc = new Consensus_proc_change_leader(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_change_leader::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_change_leader::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_string *ip_port = dynamic_cast<Item_string *>(it++);
  res = consensus_ptr->leaderTransfer(std::string(ip_port->val_str(NULL)->ptr()));
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0);
}

/**
  dbms_consensus.add_learner(...)
*/
Proc *Consensus_proc_add_learner::instance() {
  static Proc *proc = new Consensus_proc_add_learner(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_add_learner::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_add_learner::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_string *ip_port = dynamic_cast<Item_string *>(it++);
  std::vector<std::string> info_vector;
  info_vector.push_back(ip_port->val_str(NULL)->ptr());
  res = consensus_ptr->changeLearners(alisql::Paxos::CCOpType::CCAddNode, info_vector);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0);
}

bool Sql_cmd_consensus_proc_add_learner::prepare(THD *thd) {
  if (Sql_cmd_proc::prepare(thd))
    return true;
  /* check max node number */
  if (consensus_ptr->getClusterSize() > CONSENSUS_MAX_NODE_NUMBER) {
    my_error(ER_CONSENSUS_TOO_MANY_NODE, MYF(0));
    return true;
  }
  return false;
}

/**
  dbms_consensus.add_follower(...)
*/
Proc *Consensus_proc_add_follower::instance() {
  static Proc *proc = new Consensus_proc_add_follower(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_add_follower::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_add_follower::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_string *ip_port = dynamic_cast<Item_string *>(it++);
  std::string addr(ip_port->val_str(NULL)->ptr());
  res = consensus_ptr->changeMember(alisql::Paxos::CCOpType::CCAddLearnerAutoChange, addr);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0);
}

bool Sql_cmd_consensus_proc_add_follower::prepare(THD *thd) {
  if (Sql_cmd_proc::prepare(thd))
    return true;
  /* check max node number */
  if (consensus_ptr->getClusterSize() > CONSENSUS_MAX_NODE_NUMBER) {
    my_error(ER_CONSENSUS_TOO_MANY_NODE, MYF(0));
    return true;
  }
  return false;
}

/**
  dbms_consensus.drop_learner(...)
*/
Proc *Consensus_proc_drop_learner::instance() {
  static Proc *proc = new Consensus_proc_drop_learner(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_drop_learner::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_drop_learner::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_string *ip_port = dynamic_cast<Item_string *>(it++);
  std::vector<std::string> info_vector;
  info_vector.push_back(ip_port->val_str(NULL)->ptr());
  res = consensus_ptr->changeLearners(alisql::Paxos::CCOpType::CCDelNode, info_vector);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0);
}

/**
  dbms_consensus.upgrade_learner(...)
*/
Proc *Consensus_proc_upgrade_learner::instance() {
  static Proc *proc = new Consensus_proc_upgrade_learner(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_upgrade_learner::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_upgrade_learner::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_string *ip_port = dynamic_cast<Item_string *>(it++);
  std::string addr(ip_port->val_str(NULL)->ptr());
  res = consensus_ptr->changeMember(alisql::Paxos::CCOpType::CCAddNode, addr);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0);
}

/**
  dbms_consensus.downgrade_follower(...)
*/
Proc *Consensus_proc_downgrade_follower::instance() {
  static Proc *proc = new Consensus_proc_downgrade_follower(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_downgrade_follower::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_downgrade_follower::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_string *ip_port = dynamic_cast<Item_string *>(it++);
  std::string addr(ip_port->val_str(NULL)->ptr());
  res = consensus_ptr->downgradeMember(addr);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0);
}

/**
  dbms_consensus.refresh_learner_meta()
*/
Proc *Consensus_proc_refresh_learner_meta::instance() {
  static Proc *proc = new Consensus_proc_refresh_learner_meta(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_refresh_learner_meta::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_refresh_learner_meta::pc_execute(THD *thd) {
  int res = 0;
  std::vector<std::string> info_vector;
  res = consensus_ptr->changeLearners(alisql::Paxos::CCSyncLearnerAll, info_vector);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0);
}

/**
  dbms_consensus.configure_follower(...)
*/
Proc *Consensus_proc_configure_follower::instance() {
  static Proc *proc = new Consensus_proc_configure_follower(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_configure_follower::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_configure_follower::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_string *ip_port = dynamic_cast<Item_string *>(it++);
  std::string addr(ip_port->val_str(NULL)->ptr());
  Item_int *w = dynamic_cast<Item_int *>(it++);
  uint election_weight = w->val_uint();
  /* force_sync is option, default 0 */
  Item_int *fs = dynamic_cast<Item_int *>(it++);
  bool force_sync = fs? fs->val_uint(): 0;
  res = consensus_ptr->configureMember(addr, force_sync, election_weight);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res !=0 && res != 1)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0 && res != 1);
}

/**
  dbms_consensus.configure_learner(...)
*/
Proc *Consensus_proc_configure_learner::instance() {
  static Proc *proc = new Consensus_proc_configure_learner(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_configure_learner::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_configure_learner::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_string *ip_port = dynamic_cast<Item_string *>(it++);
  std::string addr(ip_port->val_str(NULL)->ptr());
  Item_string *s = dynamic_cast<Item_string *>(it++);
  std::string source(s->val_str(NULL)->ptr());
  /* force_sync is option, default 0 */
  Item_int *ua = dynamic_cast<Item_int *>(it++);
  bool use_applied = ua? ua->val_uint(): 0;
  res = consensus_ptr->configureLearner(addr, source, use_applied);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res != 0 && res != 1)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0 && res != 1);
}

/**
  dbms_consensus.force_single_mode()
*/
Proc *Consensus_proc_force_single_mode::instance() {
  static Proc *proc = new Consensus_proc_force_single_mode(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_force_single_mode::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_force_single_mode::pc_execute(THD *thd) {
  int res = 0;
  res = consensus_ptr->forceSingleLeader();
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0);
}

/**
  dbms_consensus.fix_cluster_id(...)
*/
Proc *Consensus_proc_fix_cluster_id::instance() {
  static Proc *proc = new Consensus_proc_fix_cluster_id(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_fix_cluster_id::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_fix_cluster_id::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_int *ci = dynamic_cast<Item_int *>(it++);
  uint64_t cluster_id = ci->val_uint();
  res = consensus_ptr->setClusterId(cluster_id);
  opt_cluster_id= cluster_id;
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0);
}

/**
  dbms_consensus.fix_matchindex(...)
*/
Proc *Consensus_proc_fix_matchindex::instance() {
  static Proc *proc = new Consensus_proc_fix_matchindex(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_fix_matchindex::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_fix_matchindex::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_string *ip_port = dynamic_cast<Item_string *>(it++);
  std::string addr(ip_port->val_str(NULL)->ptr());
  Item_int *mi = dynamic_cast<Item_int *>(it++);
  uint64_t matchindex = mi->val_uint();
  consensus_ptr->forceFixMatchIndex(addr, matchindex);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(res));
  return (res != 0);
}

/**
  dbms_consensus.show_cluster_global()
*/
Proc *Consensus_proc_show_global::instance() {
 static Proc *proc = new Consensus_proc_show_global(key_memory_package);
 return proc;
}
Sql_cmd *Consensus_proc_show_global::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

void Sql_cmd_consensus_proc_show_global::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  std::vector<Consensus_show_global_result *> results;
  if (error) {
    DBUG_ASSERT(thd->is_error());
    return;
  }

  // fetch the results
  collect_show_global_results(thd->mem_root, results);

  if (m_proc->send_result_metadata(thd)) return;

  for (auto it = results.cbegin(); it != results.cend(); it++) {
    Consensus_show_global_result *result = *it;
    protocol->start_row();
    protocol->store(result->id);
    protocol->store_string(result->ip_port.str, result->ip_port.length, system_charset_info);
    protocol->store(result->match_index);
    protocol->store(result->next_index);
    protocol->store_string(result->role.str, result->role.length,
                    system_charset_info);
    protocol->store_string(result->force_sync.str, result->force_sync.length,
                    system_charset_info);
    protocol->store(result->election_weight);
    protocol->store(result->learner_source);
    protocol->store(result->applied_index);
    protocol->store_string(result->pipelining.str, result->pipelining.length,
                    system_charset_info);
    protocol->store_string(result->send_applied.str, result->send_applied.length,
                    system_charset_info);
    if (protocol->end_row()) return;
  }

  my_eof(thd);
  return;
}

/**
  dbms_consensus.show_cluster_local()
*/
Proc *Consensus_proc_show_local::instance() {
 static Proc *proc = new Consensus_proc_show_local(key_memory_package);
 return proc;
}
Sql_cmd *Consensus_proc_show_local::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

void Sql_cmd_consensus_proc_show_local::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  Consensus_show_local_result *result = new (thd->mem_root) Consensus_show_local_result();
  if (error) {
    DBUG_ASSERT(thd->is_error());
    return;
  }

  // fetch the results
  collect_show_local_results(thd->mem_root, result);

  if (m_proc->send_result_metadata(thd)) return;

  protocol->start_row();
  protocol->store(result->id);
  protocol->store(result->current_term);
  protocol->store_string(result->current_leader.str, result->current_leader.length,
                  system_charset_info);
  protocol->store(result->commit_index);
  protocol->store(result->last_log_term);
  protocol->store(result->last_log_index);
  protocol->store_string(result->role.str, result->role.length,
                  system_charset_info);
  protocol->store(result->vote_for);
  protocol->store(result->applied_index);
  protocol->store_string(result->server_ready_for_rw.str, result->server_ready_for_rw.length,
                  system_charset_info);
  protocol->store_string(result->instance_type.str, result->instance_type.length,
                  system_charset_info);
  if (protocol->end_row()) return;

  my_eof(thd);
  return;
}

/**
  dbms_consensus.show_logs()
*/
Proc *Consensus_proc_show_logs::instance() {
 static Proc *proc = new Consensus_proc_show_logs(key_memory_package);
 return proc;
}
Sql_cmd *Consensus_proc_show_logs::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_show_logs::check_access(THD *thd) {
  Security_context *sctx = thd->security_context();

  if (!sctx->check_access(SUPER_ACL | REPL_CLIENT_ACL)) {
    my_error(ER_SPECIFIC_ACCESS_DENIED_ERROR, MYF(0), "SUPER or REPL_CLIENT");
    return true;
  }
  return false;
}

void Sql_cmd_consensus_proc_show_logs::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  std::vector<Consensus_show_logs_result *> results;
  if (error) {
    DBUG_ASSERT(thd->is_error());
    return;
  }

  // fetch the results
  collect_show_logs_results(thd->mem_root, results);

  if (m_proc->send_result_metadata(thd)) return;

  for (auto it = results.cbegin(); it != results.cend(); it++) {
    Consensus_show_logs_result *result = *it;
    protocol->start_row();
    protocol->store_string(result->log_name.str, result->log_name.length,
                    system_charset_info);
    protocol->store(result->file_size);
    protocol->store(result->start_log_index);
    if (protocol->end_row()) return;
  }

  my_eof(thd);
  return;
}

/**
  dbms_consensus.purge_log(...)
*/
Proc *Consensus_proc_purge_log::instance() {
  static Proc *proc = new Consensus_proc_purge_log(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_purge_log::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_purge_log::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_int *item = dynamic_cast<Item_int *>(it++);
  uint64 index = item->val_uint();
  res = consensus_ptr->forcePurgeLog(false /* local */, index);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(alisql::PaxosErrorCode::PE_DEFAULT));
  return (res != 0);
}

/**
  dbms_consensus.local_purge_log(...)
*/
Proc *Consensus_proc_local_purge_log::instance() {
  static Proc *proc = new Consensus_proc_local_purge_log(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_local_purge_log::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_local_purge_log::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_int *item = dynamic_cast<Item_int *>(it++);
  uint64 index = item->val_uint();
  res = consensus_ptr->forcePurgeLog(true /* local */, index);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(alisql::PaxosErrorCode::PE_DEFAULT));
  return (res != 0);
}

/**
  dbms_consensus.force_purge_log(...)
*/
Proc *Consensus_proc_force_purge_log::instance() {
  static Proc *proc = new Consensus_proc_force_purge_log(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_force_purge_log::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_force_purge_log::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_int *item = dynamic_cast<Item_int *>(it++);
  uint64 index = item->val_uint();
  res = consensus_log_manager.purge_log(index);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(alisql::PaxosErrorCode::PE_DEFAULT));
  return (res != 0);
}

/**
  dbms_consensus.drop_prefetch_channel(...)
*/
Proc *Consensus_proc_drop_prefetch_channel::instance() {
  static Proc *proc = new Consensus_proc_drop_prefetch_channel(key_memory_package);
  return proc;
}

Sql_cmd *Consensus_proc_drop_prefetch_channel::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_consensus_proc_drop_prefetch_channel::pc_execute(THD *thd) {
  int res = 0;
  List_iterator_fast<Item> it(*m_list);
  Item_int *item = dynamic_cast<Item_int *>(it++);
  uint64 channel_id = item->val_uint();
  res = consensus_log_manager.get_prefetch_manager()->drop_prefetch_channel(channel_id);
  LogErr(INFORMATION_LEVEL, ER_CONSENSUS_CMD_LOG,
                        thd->m_main_security_ctx.user().str,
                        thd->m_main_security_ctx.host_or_ip().str,
                        thd->query().str,
                        res);
  if (res)
    my_error(ER_CONSENSUS_COMMAND_ERROR, MYF(0), res, alisql::pxserror(alisql::PaxosErrorCode::PE_DEFAULT));
  return (res != 0);
}

}
