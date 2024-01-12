/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef CONSENSUS_ADMIN_INCLUDE
#define CONSENSUS_ADMIN_INCLUDE

class THD;
class MYSQL_BIN_LOG;
class Relay_log_info;
class Log_event;

bool show_consensuslog_events(THD *thd, MYSQL_BIN_LOG *binary_log,
                              unsigned long long consensus_index);
bool mysql_show_consensuslog_events(THD *thd,
                                    unsigned long long consensus_index);
bool show_consensus_logs(THD *thd);
int consensus_command_limit(THD *thd);
void killall_threads();
void killall_dump_threads();
int start_consensus_apply_threads();
void binlog_commit_pos_watcher(bool *is_running);
int check_exec_consensus_log_end_condition(Relay_log_info *rli,
                                           bool is_xpaxos_replication);
void update_consensus_apply_pos(Relay_log_info *rli, Log_event *ev,
                                bool is_xpaxos_replication);
int calculate_consensus_apply_start_pos(Relay_log_info *rli,
                                        bool is_xpaxos_channel);

#endif
