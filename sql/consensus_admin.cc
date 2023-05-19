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

#include "consensus_admin.h"

#include "sql/binlog.h"
#include "sql/consensus_log_manager.h"
#include "sql/bl_consensus_log.h"
#include "sql/appliedindex_checker.h"
#include "sql/auth/auth_acls.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/rpl_replica.h"
#include "sql/rpl_msr.h"
#include "sql/rpl_mi.h"
#include "sql/rpl_rli.h"
#include "consensus_recovery_manager.h"

#include "my_config.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "my_dbug.h"
#include "sql/sql_lex.h"
#include "sql/debug_sync.h"  // DEBUG_SYNC
#include "sql/log.h"
//#include "sql/binlog_ext.h"
#include "replica_read_manager.h"

using std::max;

/* wait no commit index when exec event in xpaxos_channel */
bool opt_disable_wait_commitindex = false;

/* Helper function for SHOW BINLOG/RELAYLOG EVENTS */


class Kill_all_conn : public Do_THD_Impl
{
public:
  Kill_all_conn() {}

  virtual void operator()(THD *thd_to_kill)
  {
    mysql_mutex_lock(&thd_to_kill->LOCK_thd_data);

    /* kill all connections */
    if (thd_to_kill->security_context()->has_account_assigned()
       && thd_to_kill->killed != THD::KILL_CONNECTION
       && !thd_to_kill->slave_thread)
      thd_to_kill->awake(THD::KILL_CONNECTION);

    mysql_mutex_unlock(&thd_to_kill->LOCK_thd_data);
  }
};

void killall_threads()
{
  Kill_all_conn kill_all_conn;
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  thd_manager->do_for_all_thd(&kill_all_conn);
}

class Kill_all_dump_conn : public Do_THD_Impl
{
public:
  Kill_all_dump_conn() {}

  virtual void operator()(THD *thd_to_kill)
  {
    mysql_mutex_lock(&thd_to_kill->LOCK_thd_data);

    /* Kill all binlog dump connections */
    if (thd_to_kill->security_context()->has_account_assigned()
        && (thd_to_kill->get_command() == COM_BINLOG_DUMP
           || thd_to_kill->get_command() == COM_BINLOG_DUMP_GTID)
        && thd_to_kill->killed != THD::KILL_CONNECTION
        && !thd_to_kill->slave_thread)
      thd_to_kill->awake(THD::KILL_CONNECTION);

    mysql_mutex_unlock(&thd_to_kill->LOCK_thd_data);
  }
};

void killall_dump_threads()
{
  Kill_all_dump_conn Kill_all_dump_conn;
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  thd_manager->do_for_all_thd(&Kill_all_dump_conn);
}

int start_consensus_apply_threads()
{
  DBUG_ENTER("start_consensus_apply_threads");
  Master_info *mi = NULL;
  int thread_mask = SLAVE_SQL;
  int error = 0;
  channel_map.wrlock();
  if (!opt_skip_replica_start || !opt_initialize)
  {
    for (mi_map::iterator it = channel_map.begin(); it != channel_map.end(); it++)
    {
      mi = it->second;

      // Todo: mi must be itself
      /* If server id is not set, start_slave_thread() will say it */
      if (mi && channel_map.is_xpaxos_replication_channel_name(mi->get_channel()))
      {
        /* same as in start_slave() cache the global var values into rli's members */
        mi->rli->opt_replica_parallel_workers = opt_mts_replica_parallel_workers;
        mi->rli->checkpoint_group = opt_mta_checkpoint_group;
        if (mts_parallel_option == MTS_PARALLEL_TYPE_DB_NAME)
          mi->rli->channel_mts_submode = MTS_PARALLEL_TYPE_DB_NAME;
        else
          mi->rli->channel_mts_submode = MTS_PARALLEL_TYPE_LOGICAL_CLOCK;
        /* wait intergrity consensus log  */
        while(mi->rli->get_consensus_apply_index() > consensus_log_manager.get_sync_index())
          my_sleep(2000000);
        if (start_slave_threads(true/*need_lock_slave=true*/,
          false/*wait_for_start=false*/,
          mi,
          thread_mask))
        {
          /*
          Creation of slave threads for subsequent channels are stopped
          if a failure occurs in this iteration.
          @todo:have an option if the user wants to continue
          the replication for other channels.
          */
          raft::error(ER_RAFT_0) << "Failed to create slave threads";
          error = 1;
        }
      }
    }
  }
  channel_map.unlock();
  DBUG_RETURN(error);
}
