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
#include "m_string.h"
#include "my_dbug.h"
#include "sql/consensus_info.h"
#include "sql/rpl_info_factory.h"
#include "sql/rpl_msr.h"
#include "sql/table.h"

#include "sql/consensus/consensus_err.h"
#include "sql/consensus/consensus_rpl_info_factory.h"
#include "sql/consensus/consensus_rpl_mi.h"
#include "sql/consensus/consensus_rpl_rli.h"

/* Consensus info table name */
LEX_CSTRING CONSENSUS_INFO_NAME = {STRING_WITH_LEN("consensus_info")};

Rpl_info_factory::struct_table_data Rpl_info_factory::consensus_table_data;
Rpl_info_factory::struct_file_data Rpl_info_factory::consensus_file_data;

/**
 * Create different channel style master info object according to channel
 * name.
 *
 * @retval	nullptr if error.
 */
Master_info *Rpl_info_factory::new_mi_object(uint instances,
                                             const char *channel) {
  Master_info *mi = nullptr;
  bool is_xpaxos =
      Multisource_info::is_xpaxos_replication_channel_name(channel);

  if (is_xpaxos) {
    mi = new XPaxos_master_info(
#ifdef HAVE_PSI_INTERFACE
        &key_source_info_run_lock, &key_source_info_data_lock,
        &key_source_info_sleep_lock, &key_source_info_thd_lock,
        &key_source_info_rotate_lock, &key_source_info_data_cond,
        &key_source_info_start_cond, &key_source_info_stop_cond,
        &key_source_info_sleep_cond, &key_source_info_rotate_cond,
#endif
        instances, channel);
  } else {
    mi = new Master_info(
#ifdef HAVE_PSI_INTERFACE
        &key_source_info_run_lock, &key_source_info_data_lock,
        &key_source_info_sleep_lock, &key_source_info_thd_lock,
        &key_source_info_rotate_lock, &key_source_info_data_cond,
        &key_source_info_start_cond, &key_source_info_stop_cond,
        &key_source_info_sleep_cond, &key_source_info_rotate_cond,
#endif
        instances, channel);
  }
  return mi;
}

/**
 * Create different channel style relay log info object according to channel
 * name.
 *
 * @retval	nullptr if error.
 */
Relay_log_info *Rpl_info_factory::new_rli_object(bool is_slave_recovery,
                                                 uint instances,
                                                 const char *channel,
                                                 bool is_rli_fake) {
  Relay_log_info *rli = nullptr;
  bool is_xpaxos =
      Multisource_info::is_xpaxos_replication_channel_name(channel);

  if (is_xpaxos) {
    rli = new XPaxos_relay_log_info(
        false,
#ifdef HAVE_PSI_INTERFACE
        &key_relay_log_info_run_lock, &key_relay_log_info_data_lock,
        &key_relay_log_info_sleep_lock, &key_relay_log_info_thd_lock,
        &key_relay_log_info_data_cond, &key_relay_log_info_start_cond,
        &key_relay_log_info_stop_cond, &key_relay_log_info_sleep_cond,
#endif
        instances, channel, is_rli_fake);

  } else {
    rli = new Relay_log_info(
        is_slave_recovery,
#ifdef HAVE_PSI_INTERFACE
        &key_relay_log_info_run_lock, &key_relay_log_info_data_lock,
        &key_relay_log_info_sleep_lock, &key_relay_log_info_thd_lock,
        &key_relay_log_info_data_cond, &key_relay_log_info_start_cond,
        &key_relay_log_info_stop_cond, &key_relay_log_info_sleep_cond,
#endif
        instances, channel, is_rli_fake);
  }

  return rli;
}

/**
 * Create consensus info table handler.
 *
 * @retval 	nullptr if error.
 */
Consensus_info *Rpl_info_factory::create_consensus_info() {
  Consensus_info *consensus_info = nullptr;
  Rpl_info_handler *handler_src = nullptr;
  Rpl_info_handler *handler_dest = nullptr;
  // uint instances = 1;
  const char *msg =
      "Failed to allocate memory for consensus info "
      "structure";

  DBUG_ENTER("Rpl_info_factory::create_consensus_info");

  if (!(consensus_info = new Consensus_info(
#ifdef HAVE_PSI_INTERFACE
            &key_consensus_info_run_lock, &key_consensus_info_data_lock,
            &key_consensus_info_sleep_lock, &key_consensus_info_thd_lock,
            &key_consensus_info_data_cond, &key_consensus_info_start_cond,
            &key_consensus_info_stop_cond, &key_consensus_info_sleep_cond
#endif
            )))
    goto err;

  if (init_repositories(consensus_table_data, consensus_file_data,
                        INFO_REPOSITORY_TABLE, &handler_src, &handler_dest,
                        &msg)) {
    goto err;
  }

  if (handler_dest->get_rpl_info_type() != INFO_REPOSITORY_TABLE) {
    xp::error(ER_XP_0, "Consensus Info Respository should be TABLE");
    goto err;
  }

  consensus_info->set_rpl_info_handler(handler_dest);

  if (consensus_info->set_info_search_keys(handler_dest)) goto err;

  delete handler_src;

  DBUG_RETURN(consensus_info);

err:
  delete handler_src;
  delete handler_dest;
  if (consensus_info) {
    /*
    The handler was previously deleted so we need to remove
    any reference to it.
    */
    consensus_info->set_rpl_info_handler(nullptr);
    delete consensus_info;
  }
  xp::error(ER_XP_0) << "Creating consensus info " << msg;
  DBUG_RETURN(nullptr);
}

/**
 * Init repo for consensus info table.
 */
void Rpl_info_factory::init_consensus_repo_metadata() {
  consensus_table_data.n_fields =
      Consensus_info::get_number_info_consensus_fields();
  consensus_table_data.schema = MYSQL_SCHEMA_NAME.str;
  consensus_table_data.name = CONSENSUS_INFO_NAME.str;
  consensus_file_data.n_fields =
      Consensus_info::get_number_info_consensus_fields();
  my_stpcpy(consensus_file_data.name, "consensus_info");
  my_stpcpy(consensus_file_data.pattern, "consensus_info");
  consensus_file_data.name_indexed = false;
  Consensus_info::set_nullable_fields(&consensus_table_data.nullable_fields);
  Consensus_info::set_nullable_fields(&consensus_file_data.nullable_fields);
}
