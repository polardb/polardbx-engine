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

#include "sql/consensus/consensus_rpl_rli.h"

#include "sql/bl_consensus_log.h"
#include "sql/consensus/consensus_err.h"
#include "sql/consensus_log_manager.h"
#include "sql/log.h"
#include "sql/mysqld.h"
#include "sql/rpl_replica.h"
#include "sql/rpl_rli_pdb.h"

XPaxos_relay_log_info::~XPaxos_relay_log_info() {
  consensus_log_manager.set_relay_log_info(nullptr);
}

/**
 * Overwrite log name and index log name if needed.
 *
 * @param[in/out]	log name
 * @param[in/out]	log index name
 */
void Relay_log_info::overwrite_log_name(const char **, const char **) {
  /** Normal relay log name has been generated when rli_init_info(),
   *  so, here only confirm it.
   */
  return;
}

/**
 * Overwrite log name and index log name if needed.
 *
 * @param[in/out]	log name
 * @param[in/out]	log index name
 */
void XPaxos_relay_log_info::overwrite_log_name(const char **ln,
                                               const char **log_index_name) {
  assert(relay_log.is_relay_log);

  /** Use binlog file name although it's relay log object. */
  *ln = opt_bin_logname;
  *log_index_name = opt_binlog_index_name;

  relay_log.is_xpaxos_log = true;
  return;
}

LOG_POS_COORD Relay_log_info::get_log_pos_coord(Relay_log_info *rli) {
  return {const_cast<char *>(rli->get_group_master_log_name()),
          rli->get_group_master_log_pos(), 0};
}

LOG_POS_COORD XPaxos_relay_log_info::get_log_pos_coord(Relay_log_info *rli) {
  return {const_cast<char *>(rli->get_group_relay_log_name()),
          rli->get_group_relay_log_pos(), rli->get_consensus_apply_index()};
}

int Relay_log_info::get_log_position(LOG_INFO *linfo, my_off_t &log_position) {
  if (relay_log.find_log_pos(linfo, get_group_relay_log_name(), 1)) {
    LogErr(ERROR_LEVEL, ER_RPL_ERROR_LOOKING_FOR_LOG,
           get_group_relay_log_name());
    return 1;
  }
  log_position = get_group_relay_log_pos();
  return 0;
}

int XPaxos_relay_log_info::get_log_position(LOG_INFO *linfo,
                                            my_off_t &log_position) {
  uint64 log_pos = 0;
  char log_name[FN_REFLEN];
  uint64 next_index =
      consensus_log_manager.get_next_trx_index(get_consensus_apply_index());
  if (consensus_log_manager.get_log_position(next_index, false, log_name,
                                             &log_pos)) {
    sql_print_error("Mts recover cannot find start index %llu.", next_index);
    return 1;
  }
  if (relay_log.find_log_pos(linfo, log_name, 1)) {
    LogErr(ERROR_LEVEL, ER_RPL_ERROR_LOOKING_FOR_LOG, log_name);
    return 1;
  }
  log_position = log_pos;
  return 0;
}

void XPaxos_relay_log_info::set_xpaxos_relay_log_info() {
  consensus_log_manager.set_relay_log_info(this);
}

void XPaxos_relay_log_info::set_xpaxos_apply_ev_sequence() {
  consensus_log_manager.set_apply_ev_sequence(1);
}

void XPaxos_relay_log_info::update_xpaxos_applied_index() {
  ulonglong rli_appliedindex = 0;
  set_consensus_apply_index(gaq->lwm.consensus_index);
  rli_appliedindex = get_consensus_apply_index();
  rli_appliedindex = opt_appliedindex_force_delay >= rli_appliedindex
                         ? 0
                         : rli_appliedindex - opt_appliedindex_force_delay;
  mts_force_consensus_apply_index(this, rli_appliedindex);
}

/**
   Reset group_relay_log_name and group_relay_log_pos to nullptr.
   The caller must hold data_lock.

   @param[out]     errmsg    An error message is set into it if error happens.

   @retval    false    Success
   @retval    true     Error
 */
bool XPaxos_relay_log_info::reset_group_relay_log_pos(const char **) {
  group_relay_log_name[0] = 0;
  group_relay_log_pos = 0;
  event_relay_log_name[0] = 0;
  event_relay_log_pos = 0;
  consensus_apply_index = 0;
  return false;
}

/**
   Check if group_relay_log_name is in index file.

   @param [out] errmsg An error message is returned if error happens.

   @retval    false    It is valid.
   @retval    true     It is invalid. In this case, *errmsg is set to point to
                       the error message.
*/
bool XPaxos_relay_log_info::is_group_relay_log_name_invalid(const char **) {
  /** Didn't confirm it. */
  return false;
}

void XPaxos_relay_log_info::relay_log_number_to_name(uint number,
                                                     char name[FN_REFLEN + 1]) {
  char *str = strmake(name, log_bin_basename, FN_REFLEN + 1);
  sprintf(str, ".%06u", number);
}
