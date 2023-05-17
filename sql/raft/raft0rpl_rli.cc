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

#include "sql/raft/raft0rpl_rli.h"

#include "sql/mysqld.h"
#include "sql/rpl_replica.h"

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
void Raft_relay_log_info::overwrite_log_name(const char **ln,
                                             const char **log_index_name) {
  assert(relay_log.is_relay_log);

  /** Use binlog file name although it's relay log object. */
  *ln = opt_bin_logname;
  *log_index_name = opt_binlog_index_name;

  relay_log.is_raft_log = true;
  return;
}

/**
   Reset group_relay_log_name and group_relay_log_pos to nullptr.
   The caller must hold data_lock.

   @param[out]     errmsg    An error message is set into it if error happens.

   @retval    false    Success
   @retval    true     Error
 */
bool Raft_relay_log_info::reset_group_relay_log_pos(const char **) {
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
bool Raft_relay_log_info::is_group_relay_log_name_invalid(
    const char **)  {
  /** Didn't confirm it. */
  return false;
}
