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

#ifndef RAFT_RAFT0RPL_RLI_H
#define RAFT_RAFT0RPL_RLI_H

#include "sql/rpl_rli.h"
#include "sql/raft/channel.h"

class Raft_relay_log_info : public Relay_log_info {
 public:
  Raft_relay_log_info(bool is_slave_recovery,
#ifdef HAVE_PSI_INTERFACE
                      PSI_mutex_key *param_key_info_run_lock,
                      PSI_mutex_key *param_key_info_data_lock,
                      PSI_mutex_key *param_key_info_sleep_lock,
                      PSI_mutex_key *param_key_info_thd_lock,
                      PSI_mutex_key *param_key_info_data_cond,
                      PSI_mutex_key *param_key_info_start_cond,
                      PSI_mutex_key *param_key_info_stop_cond,
                      PSI_mutex_key *param_key_info_sleep_cond,
#endif
                      uint param_id, const char *param_channel,
                      bool is_rli_fake)
      : Relay_log_info(is_slave_recovery,
#ifdef HAVE_PSI_INTERFACE
                       param_key_info_run_lock, param_key_info_data_lock,
                       param_key_info_sleep_lock, param_key_info_thd_lock,
                       param_key_info_data_cond, param_key_info_start_cond,
                       param_key_info_stop_cond, param_key_info_sleep_cond,
#endif
                       param_id, param_channel, is_rli_fake) {
  }

  virtual ~Raft_relay_log_info() {}

  virtual Channel_style style() const override { return Channel_style::Raft; }

  /**
   * Overwrite log name and index log name if needed.
   *
   * @param[in/out]	log name
   * @param[in/out]	log index name
   */
  virtual void overwrite_log_name(const char **ln,
                                  const char **index_name) override;
  /**
     Reset group_relay_log_name and group_relay_log_pos to nullptr.
     The caller must hold data_lock.

     @param[out]     errmsg    An error message is set into it if error happens.

     @retval    false    Success
     @retval    true     Error
   */
  virtual bool reset_group_relay_log_pos(const char **errmsg) override;

  /**
     Check if group_relay_log_name is in index file.

     @param [out] errmsg An error message is returned if error happens.

     @retval    false    It is valid.
     @retval    true     It is invalid. In this case, *errmsg is set to point to
                         the error message.
*/
  virtual bool is_group_relay_log_name_invalid(const char **errmsg) override;
};

#endif
