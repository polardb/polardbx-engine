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

#ifndef XPAXOS_RPL_MI_H
#define XPAXOS_RPL_MI_H

#include "sql/consensus/consensus_channel.h"
#include "sql/rpl_mi.h"

class XPaxos_master_info final : public Master_info {
 public:
  XPaxos_master_info(
#ifdef HAVE_PSI_INTERFACE
      PSI_mutex_key *param_key_info_run_lock,
      PSI_mutex_key *param_key_info_data_lock,
      PSI_mutex_key *param_key_info_sleep_lock,
      PSI_mutex_key *param_key_info_thd_lock,
      PSI_mutex_key *param_key_info_rotate_lock,
      PSI_mutex_key *param_key_info_data_cond,
      PSI_mutex_key *param_key_info_start_cond,
      PSI_mutex_key *param_key_info_stop_cond,
      PSI_mutex_key *param_key_info_sleep_cond,
      PSI_mutex_key *param_key_info_rotate_cond,
#endif
      uint param_id, const char *param_channel)
      : Master_info(
#ifdef HAVE_PSI_INTERFACE
            param_key_info_run_lock, param_key_info_data_lock,
            param_key_info_sleep_lock, param_key_info_thd_lock,
            param_key_info_rotate_lock, param_key_info_data_cond,
            param_key_info_start_cond, param_key_info_stop_cond,
            param_key_info_sleep_cond, param_key_info_rotate_cond,
#endif
            param_id, param_channel) {
  }

  ~XPaxos_master_info() override {}

  /** XPaxos channel */
  virtual Channel_style style() const override { return Channel_style::XPaxos; }
};
#endif
