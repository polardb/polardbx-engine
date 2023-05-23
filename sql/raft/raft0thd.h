/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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
#ifndef MYSQL_RAFT0THD_H
#define MYSQL_RAFT0THD_H

#include "my_inttypes.h"

class Consensus_thd {
 public:
  Consensus_thd()
      : consensus_error(CSS_NONE),
        raft_channel(false),
        consensus_index(0),
        consensus_term(0) {}

  void set_raft_channel(bool raft_channel_arg) {
    raft_channel = raft_channel_arg;
  }

  void set_consensus_index(uint64 consensus_index_arg) {
    consensus_index = consensus_index_arg;
  }

  void set_consensus_term(uint64 consensus_term_arg) {
    consensus_term = consensus_term_arg;
  }

  bool is_raft_channel() const { return raft_channel; }

  uint64 get_consensus_index() const { return consensus_index; }

  uint64 get_consensus_term() const { return consensus_term; }

 public:
  enum Consensus_error {
    CSS_NONE = 0,
    CSS_LEADERSHIP_CHANGE,
    CSS_LOG_TOO_LARGE,
    CSS_SHUTDOWN,
    CSS_GU_ERROR,
    CSS_OTHER
  } consensus_error;

 private:
  bool raft_channel;
  uint64 consensus_index;
  uint64 consensus_term;
};

#endif  // MYSQL_RAFT0THD_H
