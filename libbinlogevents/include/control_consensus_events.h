/* Copyright (c) 2014, 2018, Oracle and/or its affiliates. Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All rights reserved.

This program is free software; you can redistribute it and/or modify
                                     it under the terms of the GNU General
Public License, version 2.0, as published by the Free Software Foundation.

    This program is also distributed with certain software (including
                 but not limited to OpenSSL) that is licensed under separate
terms, as designated in a particular file or component or in included license
                                                       documentation.  The
authors of MySQL hereby grant you an additional permission to link the program
and your derivative works with the separately licensed software that they have
included with MySQL.

                                                       This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
PURPOSE.  See the GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
        Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
*/

/**
  @addtogroup Replication
  @{

  @file control_consensus_events.h
*/

#ifndef CONTROL_CONSENSUS_EVENT_INCLUDED
#define CONTROL_CONSENSUS_EVENT_INCLUDED

#include <sys/types.h>
#include <ctime>
#include <list>
#include <map>
#include <vector>

#include "binlog_event.h"
#include "template_utils.h"
#include "uuid.h"

namespace binary_log {

class Consensus_event : public Binary_log_event {
 public:
  Consensus_event(const char *buf, unsigned int event_len,
                  const Format_description_event *desciption_event);
  /**
  Constructor.
  */
  explicit Consensus_event(unsigned int flag_arg,
                           unsigned long long int term_arg,
                           unsigned long long int index_arg,
                           unsigned long long int length_arg)
      : Binary_log_event(CONSENSUS_LOG_EVENT),
        flag(flag_arg),
        term(term_arg),
        index(index_arg),
        length(length_arg),
        reserve(0) {}

 protected:
  static const int ENCODED_FLAG_LENGTH = 4;
  static const int ENCODED_TERM_LENGTH = 8;
  static const int ENCODED_INDEX_LENGTH = 8;
  static const int ENCODED_LENGTH_LENGTH = 8;
  static const int ENCODED_RESERVE_LENGTH = 8;

  unsigned int flag;             /** preserved */
  unsigned long long int term;   /** term when entry was received by leader */
  unsigned long long int index;  /** position if entry in the log */
  unsigned long long int length; /** log length */
  /* currently, reserve is used for checksum */
  unsigned long long int reserve; /** reserved  */

 public:
  static const int POST_HEADER_LENGTH =
      ENCODED_FLAG_LENGTH + ENCODED_TERM_LENGTH + ENCODED_INDEX_LENGTH +
      ENCODED_LENGTH_LENGTH + ENCODED_RESERVE_LENGTH;
  static const int CONSENSUS_INDEX_OFFSET =
      LOG_EVENT_HEADER_LEN + ENCODED_FLAG_LENGTH + ENCODED_TERM_LENGTH;

  static const int MAX_EVENT_LENGTH = LOG_EVENT_HEADER_LEN + POST_HEADER_LENGTH;
};

class Previous_consensus_index_event : public Binary_log_event {
 public:
  Previous_consensus_index_event(
      const char *buf, unsigned int event_len,
      const Format_description_event *desciption_event);
  /**
  Constructor.
  */
  explicit Previous_consensus_index_event(unsigned long long int index_arg)
      : Binary_log_event(PREVIOUS_CONSENSUS_INDEX_LOG_EVENT),
        index(index_arg) {}

 protected:
  static const int ENCODED_INDEX_LENGTH = 8;

  unsigned long long int
      index; /** max consensus log index in the previous binlog file */

 public:
  static const int POST_HEADER_LENGTH = ENCODED_INDEX_LENGTH;
};

class Consensus_cluster_info_event : public Binary_log_event {
 public:
  Consensus_cluster_info_event(
      const char *buf, unsigned int event_len,
      const Format_description_event *desciption_event);
  /**
  Constructor.
  */
  explicit Consensus_cluster_info_event(unsigned int info_length_arg,
                                        const char *info_arg)
      : Binary_log_event(CONSENSUS_CLUSTER_INFO_EVENT),
        info_length(info_length_arg),
        info(info_arg) {}

 protected:
  static const int ENCODED_INFO_LENGTH_LENGTH = 4;

  unsigned int info_length; /** info length */
  const char *info;         /** info content */

 public:
  static const int POST_HEADER_LENGTH = ENCODED_INFO_LENGTH_LENGTH;
};

class Consensus_empty_event : public Binary_log_event {
 public:
  Consensus_empty_event(const char *buf, unsigned int event_len,
                        const Format_description_event *desciption_event);
  /**
  Constructor.
  */
  explicit Consensus_empty_event() : Binary_log_event(CONSENSUS_EMPTY_EVENT) {}
};

}  // end namespace binary_log
/**
  @} (end of group Replication)
*/
#endif /* CONTROL_EVENTS_INCLUDED */
