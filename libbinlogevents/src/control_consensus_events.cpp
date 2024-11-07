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

#include "control_consensus_events.h"

namespace binary_log {

Consensus_event::Consensus_event(
    const char *buf, unsigned int event_len __attribute__((unused)),
    const Format_description_event *description_event)
    : Binary_log_event(&buf, description_event) {
  /*
  The layout of the buffer is as follows:
  +------+--------+--------+---------+-----------+
  |FLAG  |TERM    |INDEX   |LENGTH   |RESERVE    |
  |4 byte|8 bytes |8 bytes |8 bytes  |8 bytes    |
  +------+--------+--------+---------+-----------+
  skip the event header len
  */
  char const *ptr_buffer = buf + LOG_EVENT_HEADER_LEN;
  memcpy(&flag, ptr_buffer, sizeof(flag));
  ptr_buffer += ENCODED_FLAG_LENGTH;
  memcpy(&term, ptr_buffer, sizeof(term));
  ptr_buffer += ENCODED_TERM_LENGTH;
  memcpy(&index, ptr_buffer, sizeof(index));
  ptr_buffer += ENCODED_INDEX_LENGTH;
  memcpy(&length, ptr_buffer, sizeof(reserve));
  ptr_buffer += ENCODED_LENGTH_LENGTH;
  memcpy(&reserve, ptr_buffer, sizeof(reserve));
  ptr_buffer += ENCODED_RESERVE_LENGTH;
  return;
}

Previous_consensus_index_event::Previous_consensus_index_event(
    const char *buf, unsigned int event_len __attribute__((unused)),
    const Format_description_event *description_event)
    : Binary_log_event(&buf, description_event), index(0) {
  /*
  The layout of the buffer is as follows:
  +--------+
  |INDEX   |
  |8 bytes |
  +--------+
  skip the event header data
  */
  char const *ptr_buffer = buf + LOG_EVENT_HEADER_LEN;
  memcpy(&index, ptr_buffer, sizeof(index));
  ptr_buffer += ENCODED_INDEX_LENGTH;
  return;
}

Consensus_cluster_info_event::Consensus_cluster_info_event(
    const char *buf, unsigned int event_len __attribute__((unused)),
    const Format_description_event *description_event)
    : Binary_log_event(&buf, description_event) {
  /*
  The layout of the buffer is as follows:
  +------------+------------+
  |INFO_LENGTH | INFO       |
  |4 bytes     | N bytes    |
  +------------+------------+
  skip the event header data
  */
  char const *ptr_buffer = buf + LOG_EVENT_HEADER_LEN;
  memcpy(&info_length, ptr_buffer, sizeof(info_length));
  ptr_buffer += ENCODED_INFO_LENGTH_LENGTH;
  info = bapi_strndup(ptr_buffer, info_length);
}

Consensus_empty_event::Consensus_empty_event(
    const char *buf, unsigned int event_len __attribute__((unused)),
    const Format_description_event *description_event)
    : Binary_log_event(&buf, description_event) {
  /*
  The buffer is advanced in Binary_log_event constructor to point to
  beginning of post-header
  */
  return;
}

}  // end namespace binary_log
