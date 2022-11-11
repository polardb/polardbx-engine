/* Copyright (c) 2014, 2019, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @addtogroup Replication
  @{

  @file control_events_ext.h

  @brief Contains the classes representing events operating in the replication
  stream properties. Each event is represented as a byte sequence with logical
  divisions as event header, event specific data and event footer. The header
  and footer are common to all the events and are represented as two different
  subclasses.
*/

#ifndef CONTROL_EVENT_EXT_INCLUDED
#define CONTROL_EVENT_EXT_INCLUDED

#include "control_events.h"
#include "statement_events.h"
#include "sql/xa.h"

namespace binary_log {

/**
  @class Gcn_event
  Gcn stands for Global Query Sequence

  @section Gcn_event_binary_format Binary Format

  The Body can have up to two components:

  <table>
  <caption>Body for Gtid_event</caption>

  <tr>
    <th>Name</th>
    <th>Format</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>flag</td>
    <td>1 byte integer</td>
    <td>Store bitmap</td>
  </tr>
  <tr>
    <td>commit_gcn</td>
    <td>8 byte integer</td>
    <td>Store the Global Query Sequence</td>
  </tr>
  </table>

*/
class Gcn_event : public Binary_log_event {
public:
  /** Whether there is committed gcn */
  static const unsigned char FLAG_HAVE_COMMITTED_GCN = 0x01;
  /** Whether there is snapshot seq passed external */
  static const unsigned char FLAG_HAVE_SNAPSHOT_SEQ = 0x02;
  /** Whether there is committed seq passed external */
  static const unsigned char FLAG_HAVE_COMMITTED_SEQ = 0x04;

  static const int FLAGS_LENGTH = 1;
  static const int COMMITTED_GCN_LENGTH = 8;

public:
  uint8_t flags;
  uint64_t commit_gcn;

 public:
  // Total length of post header
  static const int POST_HEADER_LENGTH = FLAGS_LENGTH + COMMITTED_GCN_LENGTH;
 /**
    Ctor of Gcn_event

    The layout of the buffer is as follows
    <pre>
    +----------+---+---+-------+--------------+---------+----------+
    |flag|commit_gcn|
    +----------+---+---+-------+------------------------+----------+
    </pre>
   
    @param buf  Contains the serialized event.
    @param fde  An FDE event (see Rotate_event constructor for more info).
  */

  Gcn_event(const char *buf, const Format_description_event *fde);
  /**
    Constructor.
  */
  explicit Gcn_event();

#ifndef HAVE_MYSYS
  // TODO(WL#7684): Implement the method print_event_info and print_long_info
  //               for all the events supported  in  MySQL Binlog
  void print_event_info(std::ostream &) {}
  void print_long_info(std::ostream &) {}
#endif

  bool have_commit_gcn() { return flags & FLAG_HAVE_COMMITTED_GCN; }
  uint64_t get_commit_gcn() { return commit_gcn; }
};

}  // end namespace binary_log
/**
  @} (end of group Replication)
*/
#endif /* CONTROL_EVENTS_EXT_INCLUDED */
