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

/** @file libbinlogevents/include/gcn_event.h

  Global Commit Number event.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/
#ifndef GCN_EVENT_H
#define GCN_EVENT_H

#include "control_events.h"
#include "sql/lizard/lizard_service.h"  // MyVisionGCN...

class THD;

namespace lizard {
class Commit_policy;
}

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
    <td>n_branch</td>
    <td>2 byte integer</td>
    <td>Store the Global Query Sequence</td>
    <td>n_local_branch</td>
    <td>2 byte integer</td>
    <td>Store the Global Query Sequence</td>
  </tr>
  </table>

*/
class Gcn_event : public Binary_log_event {
 public:
  /** Whether there is gcn */
  static const unsigned char FLAG_HAVE_GCN = 0x01;
  /** Whether there is snapshot seq passed external */
  static const unsigned char FLAG_HAVE_SNAPSHOT_SEQ = 0x02;
  /** Whether there is committed seq passed external */
  static const unsigned char FLAG_HAVE_COMMITTED_SEQ = 0x04;
  /** If the source of the commit_gcn is assigned, the flag will be set.
  If the source of the commit_gcn is automatic, the flag will not be set.
  This flag is only meaningful when FLAG_HAVE_COMMITTED_GCN was set. */
  static const unsigned char FLAG_GCN_ASSIGNED = 0x08;
  /** If it's a proposal GCN at prepare for async commit, the flag will be set.
  Otherwise it will not be set. */
  static const unsigned char FLAG_GCN_PROPOSAL = 0x10;
  /** Whether there is branch count info. */
  static const unsigned char FLAG_HAVE_BRANCH_COUNT = 0x20;

  static const int ENCODED_FLAG_LENGTH = 1;
  static const int GCN_LENGTH = 8;
  static const int BRANCH_NUMBER_LENGTH = 2;

 public:
  uint8_t flags;
  uint64_t gcn;
  xa_branch_t branch;

 public:
  // Total length of post header
  static const int POST_HEADER_LENGTH = ENCODED_FLAG_LENGTH;

  // Total length of max data body length.
  static const int MAX_DATA_BODY_LENGTH =
      GCN_LENGTH + BRANCH_NUMBER_LENGTH + BRANCH_NUMBER_LENGTH;

  /**
     Ctor of Gcn_event

     The layout of the buffer is as follows
     <pre>
     +----------+---+---+-------+--------------+---------+----------+
     |flag|commit_gcn|n_branch|n_local_branch|
     +----------+---+---+-------+------------------------+----------+
     </pre>

     @param buf  Contains the serialized event.
     @param fde  An FDE event (see Rotate_event constructor for more info).
   */

  Gcn_event(const char *buf, const Format_description_event *fde);
  /**
    Constructor.
  */
  explicit Gcn_event(const lizard::Commit_policy *policy,
                     const uint64_t innodb_commit_gcn,
                     const uint64_t innodb_snapshot_gcn);

  void build_gcn(const gcn_tuple_t &tuple, bool is_proposal);

  void build_branch(const xa_branch_t &in_branch);

#ifndef HAVE_MYSYS
  // TODO(WL#7684): Implement the method print_event_info and print_long_info
  //               for all the events supported  in  MySQL Binlog
  void print_event_info(std::ostream &) override {}
  void print_long_info(std::ostream &) override {}
#endif

  bool have_gcn() const { return flags & FLAG_HAVE_GCN; }
  csr_t csr() const {
    return flags & FLAG_GCN_ASSIGNED ? csr_t::CSR_ASSIGNED
                                     : csr_t::CSR_AUTOMATIC;
  }
  bool is_pmmt_gcn() const { return flags & FLAG_GCN_PROPOSAL; }
  bool is_cmmt_gcn() const { return !is_pmmt_gcn(); }

  bool have_branch_count() const { return flags & FLAG_HAVE_BRANCH_COUNT; }

  const gcn_tuple_t clone_pmmt() const;

  const gcn_tuple_t clone_cmmt() const;

  const xa_branch_t clone_branch() const;
};

}  // end namespace binary_log

#endif
