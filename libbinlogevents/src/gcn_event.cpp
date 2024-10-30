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

/** @file libbinlogevents/src/gcn_event.cpp

  Global Commit Number event.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/

#include "gcn_event.h"
#include "binlog_event.h"
#include "event_reader.h"
#include "event_reader_macros.h"

#include "sql/xa/lizard_cmmt_policy.h"

namespace binary_log {

Gcn_event::Gcn_event(const lizard::Commit_policy *policy,
                     const uint64_t innodb_commit_gcn,
                     const uint64_t innodb_snapshot_gcn)
    : Binary_log_event(GCN_LOG_EVENT), flags(0), gcn(GCN_NULL) {
  if (innodb_commit_gcn != GCN_NULL) {
    flags |= FLAG_HAVE_COMMITTED_SEQ;
  }

  if (innodb_snapshot_gcn != GCN_NULL) {
    flags |= FLAG_HAVE_SNAPSHOT_SEQ;
  }

  policy->build_gcn_event(this);
}

Gcn_event::Gcn_event(const char *buf, const Format_description_event *fde)
    : Binary_log_event(&buf, fde), flags(0), gcn(GCN_NULL), branch() {
  /*
     The layout of the buffer is as follows:
     +------------+
     |     1 bytes| flags
     +------------+
     |     8 bytes| commit_gcn
     +------------+
     |     2 bytes| n_branch
     +------------+
     |     8 bytes| n_local_branch
     +------------+
   */
  BAPI_ENTER("Gcn_event::Gcn_event(const char*, ...)");
  READER_TRY_INITIALIZATION;
  READER_ASSERT_POSITION(fde->common_header_len);

  READER_TRY_SET(flags, read<uint8_t>);

  // DBUG_ASSERT(flags != 0);

  if (flags & FLAG_HAVE_GCN) {
    READER_TRY_SET(gcn, read<uint64_t>);
  }

  if (flags & FLAG_HAVE_BRANCH_COUNT) {
    READER_TRY_SET(branch.n_global, read<branch_num_t>);
    READER_TRY_SET(branch.n_local, read<branch_num_t>);
  }

  READER_CATCH_ERROR;
  BAPI_VOID_RETURN;
}

void Gcn_event::build_gcn(const gcn_tuple_t &tuple, bool is_proposal) {
  DBUG_EXECUTE_IF("simulate_old_8018_allow_null_gcn",
                  { (const_cast<gcn_tuple_t &>(tuple)).reset(); });

  if (!tuple.is_null()) {
    flags |= FLAG_HAVE_GCN;
    gcn = tuple.gcn;

    if (tuple.csr == csr_t::CSR_ASSIGNED) {
      flags |= FLAG_GCN_ASSIGNED;
    }

    if (is_proposal) {
      flags |= FLAG_GCN_PROPOSAL;
    }
  }
}

void Gcn_event::build_branch(const xa_branch_t &in_branch) {
  if (!in_branch.is_null()) {
    flags |= FLAG_HAVE_BRANCH_COUNT;
    branch = in_branch;
  }
}

const gcn_tuple_t Gcn_event::clone_pmmt() const {
  assert(is_pmmt_gcn());
  return {gcn, csr()};
}

const gcn_tuple_t Gcn_event::clone_cmmt() const {
  assert(is_cmmt_gcn());
  return {gcn, csr()};
}

const xa_branch_t Gcn_event::clone_branch() const { return branch; }

}  // end namespace binary_log
