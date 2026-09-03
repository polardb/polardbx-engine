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

/** @file sql/komodo0xlog0meta.cc

  Binlog behaviours related to komodo lizard system.

  Created 2023-06-20 by Jianhua.sjh
 *******************************************************/

#include "sql/komodo0xlog0meta.h"
#include <cassert>
#include "sql/consensus_log_manager.h"
#include "sql/rpl_info_factory.h"
namespace komodo {

/**
 * Constructor for XLog_meta, prepare all meta info about cluster.
 *
 * @param force_learner Boolean indicating if this is a learner node.
 * @param initialize Boolean indicating if we are initializing the server.
 *
 */
XLog_meta::XLog_meta(const bool force_learner, const bool initialize,
                     const bool enable_consensus)
    : m_force_learner(force_learner) {
  /** 1. Prepare consensus repo metadata, consensus_info object */
  Rpl_info_factory::init_consensus_repo_metadata();
  consensus_info = Rpl_info_factory::create_consensus_info();
  if (!consensus_info) unireg_abort(MYSQLD_ABORT_EXIT);

  /** If we are initializing the server, we will init consensus_info later. */
  if (initialize) return;

  /** 2. Initialize consensus info. */
  consensus_info->consensus_init_info();

  /** We never modify consensus info if consensus is disabled. */
  if (!enable_consensus) return;

  /** 3. Adjust cluster info if not specified during initialization. */
  if (consensus_info->get_cluster_info() == "" &&
      consensus_info->get_cluster_learner_info() == "" &&
      consensus_info->reinit_cluster_info(m_force_learner)) {
    unireg_abort(MYSQLD_ABORT_EXIT);
  }

  /** 4. Force flush consensus info. */
  consensus_info->flush_info(true, true);
}

XLog_meta::~XLog_meta() {
  if (consensus_info) consensus_info->end_info();
  delete consensus_info;
}

/**
 * Lazy initialization of consensus info. This could happend only when
 * initializing the server, i.e opt_initialize.
 */
void XLog_meta::try_init_consensus_info() {
  assert(consensus_info);
  assert(opt_initialize);

  /** 1. Initialize consensus info. */
  consensus_info->consensus_init_info();

  /** 2. Adjust cluster or learner info if specified.  */
  if (opt_cluster_info) {
    consensus_info->reinit_cluster_info(m_force_learner);
  }

  /** 3. Flush consensus info. */
  consensus_info->flush_info(true, true);

  xp::system(ER_XP_0) << "try_init_consensus_info successfully "
                      << opt_cluster_info;
}
}  // namespace komodo
