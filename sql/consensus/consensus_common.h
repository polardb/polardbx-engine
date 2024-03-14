/* Copyright (c) 2018, 2023, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef CONSENSUS_CORE_INCLUDED
#define CONSENSUS_CORE_INCLUDED

#include "lex_string.h"
#include "sql/bl_consensus_log.h"

#define CONSENSUS_MAX_NODE_NUMBER 100

struct MEM_ROOT;

namespace im {

/* Result structure for show_cluster_global */
typedef struct Consensus_show_global_result {
  ulonglong id;
  LEX_STRING ip_port;
  ulonglong match_index;
  ulonglong next_index;
  LEX_CSTRING role;
  LEX_CSTRING force_sync;
  ulonglong election_weight;
  ulonglong learner_source;
  ulonglong applied_index;
  LEX_CSTRING pipelining;
  LEX_CSTRING send_applied;
} Consensus_show_global_result;

/* Result structure for show_cluster_local */
typedef struct Consensus_show_local_result {
  ulonglong id;
  ulonglong current_term;
  LEX_STRING current_leader;
  ulonglong commit_index;
  ulonglong last_log_term;
  ulonglong last_log_index;
  LEX_CSTRING role;
  ulonglong vote_for;
  ulonglong applied_index;
  LEX_CSTRING server_ready_for_rw;
  LEX_CSTRING instance_type;
} Consensus_show_local_result;

/* Result structure for show_logs */
typedef struct Consensus_show_logs_result {
  LEX_STRING log_name;
  ulonglong file_size;
  ulonglong start_log_index;
} Consensus_show_logs_result;

void collect_show_global_results(
    MEM_ROOT *mem_root, std::vector<Consensus_show_global_result *> &results);
void collect_show_local_results(MEM_ROOT *mem_root,
                                Consensus_show_local_result *results);
void collect_show_logs_results(
    MEM_ROOT *mem_root, std::vector<Consensus_show_logs_result *> &results);

} /* namespace im */

#endif
