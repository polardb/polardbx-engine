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
#include "sql/consensus/consensus_common.h"
#include <sstream>
#include "my_alloc.h"
#include "mysql/psi/mysql_file.h"
#include "sql/komodo0xlog.h"
#include "sql/sys_vars_consensus.h"

namespace im {

void collect_show_logs_results(
    MEM_ROOT *mem_root, std::vector<Consensus_show_logs_result *> &results) {
  IO_CACHE *index_file;
  LOG_INFO cur;
  File file;
  char fname[FN_REFLEN];
  size_t length;
  size_t cur_dir_len;
  GUARDED_READ_CONSENSUS_LOG();

  if (!consensus_log->is_open()) {
    my_error(ER_NO_BINARY_LOGGING, MYF(0));
    return;
  }

  mysql_mutex_lock(consensus_log->get_log_lock());
  auto index_guard = create_lock_guard(
    [&] { consensus_log->lock_index(); },
    [&] { consensus_log->unlock_index(); }
  );

  index_file = consensus_log->get_index_file();
  consensus_log->raw_get_current_log(&cur);           // dont take mutex

  mysql_mutex_unlock(consensus_log->get_log_lock());  // lockdep, OK

  cur_dir_len = dirname_length(cur.log_file_name);

  reinit_io_cache(index_file, READ_CACHE, (my_off_t)0, 0, 0);

  /* The file ends with EOF or empty line */
  while ((length = my_b_gets(index_file, fname, sizeof(fname))) > 1) {
    size_t dir_len;
    ulonglong file_length = 0;  // Length if open fails
    fname[--length] = '\0';     // remove the newline

    Consensus_show_logs_result *result =
        new (mem_root) Consensus_show_logs_result();
    dir_len = dirname_length(fname);
    length -= dir_len;
    result->log_name.str = strmake_root(mem_root, fname + dir_len, length);
    result->log_name.length = length;

    if (!(strncmp(fname + dir_len, cur.log_file_name + cur_dir_len, length))) {
      file_length = cur.pos; /* The active log, use the active position */
    } else {
      /* this is an old log, open it and find the size */
      if ((file = mysql_file_open(key_file_binlog, fname,
                                  O_RDONLY | O_SHARE | O_BINARY, MYF(0))) >=
          0) {
        file_length = (ulonglong)mysql_file_seek(file, 0L, MY_SEEK_END, MYF(0));
        mysql_file_close(file, MYF(0));
      }
    }
    result->file_size = file_length;

    uint64 start_index =
        komodo::xlog_engine_get_log_file_index()->get_start_index_of_file(
            std::string(fname));
    result->start_log_index = start_index;
    results.push_back(result);
  }

  return;
}

} /* namespace im */
