/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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


//
// Created by luoyanxin.pt on 2023/6/27.
//

#ifndef MYSQL_TABLESPACEDATAFILE_H
#define MYSQL_TABLESPACEDATAFILE_H

#ifdef _WIN32
#define OS_PATH_SEPARATOR '\\'
#define OS_PATH_SEPARATOR_ALT '/'
#else
#define OS_PATH_SEPARATOR '/'
#define OS_PATH_SEPARATOR_ALT '\\'
#endif

#endif  // MYSQL_TABLESPACEDATAFILE_H

#include "../coders/protocol_fwd.h"
#include "../polarx_rpc.h"
#include "../utility/error.h"
#include "coders/command_delegate.h"
#ifdef MYSQL8
#include "storage/innobase/include/fil0fil.h"
#else
#include "myfs_helper.h"
#endif
#include "../global_defines.h"

#ifndef MYSQL8
extern const char *fil_path_to_mysql_datadir;
#endif
namespace rpc_executor {

class Physical_backfill {
 public:
  static Physical_backfill &instance() {
    static Physical_backfill physical_backfill;
    return physical_backfill;
  }

  class File_desc_info {
   public:
    File file;

    File_desc_info() { file = -1; }

    ~File_desc_info() {
      if (file > 0) {
        my_close(file, MYF(0));
        file = -1;
      }
    }
  };

  polarx_rpc::err_t check_file_existence(
      const PolarXRPC::PhysicalBackfill::GetFileInfoOperator &msg,
      PolarXRPC::PhysicalBackfill::GetFileInfoOperator &out_msg);

  polarx_rpc::err_t pre_allocate(
      const PolarXRPC::PhysicalBackfill::FileManageOperator &msg);

  polarx_rpc::err_t read_buffer(
      const PolarXRPC::PhysicalBackfill::TransferFileDataOperator &msg,
      PolarXRPC::PhysicalBackfill::TransferFileDataOperator &out_msg);

  polarx_rpc::err_t write_buffer(
      const PolarXRPC::PhysicalBackfill::TransferFileDataOperator &msg,
      PolarXRPC::PhysicalBackfill::TransferFileDataOperator &out_msg);

  polarx_rpc::err_t clone_file(
      const PolarXRPC::PhysicalBackfill::FileManageOperator &msg,
      MYSQL_THD thd);

  polarx_rpc::err_t delete_file(
      const PolarXRPC::PhysicalBackfill::FileManageOperator &msg);

  void normalize_table_name_low(char *norm_name, const char *name);
#ifndef MYSQL8
  void fil_make_filepath(const char *path, const char *name, bool trim_name,
                         std::unique_ptr<char[]> &output);
#endif
  int my_copy_interrutable(const char *from, const char *to, myf MyFlags,
                           MYSQL_THD thd);
};

inline void file_status(const char *path, bool *exists) {
#ifdef _WIN32
  int ret;
  struct _stat64 stat_info;

  ret = _stat64(path, &stat_info);

  *exists = !ret;
#else
  struct stat stat_info;

#ifdef MYSQL8
  int ret = stat(path, &stat_info);
#else
  int ret = MyFSHelper::stat(path, &stat_info);
#endif

  *exists = !ret;
#endif /* _WIN32 */
}

inline void os_normalize_path(char *str) {
  if (str != NULL) {
    for (; *str; str++) {
      if (*str == OS_PATH_SEPARATOR_ALT) {
        *str = OS_PATH_SEPARATOR;
      }
    }
  }
}
}  // namespace rpc_executor