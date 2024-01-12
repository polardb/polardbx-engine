//
// Created by luoyanxin.pt on 2023/6/27.
//

#include "../global_defines.h"
#ifndef MYSQL8
#define MYSQL_SERVER
#endif

#include "handler_api.h"
#include "log.h"
#include "mysql/psi/mysql_file.h"
#include "mysys_err.h"
#include "physical_backfill.h"
#include "sql/mysqld.h"
#include "sql/sql_class.h"
#include "sql/sql_table.h"  // build_table_filename

#ifndef _WIN32

#include <utime.h>

#else
#include <sys/utime.h>
#endif

#ifdef MYSQL8
#define my_bool bool
#endif

#include "../session/session_base.h"

#ifdef MYSQL8PLUS
const char *part_sep_mark = "#p#";
const char *sub_sep_mark = "#sp#";
#else
#ifdef _WIN32
const char *part_sep_mark = "#p#";
const char *sub_sep_mark = "#sp#";
#else
const char *part_sep_mark = "#P#";
const char *sub_sep_mark = "#SP#";
#endif
#endif

namespace rpc_executor {

polarx_rpc::err_t Physical_backfill::check_file_existence(
    const PolarXRPC::PhysicalBackfill::GetFileInfoOperator &msg,
    PolarXRPC::PhysicalBackfill::GetFileInfoOperator &out_msg) {
  std::string m;
  auto out_table_info = out_msg.mutable_table_info();
  if (msg.has_table_info()) {
    const auto table_info = msg.table_info();
    if (!table_info.has_table_schema() || !table_info.has_table_name()) {
      log_exec_error("ER_X_MISSING_TABLE_INFO missing table info");
      return polarx_rpc::err_t(ER_X_MISSING_TABLE_INFO, " missing table info");
    } else {
      bool use_file_info = (table_info.file_info_size() > 0);
      auto table_schema = table_info.table_schema();
      auto table_name = table_info.table_name();
      bool partitioned = table_info.partitioned();
      int part_count = 1;
      if (partitioned) {
        part_count = table_info.physical_partition_name_size();
        if (part_count == 0) {
          log_exec_error("ER_X_MISSING_PARTITION_NAMES missing partition info");
          return polarx_rpc::err_t(ER_X_MISSING_PARTITION_NAMES,
                                   " missing partition info");
        }
      }

      for (int i = 0; i < part_count; i++) {
        char tbbuff[FN_REFLEN];
        char norm_name[FN_REFLEN];
        memset(tbbuff, 0, FN_REFLEN);
        memset(norm_name, 0, FN_REFLEN);
        std::unique_ptr<char[]> full_name_ptr;
        char *full_file_name;
        if (!use_file_info) {
          build_table_filename(tbbuff, sizeof(tbbuff), table_schema.c_str(),
                               table_name.c_str(), "", 0);
          normalize_table_name_low(norm_name, tbbuff);
          if (partitioned) {
            int cur_len = strlen(norm_name);
            memcpy(norm_name + cur_len, part_sep_mark, 3);
            cur_len += 3;
            memcpy(norm_name + cur_len,
                   table_info.physical_partition_name(i).c_str(),
                   table_info.physical_partition_name(i).size());
          }
#ifdef MYSQL8
          full_file_name = Fil_path::make("", norm_name, IBD, false);
#else
          fil_make_filepath(NULL, norm_name, false, full_name_ptr);
          full_file_name = full_name_ptr.get();
#endif
          if (full_file_name == NULL) {
            log_exec_error(
                "ER_X_CAN_NOT_ALLOCATE_MEMORY can not allocate memory");
            return polarx_rpc::err_t(ER_X_CAN_NOT_ALLOCATE_MEMORY,
                                     "can not allocate memory");
          }
        } else {
          full_file_name =
              (char *)table_info.file_info().Get(i).directory().c_str();
        }
        bool exists;
        // it maybe danger to call the filesystem interface directly......
        file_status(full_file_name, &exists);
        if (!exists) {
          /* Not found should report error. */
          log_exec_error("ER_X_FILE_NOT_EXISTS file:%s is not exists",
                         full_file_name);
          m = "file:";
          m += full_file_name;
          m += " is not exists";
          return polarx_rpc::err_t(ER_X_FILE_NOT_EXISTS, m);
        } else {
          File_desc_info file_desc_info;
          if ((file_desc_info.file = my_open(full_file_name, O_RDONLY | O_SHARE,
                                             MYF(MY_WME))) >= 0) {
            uint64_t file_size = mysql_file_seek(file_desc_info.file, 0L,
                                                 MY_SEEK_END, MYF(MY_WME));
            out_table_info->set_table_schema(table_schema);
            out_table_info->set_table_name(table_name);
            out_table_info->set_partitioned(partitioned);
            auto file_info = out_table_info->add_file_info();
            file_info->set_existence(exists);
            if (!use_file_info) {
              file_info->set_file_name(norm_name);
            } else {
              file_info->set_file_name(
                  table_info.file_info().Get(i).file_name());
            }
            if (partitioned) {
              out_table_info->add_physical_partition_name(
                  table_info.physical_partition_name(i));
              file_info->set_partition_name(
                  table_info.physical_partition_name(i));
            } else {
              file_info->set_partition_name("");
            }
            file_info->set_directory(full_file_name);
            file_info->set_data_size(file_size);
          }
          if (file_desc_info.file < 0) {
            log_exec_error("ER_X_OPEN_FILE_FAIL can not open file:%s",
                           full_file_name);
            m = "can not open file:";
            m += full_file_name;

            return polarx_rpc::err_t(ER_X_OPEN_FILE_FAIL, m);
          };
        }
      }
    }
  } else {
    log_exec_error("ER_X_MISSING_TABLE_INFO missing table info");
    return polarx_rpc::err_t(ER_X_MISSING_TABLE_INFO, "missing table info");
  }
  return polarx_rpc::err_t::Success();
}

polarx_rpc::err_t Physical_backfill::read_buffer(
    const PolarXRPC::PhysicalBackfill::TransferFileDataOperator &msg,
    PolarXRPC::PhysicalBackfill::TransferFileDataOperator &out_msg) {
  std::string m;
  if (msg.has_file_info()) {
    const auto file_info = msg.file_info();
    const auto file_path = file_info.directory();
    bool exists;
    file_status(file_path.c_str(), &exists);
    File_desc_info file_desc_info;
    if (!exists) {
      log_exec_error("ER_X_FILE_NOT_EXISTS file:%s is not exists",
                     file_path.c_str());
      m = "file:";
      m += file_path.c_str();
      m += " is not exists";
      return polarx_rpc::err_t(ER_X_FILE_NOT_EXISTS, m);
    } else {
      if ((file_desc_info.file =
               my_open(file_path.c_str(), O_RDONLY, MYF(MY_WME))) >= 0 &&
          msg.offset() == my_seek(file_desc_info.file, msg.offset(), SEEK_SET,
                                  MYF(MY_WME))) {
        std::unique_ptr<uchar[]> buf_ptr(new uchar[msg.buffer_len()]);
        if (buf_ptr.get() == NULL) {
          log_exec_error(
              "ER_X_CAN_NOT_ALLOCATE_MEMORY can not allocate memory %ld",
              msg.buffer_len());
          m = "can not allocate memory ";
          m += msg.buffer_len();
          return polarx_rpc::err_t(ER_X_CAN_NOT_ALLOCATE_MEMORY, m);
        }
        size_t read_buf_size = my_read(file_desc_info.file, buf_ptr.get(),
                                       msg.buffer_len(), MYF(MY_WME));
        if (((size_t)-1) == read_buf_size) {
          log_exec_error("ER_X_CAN_NOT_READ_BUF can not read file:%s",
                         file_path.c_str());
          m = "can not read file:";
          m += file_path.c_str();
          return polarx_rpc::err_t(ER_X_CAN_NOT_READ_BUF, m);
        } else {
          out_msg.set_offset(msg.offset());
          out_msg.set_buffer(buf_ptr.get(), read_buf_size);
          out_msg.set_buffer_len(read_buf_size);

          auto *const return_file_info = out_msg.mutable_file_info();
          return_file_info->set_file_name(file_info.file_name());
          return_file_info->set_directory(file_info.directory());
          return_file_info->set_partition_name("");
        }
      }
      if (file_desc_info.file < 0) {
        log_exec_error("ER_X_OPEN_FILE_FAIL can not open file:%s",
                       file_path.c_str());
        m = "can not open file:";
        m += file_path.c_str();
        return polarx_rpc::err_t(ER_X_OPEN_FILE_FAIL, m);
      }
    }
  } else {
    log_exec_error("ER_X_MISSING_FILE_INFO missing file info");
    return polarx_rpc::err_t(ER_X_MISSING_FILE_INFO, "missing file info");
  }
  return polarx_rpc::err_t::Success();
}

polarx_rpc::err_t Physical_backfill::write_buffer(
    const PolarXRPC::PhysicalBackfill::TransferFileDataOperator &msg,
    PolarXRPC::PhysicalBackfill::TransferFileDataOperator &out_msg) {
  std::string m;
  if (msg.has_file_info()) {
    const auto file_info = msg.file_info();
    const auto file_path = file_info.directory();

    File_desc_info file_desc_info;
    if ((file_desc_info.file = my_open(file_path.c_str(), O_WRONLY | O_CREAT,
                                       MYF(MY_WME))) >= 0 &&
        msg.offset() ==
            my_seek(file_desc_info.file, msg.offset(), SEEK_SET, MYF(MY_WME))) {
      size_t written_len =
          my_write(file_desc_info.file, (const uchar *)msg.buffer().c_str(),
                   msg.buffer_len(), MYF(MY_WME));
      if (written_len != msg.buffer_len()) {
        log_exec_error(
            "ER_X_CAN_NOT_WRITE_BUF the written buffer len %ld is "
            "not equal to the request len %ld for file:%s",
            written_len, msg.buffer_len(), file_path.c_str());
        m = "the written buffer len ";
        m += written_len;
        m += " is not equal to the request len ";
        m += msg.buffer_len();
        m += " for file:";
        m += file_path.c_str();
        return polarx_rpc::err_t(ER_X_CAN_NOT_WRITE_BUF, m);
      } else {
        out_msg.set_offset(msg.offset());
        out_msg.set_buffer_len(written_len);
        auto *const return_file_info = out_msg.mutable_file_info();
        return_file_info->set_file_name(file_info.file_name());
        return_file_info->set_directory(file_info.directory());
        return_file_info->set_partition_name("");
      }
    }
    if (file_desc_info.file < 0) {
      log_exec_error("ER_X_OPEN_FILE_FAIL can not open file:%s",
                     file_path.c_str());
      m = "can not open file:";
      m += file_path.c_str();
      return polarx_rpc::err_t(ER_X_OPEN_FILE_FAIL, m);
    }
    return polarx_rpc::err_t::Success();
  } else {
    log_exec_error("ER_X_MISSING_FILE_INFO missing file info");
    return polarx_rpc::err_t(ER_X_MISSING_FILE_INFO, "missing file info");
  }
}

polarx_rpc::err_t Physical_backfill::pre_allocate(
    const PolarXRPC::PhysicalBackfill::FileManageOperator &msg) {
  std::string m;
  if (msg.has_table_info()) {
    const auto file_infos = msg.table_info().file_info();
    if (file_infos.size() != 1) {
      log_exec_error(
          "ER_X_BAD_FILE_INFO only support to pre_allocate one file per time");
      return polarx_rpc::err_t(
          ER_X_BAD_FILE_INFO, "only support to pre_allocate one file per time");
    }
    const auto file_info = file_infos.Get(0);
    const auto full_file_name = file_info.directory();

    File_desc_info file_desc_info;

    bool exists;
    file_status(full_file_name.c_str(), &exists);
    if (exists) {
      return polarx_rpc::err_t::Success();
    } else {
      if ((file_desc_info.file = my_open(full_file_name.c_str(),
                                         O_CREAT | O_RDWR, MYF(MY_WME))) >= 0) {
#if !defined(NO_FALLOCATE) && defined(UNIV_LINUX)
        uint64 size = file_info.data_size();
        /* This is required by FusionIO HW/Firmware */
#ifdef MYSQL8
        int ret = fallocate(file_desc_info.file, 0, 0, size);
#else
        int ret = MyFSHelper::fallocate(file_desc_info.file, 0, 0, size);
#endif
        if (ret != 0) {
          log_exec_error(
              "posix_fallocate(): Failed to preallocate data for file %s , "
              "desired size %llu Operating system error number %d. "
              "Check that the disk is not full or a disk quota"
              " exceeded. Make sure the file system supports"
              " this function.",
              full_file_name.c_str(), size, ret);
        }

#endif /* !NO_FALLOCATE && UNIV_LINUX */
      }
      if (file_desc_info.file < 0) {
        log_exec_error("ER_X_OPEN_FILE_FAIL can not open file:%s",
                       full_file_name.c_str());
        m = "can not open file:";
        m += full_file_name.c_str();
        return polarx_rpc::err_t(ER_X_OPEN_FILE_FAIL, m);
      }
    }
    return polarx_rpc::err_t::Success();
  } else {
    log_exec_error("ER_X_MISSING_FILE_INFO missing file info");
    return polarx_rpc::err_t(ER_X_MISSING_FILE_INFO, "missing file info");
  }
}

polarx_rpc::err_t Physical_backfill::delete_file(
    const PolarXRPC::PhysicalBackfill::FileManageOperator &msg) {
  if (msg.has_table_info()) {
    const auto file_infos = msg.table_info().file_info();
    if (file_infos.size() != 1) {
      log_exec_error(
          "ER_X_BAD_FILE_INFO only support to delete one file per time");
      return polarx_rpc::err_t(ER_X_BAD_FILE_INFO,
                               "only support to delete one file per time");
    } else {
      const auto temp_file_path = file_infos.Get(0).directory();
      if (my_delete(temp_file_path.c_str(), MYF(0)) == 0) {
      } else {
        // unexpect_msg = true;
        // ignore error
      }
    }
  } else {
    log_exec_error("ER_X_MISSING_FILE_INFO missing file info");
    return polarx_rpc::err_t(ER_X_MISSING_FILE_INFO, "missing file info");
  }
  return polarx_rpc::err_t::Success();
}

polarx_rpc::err_t Physical_backfill::clone_file(
    const PolarXRPC::PhysicalBackfill::FileManageOperator &msg, MYSQL_THD thd) {
  std::string m;
  if (msg.has_table_info()) {
    const auto file_infos = msg.table_info().file_info();
    if (file_infos.size() % 2 != 0 || file_infos.size() == 0) {
      log_exec_error(
          "ER_X_BAD_FILE_INFO expect to specified even files input, but get %d",
          file_infos.size());
      m = "expect to specified even files input, but get";
      m += file_infos.size();
      return polarx_rpc::err_t(ER_X_BAD_FILE_INFO, m);
    } else {
      for (int i = 0; i < file_infos.size(); i = i + 2) {
        const auto src_file_path = file_infos.Get(i).directory();
        const auto clone_file_path = file_infos.Get(i + 1).directory();

        std::string info;
        info = "clone file ";
        info += src_file_path.c_str();

        thd->reset_for_next_command();
        polarx_rpc::CsessionBase::begin_query(thd, info.c_str(), info.length());

        if (my_copy_interrutable(src_file_path.c_str(), clone_file_path.c_str(),
                                 MYF(MY_WME), thd) == 0) {
          polarx_rpc::CsessionBase::end_query(thd);
        } else {
          my_delete(clone_file_path.c_str(), MYF(0));
          log_exec_error(
              "ER_X_COPY_FILE_FAIL can not copy the file from %s, to %s",
              src_file_path.c_str(), clone_file_path.c_str());
          m = "can not copy the file from ";
          m += src_file_path.c_str();
          m += " to ";
          m += clone_file_path.c_str();
          polarx_rpc::CsessionBase::end_query(thd);
          return polarx_rpc::err_t(ER_X_COPY_FILE_FAIL, m);
        }
      }
    }
  } else {
    log_exec_error("ER_X_MISSING_FILE_INFO missing file info");
    return polarx_rpc::err_t(ER_X_MISSING_FILE_INFO, "missing file info");
  }
  return polarx_rpc::err_t::Success();
}

void Physical_backfill::normalize_table_name_low(char *norm_name,
                                                 const char *name) {
  char *name_ptr;
  unsigned long int name_len;
  char *db_ptr;
  unsigned long int db_len;
  char *ptr;
  unsigned long int norm_len;

#ifdef _WIN32
  bool set_lower_case = 1;
#else
  bool set_lower_case = 0;
#endif /* _WIN32 */
  /* Scan name from the end */

  ptr = const_cast<char *>(strend(name)) - 1;

  /* seek to the last path separator */
  while (ptr >= name && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }

  name_ptr = ptr + 1;
  name_len = strlen(name_ptr);

  /* skip any number of path separators */
  while (ptr >= name && (*ptr == '\\' || *ptr == '/')) {
    ptr--;
  }

  assert(ptr >= name);

  /* seek to the last but one path separator or one char before
  the beginning of name */
  db_len = 0;
  while (ptr >= name && *ptr != '\\' && *ptr != '/') {
    ptr--;
    db_len++;
  }

  db_ptr = ptr + 1;

  norm_len = db_len + name_len + sizeof "/";
  LIKELY(norm_len < FN_REFLEN - 1);

  memcpy(norm_name, db_ptr, db_len);

  norm_name[db_len] = '/';

  /* Copy the name and null-byte. */
  memcpy(norm_name + db_len + 1, name_ptr, name_len + 1);

  if (set_lower_case) {
    my_casedn_str(system_charset_info, norm_name);
  }
}

#ifndef MYSQL8
void Physical_backfill::fil_make_filepath(const char *path, const char *name,
                                          bool trim_name,
                                          std::unique_ptr<char[]> &output) {
  /* The path may contain the basename of the file, if so we do not
  need the name.  If the path is NULL, we can use the default path,
  but there needs to be a name. */
  LIKELY(path != NULL || name != NULL);

  /* If we are going to strip a name off the path, there better be a
  path and a new name to put back on. */
  LIKELY(!trim_name || (path != NULL && name != NULL));

  if (path == NULL) {
    path = fil_path_to_mysql_datadir;
  }

  unsigned long int len = 0; /* current length */
  unsigned long int path_len = strlen(path);
  unsigned long int name_len = (name ? strlen(name) : 0);
  const char *suffix = ".ibd";
  unsigned long int suffix_len = strlen(suffix);
  unsigned long int full_len = path_len + 1 + name_len + suffix_len + 1;

  std::unique_ptr<char[]> full_name_ptr(new char[full_len]);
  char *full_name = full_name_ptr.get();
  if (full_name == NULL) {
    return;
  }

  /* If the name is a relative path, do not prepend "./". */
  if (path[0] == '.' && (path[1] == '\0' || path[1] == OS_PATH_SEPARATOR) &&
      name != NULL && name[0] == '.') {
    path = NULL;
    path_len = 0;
  }

  if (path != NULL) {
    memcpy(full_name, path, path_len);
    len = path_len;
    full_name[len] = '\0';
    os_normalize_path(full_name);
  }

  if (trim_name) {
    /* Find the offset of the last DIR separator and set it to
    null in order to strip off the old basename from this path. */
    char *last_dir_sep = strrchr(full_name, OS_PATH_SEPARATOR);
    if (last_dir_sep) {
      last_dir_sep[0] = '\0';
      len = strlen(full_name);
    }
  }

  if (name != NULL) {
    if (len && full_name[len - 1] != OS_PATH_SEPARATOR) {
      /* Add a DIR separator */
      full_name[len] = OS_PATH_SEPARATOR;
      full_name[++len] = '\0';
    }

    char *ptr = &full_name[len];
    memcpy(ptr, name, name_len);
    len += name_len;
    full_name[len] = '\0';
    os_normalize_path(ptr);
  }

  /* Make sure that the specified suffix is at the end of the filepath
  string provided. This assumes that the suffix starts with '.'.
  If the first char of the suffix is found in the filepath at the same
  length as the suffix from the end, then we will assume that there is
  a previous suffix that needs to be replaced. */
  if (suffix != NULL) {
    /* Need room for the trailing null byte. */
    LIKELY(len < full_len);

    if ((len > suffix_len) && (full_name[len - suffix_len] == suffix[0])) {
      /* Another suffix exists, make it the one requested. */
      memcpy(&full_name[len - suffix_len], suffix, suffix_len);

    } else {
      /* No previous suffix, add it. */
      LIKELY(len + suffix_len < full_len);
      memcpy(&full_name[len], suffix, suffix_len);
      full_name[len + suffix_len] = '\0';
    }
  }
  output = std::move(full_name_ptr);
}
#endif

// the code is copy from my_copy.c
int Physical_backfill::my_copy_interrutable(const char *from, const char *to,
                                            myf MyFlags, MYSQL_THD thd) {
  size_t Count;
  my_bool new_file_stat = 0; /* 1 if we could stat "to" */
  int create_flag;
  File from_file, to_file;
  uchar buff[IO_SIZE];
  MY_STAT stat_buff, new_stat_buff;
  DBUG_ENTER("my_copy_interrutable");
  DBUG_PRINT("my", ("from %s to %s MyFlags %d", from, to, MyFlags));

  from_file = to_file = -1;
  memset(&new_stat_buff, 0, sizeof(MY_STAT));
  assert(!(MyFlags & (MY_FNABP | MY_NABP))); /* for my_read/my_write */
  if (MyFlags & MY_HOLD_ORIGINAL_MODES)      /* Copy stat if possible */
    new_file_stat = my_stat((char *)to, &new_stat_buff, MYF(0)) ? 1 : 0;
  THD_STAGE_INFO(thd, stage_executing);
  thd->set_command(COM_QUERY);
  if ((from_file = my_open(from, O_RDONLY | O_SHARE, MyFlags)) >= 0) {
    if (!my_stat(from, &stat_buff, MyFlags)) {
      set_my_errno(errno);
      goto err;
    }
    if (MyFlags & MY_HOLD_ORIGINAL_MODES && new_file_stat)
      stat_buff = new_stat_buff;
    create_flag = (MyFlags & MY_DONT_OVERWRITE_FILE) ? O_EXCL : O_TRUNC;

    if ((to_file = my_create(to, (int)stat_buff.st_mode,
                             O_WRONLY | create_flag | O_BINARY | O_SHARE,
                             MyFlags)) < 0)
      goto err;

    while ((Count = my_read(from_file, buff, sizeof(buff), MyFlags)) != 0) {
      if (thd_killed(thd)) {
        log_exec_error("ER_X_COPY_FILE_FAIL session was killed");
        thd->send_kill_message();
        goto err;
      }
      if (Count == (uint)-1 ||
          my_write(to_file, buff, Count, MYF(MyFlags | MY_NABP)))
        goto err;
    }

    /* sync the destination file */
    if (MyFlags & MY_SYNC) {
      if (my_sync(to_file, MyFlags)) goto err;
    }

    if (my_close(from_file, MyFlags) | my_close(to_file, MyFlags))
      DBUG_RETURN(-1); /* Error on close */

    /* Reinitialize closed fd, so they won't be closed again. */
    from_file = -1;
    to_file = -1;

    /* Copy modes if possible */

    if (MyFlags & MY_HOLD_ORIGINAL_MODES && !new_file_stat)
      DBUG_RETURN(0); /* File copyed but not stat */
    /* Copy modes */
    if (chmod(to, stat_buff.st_mode & 07777)) {
      set_my_errno(errno);
      if (MyFlags & (MY_FAE + MY_WME)) {
        char errbuf[MYSYS_STRERROR_SIZE];
        my_error(EE_CHANGE_PERMISSIONS, MYF(0), from, errno,
                 my_strerror(errbuf, sizeof(errbuf), errno));
      }
      goto err;
    }
#if !defined(_WIN32)
    /* Copy ownership */
    if (chown(to, stat_buff.st_uid, stat_buff.st_gid)) {
      set_my_errno(errno);
      if (MyFlags & (MY_FAE + MY_WME)) {
        char errbuf[MYSYS_STRERROR_SIZE];
        my_error(EE_CHANGE_OWNERSHIP, MYF(0), from, errno,
                 my_strerror(errbuf, sizeof(errbuf), errno));
      }
      goto err;
    }
#endif

    if (MyFlags & MY_COPYTIME) {
      struct utimbuf timep;
      timep.actime = stat_buff.st_atime;
      timep.modtime = stat_buff.st_mtime;
      (void)utime((char *)to, &timep); /* last accessed and modified times */
    }

    DBUG_RETURN(0);
  }

err:
  if (from_file >= 0) (void)my_close(from_file, MyFlags);
  if (to_file >= 0) {
    (void)my_close(to_file, MyFlags);
    /* attempt to delete the to-file we've partially written */
    (void)my_delete(to, MyFlags);
  }
  DBUG_RETURN(-1);
} /* my_copy */
}  // namespace rpc_executor
