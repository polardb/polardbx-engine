/**Copyright (c) 2016, 2018, Alibaba and/or its affiliates. All rights reserved.

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

#include "plugin/keyring_rds/keyring_rds_file_io.h"

#include "my_config.h"

#include <errno.h>
#include <mysql/psi/mysql_file.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sstream>
#include <utility>

#include "my_dbug.h"
#include "mysqld_error.h"
#include "mysys_err.h"
#include "sql/current_thd.h"
#include "sql/mysqld.h"
#include "sql/sql_error.h"

#include "plugin/keyring_rds/keyring_rds_logger.h"

namespace keyring_rds {

static const size_t FILE_IO_ERROR = (size_t)-1;

/**
  Open a file.

  @param [in]  key      Instrumented file key
  @param [in]  name     Path of file to be opened
  @param [in]  flags    Open mode flags
  @param [in]  myFlags  Flags on what to do on error
  @retval      true     Error occurs
  @retval      false    Successfully
*/
bool File_io::open(PSI_file_key file_data_key MY_ATTRIBUTE((unused)),
                   const char *filename, int flags, myf myFlags) {
  File file = mysql_file_open(file_data_key, filename, flags, MYF(0));
  if (file < 0 && (myFlags & MY_WME)) {
    char error_buffer[MYSYS_STRERROR_SIZE];
    uint error_message_number = EE_FILENOTFOUND;
    if (my_errno() == EMFILE) error_message_number = EE_OUT_OF_FILERESOURCES;
    my_warning(error_message_number, filename, my_errno(),
               my_strerror(error_buffer, sizeof(error_buffer), my_errno()));
    return true;
  }

  m_file = file;
  return false;
}

/**
  Close the opened file.

  @param [in]  myFlags Flags on what to do on error
  @retval      non 0   Error occurs
  @retval      0       Successfully
*/
int File_io::close(myf myFlags) {
  if (m_file < 0) return 0;

  int result = mysql_file_close(m_file, MYF(0));
  if (result && (myFlags & MY_WME)) {
    char error_buffer[MYSYS_STRERROR_SIZE];
    my_warning(EE_BADCLOSE, my_filename(m_file), my_errno(),
               my_strerror(error_buffer, sizeof(error_buffer), my_errno()));
  }

  m_file = -1;
  return result;
}

/**
  Read a chunk of bytes specified, or until to EOF.

  @param [out] buffer   Buffer to hold at least Count bytes
  @param [in]  count    Bytes to read
  @param [in]  myFlags  Flags on what to do on error
  @retval      -1       On error
  @retval      N        Number of bytes read actually
*/
size_t File_io::read(uchar *buffer, size_t count, myf myFlags) {
  if (m_file < 0) return FILE_IO_ERROR;

  size_t bytes_read = mysql_file_read(m_file, buffer, count, MYF(MY_FULL_IO));
  if (bytes_read != count && (myFlags & MY_WME)) {
    char error_buffer[MYSYS_STRERROR_SIZE];
    my_warning(EE_READ, my_filename(m_file), my_errno(),
               my_strerror(error_buffer, sizeof(error_buffer), my_errno()));
  }
  return bytes_read;
}

/**
  Write a chunk of bytes to the file.

  @param [in] buffer   Buffer to write
  @param [in] count    Bytes to write
  @param [in] myFlags  Flags on what to do on error
  @retval      -1      On error
  @retval      N       Number of bytes write
*/
size_t File_io::write(const uchar *buffer, size_t count, myf myFlags) {
  if (m_file < 0) return FILE_IO_ERROR;

  size_t bytes_written = mysql_file_write(m_file, buffer, count, MYF(0));
  if (bytes_written != count && (myFlags & (MY_WME))) {
    char error_buffer[MYSYS_STRERROR_SIZE];
    my_warning(EE_WRITE, my_filename(m_file), my_errno(),
               my_strerror(error_buffer, sizeof(error_buffer), my_errno()));
  }
  return bytes_written;
}

/**
  Seek to a position in the file.

  @param [in] pos      The expected position (absolute or relative)
  @param [in] whence   A direction parameter and one of
                       { SEEK_SET, SEEK_CUR, SEEK_END }
  @param [in] myFlags  Flags on what to do on error
  @retval     newpos   The new position in the file.
  @retval     -1       An error was encountered
*/
my_off_t File_io::seek(my_off_t pos, int whence, myf myFlags) {
  if (m_file < 0) return MY_FILEPOS_ERROR;

  my_off_t moved_to_position = mysql_file_seek(m_file, pos, whence, MYF(0));
  if (moved_to_position == MY_FILEPOS_ERROR && (myFlags & MY_WME)) {
    char error_buffer[MYSYS_STRERROR_SIZE];
    my_warning(EE_CANT_SEEK, my_filename(m_file), my_errno(),
               my_strerror(error_buffer, sizeof(error_buffer), my_errno()));
  }
  return moved_to_position;
}

/**
  Tell current position of the file.

  @param [in] myFlags  Flags on what to do on error
  @retval     pos      The position in the file
  @retval     -1       An error was encountered
*/
my_off_t File_io::tell(myf myFlags) {
  if (m_file < 0) return MY_FILEPOS_ERROR;

  my_off_t position = mysql_file_tell(m_file, MYF(0));
  if (position == MY_FILEPOS_ERROR && (myFlags & MY_WME)) {
    char error_buffer[MYSYS_STRERROR_SIZE];
    my_warning(EE_CANT_SEEK, my_filename(m_file), my_errno(),
               my_strerror(error_buffer, sizeof(error_buffer), my_errno()));
  }
  return position;
}

/**
  Sync data in file to disk.

  @param [in] myFlags  Flags on what to do on error
  @retval     0        OK
  @retval     -1       An error was encountered
*/
int File_io::sync(myf myFlags) {
  if (m_file < 0) return -1;

  int result = my_sync(m_file, MYF(0));
  if (result && (myFlags & MY_WME)) {
    char error_buffer[MYSYS_STRERROR_SIZE];
    my_warning(EE_SYNC, my_filename(m_file), my_errno(),
               my_strerror(error_buffer, sizeof(error_buffer), my_errno()));
  }
  return result;
}

/**
  Truncate a file to a specified length.

  @param [in] length   Truncated to the size of precisely length bytes
  @param [in] myFlags  Flags on what to do on error
  @retval      true     Error occurs
  @retval      false    Successfully
*/
bool File_io::truncate(size_t newlen, myf myFlags) {
  if (m_file < 0) return true;

#ifdef _WIN32
  LARGE_INTEGER length;
  length.QuadPart = (long long)newlen;
  HANDLE hFile;
  hFile = (HANDLE)my_get_osfhandle(m_file);

  if ((!SetFilePointerEx(hFile, length, NULL, FILE_BEGIN) ||
       !SetEndOfFile(hFile)) &&
      (myFlags & MY_WME)) {
    my_osmaperr(GetLastError());
    set_my_errno(errno);
#elif defined(HAVE_FTRUNCATE)
  if (ftruncate(m_file, (off_t)newlen) && (myFlags & MY_WME)) {
#else
  assert(0);
  {
#endif
    Logger::log(ERROR_LEVEL, ER_KEYRING_FAILED_TO_TRUNCATE_FILE,
                my_filename(m_file), strerror(errno));
    return true;
  }

  return false;
}

void File_io::my_warning(int nr, ...) {
  const char *format;

  if (!(format = my_get_err_msg(nr))) {
    std::stringstream error_message;
    error_message << "Unknown error " << nr;
    Logger::log(ERROR_LEVEL, ER_KEYRING_UNKNOWN_ERROR, nr);
  } else {
    char warning[MYSQL_ERRMSG_SIZE];
    va_list args;
    va_start(args, nr);
    vsnprintf(warning, sizeof(warning), format, args);
    va_end(args);
    Logger::log(ERROR_LEVEL, ER_KEYRING_FILE_IO_ERROR, warning);
  }
}

}  // namespace keyring_rds
