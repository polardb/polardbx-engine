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

#ifndef KEYRING_RDS_MYSQL_FILE_IO_H
#define KEYRING_RDS_MYSQL_FILE_IO_H

#include "my_dir.h"
#include "my_inttypes.h"
#include "my_io.h"
#include "plugin/keyring_rds/keyring_rds_memory.h"

namespace keyring_rds {

/**
  This class wraps mysql_file_xxx routines, but myFlags will be processed by
  itself instead of the rwapped ones. If error occurs, log error messsages.
*/
class File_io : public Keyring_obj_alloc {
 public:
  File_io() : m_file(-1) {}
  ~File_io() { close(); }

  /**
    Open a file.

    @param [in]  key      Instrumented file key
    @param [in]  name     Path of file to be opened
    @param [in]  flags    Open mode flags
    @param [in]  myFlags  Flags on what to do on error

    @retval      true     Error occurs
    @retval      false    Successfully
  */
  bool open(PSI_file_key key, const char *name, int flags,
            myf myFlags = MYF(MY_WME));

  /**
    Close the opened file.

    @param [in]  myFlags  Flags on what to do on error

    @retval      non 0   Error occurs
    @retval      0       Successfully
  */
  int close(myf myFlags = MYF(MY_WME));

  /**
    Read a chunk of bytes specified, or until to EOF.

    @param [out] buffer   Buffer to hold at least Count bytes
    @param [in]  count    Bytes to read
    @param [in]  myFlags  Flags on what to do on error

    @retval      -1       On error
    @retval      N        Number of bytes read
  */
  size_t read(uchar *buffer, size_t count, myf myFlags = MYF(MY_WME));

  /**
    Write a chunk of bytes to the file.

    @param [in] buffer   Buffer to write
    @param [in] count    Bytes to write
    @param [in] myFlags  Flags on what to do on error

    @retval      -1      On error
    @retval      N       Number of bytes write
  */
  size_t write(const uchar *buffer, size_t count, myf myFlags = MYF(MY_WME));

  /**
    Seek to a position in the file.

    @param [in] pos      The expected position (absolute or relative)
    @param [in] whence   A direction parameter and one of
                         { SEEK_SET, SEEK_CUR, SEEK_END }
    @param [in] myFlags  Flags on what to do on error

    @retval     newpos   The new position in the file.
    @retval     -1       An error was encountered
  */
  my_off_t seek(my_off_t pos, int whence, myf myFlags = MYF(MY_WME));

  /**
    Tell current position of the file.

    @param [in] myFlags  Flags on what to do on error

    @retval     pos      The position in the file
    @retval     -1       An error was encountered
  */
  my_off_t tell(myf flags = MYF(MY_WME));

  /**
    Sync data in file to disk.

    @param [in] myFlags  Flags on what to do on error

    @retval     0        OK
    @retval     -1       An error was encountered
  */
  int sync(myf myFlags = MYF(MY_WME));

  /**
    Truncate a file to a specified length.

    @param [in] length   Truncated to the size of precisely length bytes
    @param [in] myFlags  Flags on what to do on error

    @retval      true     Error occurs
    @retval      false    Successfully
  */
  bool truncate(size_t length, myf myFlags = MYF(MY_WME));

 private:
  /* Helper routine to log plugin error message */
  void my_warning(int nr, ...);

 private:
  File m_file; /* File descriptor */
};

}  // namespace keyring_rds

#endif  // KEYRING_RDS_MYSQL_FILE_IO_H
