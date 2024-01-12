/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0fsp.h
 Special lizard tablespace implementation.

 Created 2020-03-19 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0fsp_h
#define lizard0fsp_h

#include "db0err.h"
#include "fsp0space.h"

namespace undo {
class Tablespace;
class Tablespaces;
}  // namespace undo

/**
  Lizard Tablespace predefinition:

    a) Tablespace name is defined as lizard_space, and file is named as
  lizard.ibd
    b) File is auto extend, and increment 1MB every time
    ...

  Lizard Tablesapce format:
    ...

  Lizard Tablespace file:
    a) Ignore the m_ignore_read_only setting since it's persist data.

*/

namespace lizard {

/** File status */
enum file_status_t {
  FILE_STATUS_VOID = 0,              /** Status void */
  FILE_STATUS_RW_PERMISSION_ERROR,   /** permisssion error */
  FILE_STATUS_READ_WRITE_ERROR,      /** not readable/writeable*/
  FILE_STATUS_NOT_REGULAR_FILE_ERROR /** not a regular file */
};

/** Judge whether it's lizard tablespace according to space id
@param[in]      space id
@return         true        yes */
extern bool fsp_is_lizard_tablespace(space_id_t space_id);

/** Init the lizard tablespace header when install db
@param[in]      size        tablesapce inited size
@return         true        sucesss */
extern bool fsp_header_init_for_lizard(page_no_t size);

/** Get the size of lizard tablespace from header */
extern page_no_t fsp_header_get_lizard_tablespace_size(void);

/** Lizard system tablespace */
class LizardTablespace : public Tablespace {
 public:
  LizardTablespace()
      : m_auto_extend_last_file(false), m_last_file_size_max(0) {}

  virtual ~LizardTablespace() {
    m_auto_extend_last_file = false;
    m_last_file_size_max = 0;
  }

  /** Verify the size of the physical file.
  @param[in]	file	data file object
  @return DB_SUCCESS if OK else error code. */
  dberr_t check_size(Datafile &file);

  /** Check the data file when innodb_init_files()
  @param[in]      create_new_db     Whether init db or restart
  @param[in]      min_expected_size The min siz of files (bytes)

  @return         DB_SUCCESS        success
 */
  dberr_t check_file_spec(bool create_new_db, ulint min_expected_size);

  /** Check the data file status.
  @param[in]      datafile          file
  @param[in/out]  reason_if_failed
  @return         DB_SUCCESS        success */
  dberr_t check_file_status(const Datafile &data_file,
                            file_status_t &reason_if_failed);

  /** Deal with the file when it's not found
  @param[in]        file              Datafile object
  @param[in]        create_new_db
  @return           DB_SUCCESS        success */
  dberr_t file_not_found(Datafile &file, bool create_new_db);

  /** Deal with the file when it's found
  @param[in]        file              Datafile object
  @return           DB_SUCCESS        success */
  void file_found(Datafile &file);

  /**
    Interpret the Zeus tablespace filename, path and so on.
  */
  bool interpret_file();

  /** Open or create the zeus space
  @param[in]        create_new_db
  @param[out]       size of files
  @return           DB_SUCCESS        success */
  dberr_t open_or_create(bool create_new_db, page_no_t *sum_of_zeus_sizes);

  /** Open file */
  dberr_t open_file(Datafile &file);

  /** Create file */
  dberr_t create_file(Datafile &file);

  dberr_t set_size(Datafile &file);

  void shutdown();

  /** Set the last file size.
  @param[in]	size	the size to set */
  void set_last_file_size(page_no_t size) {
    ut_ad(!m_files.empty());
    m_files.back().m_size = size;
  }

 private:
  bool m_auto_extend_last_file;
  page_no_t m_last_file_size_max;
};

/** Global lizard tablespace object */
extern LizardTablespace srv_lizard_space;

}  // namespace lizard

#endif  // lizard0fsp_h
