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

/** @file fsp/lizard0fsp.cc
 Special Lizard tablespace implementation.

 Created 2020-03-19 by Jianwei.zhao
 *******************************************************/
#include "mtr0mtr.h"
#include "trx0purge.h"

#include "lizard0dict.h"
#include "lizard0fil.h"
#include "lizard0fsp.h"
#include "lizard0fspspace.h"
#include "lizard0txn.h"

namespace lizard {

/** Global lizard tablespace object */
LizardTablespace srv_lizard_space;

/** Judge whether it's lizard tablespace according to space id
@param[in]      space id
@return         true        yes */
bool fsp_is_lizard_tablespace(space_id_t space_id) {
  return space_id == dict_lizard::s_lizard_space_id;
}

/** Init the lizard tablespace header when install db
@param[in]      size        tablesapce inited size
@return         true        sucesss */
bool fsp_header_init_for_lizard(page_no_t size) {
  DBUG_TRACE;
  mtr_t mtr;
  mtr_start(&mtr);

  bool ret = fsp_header_init(dict_lizard::s_lizard_space_id, size, &mtr);

  mtr_commit(&mtr);

  return ret;
}

/** Get the size of lizard tablespace from header */
page_no_t fsp_header_get_lizard_tablespace_size(void) {
  fil_space_t *space = fil_space_get_lizard_space();
  mtr_t mtr;

  mtr_start(&mtr);

  mtr_x_lock_space(space, &mtr);

  fsp_header_t *header;

  header = fsp_get_space_header(dict_lizard::s_lizard_space_id, univ_page_size,
                                &mtr);

  page_no_t size;
  size = mach_read_from_4(header + FSP_SIZE);

  ut_ad(space->size_in_header == size);

  mtr_commit(&mtr);

  return size;
}

/** Frees the memory */
void LizardTablespace::shutdown() {
  Tablespace::shutdown();

  m_auto_extend_last_file = 0;
  m_last_file_size_max = 0;
}

/**
  Interpret the lizard tablespace configure.
  For the simplicity most of the variables are hard code.

  @retval   true    Success
*/
bool LizardTablespace::interpret_file() {
  DBUG_TRACE;

  const char *file_name;
  page_no_t size;

  ut_ad(!m_auto_extend_last_file);
  ut_ad(m_last_file_size_max == 0);

  /** flags, path, and name should have been set in advance */
  ut_ad(flags() != 0);
  ut_ad(path() && name());

  /** The page size is system page size from srv_page_size. */
  size = static_cast<page_no_t>(dict_lizard::s_file_init_size / UNIV_PAGE_SIZE);
  ut_ad(size > 16);

  ut_a(dict_lizard::s_n_files == 1);
  file_name = dict_lizard::s_lizard_space_file_name;
  m_auto_extend_last_file = dict_lizard::s_file_auto_extend;
  m_last_file_size_max = dict_lizard::s_last_file_max_size;

  m_files.push_back(Datafile(file_name, flags(), size, 0));
  Datafile *df = &m_files.back();
  df->make_filepath(path(), file_name, NO_EXT);

  /** TODO: here didn't support raw device */
  df->m_type = SRV_NOT_RAW;

  ut_ad(dict_lizard::s_n_files == ulint(m_files.size()));

  return true;
}

/**
  Check the data file status.

  @param[in]      datafile          file
  @param[in/out]  reason_if_failed

  @retval         DB_SUCCESS        success
*/
dberr_t LizardTablespace::check_file_status(const Datafile &file,
                                            file_status_t &reason_if_failed) {
  dberr_t err = DB_SUCCESS;
  os_file_stat_t stat;

  /** Lizard is persist data tablespace, so affected by srv_read_only_mode */
  ut_ad(!m_ignore_read_only);

  memset(&stat, 0x0, sizeof(stat));
  err = os_file_get_status(file.m_filepath, &stat, true, srv_read_only_mode);

  reason_if_failed = FILE_STATUS_VOID;
  switch (err) {
    case DB_FAIL:
      lizard_error(ER_LIZARD) << "Check file failed on " << file.name();
      err = DB_ERROR;
      reason_if_failed = FILE_STATUS_RW_PERMISSION_ERROR;
      break;
    case DB_SUCCESS:
      if (stat.type == OS_FILE_TYPE_FILE) {
        if (!stat.rw_perm) {
          lizard_error(ER_LIZARD)
              << "The " << file.name() << " must be writable or readable";
          err = DB_ERROR;
          reason_if_failed = FILE_STATUS_READ_WRITE_ERROR;
        }
      } else {
        lizard_error(ER_LIZARD) << "The " << file.name() << " file is not "
                                << " a regular InnoDB data file";
        err = DB_ERROR;
        reason_if_failed = FILE_STATUS_NOT_REGULAR_FILE_ERROR;
      }
      break;
    case DB_NOT_FOUND:
      break;
    default:
      ut_ad(0);
  }

  return err;
}

/**
  Deal with the file when it's found

  @param[in]        file              Datafile object

  @retval           DB_SUCCESS        success */
void LizardTablespace::file_found(Datafile &file) {
  file.m_exists = true;
  file.set_open_flags(OS_FILE_OPEN);
}

/**
  Deal with the file when it's not found

  @param[in]        file              Datafile object
  @param[in]        create_new_db

  @retval           DB_SUCCESS        success */
dberr_t LizardTablespace::file_not_found(Datafile &file, bool create_new_db) {
  file.m_exists = false;

  if (!create_new_db) {
    lizard_error(ER_LIZARD)
        << "Data file " << file.name() << " didn't found when boot InnoDB";
    return DB_ERROR;
  }
  lizard_info(ER_LIZARD) << "Create new data file " << file.name() << " in "
                         << name();
  file.set_open_flags(OS_FILE_CREATE);
  return DB_SUCCESS;
}

/**
  Check the data file when innodb_init_files()

  @param[in]      create_new_db     Whether init db or restart
  @param[in]      min_expected_size The min siz of files (bytes)

  @return         DB_SUCCESS        success
*/
dberr_t LizardTablespace::check_file_spec(bool create_new_db,
                                          ulint min_expected_size) {
  dberr_t err = DB_SUCCESS;

  ut_ad(!m_ignore_read_only);

  /** Data file amount limit check */
  if (m_files.size() >= 2) {
    lizard_error(ER_LIZARD) << "It must be less than 2 files in " << name()
                            << " But actually has " << m_files.size();
    return DB_ERROR;
  }

  /** Data file size check */
  if (get_sum_of_sizes() < min_expected_size / UNIV_PAGE_SIZE) {
    lizard_error(ER_LIZARD) << "Tablespace files size is less than "
                            << min_expected_size / 1024 / 1024 << "MB";
    return DB_ERROR;
  }

  for (auto it = m_files.begin(); it != m_files.end(); it++) {
    file_status_t reason_if_failed;
    err = check_file_status(*it, reason_if_failed);

    if (err == DB_NOT_FOUND) {
      err = file_not_found(*it, create_new_db);
      if (err != DB_SUCCESS) break;
    } else if (err != DB_SUCCESS) {
      lizard_error(ER_LIZARD)
          << "Check file status " << it->name() << " failed ";
      break;
    } else if (create_new_db) {
      lizard_error(ER_LIZARD)
          << "Find the data file " << it->name() << " when create database";
      err = DB_ERROR;
      break;
    } else {
      file_found(*it);
    }
  }
  return err;
}

/** Verify the size of the physical file.
@param[in]	file	data file object
@return DB_SUCCESS if OK else error code. */
dberr_t LizardTablespace::check_size(Datafile &file) {
  os_offset_t size = os_file_get_size(file.m_handle);
  ut_a(size != (os_offset_t)-1);
  ut_a(m_files.size() == 1);

  /* Under some error conditions like disk full scenarios
  or file size reaching filesystem limit the data file
  could contain an incomplete extent at the end. When we
  extend a data file and if some failure happens, then
  also the data file could contain an incomplete extent.
  So we need to round the size downward to a megabyte. */

  page_no_t rounded_size_pages = static_cast<page_no_t>(
      ((size / (1024 * 1024)) * ((1024 * 1024) / UNIV_PAGE_SIZE)));

  /* If last file */
  if (&file == &m_files.back() && m_auto_extend_last_file) {
    if (file.m_size > rounded_size_pages ||
        (m_last_file_size_max > 0 &&
         m_last_file_size_max < rounded_size_pages)) {
      lizard_error(ER_LIZARD)
          << "The Auto-extending " << name() << " data file '"
          << file.filepath()
          << "' is"
             " of a different size "
          << rounded_size_pages
          << " pages (rounded down to MB) than specified"
             " in the .cnf file: initial "
          << file.m_size << " pages, max " << m_last_file_size_max
          << " (relevant if non-zero) pages!";
      return (DB_ERROR);
    }

    file.m_size = rounded_size_pages;
  }

  if (rounded_size_pages != file.m_size) {
    lizard_error(ER_LIZARD)
        << "The " << name() << " data file '" << file.filepath()
        << "' is of a different size " << rounded_size_pages
        << " pages (rounded down to MB)"
           " than the "
        << file.m_size
        << " pages specified in"
           " the .cnf file!";
    return (DB_ERROR);
  }

  return (DB_SUCCESS);
}

/**
  Open or create the lizard space

  @param[in]        create_new_db
  @param[out]       size of files

  @retval           DB_SUCCESS        success */
dberr_t LizardTablespace::open_or_create(bool create_new_db,
                                         page_no_t *sum_of_lizard_sizes) {
  dberr_t err = DB_SUCCESS;
  fil_space_t *space = nullptr;
  *sum_of_lizard_sizes = 0;

  ut_ad(!m_files.empty());

  for (auto it = m_files.begin(); it != m_files.end(); it++) {
    if (it->m_exists) {
      err = open_file(*it);
    } else {
      err = create_file(*it);
    }

    if (err == DB_SUCCESS) {
      file_found(*it);
    }

    if (err != DB_SUCCESS) return err;

#if !defined(NO_FALLOCATE) && defined(UNIV_LINUX)
    /* Note: This should really be per node and not per
    tablespace because a tablespace can contain multiple
    files (nodes). The implication is that all files of
    the tablespace should be on the same medium. */
    if (fil_fusionio_enable_atomic_write(it->m_handle)) {
      if (dblwr::is_enabled()) {
        ib::info(ER_IB_MSG_456) << "FusionIO atomic IO enabled,"
                                   " disabling the double write buffer";

        dblwr::g_mode = dblwr::Mode::OFF;
      }
      it->m_atomic_write = true;
    } else {
      it->m_atomic_write = false;
    }
#else
    it->m_atomic_write = false;
#endif /* !NO_FALLOCATE && UNIV_LINUX*/
  }

  for (auto it = m_files.begin(); it != m_files.end(); it++) {
    ut_ad(it->m_exists);
    it->close();

    if (it == m_files.begin()) {
      space =
          fil_space_create(name(), space_id(), flags(), FIL_TYPE_TABLESPACE);
    } else {
      /** Only support one file rigth now. */
      ut_a(0);
    }
    page_no_t max_size = PAGE_NO_MAX;

    if (!fil_node_create(it->m_filepath, it->m_size, space, false,
                         it->m_atomic_write, max_size)) {
      err = DB_ERROR;
      break;
    }
    *sum_of_lizard_sizes += it->m_size;
  }
  return err;
}

/** Open file */
dberr_t LizardTablespace::open_file(Datafile &file) {
  dberr_t err = DB_SUCCESS;
  ut_a(file.m_exists);
  ut_a(file.m_type == SRV_NOT_RAW);

  err = file.open_or_create(srv_read_only_mode);

  check_size(file);

  return err;
}

/** create file */
dberr_t LizardTablespace::create_file(Datafile &file) {
  dberr_t err = DB_SUCCESS;
  ut_a(!file.m_exists);
  ut_a(file.m_type == SRV_NOT_RAW);

  err = file.open_or_create(srv_read_only_mode);
  if (err == DB_SUCCESS) err = set_size(file);
  return err;
}

/** set size */
dberr_t LizardTablespace::set_size(Datafile &file) {
  /* We created the data file and now write it full of zeros */
  ib::info(ER_IB_MSG_440)
      << "Setting file '" << file.filepath() << "' size to "
      << (file.m_size >> (20 - UNIV_PAGE_SIZE_SHIFT))
      << " MB."
         " Physically writing the file full; Please wait ...";

  bool success = os_file_set_size(
      file.m_filepath, file.m_handle, 0,
      static_cast<os_offset_t>(file.m_size) << UNIV_PAGE_SIZE_SHIFT, true);

  if (success) {
    ib::info(ER_IB_MSG_441)
        << "File '" << file.filepath() << "' size is now "
        << (file.m_size >> (20 - UNIV_PAGE_SIZE_SHIFT)) << " MB.";
  } else {
    ib::error(ER_IB_MSG_442) << "Could not set the file size of '"
                             << file.name() << "'. Probably out of disk space";

    return (DB_ERROR);
  }

  return DB_SUCCESS;
}

}  // namespace lizard
