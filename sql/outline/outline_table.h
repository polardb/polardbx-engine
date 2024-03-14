/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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
#ifndef SQL_OUTLINE_OUTLINE_TABLE_INCLUDED
#define SQL_OUTLINE_OUTLINE_TABLE_INCLUDED

#include "sql/common/table.h"
#include "sql/common/table_common.h"
#include "sql/outline/outline_table_common.h"

namespace im {

/**
  Outline table reader helper.
*/
class Outline_reader : public Conf_reader {
 public:
  explicit Outline_reader(THD *thd, TABLE *table, MEM_ROOT *mem_root)
      : Conf_reader(thd, table, mem_root) {}

  ~Outline_reader() override {}
  /**
    Push invalid outline record warning

    @param[in]      record        Outline record
    @param[in]      when          Operation
    @param[in]      msg           Error message
  */
  virtual void row_warning(Conf_record *record, const char *when,
                           const char *msg) override;
  /**
    Reconstruct and Report error by adding handler error.

    @param[in]      errcode     handler error.
  */
  virtual void print_ha_error(int errcode) override;
  /**
    Log the conf table error.

    @param[in]    err     Conf error type
  */
  virtual void log_error(Conf_error err) override;
  /**
    Save the row value into outline_record structure.

    @param[out]   record      Outline record
  */
  virtual void read_attributes(Conf_record *record) override;

  /* Create new outline record */
  virtual Conf_record *new_record() override;
  /**
    Validate the optimizer outline, try parse it,
    remove it from container and report warning if invalid.

    @parma[in]      records       Outline records

    @retval         num           invalid optimizer outline count
  */
  size_t validate_optimizer_outline(Conf_records *records);
};

class Outline_writer : public Conf_writer {
 public:
  explicit Outline_writer(THD *thd, TABLE *table, MEM_ROOT *mem_root,
                          Conf_table_op op_type)
      : Conf_writer(thd, table, mem_root, op_type) {}

  virtual void print_ha_error(int errcode) override;
  /**
    Store the outline attributes into table->field

    @param[in]      record        the table row object
  */
  virtual void store_attributes(const Conf_record *r) override;
  /**
    Whether has the auto increment column
  */
  virtual bool has_autoinc() override { return true; }
  /**
    Store the record outline id into table->field[ID].

    @param[in]      record        the table row
  */
  virtual void store_id(const Conf_record *r) override;
  /**
    Retrieve outline type from table->fields into record.

    @param[out]     record        the table row object
  */
  virtual void retrieve_attr(Conf_record *r) override;
  /**
    Push row not found warning to client.

    @param[in]      record        table table row object
  */
  virtual void row_not_found_warning(Conf_record *r) override;
};
/**
  Open the outline table, report error if failed.*

  Attention:
  It didn't open attached transaction, so it must commit
  current transaction context when close ccl table.

  Make sure it launched within main thread booting
  or statement that cause implicit commit.

  Report client error if failed.

  @param[in]      thd           Thread context
  @param[in]      table_list    Outline table
  @param[in]      write         read or write

  @retval         Outline_error
*/
extern Conf_error open_outline_table(THD *thd, TABLE_LIST_PTR &table_list,
                                     bool write);

/**
  Flush all outlines and load outlines from table.

  Report client error if failed, whether log error
  message decided by caller.

  @param[in]      thd           Thread context

  @retval         Outline_error
*/
extern Conf_error reload_outlines(THD *thd);

/**
  Flush outline cache if force_clean is true and fill new outlines.

  @param[in]      records         outlines
  @param[in]      force_clean     whether clear cache
*/
extern void refresh_outline_cache(Conf_records *records, bool force_clean);

/**
  Add new outline into outline table and insert outline cache.
  Report client error if failed.

  @param[in]      thd         Thread context
  @param[in]      record      outline

  @retval         false       Success
  @retval         true        Failure
*/
bool add_outline(THD *thd, Conf_record *r);

/**
  Delete the outline.

  Only report warning message if row or cache not found
*/
bool del_outline(THD *thd, Conf_record *record);
} /* namespace im */

#endif
