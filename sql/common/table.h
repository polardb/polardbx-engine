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
#ifndef SQL_COMMON_TABLE_H_INCLUDED
#define SQL_COMMON_TABLE_H_INCLUDED

#include "sql/common/table_common.h"
#include "sql/iterators/row_iterator.h"
#include "sql/table.h"

class THD;

namespace im {
/**
  Check the configure table definition.
*/
class Conf_table_intact : public Table_check_intact {
 public:
  explicit Conf_table_intact(THD *thd) : Table_check_intact(), m_thd(thd) {
    has_keys = true;
  }

 protected:
  void report_error(uint ecode, const char *fmt, ...) override
      MY_ATTRIBUTE((format(printf, 3, 4)));

 private:
  THD *m_thd;
};

class Conf_reader {
 public:
  explicit Conf_reader(THD *thd, TABLE *table, MEM_ROOT *mem_root)
      : m_thd(thd), m_table(table), m_mem_root(mem_root) {}

  virtual ~Conf_reader() {}

  virtual void row_warning(Conf_record *record, const char *when,
                           const char *msg) = 0;

  virtual void print_ha_error(int errcode) = 0;

  virtual void log_error(Conf_error err) = 0;
  /**
    Setup table reader context, report error if failed.

    @retval         true        Failure
    @retval         false       Success
  */
  virtual bool setup_table();
  /**
    Read all rows from table.

    @param[out]     error     ccl error

    @retval         records   all records object from table rows
  */
  virtual Conf_records *read_all_rows(Conf_error *error);

  virtual void read_attributes(Conf_record *record) = 0;

  virtual Conf_record *new_record() = 0;

 protected:
  THD *m_thd;
  TABLE *m_table;
  MEM_ROOT *m_mem_root;
  unique_ptr_destroy_only<RowIterator> m_read_record_info;
};

class Conf_writer {
 public:
  explicit Conf_writer(THD *thd, TABLE *table, MEM_ROOT *mem_root,
                       Conf_table_op op_type)
      : m_thd(thd), m_table(table), m_mem_root(mem_root), m_op_type(op_type) {}

  virtual ~Conf_writer() {}
  /**
    Setup table writer context
  */
  virtual void setup_table();
  /**
    Whether has the auto increment column
  */
  virtual bool has_autoinc() = 0;

  virtual void print_ha_error(int errcode) = 0;

  /**
    Store the rule attributes into table->field

    @param[in]      record        the table row object
  */
  virtual void store_attributes(const Conf_record *record) = 0;

  /**
    Write the record into table.

    @param[in]      record     row

    @retval         error number
  */
  virtual int write_row(Conf_record *record);
  /**
    Delete the record from table.
    Only push warning if not found the record in table.

    @param[in/out]  record        the row

    @retval         false         Success
    @retval         true          Failure
  */
  virtual bool delete_row_by_id(Conf_record *record);
  /**
    Store the record id into table->field[ID_POS].

    @param[in]      record        the table row object
  */
  virtual void store_id(const Conf_record *r) = 0;
  /**
    Retrieve some attributes from table->fields into record.

    @param[out]     record        the table row object
  */
  virtual void retrieve_attr(Conf_record *r) = 0;

  /**
    Push row not found warning to client.

    @param[in]      record        table table row object
  */
  virtual void row_not_found_warning(Conf_record *r) = 0;

 protected:
  THD *m_thd;
  TABLE *m_table;
  MEM_ROOT *m_mem_root;
  Conf_table_op m_op_type;
};

using TABLE_LIST_PTR = std::unique_ptr<Table_ref>;

/**
  Open a configure table, and check the definition.

  Report client error if failed.

  @param[in]      thd           Thread context
  @param[in]      table_list    CONF table
  @param[in]      schema        Schema name
  @param[in]      table         Table name
  @param[in]      write         Read or write

  @retval         false         Success
  @retval         true          Failure
*/
bool open_conf_table(THD *thd, TABLE_LIST_PTR &table_list,
                     const LEX_CSTRING &schema, const LEX_CSTRING &table,
                     const char *alias, const TABLE_FIELD_DEF *def, bool write);

/**
  Commit current transaction, close the opened tables
  release the MDL transactional locks.

  @param[in]      thd           Thread context
*/
void commit_and_close_conf_table(THD *thd);

/**
  Commit the conf transaction.

  @param[in]      thd           Thread context
  @param[in]      rollback      Rollback request
*/
bool conf_end_trans(THD *thd, bool rollback);
} /* namespace im */
#endif
