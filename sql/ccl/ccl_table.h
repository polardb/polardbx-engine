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

#ifndef SQL_CCL_CCL_TABLE_INCLUDED
#define SQL_CCL_CCL_TABLE_INCLUDED

#include "sql/ccl/ccl_table_common.h"
#include "sql/common/table.h"
#include "sql/sql_class.h"
#include "sql/table.h"

class THD;

namespace im {

extern bool is_ccl_table(const char *table_name, size_t len);

class Disable_binlog {
 public:
  explicit Disable_binlog(THD *thd) : m_thd(thd) {
    m_saved_options = m_thd->variables.option_bits;
    m_thd->variables.option_bits &= ~OPTION_BIN_LOG;
  }

  virtual ~Disable_binlog() { m_thd->variables.option_bits = m_saved_options; }

 private:
  THD *m_thd;
  ulonglong m_saved_options;
};

/**
  CCL table reader helper.
*/
class Ccl_reader : public Conf_reader {
 public:
  explicit Ccl_reader(THD *thd, TABLE *table, MEM_ROOT *mem_root)
      : Conf_reader(thd, table, mem_root) {}

  ~Ccl_reader() override {}

  /**
    Push invalid ccl record warning

    @param[in]      record        Ccl record
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
    Save the row value into ccl_record structure.
  */
  virtual void read_attributes(Conf_record *record) override;

  /* Create new ccl record */
  virtual Conf_record *new_record() override;
};

class Ccl_writer : public Conf_writer {
 public:
  explicit Ccl_writer(THD *thd, TABLE *table, MEM_ROOT *mem_root,
                      Conf_table_op op_type)
      : Conf_writer(thd, table, mem_root, op_type), m_disable_binlog(thd) {}

  virtual void print_ha_error(int errcode) override;
  /**
    Store the rule attributes into table->field

    @param[in]      record        the table row object
  */
  virtual void store_attributes(const Conf_record *r) override;
  /**
    Whether has the auto increment column
  */
  virtual bool has_autoinc() override { return true; }
  /**
    Store the record rule id into table->field[ID].

    @param[in]      record        the table row
  */
  virtual void store_id(const Conf_record *r) override;
  /**
    Retrieve ccl type from table->fields into record.

    @param[out]     record        the table row object
  */
  virtual void retrieve_attr(Conf_record *r) override;
  /**
    Push row not found warning to client.

    @param[in]      record        table table row object
  */
  virtual void row_not_found_warning(Conf_record *r) override;

 private:
  Disable_binlog m_disable_binlog;
};

/**
  Open the ccl table, report error if failed.*

  Attention:
  It didn't open attached transaction, so it must commit
  current transaction context when close ccl table.

  Make sure it launched within main thread booting
  or statement that cause implicit commit.

  Report client error if failed.

  @param[in]      thd           Thread context
  @param[in]      table_list    CCL table
  @param[in]      write         read or write

  @retval         Conf_error
*/
Conf_error open_ccl_table(THD *thd, TABLE_LIST_PTR &table_list, bool write);

/**
  Commit the ccl transaction.
  Call reload_ccl_rules() if commit failed.

  @param[in]      thd           Thread context
  @param[in]      rollback      Rollback request
*/
bool ccl_end_trans(THD *thd, bool rollback);

/**
  Flush all rules and load rules from table.

  Report client error if failed, whether log error
  message decided by caller.

  @param[in]      thd           Thread context

  @retval         Ccl_error
*/
Conf_error reload_ccl_rules(THD *thd);
/**
  Flush ccl cache if force_clean is true and fill new rules.

  Only report  warning message if some rule failed.

  @param[in]      records         rule record set
  @param[in]      force_clean     whether clear the rule cache

  @retval         number          how many rule added.
*/
bool reflush_ccl_cache(Conf_records *records, bool force_clean);
/**
  Add new rule into concurrency_table and insert ccl cache.
  Report client error if failed.

  @param[in]      thd         Thread context
  @param[in]      record      Rule

  @retval         false       Success
  @retval         true        Failure
*/
bool add_ccl_rule(THD *thd, Conf_record *record);
/**
  Delete the rule.
  Delete the row or clear the rule cache or both.

  Only report warning message if row or cache not found
*/
bool del_ccl_rule(THD *thd, Conf_record *record);

} /* namespace im */

#endif
