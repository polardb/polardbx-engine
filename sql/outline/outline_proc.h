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

#ifndef SQL_OUTLINE_OUTLINE_PROC_INCLUDED
#define SQL_OUTLINE_OUTLINE_PROC_INCLUDED

#include "sql/outline/outline_table_common.h"
#include "sql/package/proc.h"

/**
  Statement outline procedure (dbms_outln package)

  1) add_index_outline
*/
namespace im {

extern LEX_CSTRING OUTLINE_PROC_SCHEMA;

/**
  Procedure base for dbms_outln

  1) Uniform schema : dbms_outln
*/

class Outline_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Outline_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << OUTLINE_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Sql command base for dbms_outln

  1) dbms_outln didn't require any privileges;
*/

class Sql_cmd_outline_proc_base : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_outline_proc_base(THD *thd, mem_root_deque<Item *> *list,
                                     const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {
    set_priv_type(Priv_type::PRIV_NONE_ACL);
  }
};

/**
  2) dbms_outln.add_optimizer_outline(...);

  It will add index hint outline into mysql.outline table,
  and the outline cache that take effect immediately.
*/
class Sql_cmd_optimizer_outline_proc_add : public Sql_cmd_outline_proc_base {
 public:
  explicit Sql_cmd_optimizer_outline_proc_add(THD *thd,
                                              mem_root_deque<Item *> *list,
                                              const Proc *proc)
      : Sql_cmd_outline_proc_base(thd, list, proc) {}
  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;
  /**
    Create record from parameters.

    @param[in]      thd       Thread context
    @param[in]      list      Parameters

    @retval         record    Outline record
  */
  Outline_record *get_record(THD *thd);
};

class Outline_optimizer_proc_add : public Outline_proc_base {
  using Sql_cmd_type = Sql_cmd_optimizer_outline_proc_add;

  /* All the parameters */
  enum enum_parameter {
    OUTLINE_PARAM_SCHEMA = 0,
    OUTLINE_PARAM_DIGEST,
    OUTLINE_PARAM_POSITION,
    OUTLINE_PARAM_HINT,
    OUTLINE_PARAM_SQL,
    OUTLINE_PARAM_LAST
  };
  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case OUTLINE_PARAM_SCHEMA:
      case OUTLINE_PARAM_DIGEST:
      case OUTLINE_PARAM_HINT:
      case OUTLINE_PARAM_SQL:
        return MYSQL_TYPE_VARCHAR;
      case OUTLINE_PARAM_POSITION:
        return MYSQL_TYPE_LONGLONG;
      case OUTLINE_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

 public:
  explicit Outline_optimizer_proc_add(PSI_memory_key key)
      : Outline_proc_base(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;

    /* Init parameters */
    for (size_t i = OUTLINE_PARAM_SCHEMA; i < OUTLINE_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }

  /* Singleton instance for add_optimizer_outline */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for add_optimizer_outline() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Outline_optimizer_proc_add() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("add_optimizer_outline");
  }
};

/**
  1) dbms_outln.add_index_outline(...);

  It will add index hint outline into mysql.outline table,
  and the outline cache that take effect immediately.
*/
class Sql_cmd_index_outline_proc_add : public Sql_cmd_outline_proc_base {
 public:
  explicit Sql_cmd_index_outline_proc_add(THD *thd,
                                          mem_root_deque<Item *> *list,
                                          const Proc *proc)
      : Sql_cmd_outline_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;
  /**
    Create record from parameters.

    @param[in]      thd       Thread context
    @param[in]      list      Parameters

    @retval         record    Outline record
  */
  Outline_record *get_record(THD *thd);
};

class Outline_index_proc_add : public Outline_proc_base {
  using Sql_cmd_type = Sql_cmd_index_outline_proc_add;

  /* All the parameters */
  enum enum_parameter {
    OUTLINE_PARAM_SCHEMA = 0,
    OUTLINE_PARAM_DIGEST,
    OUTLINE_PARAM_POSITION,
    OUTLINE_PARAM_TYPE,
    OUTLINE_PARAM_HINT,
    OUTLINE_PARAM_SCOPE,
    OUTLINE_PARAM_SQL,
    OUTLINE_PARAM_LAST
  };
  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case OUTLINE_PARAM_SCHEMA:
      case OUTLINE_PARAM_DIGEST:
      case OUTLINE_PARAM_TYPE:
      case OUTLINE_PARAM_HINT:
      case OUTLINE_PARAM_SCOPE:
      case OUTLINE_PARAM_SQL:
        return MYSQL_TYPE_VARCHAR;
      case OUTLINE_PARAM_POSITION:
        return MYSQL_TYPE_LONGLONG;
      case OUTLINE_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

 public:
  explicit Outline_index_proc_add(PSI_memory_key key) : Outline_proc_base(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;

    /* Init parameters */
    for (size_t i = OUTLINE_PARAM_SCHEMA; i < OUTLINE_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }

  /* Singleton instance for add_index_outline */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for add_index_outline() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Outline_index_proc_add() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("add_index_outline");
  }
};

/**
  2) dbms_outln.del_outline()

    Delete the outline from mysql.outline and outline cache.
*/
class Sql_cmd_outline_proc_del : public Sql_cmd_outline_proc_base {
 public:
  explicit Sql_cmd_outline_proc_del(THD *thd, mem_root_deque<Item *> *list,
                                    const Proc *proc)
      : Sql_cmd_outline_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;
};

class Outline_proc_del : public Outline_proc_base {
  using Sql_cmd_type = Sql_cmd_outline_proc_del;

  /* All the parameters */
  enum enum_parameter { OUTLINE_PARAM_ID = 0, OUTLINE_PARAM_LAST };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case OUTLINE_PARAM_ID:
        return MYSQL_TYPE_LONGLONG;
      case OUTLINE_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

 public:
  explicit Outline_proc_del(PSI_memory_key key) : Outline_proc_base(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;

    /* Init parameters */
    for (size_t i = OUTLINE_PARAM_ID; i < OUTLINE_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  /* Singleton instance for del_outline */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for del_outline() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Outline_proc_del() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("del_outline");
  }
};

/**
  3) dbms_outln.flush_outline();

  It will flush all outline cache and read the outlines from
  mysql.outline and add into cache.
*/
class Sql_cmd_outline_proc_flush : public Sql_cmd_outline_proc_base {
 public:
  explicit Sql_cmd_outline_proc_flush(THD *thd, mem_root_deque<Item *> *list,
                                      const Proc *proc)
      : Sql_cmd_outline_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Inherit the default send_result */
};

class Outline_proc_flush : public Outline_proc_base {
  using Sql_cmd_type = Sql_cmd_outline_proc_flush;

 public:
  explicit Outline_proc_flush(PSI_memory_key key) : Outline_proc_base(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;
  }
  /* Singleton instance for flush_outline */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for flush_outline proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Outline_proc_flush() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("flush_outline");
  }
};

/**
  4) dbms_outln.show_outline();

  It will show outline in cache.
*/
class Sql_cmd_outline_proc_show : public Sql_cmd_outline_proc_base {
 public:
  explicit Sql_cmd_outline_proc_show(THD *thd, mem_root_deque<Item *> *list,
                                     const Proc *proc)
      : Sql_cmd_outline_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;
};

class Outline_proc_show : public Outline_proc_base {
  using Sql_cmd_type = Sql_cmd_outline_proc_show;

  enum enum_column {
    COLUMN_ID = 0,
    COLUMN_SCHEMA,
    COLUMN_DIGEST,
    COLUMN_TYPE,
    COLUMN_SCOPE,
    COLUMN_POS,
    COLUMN_HINT,
    COLUMN_HIT,
    COLUMN_OVERFLOW,
    COLUMN_DIGEST_TEXT,
    COLUMN_LAST
  };

 public:
  explicit Outline_proc_show(PSI_memory_key key) : Outline_proc_base(key) {
    /* Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("ID"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("SCHEMA"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("DIGEST"), 128},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("TYPE"), 32},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("SCOPE"), 64},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("POS"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("HINT"), 1024},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("HIT"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("OVERFLOW"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("DIGEST_TEXT"), 2048}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }
  /* Singleton instance for show_outline */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for show_outline proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Outline_proc_show() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("show_outline");
  }
};

/**
  5) dbms_outln.preview_outline();

  Show the matched outline for the query.
*/

class Sql_cmd_outline_proc_preview : public Sql_cmd_outline_proc_base {
 public:
  explicit Sql_cmd_outline_proc_preview(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_outline_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /**
    Override the default send result;
    Try to parse the query statement and invoke the outlines.

    @param[in]        thd       thread context
    @param[in]        error     pc_execute() result
  */
  virtual void send_result(THD *thd, bool error) override;
  /**
    Create record from parameters.

    @param[in]      thd       Thread context
    @param[in]      list      Parameters

    @retval         record    Outline record
  */
  Outline_record *get_record(THD *thd);
};

class Outline_proc_preview : public Outline_proc_base {
  using Sql_cmd_type = Sql_cmd_outline_proc_preview;

  /* All the parameters */
  enum enum_parameter {
    OUTLINE_PARAM_SCHEMA = 0,
    OUTLINE_PARAM_QUERY,
    OUTLINE_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case OUTLINE_PARAM_SCHEMA:
      case OUTLINE_PARAM_QUERY:
        return MYSQL_TYPE_VARCHAR;
      case OUTLINE_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

  enum enum_column {
    COLUMN_SCHEMA = 0,
    COLUMN_DIGEST,
    COLUMN_BLOCK_TYPE,
    COLUMN_BLOCK_NAME,
    COLUMN_BLOCK,
    COLUMN_HINT,
    COLUMN_LAST
  };

 public:
  explicit Outline_proc_preview(PSI_memory_key key) : Outline_proc_base(key) {
    /* Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    /* Init parameters */
    for (size_t i = OUTLINE_PARAM_SCHEMA; i < OUTLINE_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("SCHEMA"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("DIGEST"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("BLOCK_TYPE"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("BLOCK_NAME"), 64},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("BLOCK"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("HINT"), 2048}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  /* Singleton instance for preview_outline */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for preview_outline() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Outline_proc_preview() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("preview_outline");
  }
};

} /* namespace im */

#endif
