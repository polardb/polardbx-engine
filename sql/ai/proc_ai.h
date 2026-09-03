/*****************************************************************************

Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

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

#ifndef SQL_AI_PROC_AI_INCLUDED
#define SQL_AI_PROC_AI_INCLUDED

/**
  @file sql/ai/proc_ai.h

  AI model management procedures (dbms_ai package).

  Procedures:
    1) dbms_ai.register_model(name, provider, endpoint, model, options_json)
    2) dbms_ai.list_models()
    3) dbms_ai.describe_model(model_name)
    4) dbms_ai.update_model(model_name, options_json)
    5) dbms_ai.update_function(function_name, model_name)
    6) dbms_ai.show_ai_function()
    7) dbms_ai.drop_model(model_name)
*/

#include "sql/mem_root_array.h"
#include "sql/package/proc.h"

namespace im {

extern const LEX_CSTRING AI_PROC_SCHEMA;

/* ================================================================
   Base classes
   ================================================================ */

/**
  Proc base for dbms_ai -- uniform schema name.
*/
class Ai_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Ai_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << AI_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Sql_cmd base for dbms_ai management procedures -- requires SUPER_ACL
  (admin) privilege.

  AI model management controls external HTTP endpoints and stores third-party
  API keys, which presents a different attack surface from CCL/Outline.
  Require elevated privileges to mitigate SSRF and credential abuse risks.
*/
class Sql_cmd_ai_admin_proc_base : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_ai_admin_proc_base(THD *thd, mem_root_deque<Item *> *list,
                                      const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}
};

/**
  Sql_cmd base for dbms_ai read-only procedures -- no privilege required.

  list_models / describe_model / show_ai_function only inspect the AI
  configuration (describe_model masks the api_key), so normal users are
  allowed to execute them.  Keep SQLCOM_ADMIN_PROC to preserve the
  auto-commit behavior shared with the management procedures.
*/
class Sql_cmd_ai_readonly_proc_base : public Sql_cmd_proc {
 public:
  explicit Sql_cmd_ai_readonly_proc_base(THD *thd, mem_root_deque<Item *> *list,
                                         const Proc *proc)
      : Sql_cmd_proc(thd, list, proc, Priv_type::PRIV_NONE_ACL) {}

  virtual enum_sql_command sql_command_code() const override {
    return SQLCOM_ADMIN_PROC;
  }
};

/* ================================================================
   1) dbms_ai.register_model(name, provider, endpoint, model, options)
   ================================================================ */

class Sql_cmd_ai_register_model : public Sql_cmd_ai_admin_proc_base {
 public:
  explicit Sql_cmd_ai_register_model(THD *thd, mem_root_deque<Item *> *list,
                                     const Proc *proc)
      : Sql_cmd_ai_admin_proc_base(thd, list, proc) {}

  virtual bool pc_execute(THD *thd) override;
};

class Ai_proc_register_model : public Ai_proc_base {
  using Sql_cmd_type = Sql_cmd_ai_register_model;

  enum enum_parameter {
    PARAM_MODEL_NAME = 0,
    PARAM_PROVIDER,
    PARAM_ENDPOINT,
    PARAM_MODEL,
    PARAM_OPTIONS,
    PARAM_LAST
  };

 public:
  explicit Ai_proc_register_model(PSI_memory_key key) : Ai_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
    for (size_t i = PARAM_MODEL_NAME; i < PARAM_LAST; i++) {
      m_parameters.assign_at(i, MYSQL_TYPE_VARCHAR);
    }
  }

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Ai_proc_register_model() override {}

  virtual const std::string str() const override {
    return std::string("register_model");
  }
};

/* ================================================================
   2) dbms_ai.list_models()
   ================================================================ */

class Sql_cmd_ai_list_models : public Sql_cmd_ai_readonly_proc_base {
 public:
  explicit Sql_cmd_ai_list_models(THD *thd, mem_root_deque<Item *> *list,
                                  const Proc *proc)
      : Sql_cmd_ai_readonly_proc_base(thd, list, proc),
        m_records(thd->mem_root) {}

  virtual bool pc_execute(THD *thd) override;
  virtual void send_result(THD *thd, bool error) override;

 private:
  /*
    Use Mem_root_array (allocates on thd->mem_root) instead of std::vector
    so that the internal buffer is reclaimed together with the THD's
    mem_root.  Sql_cmd objects themselves are placement-new'd onto
    thd->mem_root and their destructors are never called, so a std::vector
    member would leak its heap buffer (caught by ASan/LSan).
  */
  Mem_root_array<struct Ai_record *> m_records;
};

class Ai_proc_list_models : public Ai_proc_base {
  using Sql_cmd_type = Sql_cmd_ai_list_models;

  enum enum_column {
    COLUMN_MODEL_NAME = 0,
    COLUMN_PROVIDER,
    COLUMN_ENDPOINT,
    COLUMN_MODEL,
    COLUMN_STATUS,
    COLUMN_LAST
  };

 public:
  explicit Ai_proc_list_models(PSI_memory_key key) : Ai_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("MODEL_NAME"), 128},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("PROVIDER"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ENDPOINT"), 512},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("MODEL"), 128},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("STATUS"), 16},
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Ai_proc_list_models() override {}

  virtual const std::string str() const override {
    return std::string("list_models");
  }
};

/* ================================================================
   3) dbms_ai.describe_model(model_name)
   ================================================================ */

class Sql_cmd_ai_describe_model : public Sql_cmd_ai_readonly_proc_base {
 public:
  explicit Sql_cmd_ai_describe_model(THD *thd, mem_root_deque<Item *> *list,
                                     const Proc *proc)
      : Sql_cmd_ai_readonly_proc_base(thd, list, proc) {}

  virtual bool pc_execute(THD *thd) override;
  virtual void send_result(THD *thd, bool error) override;

 private:
  struct Ai_record *m_record = nullptr;
};

class Ai_proc_describe_model : public Ai_proc_base {
  using Sql_cmd_type = Sql_cmd_ai_describe_model;

  enum enum_column {
    COLUMN_MODEL_NAME = 0,
    COLUMN_PROVIDER,
    COLUMN_ENDPOINT,
    COLUMN_MODEL,
    COLUMN_API_KEY,
    COLUMN_MODEL_PARAMS,
    COLUMN_STATUS,
    COLUMN_DESCRIPTION,
    COLUMN_LAST
  };

 public:
  explicit Ai_proc_describe_model(PSI_memory_key key) : Ai_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("MODEL_NAME"), 128},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("PROVIDER"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ENDPOINT"), 512},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("MODEL"), 128},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("API_KEY"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("MODEL_PARAMS"), 1024},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("STATUS"), 16},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("DESCRIPTION"), 1024},
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }

    /* 1 parameter: model_name */
    m_parameters.assign_at(0, MYSQL_TYPE_VARCHAR);
  }

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Ai_proc_describe_model() override {}

  virtual const std::string str() const override {
    return std::string("describe_model");
  }
};

/* ================================================================
   4) dbms_ai.update_model(model_name, options_json)
   ================================================================ */

class Sql_cmd_ai_update_model : public Sql_cmd_ai_admin_proc_base {
 public:
  explicit Sql_cmd_ai_update_model(THD *thd, mem_root_deque<Item *> *list,
                                   const Proc *proc)
      : Sql_cmd_ai_admin_proc_base(thd, list, proc) {}

  virtual bool pc_execute(THD *thd) override;
};

class Ai_proc_update_model : public Ai_proc_base {
  using Sql_cmd_type = Sql_cmd_ai_update_model;

  enum enum_parameter {
    PARAM_MODEL_NAME = 0,
    PARAM_OPTIONS,
    PARAM_LAST
  };

 public:
  explicit Ai_proc_update_model(PSI_memory_key key) : Ai_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
    for (size_t i = PARAM_MODEL_NAME; i < PARAM_LAST; i++) {
      m_parameters.assign_at(i, MYSQL_TYPE_VARCHAR);
    }
  }

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Ai_proc_update_model() override {}

  virtual const std::string str() const override {
    return std::string("update_model");
  }
};

/* ================================================================
   5) dbms_ai.update_function(function_name, model_name)
   ================================================================ */

class Sql_cmd_ai_update_function : public Sql_cmd_ai_admin_proc_base {
 public:
  explicit Sql_cmd_ai_update_function(THD *thd, mem_root_deque<Item *> *list,
                                      const Proc *proc)
      : Sql_cmd_ai_admin_proc_base(thd, list, proc) {}

  virtual bool pc_execute(THD *thd) override;
};

class Ai_proc_update_function : public Ai_proc_base {
  using Sql_cmd_type = Sql_cmd_ai_update_function;

  enum enum_parameter {
    PARAM_FUNCTION_NAME = 0,
    PARAM_MODEL_NAME,
    PARAM_LAST
  };

 public:
  explicit Ai_proc_update_function(PSI_memory_key key) : Ai_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
    for (size_t i = PARAM_FUNCTION_NAME; i < PARAM_LAST; i++) {
      m_parameters.assign_at(i, MYSQL_TYPE_VARCHAR);
    }
  }

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Ai_proc_update_function() override {}

  virtual const std::string str() const override {
    return std::string("update_function");
  }
};

/* ================================================================
   6) dbms_ai.show_ai_function()
   ================================================================ */

struct Ai_function_display_record {
  const char *function_name;
  const char *default_model;
  const char *actual_model;
  const char *description;
  size_t display_order;
};

class Sql_cmd_ai_show_ai_function : public Sql_cmd_ai_readonly_proc_base {
 public:
  explicit Sql_cmd_ai_show_ai_function(THD *thd, mem_root_deque<Item *> *list,
                                       const Proc *proc)
      : Sql_cmd_ai_readonly_proc_base(thd, list, proc),
        m_records(thd->mem_root) {}

  virtual bool pc_execute(THD *thd) override;
  virtual void send_result(THD *thd, bool error) override;

 private:
  Mem_root_array<struct Ai_function_display_record *> m_records;
};

class Ai_proc_show_ai_function : public Ai_proc_base {
  using Sql_cmd_type = Sql_cmd_ai_show_ai_function;

  enum enum_column {
    COLUMN_FUNCTION = 0,
    COLUMN_DEFAULT_MODEL,
    COLUMN_ACTUAL_MODEL,
    COLUMN_DESCRIPTION,
    COLUMN_LAST
  };

 public:
  explicit Ai_proc_show_ai_function(PSI_memory_key key) : Ai_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("FUNCTION"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("DEFAULT_MODEL"), 128},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ACTUAL_MODEL"), 128},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("DESCRIPTION"), 1024},
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Ai_proc_show_ai_function() override {}

  virtual const std::string str() const override {
    return std::string("show_ai_function");
  }
};

/* ================================================================
   7) dbms_ai.drop_model(model_name)
   ================================================================ */

class Sql_cmd_ai_drop_model : public Sql_cmd_ai_admin_proc_base {
 public:
  explicit Sql_cmd_ai_drop_model(THD *thd, mem_root_deque<Item *> *list,
                                 const Proc *proc)
      : Sql_cmd_ai_admin_proc_base(thd, list, proc) {}

  virtual bool pc_execute(THD *thd) override;
};

class Ai_proc_drop_model : public Ai_proc_base {
  using Sql_cmd_type = Sql_cmd_ai_drop_model;

 public:
  explicit Ai_proc_drop_model(PSI_memory_key key) : Ai_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
    /* 1 parameter: model_name */
    m_parameters.assign_at(0, MYSQL_TYPE_VARCHAR);
  }

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Ai_proc_drop_model() override {}

  virtual const std::string str() const override {
    return std::string("drop_model");
  }
};

} /* namespace im */

#endif /* SQL_AI_PROC_AI_INCLUDED */
