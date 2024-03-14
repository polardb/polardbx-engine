/* Copyright (c) 2018, 2023, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_XA_PROC_XA_INCLUDED
#define SQL_XA_PROC_XA_INCLUDED

#include "sql/package/proc.h"

#include "sql/xa/lizard_xa_trx.h"

/**
  XA procedures (dbms_xa package)

  1) find_by_xid(xid)
  2) prepare_with_trx_slot(gtrid, bqual, formatID)
  3) send_heartbeat()
*/

namespace im {
extern const LEX_CSTRING XA_PROC_SCHEMA;

/**
  Proc base for dbms_xa

  1) Uniform schema: dbms_xa
*/
class Xa_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Xa_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << XA_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Sql command base for dbms_xa

  1) dbms_xa didn't require any privileges;
  2) dbms_xa didn't auto commit trans (is trans_proc).
*/
class Sql_cmd_xa_proc_base : public Sql_cmd_trans_proc {
 public:
  explicit Sql_cmd_xa_proc_base(THD *thd, mem_root_deque<Item *> *list,
                                const Proc *proc)
      : Sql_cmd_trans_proc(thd, list, proc) {
    set_priv_type(Priv_type::PRIV_NONE_ACL);
  }
};

/**
  Sql command base for dbms_xa

  1) dbms_xa didn't require any privileges;
  2) dbms_xa didn't auto commit trans (is admin_proc).
*/
class Sql_cmd_xa_proc_trans_base : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_xa_proc_trans_base(THD *thd, mem_root_deque<Item *> *list,
                                      const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {
    set_priv_type(Priv_type::PRIV_NONE_ACL);
  }
};

/**
  1) dbms_xa.find_by_xid(xid)

  Find transactions status in the finalized state by XID.
*/
class Sql_cmd_xa_proc_find_by_xid : public Sql_cmd_xa_proc_base {
 public:
  explicit Sql_cmd_xa_proc_find_by_xid(THD *thd, mem_root_deque<Item *> *list,
                                       const Proc *proc)
      : Sql_cmd_xa_proc_base(thd, list, proc) {}

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

class Xa_proc_find_by_xid : public Xa_proc_base {
  using Sql_cmd_type = Sql_cmd_xa_proc_find_by_xid;

  enum enum_parameter {
    XA_PARAM_GTRID = 0,
    XA_PARAM_BQUAL,
    XA_PARAM_FORMATID,
    XA_PARAM_LAST
  };

  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case XA_PARAM_GTRID:
      case XA_PARAM_BQUAL:
        return MYSQL_TYPE_VARCHAR;
      case XA_PARAM_FORMATID:
        return MYSQL_TYPE_LONGLONG;
      case XA_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

  enum enum_column {
    COLUMN_STATUS = 0,
    COLUMN_GCN = 1,
    COLUMN_CSR = 2,
    COLUMN_LAST = 3
  };

 public:
  explicit Xa_proc_find_by_xid(PSI_memory_key key) : Xa_proc_base(key) {
    /* 1. Init parameters */
    for (size_t i = XA_PARAM_GTRID; i < XA_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }

    /* 2. Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("Status"), 16},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("GCN"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("CSR"), 16},
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  /* Singleton instance for find_by_xid */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for find_by_xid() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Xa_proc_find_by_xid() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("find_by_xid");
  }
};

/**
  2) dbms_xa.prepare_with_trx_slot(gtrid, bqual, formatID)
    a) force assign a trx slot for the XA trx
    b) xa prepare
    c) return {uuid, UBA}
*/
class Sql_cmd_xa_proc_prepare_with_trx_slot : public Sql_cmd_xa_proc_base {
 public:
  explicit Sql_cmd_xa_proc_prepare_with_trx_slot(THD *thd,
                                                 mem_root_deque<Item *> *list,
                                                 const Proc *proc)
      : Sql_cmd_xa_proc_base(thd, list, proc), m_slot_ptr(0) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;

 private:
  my_slot_ptr_t m_slot_ptr;
};

class Xa_proc_prepare_with_trx_slot : public Xa_proc_base {
  using Sql_cmd_type = Sql_cmd_xa_proc_prepare_with_trx_slot;

  enum enum_parameter {
    XA_PARAM_GTRID = 0,
    XA_PARAM_BQUAL,
    XA_PARAM_FORMATID,
    XA_PARAM_LAST
  };

  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case XA_PARAM_GTRID:
      case XA_PARAM_BQUAL:
        return MYSQL_TYPE_VARCHAR;
      case XA_PARAM_FORMATID:
        return MYSQL_TYPE_LONGLONG;
      case XA_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

  enum enum_column { COLUMN_UUID = 0, COLUMN_UBA = 1, COLUMN_LAST = 2 };

 public:
  explicit Xa_proc_prepare_with_trx_slot(PSI_memory_key key)
      : Xa_proc_base(key) {
    /* 1. Init parameters */
    for (size_t i = XA_PARAM_GTRID; i < XA_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }

    /* 2. Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("UUID"), 256},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("UBA"), 0},
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  /* Singleton instance for prepare_with_trx_slot */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for find_by_xid() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Xa_proc_prepare_with_trx_slot() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("prepare_with_trx_slot");
  }
};

/**
  3) dbms_xa.send_heartbeat()

  Send heartbeat for keeping purge sys advancing.
*/
class Sql_cmd_xa_proc_send_heartbeat : public Sql_cmd_xa_proc_trans_base {
 public:
  explicit Sql_cmd_xa_proc_send_heartbeat(THD *thd,
                                          mem_root_deque<Item *> *list,
                                          const Proc *proc)
      : Sql_cmd_xa_proc_trans_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Inherit the default send_result */
};

class Xa_proc_send_heartbeat : public Xa_proc_base {
  using Sql_cmd_type = Sql_cmd_xa_proc_send_heartbeat;

 public:
  explicit Xa_proc_send_heartbeat(PSI_memory_key key) : Xa_proc_base(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;
  }

  /* Singleton instance for send_heartbeat */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for send_heartbeat() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Xa_proc_send_heartbeat() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("send_heartbeat");
  }
};

class Sql_cmd_xa_proc_advance_gcn_no_flush : public Sql_cmd_xa_proc_trans_base {
 public:
  explicit Sql_cmd_xa_proc_advance_gcn_no_flush(THD *thd,
                                                mem_root_deque<Item *> *list,
                                                const Proc *proc)
      : Sql_cmd_xa_proc_trans_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Inherit the default send_result */
};

class Xa_proc_advance_gcn_no_flush : public Xa_proc_base {
  using Sql_cmd_type = Sql_cmd_xa_proc_advance_gcn_no_flush;

  enum enum_parameter { XA_PARAM_GCN = 0, XA_PARAM_LAST };

  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case XA_PARAM_GCN:
        return MYSQL_TYPE_LONGLONG;
      case XA_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

 public:
  explicit Xa_proc_advance_gcn_no_flush(PSI_memory_key key)
      : Xa_proc_base(key) {
    /* 1. Init parameters */
    for (size_t i = XA_PARAM_GCN; i < XA_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }

    /* 2. Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;
  }

  /* Singleton instance for advance_gcn_no_flush */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for advance_gcn_no_flush() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Xa_proc_advance_gcn_no_flush() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("advance_gcn_no_flush");
  }
};

}  // namespace im

#endif
