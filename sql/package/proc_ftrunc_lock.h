/* Copyright (c) 2024, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef PROC_FTRUNC_LOCK_INCLUDED
#define PROC_FTRUNC_LOCK_INCLUDED

#include "sql/package/proc.h"
#include "sql/sql_ftrunc_lock.h"

namespace im {

namespace ftrunc_lock {

/* Uniform schema name for lock */
extern LEX_CSTRING FTRUNC_LOCK_PROC_SCHEMA;

/**
  Proc base for dbms_lock

  1) Uniform schema: dbms_lock
  2) Requires BACKUP_ADMIN privilege
*/
class Ftrunc_lock_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Ftrunc_lock_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << FTRUNC_LOCK_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Base class for Sql_cmd_ftrunc_lock_proc classes.

  Provides common implementation for check_access and send_result.
*/
class Sql_cmd_ftrunc_lock_proc_base : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_ftrunc_lock_proc_base(THD *thd, mem_root_deque<Item *> *list,
                                         const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {
    set_priv_type(Priv_type::PRIV_NONE_ACL);
  }

  /**
    Check access, require BACKUP_ADMIN privilege.
  */
  virtual bool check_access(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;
};

/**
  1) dbms_lock.acquire_s_ftrunc();
     Acquire shared ftrunc lock (S lock)
*/
class Sql_cmd_ftrunc_lock_proc_acquire_shared
    : public Sql_cmd_ftrunc_lock_proc_base {
 public:
  explicit Sql_cmd_ftrunc_lock_proc_acquire_shared(THD *thd,
                                                   mem_root_deque<Item *> *list,
                                                   const Proc *proc)
      : Sql_cmd_ftrunc_lock_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;
};

class Ftrunc_lock_proc_acquire_shared : public Ftrunc_lock_proc_base {
  using Sql_cmd_type = Sql_cmd_ftrunc_lock_proc_acquire_shared;

 public:
  explicit Ftrunc_lock_proc_acquire_shared(PSI_memory_key key)
      : Ftrunc_lock_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
    /* No parameters */
  }

  /* Singleton instance for acquire_s_ftrunc */
  static Proc *instance();

  /**
    Invoke the sql_cmd object for acquire_s_ftrunc() proc.
  */
  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Ftrunc_lock_proc_acquire_shared() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("acquire_s_ftrunc");
  }
};

/**
  2) dbms_lock.acquire_x_ftrunc();
     Acquire exclusive ftrunc lock (X lock)
*/
class Sql_cmd_ftrunc_lock_proc_acquire_exclusive
    : public Sql_cmd_ftrunc_lock_proc_base {
 public:
  explicit Sql_cmd_ftrunc_lock_proc_acquire_exclusive(
      THD *thd, mem_root_deque<Item *> *list, const Proc *proc)
      : Sql_cmd_ftrunc_lock_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;
};

class Ftrunc_lock_proc_acquire_exclusive : public Ftrunc_lock_proc_base {
  using Sql_cmd_type = Sql_cmd_ftrunc_lock_proc_acquire_exclusive;

 public:
  explicit Ftrunc_lock_proc_acquire_exclusive(PSI_memory_key key)
      : Ftrunc_lock_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
    /* No parameters */
  }

  /* Singleton instance for acquire_x_ftrunc */
  static Proc *instance();

  /**
    Invoke the sql_cmd object for acquire_x_ftrunc() proc.
  */
  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Ftrunc_lock_proc_acquire_exclusive() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("acquire_x_ftrunc");
  }
};

/**
  3) dbms_lock.release_ftrunc();
     Release ftrunc lock
*/
class Sql_cmd_ftrunc_lock_proc_release : public Sql_cmd_ftrunc_lock_proc_base {
 public:
  explicit Sql_cmd_ftrunc_lock_proc_release(THD *thd,
                                            mem_root_deque<Item *> *list,
                                            const Proc *proc)
      : Sql_cmd_ftrunc_lock_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;
};

class Ftrunc_lock_proc_release : public Ftrunc_lock_proc_base {
  using Sql_cmd_type = Sql_cmd_ftrunc_lock_proc_release;

 public:
  explicit Ftrunc_lock_proc_release(PSI_memory_key key)
      : Ftrunc_lock_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
    /* No parameters */
  }

  /* Singleton instance for release_ftrunc */
  static Proc *instance();

  /**
    Invoke the sql_cmd object for release_ftrunc() proc.
  */
  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Ftrunc_lock_proc_release() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("release_ftrunc");
  }
};

} /* namespace ftrunc_lock */

} /* namespace im */
#endif
