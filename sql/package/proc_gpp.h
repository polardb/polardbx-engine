/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/package/proc_gpp.h
 Native procedure for retrieving gpp related statistics.

 Created 2024-04-10 by Ting Yuan
 *******************************************************/

#ifndef SQL_PROC_GPP_INCLUDED
#define SQL_PROC_GPP_INCLUDED

#include "sql/package/proc.h"
#include "sql/package/gpp_stat.h"

namespace im {

extern const LEX_CSTRING PROC_STAT_SCHEMA;

/* dbms_stat.flush_gpp() */
class Sql_cmd_index_stat_flush_gpp : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_index_stat_flush_gpp(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}

  virtual bool pc_execute(THD *thd) override;
};

class Proc_index_stat_flush_gpp : public Proc {
 public:
  typedef Proc_index_stat_flush_gpp Sql_cmd_type;

 public:
  explicit Proc_index_stat_flush_gpp(PSI_memory_key key) : Proc(key) {
    m_result_type = Result_type::RESULT_OK;
  }

  virtual ~Proc_index_stat_flush_gpp() {}

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  virtual const std::string str() const override {
    return std::string("flush_gpp");
  }

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << PROC_STAT_SCHEMA.str << "." << str();
    return ss.str();
  }
};

} /* namespace im */

#endif
