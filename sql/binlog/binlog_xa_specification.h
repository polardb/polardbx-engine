/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file sql/binlog/binlog_xa_specification.h

  Transaction coordinator (binlog) log structure specification.

  Created 2023-06-14 by Jianwei.zhao
 *******************************************************/

#ifndef SQL_BINLOG_BINLOG_XA_SPECIFICATION_H
#define SQL_BINLOG_BINLOG_XA_SPECIFICATION_H

#include "sql/xa_specification.h"

#include "sql/rpl_gtid.h"

namespace binlog {

/** XA specification when TC_LOG = Binary Log */
class Binlog_xa_specification : public XA_specification {
 private:
  typedef XA_specification super;

 public:
  enum class Source {
    /** Not sure*/
    NONE,
    /** From internal commit */
    COMMIT,
    /** From xa prepare */
    XA_PREPARE,
    /** From xa commit one phase */
    XA_COMMIT_ONE_PHASE,
    /** From xa commit */
    XA_COMMIT,
    /** From xa rollback */
    XA_ROLLBACK
  };

 public:
  Binlog_xa_specification()
      : XA_specification(), m_source(Source::NONE), m_gtid(), m_sid() {
    clear();
  }

  ~Binlog_xa_specification() override {}

  Binlog_xa_specification(const Binlog_xa_specification &other)
      : XA_specification(other) {
    m_gtid = other.m_gtid;
    m_source = other.m_source;
    m_sid.copy_from(other.m_sid);
  }

  Binlog_xa_specification &operator=(const Binlog_xa_specification &other) {
    if (this != &other) {
      XA_specification::operator=(other);

      this->m_gtid = other.m_gtid;
      this->m_source = other.m_source;
      m_sid.copy_from(other.m_sid);
    }
    return *this;
  }

  bool is_legal_source() const { return m_source != Source::NONE; }

  virtual void clear() override {
    super::clear();
    m_source = Source::NONE;
    m_gtid.clear();
  }

  void transaction_end() { clear(); }

  /** Getter and Setter */
  Gtid *gtid() { return &m_gtid; }
  rpl_sid *sid() { return &m_sid; }
  Source source() const { return m_source; }

  bool has_gtid() const { return !m_gtid.is_empty(); }

  /**TODO: */
  virtual std::string print() override { return "Gtid:"; }

  [[nodiscard]] XA_specification *clone() const override {
    return new Binlog_xa_specification(*this);
  }

 public:
  Source m_source;
  Gtid m_gtid;
  rpl_sid m_sid;
};

class Commit_binlog_xa_specification {
 public:
  Commit_binlog_xa_specification(Binlog_xa_specification *spec)
      : m_spec(spec) {}

  ~Commit_binlog_xa_specification() { m_spec->transaction_end(); }

 private:
  Binlog_xa_specification *m_spec;
};

}  // namespace binlog
#endif
