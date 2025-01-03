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

/** @file sql/xa/lizard_cmmt_policy.h
  Lizard XA policy structure.

 Created 2024-11-01 by Jiyang.zhang
 *******************************************************/

#ifndef XA_LIZARD_CMMT_POLICY_INCLUDED
#define XA_LIZARD_CMMT_POLICY_INCLUDED

#include <memory>

#include "my_alloc.h"
#include "sql/lizard/lizard_service.h"

class THD;
class Gcn_log_event;
class XA_specification;
namespace binary_log {
class Gcn_event;
}

namespace lizard {

class Commit_policy {
 public:
  Commit_policy() : m_decided(false) {}
  Commit_policy(const Commit_policy &other) : m_decided(other.m_decided) {}

  Commit_policy &operator=(const Commit_policy &other) {
    if (this != &other) {
      m_decided = other.m_decided;
    }
    return *this;
  }
  virtual ~Commit_policy() {}

  virtual void decide(THD *) = 0;

  virtual void copy_to_trx(trx_t *) const = 0;

  virtual void build_gcn_event(binary_log::Gcn_event *) const = 0;

  virtual void reset() = 0;

  [[nodiscard]] virtual Commit_policy *clone() const = 0;
  [[nodiscard]] virtual Commit_policy *clone(MEM_ROOT *mem_root) const = 0;

  bool has_decided() const { return m_decided; }

 protected:
  bool m_decided;
};

class Single_shard_policy : public Commit_policy {
 public:
  Single_shard_policy() : m_commit_gcn() {}

  Single_shard_policy(const Single_shard_policy &other)
      : Commit_policy(other), m_commit_gcn(other.m_commit_gcn) {}

  Single_shard_policy &operator=(const Single_shard_policy &other) {
    if (this != &other) {
      Commit_policy::operator=(other);
      m_commit_gcn = other.m_commit_gcn;
    }
    return *this;
  }

  virtual void decide(THD *thd) override;

  virtual void copy_to_trx(trx_t *) const override;

  virtual void build_gcn_event(binary_log::Gcn_event *) const override;

  virtual void reset() override;

  [[nodiscard]] virtual Commit_policy *clone() const override;
  [[nodiscard]] virtual Commit_policy *clone(MEM_ROOT *mem_root) const override;

  ~Single_shard_policy() override {}

 private:
  gcn_tuple_t m_commit_gcn;
};

class XA_commit_policy : public Commit_policy {
 public:
  XA_commit_policy() : m_commit_gcn() {}

  XA_commit_policy(const XA_commit_policy &other)
      : Commit_policy(other), m_commit_gcn(other.m_commit_gcn) {}

  XA_commit_policy &operator=(const XA_commit_policy &other) {
    if (this != &other) {
      Commit_policy::operator=(other);
      m_commit_gcn = other.m_commit_gcn;
    }
    return *this;
  }

  void init(gcn_t commit_gcn);

  virtual void decide(THD *thd) override;

  virtual void copy_to_trx(trx_t *) const override;

  virtual void build_gcn_event(binary_log::Gcn_event *) const override;

  virtual void reset() override;

  [[nodiscard]] virtual Commit_policy *clone() const override;
  [[nodiscard]] virtual Commit_policy *clone(MEM_ROOT *mem_root) const override;
  ~XA_commit_policy() override {}

 private:
  gcn_tuple_t m_commit_gcn;
};

class AC_prepare_policy : public Commit_policy {
 public:
  AC_prepare_policy()
      : m_pre_commit_gcn(GCN_NULL), m_branch(), m_proposal_gcn() {}

  AC_prepare_policy(const AC_prepare_policy &other)
      : Commit_policy(other),
        m_pre_commit_gcn(other.m_pre_commit_gcn),
        m_branch(other.m_branch),
        m_proposal_gcn(other.m_proposal_gcn) {}

  AC_prepare_policy &operator=(const AC_prepare_policy &other) {
    if (this != &other) {
      Commit_policy::operator=(other);
      m_pre_commit_gcn = other.m_pre_commit_gcn;
      m_branch = other.m_branch;
      m_proposal_gcn = other.m_proposal_gcn;
    }
    return *this;
  }

  void init(gcn_t pre_commit_gcn, const xa_branch_t &branch);

  virtual void decide(THD *thd) override;

  virtual void copy_to_trx(trx_t *) const override;

  virtual void reset() override;

  virtual void build_gcn_event(binary_log::Gcn_event *) const override;

  [[nodiscard]] virtual Commit_policy *clone() const override;
  [[nodiscard]] virtual Commit_policy *clone(MEM_ROOT *mem_root) const override;

  const gcn_tuple_t clone_pmmt() const { return m_proposal_gcn; }

  ~AC_prepare_policy() override {}

 private:
  gcn_t m_pre_commit_gcn;

  xa_branch_t m_branch;

  gcn_tuple_t m_proposal_gcn;
};

class AC_commit_policy : public Commit_policy {
 public:
  AC_commit_policy() : m_hlc_gcn(GCN_NULL), m_master_addr(), m_commit_gcn() {}

  AC_commit_policy(const AC_commit_policy &other)
      : Commit_policy(other),
        m_hlc_gcn(other.m_hlc_gcn),
        m_master_addr(other.m_master_addr),
        m_commit_gcn(other.m_commit_gcn) {}

  AC_commit_policy &operator=(const AC_commit_policy &other) {
    if (this != &other) {
      Commit_policy::operator=(other);
      m_hlc_gcn = other.m_hlc_gcn;
      m_master_addr = other.m_master_addr;
      m_commit_gcn = other.m_commit_gcn;
    }
    return *this;
  }

  void init(gcn_t hlc_gcn, const xa_addr_t &master_addr);

  virtual void decide(THD *thd) override;

  virtual void copy_to_trx(trx_t *trx) const override;

  virtual void build_gcn_event(binary_log::Gcn_event *) const override;

  virtual void reset() override;

  [[nodiscard]] virtual Commit_policy *clone() const override;
  [[nodiscard]] virtual Commit_policy *clone(MEM_ROOT *mem_root) const override;

  ~AC_commit_policy() override {}

 private:
  gcn_t m_hlc_gcn;

  xa_addr_t m_master_addr;

  gcn_tuple_t m_commit_gcn;
};

class Binlog_ac_prepare_policy : public Commit_policy {
 public:
  Binlog_ac_prepare_policy();

  Binlog_ac_prepare_policy(const gcn_tuple_t &tuple, const xa_branch_t &branch)
      : m_proposal_gcn(tuple), m_branch(branch) {}

  Binlog_ac_prepare_policy(const Binlog_ac_prepare_policy &other)
      : Commit_policy(other),
        m_proposal_gcn(other.m_proposal_gcn),
        m_branch(other.m_branch) {}

  Binlog_ac_prepare_policy &operator=(const Binlog_ac_prepare_policy &other) {
    if (this != &other) {
      Commit_policy::operator=(other);
      m_proposal_gcn = other.m_proposal_gcn;
      m_branch = other.m_branch;
    }
    return *this;
  }

  void init(const gcn_tuple_t &tuple, const xa_branch_t &branch);

  virtual void decide(THD *thd) override;

  virtual void copy_to_trx(trx_t *trx) const override;

  virtual void build_gcn_event(binary_log::Gcn_event *) const override;

  virtual void reset() override;

  [[nodiscard]] virtual Commit_policy *clone() const override;
  [[nodiscard]] virtual Commit_policy *clone(MEM_ROOT *mem_root) const override;

  ~Binlog_ac_prepare_policy() override {}

 private:
  gcn_tuple_t m_proposal_gcn;

  xa_branch_t m_branch;
};

class Binlog_commit_policy : public Commit_policy {
 public:
  Binlog_commit_policy();
  Binlog_commit_policy(const gcn_tuple_t &tuple) : m_commit_gcn(tuple) {}

  Binlog_commit_policy(const Binlog_commit_policy &other)
      : Commit_policy(other), m_commit_gcn(other.m_commit_gcn) {}

  Binlog_commit_policy &operator=(const Binlog_commit_policy &other) {
    if (this != &other) {
      Commit_policy::operator=(other);
      m_commit_gcn = other.m_commit_gcn;
    }
    return *this;
  }

  void init(const gcn_tuple_t &tuple);

  virtual void decide(THD *thd) override;

  virtual void copy_to_trx(trx_t *trx) const override;

  virtual void build_gcn_event(binary_log::Gcn_event *) const override;

  virtual void reset() override;

  [[nodiscard]] virtual Commit_policy *clone() const override;
  [[nodiscard]] virtual Commit_policy *clone(MEM_ROOT *mem_root) const override;

  ~Binlog_commit_policy() override {}

 private:
  gcn_tuple_t m_commit_gcn;
};

/** Commit policy context. */
class Commit_policy_ctx {
 public:
  enum Type {
    /** Basic and default xa policy. */
    SINGLE_SHARD,
    /** XA COMMIT policy. */
    XA_COMMIT,
    /** AC PREPARE policy. */
    AC_PREPARE,
    /** AC COMMIT policy. */
    AC_COMMIT,
    /** Binlog rpl ac prepare policy. */
    BINLOG_AC_PREPARE,
    /** Binlog rpl commit policy */
    BINLOG_COMMIT,
  };

 public:
  Commit_policy_ctx()
      : m_single_shard(),
        m_xa_commit(),
        m_ac_prepare(),
        m_ac_commit(),
        m_binlog_ac_prepare(),
        m_binlog_commit(),
        m_current(&m_single_shard),
        m_type(SINGLE_SHARD) {}

  Commit_policy_ctx(const Commit_policy_ctx &) = delete;
  Commit_policy_ctx &operator=(Commit_policy_ctx &) = delete;

  void activate_xa_commit(const gcn_t &commit_gcn);

  void activate_ac_commit(const gcn_t &commit_gcn,
                          const xa_addr_t &master_addr);

  /** Activate ac prepare policy
   *
   * @param[in]		proposal gcn
   * @param[in]		branch info
   *
   * @retval		ac prepare policy handler.*/
  AC_prepare_policy *activate_ac_prepare(const gcn_t &pre_commit_gcn,
                                         const xa_branch_t &branch);

  void activate_binlog(const Gcn_log_event *gcn_event);

  /** TODO: call this when do real commit. */
  void activate_single_shard();

  Commit_policy *current_policy() { return m_current; }

  void reset() {
    /** Single shard policy is default policy. */
    activate_single_shard();
  }

  void copy_from(const Commit_policy_ctx &cmmt_policy_ctx) {
    /** 1. Copy all cmmmit policy. */
    m_single_shard = cmmt_policy_ctx.m_single_shard;
    m_xa_commit = cmmt_policy_ctx.m_xa_commit;
    m_ac_prepare = cmmt_policy_ctx.m_ac_prepare;
    m_ac_commit = cmmt_policy_ctx.m_ac_commit;
    m_binlog_ac_prepare = cmmt_policy_ctx.m_binlog_ac_prepare;
    m_binlog_commit = cmmt_policy_ctx.m_binlog_commit;

    /** 2. Set current policy. */
    switch (cmmt_policy_ctx.m_type) {
      case SINGLE_SHARD:
        m_current = &m_single_shard;
        break;
      case XA_COMMIT:
        m_current = &m_xa_commit;
        break;
      case AC_PREPARE:
        m_current = &m_ac_prepare;
        break;
      case AC_COMMIT:
        m_current = &m_ac_commit;
        break;
      case BINLOG_AC_PREPARE:
        m_current = &m_binlog_ac_prepare;
        break;
      case BINLOG_COMMIT:
        m_current = &m_binlog_commit;
    }
    m_type = cmmt_policy_ctx.m_type;
  }

 private:
  Single_shard_policy m_single_shard;

  XA_commit_policy m_xa_commit;

  AC_prepare_policy m_ac_prepare;

  AC_commit_policy m_ac_commit;

  Binlog_ac_prepare_policy m_binlog_ac_prepare;

  Binlog_commit_policy m_binlog_commit;

  Commit_policy *m_current;

  enum Type m_type;

 private:
  /**
   * All commit policy could be divided into two types:
   * 1. Implicit policy: to deal with single shard transaction, whose GCN comes
   * from the internal GCS. Single shard transactions are so common that implict
   * policy is the default policy.
   *
   * 2. Explicit policy: to deal with external xa transactions(not one phase) or
   * replaying binlog. Different from implicit policy, GCN of explicit policy
   * comes from the external source, which could be specified by external GCS or
   * Binlog GCN event.
   *
   * State transition:
   * 1. Explicit/Implicit -> Implicit : call @func: reset(). It happen just
   * after we commit or rollback a transaction.
   *
   * 2. Implicit -> Explicit: call @func: activate_xxx(). It happen just before
   * we do the real commit or rollback.
   *
   * 3. Explicit -> Explicit: forbidden.
   */
  bool is_implicit_policy() const { return m_type == SINGLE_SHARD; }
  bool is_explicit_policy() const { return m_type != SINGLE_SHARD; }
};

extern void commit_policy_decide(THD *thd);

extern void commit_policy_copy(THD *thd, trx_t *trx);
}

#endif
