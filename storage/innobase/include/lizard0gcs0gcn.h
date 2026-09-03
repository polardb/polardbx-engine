/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0gcs0gcn.h
 Global Commit Number implementation.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0gcs0gcn_h
#define lizard0gcs0gcn_h

#include "trx0trx.h"
#include "mtr0types.h"
#include "ut0mutex.h"

#include "lizard0cmmt0types.h"

#ifdef UNIV_PFS_MUTEX

/* GCS gcn order mutex PFS key */
extern mysql_pfs_key_t gcn_order_mutex_key;

/* GCS gcn persist mutex PFS key */
extern mysql_pfs_key_t gcn_persist_mutex_key;

#endif

namespace lizard {

/** Whether to print gcs process info. */
extern bool srv_gcs_print_process_debug;

/* The structure of gcn number generation */
class GCN {
 public:
  /** The initial global commit number value after initialize db */
  static constexpr gcn_t GCN_INSTALL_VALUE = 1024;

 public:
  typedef struct meta_t {
    /**For convience from 0. */
    gcn_t gcn{0};
    /** Set gcn if bigger */
    void set_gcn_if_bigger(const meta_t &meta) {
      ut_a(meta.gcn > 0 && meta.gcn <= GCN_MAX);

      if (meta.gcn > gcn) {
        gcn = meta.gcn;
      }
    }

    /** valid gcn value. */
    bool valid() const { return gcn > 0 && gcn <= GCN_MAX; }
  } meta_t;

  /** gcn persister */
  class GcnPersister {
   public:
    /** Write gcn value into tablespace.

        @param[in]	metadata   gcn metadata
     */
    void write(const meta_t *metadata);

    /** Write gcn value redo log.

        @param[in]	metadata   gcn metadata
        @param[in]	mini transacton context

     */
    void write_log(const meta_t *metadata, mtr_t *mtr);
    /**
       Writes a log record about gcn rise.

       @param[in]		type	redo log record type
       @param[in,out]	log_ptr		current end of mini-transaction log
       @param[in/out]	mtr	mini-transaction

       @reval	end of mtr
     */
    byte *mlog_write_initial_gcn_log_record(mlog_id_t type, byte *log_ptr,
                                            mtr_t *mtr);
    /** Read gcn value from tablespace

        @param[in/out]	metadata   gcn metadata

     */
    void read(meta_t *metadata);
    /**
       Parses an initial log record written by
       mlog_write_initial_gcn_log_record.

       @param[in]	ptr		buffer
       @param[in]	end_ptr		buffer end
       @param[out]	type		log record type, should be
                                    MLOG_GCN_METADATA
       @param[out]	gcn

       @return parsed record end, NULL if not a complete record
     */
    static byte *mlog_parse_initial_gcn_log_record(const byte *ptr,
                                                   const byte *end_ptr,
                                                   mlog_id_t *type,
                                                   meta_t *meta);
  };

 public:
  GCN();
  virtual ~GCN();

  void boot();
  /** Generate a new proposal gcn when xa prepare in_tc according to user xa
   * proposal, then assign to trx->txn_desc.pmmt and will be written to undo
   * header.
   *
   * @param[in]		user proposal number
   * @param[in]		transaction
   * @param[in]		prepare_in_tc mini-transaction
   *
   * @retval		Proposed number */
  gcn_tuple_t new_proposal_gcn(const proposal_mark_t &pmmt, trx_t *trx,
                               mtr_t *mtr);
  /** Generate a new commit gcn when commit according to user commit sequence if
   * has, then assign to trx->txn_desc.cmmt and will be written to undo header.
   *
   * @param[in]		user commit sequence
   * @param[in]		transaction
   * @param[in]		commit mini-transaction
   *
   * @retval		commit number */
  gcn_tuple_t new_commit_gcn(const gcn_tuple_t &tuple, trx_t *trx, mtr_t *mtr);

  /**
     Push up current gcn if bigger when recovery.

     @param[in]	gcn
  */
  void recover_gcn_if_bigger(const meta_t &meta);

  /**
     Push up current gcn if bigger.

     @param[in]	gcn
  */
  void set_gcn_if_bigger(const gcn_t gcn);
  /**
   * Persist gcn if current gcn > persisted gcn.
   *
   * @retval	true	if written */
  bool persist_gcn();

  gcn_t load_gcn() const { return m_gcn.load(); }
  gcn_t persisted_gcn() const { return m_persisted_gcn.load(); }

#ifdef UNIV_DEBUG
  bool order_mutex_own() { return mutex_own(&m_order_mutex); }
#endif

  /** Disable the copy and assign function */
  GCN(const GCN &) = delete;
  GCN(const GCN &&) = delete;
  GCN &operator=(const GCN &) = delete;

 private:
  /**
    Prepare a gcn number according to source type when
    commit.

    1) CSR_AUTOMATIC
      -- Generate from current gcn early, use it directly.
      -- Use current gcn as commit gcn.
    2) CSR_ASSIGNED
      -- Come from 3-party component, use it directly,
         increase current gcn if bigger.

    @param[in]	gcn
    @param[in]	csr
    @param[in]	mini transaction

    @retval	gcn
  */
  gcn_tuple_t new_gcn(const gcn_tuple_t &tuple, mtr_t *mtr);

 private:
  std::atomic<gcn_t> m_gcn;
  /** Protect m_gcn order */
  ib_mutex_t m_order_mutex;

  GcnPersister m_persister;
  /** Persisted gcn value. */
  std::atomic<gcn_t> m_persisted_gcn;
  /** Serialize gcn persist */
  ib_mutex_t m_persist_mutex;

  bool m_inited;
};

}  // namespace lizard

using gcn_meta_t = lizard::GCN::meta_t;

#endif
