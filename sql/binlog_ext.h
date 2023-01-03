/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef BINLOG_EXT_INCLUDED
#define BINLOG_EXT_INCLUDED

#include "sql/binlog.h"

extern ulong opt_recovery_apply_binlog;
extern uint opt_print_gtid_info_during_recovery;
enum gtid_info_print {DISABLED, BASIC_INFO, DETAIL_INFO};

enum enum_recovery_apply_binlog_type {
  RECOVERY_APPLY_BINLOG_OFF = 0,
  RECOVERY_APPLY_BINLOG_ON = 1,
  RECOVERY_APPLY_BINLOG_SAME_AS_GTID = 2
};

void log_gtid_set(const char *gtid_set_name, const Gtid_set *gtids);

/** Extension of MYSQL_BIN_LOG. */
class Binlog_ext {
 public:
  Binlog_ext();

  Binlog_ext &operator=(const Binlog_ext &) = delete;
  Binlog_ext(const Binlog_ext &) = delete;
  Binlog_ext(Binlog_ext &&) = delete;

  /**
     For internal XA transactions, commit process looks like:
     - MYSQL_BIN_LOG::ordered_commit()
       - flush binlog(m_atomic_prep_xids++)
       - engine commit
       - finish_commit()(m_atomic_prep_xids--)
       - rotate() (wait until m_atomic_prep_xids is 0)

     m_atomic_prep_xids is used to block binlog rotations between write binlog
     and engine commit. It guarantees that engine only lost commit state of
     transactions in the last binlog file. Thus crash recovery just need to
     recover data from the last binlog file.

     For external XA transactions, the process is different, XA_rotate_guard
     guarantees that engine only lost XA state of transactions in the last
     binlog file. the process looks like:
     - trans_xa_commit()/trans_xa_rollback()/ha_xa_prepare()
       - XA_rotate_guard()
       - MYSQL_BIN_LOG::ordered_commit()
         - write binlog(m_atomic_prep_xids++)
         - finish_commit()
       - engine prepare/commit/rollback
       - ~XA_rotate_guard()
         - m_atomic_prep_xids--
         - rotate()(wait until m_atomic_prep_xids is 0)
   */
  class XA_rotate_guard {
   public:
    XA_rotate_guard(THD *thd, const bool *has_error)
        : m_thd(thd), m_has_error(has_error) {}
    ~XA_rotate_guard();

   private:
    THD *m_thd;
    const bool *m_has_error;
  };

  /**
    XA transaction increase m_atomic_prep_xids for blocking binlog rotation.

    @param[in]  thd  THD object of the xa transaction.
  */
  void xa_inc_prep_xids(THD *thd);
  /**
    XA transaction checks whether rotate is needed. For XA transactions,
    Rotation cannot be done in ordered_commit. It is delayed until finishing
    engine prepare/commit/rollback.

    @param[in]  thd   THD object of the xa transaction.
    @param[in]  do_rotate  true means rotate is needed

    @retval true if it is a rotation happens on XA transaction
  */
  bool xa_delay_rotate(THD *thd, bool do_rotate);

 public:
  bool assign_gcn_to_flush_group(THD *first_seen);
  bool write_gcn(THD *thd, Binlog_event_writer *writer);
};
extern Binlog_ext mysql_bin_log_ext;

/**
  It provides a crash recovery mechanism which allows users to relex the
  duribility of redo log. Users could set:
  sync_binlog = 1;
  innodb_flush_log_at_trx_commit = 2|0;

  In the mechanish, crash recovery checks whether binlog has more data than
  InnoDB engine. It will apply binlog to InnodB engine if it is true.

  Users even can set
  sync_binlog = 0;
  innodb_flush_log_at_trx_commit = 2|0;

  The mechanish will log an error if Binlog has less data than InnoDB engine.
  Users can control whether to abort the server with an mysqld argument in
  the situation.
*/
class Binlog_recovery {
 public:
  Binlog_recovery();

  /**
    Create the instance if it is nullptr and return it.

    @return nullptr or a valid pointer.
  */
  static Binlog_recovery *instance();
  /** Destroy the instance */
  static void destroy();

  /**
    Actions before starting binlog recovery.
    - checks whether that is the first start since OS started or not.
      Inconsistency check and binlog replay is only done if it is the
      first start.

    @retval  false  Succeeds.
    @retval  true   Error happens.
  */
  bool begin();

  /**
     Actions after binlog recovery.
     - replays binlog events if it is needed.
     - creates the shm object to mark first start recovery is done if
       no error happens.
  */
  bool end();

  bool need_apply_binlog();

  void set_need_apply_binlog();

  /**
    Add a gtid into m_gtids_in_last_file.

    @param[in]  gtid  The gtid will be added.
    @param[in]  gtid_pos  The start pos will be added.

    @retval  false  Succeeds.
    @retval  true   Error happens.
  */
  bool add_gtid(const Gtid &gtid, my_off_t start_pos, uint64 start_index);

  bool add_skip_counter_gtid(const Gtid &gtid);
  /**
    Set end pos into m_apply_end_pos.

    @param[in]  index  The end consensus index.
  */
  void set_end_index(uint64 end_index);

  void set_apply_log_name(char *logname);

  /**
    Apply the binlog event just after innodb's binlog offset.

    @retval  false  Succeeds.
    @retval  true   Error happens.
  */
  bool apply_binlog();

  Gtid_set *get_gtids_in_last_file() { return &m_gtids_in_last_file; }

  Gtid_set *get_skip_counter_gtids() { return &m_skip_counter_gtids; }

 private:
  static Binlog_recovery *m_instance;

  /* Ture if it is the first start since OS start. */
  bool m_first_start = false;
  bool m_need_apply_binlog = false;

  Binlog_recovery(const Binlog_ext &) = delete;
  Binlog_recovery &operator=(const Binlog_ext &) = delete;

  /**
    Read the xids of the prepared transactions from innodb.

    @param[out]  xids  The xids read from engine.
  */
  bool get_engine_prepared_xids(memroot_unordered_set<my_xid> *xids);

  /**
    Clear the LOG_EVENT_BINLOG_IN_USE_F flag after binlog is applied
    successfuly.

    @retval  false  Succeeds.
    @retval  true   Error happens.
  */
  bool clear_in_use_flag(const char *log_name);

  /** Returns the shm name of the server */
  static std::string shm_name();

  /** Stores the gtids of last binlog file */
  Gtid_set m_gtids_in_last_file;

  Gtid_set m_skip_counter_gtids;

  /** Where it starts to apply the last binlog file */
  my_off_t m_apply_start_pos = {0};

  uint64 m_apply_start_index = {0};
  uint64 m_apply_end_index = {0};

  char m_apply_log_name[FN_REFLEN] = {0};
};

#endif
