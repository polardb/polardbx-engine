/* Copyright (c) 2005, 2017, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

#ifndef RPL_RLI_H
#define RPL_RLI_H

#include <sys/types.h>
#include <time.h>
#include <atomic>
#include <string>
#include <vector>

#include "binlog.h"            // MYSQL_BIN_LOG
#include "handler.h"
#include "lex_string.h"
#include "m_string.h"
#include "my_bitmap.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_io.h"
#include "my_loglevel.h"
#include "my_psi_config.h"
#include "my_sys.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/psi_base.h"
#include "mysql/thread_type.h"
#include "prealloced_array.h"  // Prealloced_array
#include "query_options.h"
#include "rpl_gtid.h"          // Gtid_set
#include "rpl_info.h"          // Rpl_info
#include "rpl_mts_submode.h"   // enum_mts_parallel_type
#include "rpl_record.h"
#include "rpl_slave_until_options.h"
#include "rpl_tblmap.h"        // table_mapping
#include "rpl_utility.h"       // Deferred_log_events
#include "sql_class.h"         // THD
#include "sql_lex.h"
#include "sql_plugin_ref.h"
#include "sql_string.h"
#include "system_variables.h"
#include "table.h"
#include "log_event.h"        //Gtid_log_event
#include "rpl_filter.h"

struct RPL_TABLE_LIST;
class Commit_order_manager;
class Format_description_log_event;
class Log_event;
class Master_info;
class Master_info;
class Rows_query_log_event;
class Rpl_info_handler;
class Slave_committed_queue;
class Slave_worker;

typedef struct st_db_worker_hash_entry db_worker_hash_entry;
extern uint sql_slave_skip_counter;

typedef Prealloced_array<Slave_worker*, 4> Slave_worker_array;

typedef struct slave_job_item
{
  Log_event *data;
  uint relay_number;
  my_off_t relay_pos;
} Slave_job_item;

/*******************************************************************************
Replication SQL Thread

Relay_log_info contains:
  - the current relay log
  - the current relay log offset
  - master log name
  - master log sequence corresponding to the last update
  - misc information specific to the SQL thread

Relay_log_info is initialized from a repository, i.e. table or file, if there is
one. Otherwise, data members are intialized with defaults by calling
init_relay_log_info().

The relay.info table/file shall be updated whenever: (i) the relay log file
is rotated, (ii) SQL Thread is stopped, (iii) while processing a Xid_log_event,
(iv) after a Query_log_event (i.e. commit or rollback) and (v) after processing
any statement written to the binary log without a transaction context.

The Xid_log_event is a commit for transactional engines and must be handled
differently to provide reliability/data integrity. In this case, positions
are updated within the context of the current transaction. So

  . If the relay.info is stored in a transactional repository and the server
  crashes before successfully committing the transaction the changes to the
  position table will be rolled back along with the data.

  . If the relay.info is stored in a non-transactional repository, for instance,
  a file or a system table created using MyIsam, and the server crashes before
  successfully committing the transaction the changes to the position table
  will not be rolled back but data will.

In particular, when there are mixed transactions, i.e a transaction that updates
both transaction and non-transactional engines, the Xid_log_event is still used
but reliability/data integrity cannot be achieved as we shall explain in what
follows.

Changes to non-transactional engines, such as MyIsam, cannot be rolled back if a
failure happens. For that reason, there is no point in updating the positions
within the boundaries of any on-going transaction. This is true for both commit
and rollback. If a failure happens after processing the pseudo-transaction but
before updating the positions, the transaction will be re-executed when the
slave is up most likely causing an error that needs to be manually circumvented.
This is a well-known issue when non-transactional statements are executed.

Specifically, if rolling back any transaction, positions are updated outside the
transaction boundaries. However, there may be a problem in this scenario even
when only transactional engines are updated. This happens because if there is a
rollback and such transaction is written to the binary log, a non-transactional
engine was updated or a temporary table was created or dropped within its
boundaries.

In particular, in both STATEMENT and MIXED logging formats, this happens because
any temporary table is automatically dropped after a shutdown/startup.
See BUG#26945 for further details.

Statements written to the binary log outside the boundaries of a transaction are
DDLs or maintenance commands which are not transactional. These means that they
cannot be rolled back if a failure happens. In such cases, the positions are
updated after processing the events. If a failure happens after processing the
statement but before updating the positions, the statement will be
re-executed when the slave is up most likely causing an error that needs to be
manually circumvented. This is a well-known issue when non-transactional
statements are executed.

The --sync-relay-log-info does not have effect when a system table, either
transactional or non-transactional is used.

To correctly recovery from failures, one should combine transactional system
tables along with the --relay-log-recovery.
*******************************************************************************/
class Relay_log_info : public Rpl_info
{
  friend class Rpl_info_factory;

public:

  /*
    The per-channel filter associated with this RLI
  */
  Rpl_filter* rpl_filter;
  /**
     Flags for the state of the replication.
   */
  enum enum_state_flag {
    /** The replication thread is inside a statement */
    IN_STMT,

    /** Flag counter.  Should always be last */
    STATE_FLAGS_COUNT
  };

  /*
    The SQL thread owns one Relay_log_info, and each client that has
    executed a BINLOG statement owns one Relay_log_info. This function
    returns zero for the Relay_log_info object that belongs to the SQL
    thread and nonzero for Relay_log_info objects that belong to
    clients.
  */
  inline bool belongs_to_client()
  {
    DBUG_ASSERT(info_thd);
    return !info_thd->slave_thread;
  }
/* Instrumentation key for performance schema for mts_temp_table_LOCK */
#ifdef HAVE_PSI_INTERFACE
  PSI_mutex_key m_key_mts_temp_table_LOCK;
#endif
  /*
     Lock to protect race condition while transferring temporary table from
     worker thread to coordinator thread and vice-versa
   */
  mysql_mutex_t mts_temp_table_LOCK;
  /*
     Lock to acquire by methods that concurrently update lwm of committed
     transactions and the min waited timestamp and its index.
  */
  mysql_mutex_t mts_gaq_LOCK;
  mysql_cond_t  logical_clock_cond;
  /*
    If true, events with the same server id should be replicated. This
    field is set on creation of a relay log info structure by copying
    the value of ::replicate_same_server_id and can be overridden if
    necessary. For example of when this is done, check sql_binlog.cc,
    where the BINLOG statement can be used to execute "raw" events.
   */
  bool replicate_same_server_id;

  /*** The following variables can only be read when protect by data lock ****/
  /*
    cur_log_fd - file descriptor of the current read  relay log
  */
  File cur_log_fd;
  /*
    Protected with internal locks.
    Must get data_lock when resetting the logs.
  */
  MYSQL_BIN_LOG relay_log;
  LOG_INFO linfo;

  /*
   cache_buf
     IO_CACHE used when opening relay logs.
   */
  IO_CACHE cache_buf;

  /*
    Identifies when the recovery process is going on.
    See sql/slave.cc:init_recovery for further details.
  */
  bool is_relay_log_recovery;

  /* The following variables are safe to read any time */

  /*
    When we restart slave thread we need to have access to the previously
    created temporary tables. Modified only on init/end and by the SQL
    thread, read only by SQL thread.
  */
  TABLE *save_temporary_tables;

  /* parent Master_info structure */
  Master_info *mi;

  /* number of temporary tables open in this channel */
  std::atomic<int32> atomic_channel_open_temp_tables{0};

  /** the status of the commit timestamps for the relay log */
  enum
  {
    /*
      no GTID log event has been processed, so it is not known if this log
      has commit timestamps
    */
    COMMIT_TS_UNKNOWN,
    // the immediate master does not support commit timestamps
    COMMIT_TS_NOT_FOUND,
    // the immediate master supports commit timestamps
    COMMIT_TS_FOUND
  } commit_timestamps_status;

  /**
    @return the pointer to the Gtid_monitoring_info.
  */
  Gtid_monitoring_info* get_gtid_monitoring_info()
  {
    return gtid_monitoring_info;
  }

  /**
    Stores the details of the transaction which has just started processing.

    This function is called by the STS applier or MTS worker when applying a
    Gtid.

    @param  gtid_arg         the gtid of the trx
    @param  original_ts_arg  the original commit timestamp of the transaction
    @param  immediate_ts_arg the immediate commit timestamp of the transaction
    @param  skipped          true if the transaction was gtid skipped
  */
  void started_processing(Gtid gtid_arg, ulonglong original_ts_arg,
                          ulonglong immediate_ts_arg, bool skipped= false)
  {
    gtid_monitoring_info->start(gtid_arg, original_ts_arg, immediate_ts_arg,
                                skipped);
  }

  /**
    Stores the details of the transaction which has just started processing.

    This function is called by the MTS coordinator when queuing a Gtid to
    a worker.

    @param  gtid_log_ev_arg the gtid log event of the trx
  */
  void started_processing(Gtid_log_event *gtid_log_ev_arg)
  {
    Gtid gtid= {0, 0};
    if (gtid_log_ev_arg->get_type() == GTID_GROUP)
    {
      gtid= {gtid_log_ev_arg->get_sidno(true), gtid_log_ev_arg->get_gno()};
    }
    started_processing(gtid,
                       gtid_log_ev_arg->original_commit_timestamp,
                       gtid_log_ev_arg->immediate_commit_timestamp);
  }


  /**
    When the processing of a transaction is completed, that timestamp is
    recorded, the information is copied to last_processed_trx and the
    information in processing_trx is cleared.

    If the transaction was "applied" but GTID-skipped, the copy will not
    happen and the last_processed_trx will keep its current value.
  */
  void finished_processing()
  {
    gtid_monitoring_info->finish();
  }

  /**
    @return True if there is a transaction being currently processed
  */
  bool is_processing_trx()
  {
    return gtid_monitoring_info->is_processing_trx_set();
  }

  /**
   Clears the processing_trx structure fields. Normally called when there is an
   error while processing the transaction.
  */
  void clear_processing_trx()
  {
    gtid_monitoring_info->clear_processing_trx();
  }

  /**
    Clears the Gtid_monitoring_info fields.
  */
  void clear_gtid_monitoring_info()
  {
    gtid_monitoring_info->clear();
  }

  /*
    If on init_info() call error_on_rli_init_info is true that means
    that previous call to init_info() terminated with an error, RESET
    SLAVE must be executed and the problem fixed manually.
   */
  bool error_on_rli_init_info;

  /**
    Variable is set to true as long as
    original_commit_timestamp > immediate_commit_timestamp so that the
    corresponding warning is only logged once.
  */
  bool gtid_timestamps_warning_logged;

  /*
    Let's call a group (of events) :
      - a transaction
      or
      - an autocommiting query + its associated events (INSERT_ID,
    TIMESTAMP...)
    We need these rli coordinates :
    - relay log name and position of the beginning of the group we currently are
    executing. Needed to know where we have to restart when replication has
    stopped in the middle of a group (which has been rolled back by the slave).
    - relay log name and position just after the event we have just
    executed. This event is part of the current group.
    Formerly we only had the immediately above coordinates, plus a 'pending'
    variable, but this dealt wrong with the case of a transaction starting on a
    relay log and finishing (commiting) on another relay log. Case which can
    happen when, for example, the relay log gets rotated because of
    max_binlog_size.
  */
protected:
  char group_relay_log_name[FN_REFLEN];
  ulonglong group_relay_log_pos;
  char event_relay_log_name[FN_REFLEN];
  /* The suffix number of relay log name */
  uint event_relay_log_number;
  ulonglong event_relay_log_pos;
  ulonglong future_event_relay_log_pos;

  /* current event's start position in relay log */
  my_off_t event_start_pos;
  /*
     Original log name and position of the group we're currently executing
     (whose coordinates are group_relay_log_name/pos in the relay log)
     in the master's binlog. These concern the *group*, because in the master's
     binlog the log_pos that comes with each event is the position of the
     beginning of the group.

    Note: group_master_log_name, group_master_log_pos must only be
    written from the thread owning the Relay_log_info (SQL thread if
    !belongs_to_client(); client thread executing BINLOG statement if
    belongs_to_client()).
  */
  char group_master_log_name[FN_REFLEN];
  volatile my_off_t group_master_log_pos;

private:
  Gtid_set *gtid_set;
  /*
    Identifies when this object belongs to the SQL thread and was not
    created for a client thread or some other purpose including
    Slave_worker instance initializations. Ends up serving the same
    purpose as the belongs_to_client method, but its value is set
    earlier on in the class constructor.
  */
  bool rli_fake;
  /* Flag that ensures the retrieved GTID set is initialized only once. */
  bool gtid_retrieved_initialized;

  /**
    Stores information on the last processed transaction or the transaction
    that is currently being processed.

    STS:
    - timestamps of the currently applying/last applied transaction

    MTS:
    - coordinator thread: timestamps of the currently scheduling/last scheduled
      transaction in a worker's queue
    - worker thread: timestamps of the currently applying/last applied
      transaction
  */
  Gtid_monitoring_info *gtid_monitoring_info;

public:
  Sid_map* get_sid_map()
  { return gtid_set->get_sid_map(); }

  Checkable_rwlock* get_sid_lock()
  { return get_sid_map()->get_sid_lock(); }

  void add_logged_gtid(rpl_sidno sidno, rpl_gno gno)
  {
    get_sid_lock()->assert_some_lock();
    DBUG_ASSERT(sidno <= get_sid_map()->get_max_sidno());
    gtid_set->ensure_sidno(sidno);
    gtid_set->_add_gtid(sidno, gno);
  }

  /**
    Adds a GTID set to received GTID set.

    @param gtid_set the gtid_set to add

    @return RETURN_STATUS_OK or RETURN_STATUS_REPORTED_ERROR.
  */
  enum_return_status add_gtid_set(const Gtid_set *gtid_set);

  const Gtid_set *get_gtid_set() const { return gtid_set; }

  int init_relay_log_pos(const char* log,
                         ulonglong pos, bool need_data_lock,
                         const char** errmsg,
                         bool keep_looking_for_fd);

  /*
    Update the error number, message and timestamp fields. This function is
    different from va_report() as va_report() also logs the error message in the
    log apart from updating the error fields.
  */
  void fill_coord_err_buf(loglevel level, int err_code,
                          const char *buff_coord) const;


  /*
    Flag that the group_master_log_pos is invalid. This may occur
    (for example) after CHANGE MASTER TO RELAY_LOG_POS.  This will
    be unset after the first event has been executed and the
    group_master_log_pos is valid again.
   */
  bool is_group_master_log_pos_invalid;

  /*
    Handling of the relay_log_space_limit optional constraint.
    ignore_log_space_limit is used to resolve a deadlock between I/O and SQL
    threads, the SQL thread sets it to unblock the I/O thread and make it
    temporarily forget about the constraint.
  */
  ulonglong log_space_limit,log_space_total;
  bool ignore_log_space_limit;

  /*
    Used by the SQL thread to instructs the IO thread to rotate 
    the logs when the SQL thread needs to purge to release some
    disk space.
   */
  bool sql_force_rotate_relay;

  time_t last_master_timestamp;

  /**
    Reset the delay.
    This is used by RESET SLAVE to clear the delay.
  */
  void clear_sql_delay()
  {
    sql_delay= 0;
  }

  /*
    Needed for problems when slave stops and we want to restart it
    skipping one or more events in the master log that have caused
    errors, and have been manually applied by DBA already.
  */
  volatile uint32 slave_skip_counter;
  volatile ulong abort_pos_wait;	/* Incremented on change master */
  mysql_mutex_t log_space_lock;
  mysql_cond_t log_space_cond;

  /*
     Condition and its parameters from START SLAVE UNTIL clause.
     
     UNTIL condition is tested with is_until_satisfied() method that is
     called by exec_relay_log_event(). is_until_satisfied() caches the result
     of the comparison of log names because log names don't change very often;
     this cache is invalidated by parts of code which change log names with
     notify_*_log_name_updated() methods. (They need to be called only if SQL
     thread is running).
   */
  enum
  {
    UNTIL_NONE= 0,
    UNTIL_MASTER_POS,
    UNTIL_RELAY_POS,
    UNTIL_SQL_BEFORE_GTIDS,
    UNTIL_SQL_AFTER_GTIDS,
    UNTIL_SQL_AFTER_MTS_GAPS,
    UNTIL_SQL_VIEW_ID,
    UNTIL_DONE
  } until_condition;

  char cached_charset[6];

  /*
    trans_retries varies between 0 to slave_transaction_retries and counts how
    many times the slave has retried the present transaction; gets reset to 0
    when the transaction finally succeeds. retried_trans is a cumulative
    counter: how many times the slave has retried a transaction (any) since
    slave started.
  */
  ulong trans_retries, retried_trans;

  /*
    If the end of the hot relay log is made of master's events ignored by the
    slave I/O thread, these two keep track of the coords (in the master's
    binlog) of the last of these events seen by the slave I/O thread. If not,
    ign_master_log_name_end[0] == 0.
    As they are like a Rotate event read/written from/to the relay log, they
    are both protected by rli->relay_log.LOCK_binlog_end_pos.
  */
  char ign_master_log_name_end[FN_REFLEN];
  ulonglong ign_master_log_pos_end;

  /* 
    Indentifies where the SQL Thread should create temporary files for the
    LOAD DATA INFILE. This is used for security reasons.
   */ 
  char slave_patternload_file[FN_REFLEN]; 
  size_t slave_patternload_file_size;

  /**
    Identifies the last time a checkpoint routine has been executed.
  */
  struct timespec last_clock;

  /**
    Invalidates cached until_log_name and event_relay_log_name comparison
    result. Should be called after switch to next relay log if
    there chances that sql_thread is running.
  */
  inline void notify_relay_log_change()
  {
    if (until_condition == UNTIL_RELAY_POS)
      dynamic_cast<Until_position *>(until_option)->notify_log_name_change();
  }

  /**
    The same as @c notify_group_relay_log_name_update but for
    @c group_master_log_name.
  */
  inline void notify_group_master_log_name_update()
  {
    if (until_condition == UNTIL_MASTER_POS)
      dynamic_cast<Until_position *>(until_option)->notify_log_name_change();
  }

  inline void inc_event_relay_log_pos()
  {
    event_relay_log_pos= future_event_relay_log_pos;
  }

  /**
    Last executed event group coordinates are updated and optionally
    forcibly flushed to a repository.
    @param log_pos         a value of the executed position to update to
    @param need_data_lock  whether data_lock should be acquired
    @param force           the value is passed to eventual flush_info()
  */
  int inc_group_relay_log_pos(ulonglong log_pos,
                              bool need_data_lock,
                              bool force= false);

  int wait_for_pos(THD* thd, String* log_name, longlong log_pos,
                   double timeout);
  int wait_for_gtid_set(THD* thd, char* gtid, double timeout);
  int wait_for_gtid_set(THD* thd, String* gtid, double timeout);
  int wait_for_gtid_set(THD* thd, const Gtid_set* wait_gtid_set,
                        double timeout);

  void close_temporary_tables();

  RPL_TABLE_LIST *tables_to_lock;           /* RBR: Tables to lock  */
  uint tables_to_lock_count;        /* RBR: Count of tables to lock */
  table_mapping m_table_map;      /* RBR: Mapping table-id to table */
  /* RBR: Record Rows_query log event */
  Rows_query_log_event* rows_query_ev;

  bool get_table_data(TABLE *table_arg, table_def **tabledef_var, TABLE **conv_table_var) const
  {
    DBUG_ASSERT(tabledef_var && conv_table_var);
    for (TABLE_LIST *ptr= tables_to_lock ; ptr != NULL ; ptr= ptr->next_global)
      if (ptr->table == table_arg)
      {
        *tabledef_var= &static_cast<RPL_TABLE_LIST*>(ptr)->m_tabledef;
        *conv_table_var= static_cast<RPL_TABLE_LIST*>(ptr)->m_conv_table;
        DBUG_PRINT("debug", ("Fetching table data for table %s.%s:"
                             " tabledef: %p, conv_table: %p",
                             table_arg->s->db.str, table_arg->s->table_name.str,
                             *tabledef_var, *conv_table_var));
        return true;
      }
    return false;
  }

  /**
    Last charset (6 bytes) seen by slave SQL thread is cached here; it helps
    the thread save 3 @c get_charset() per @c Query_log_event if the charset is not
    changing from event to event (common situation).
    When the 6 bytes are equal to 0 is used to mean "cache is invalidated".
  */
  void cached_charset_invalidate();
  bool cached_charset_compare(char *charset) const;

  void cleanup_context(THD *, bool);
  void slave_close_thread_tables(THD *);
  void clear_tables_to_lock();
  int purge_relay_logs(THD *thd, bool just_reset, const char** errmsg,
                       bool delete_only= false);

  /*
    Used to defer stopping the SQL thread to give it a chance
    to finish up the current group of events.
    The timestamp is set and reset in @c sql_slave_killed().
  */
  time_t last_event_start_time;

  /* The original master commit timestamp in microseconds since epoch */
  uint64 original_commit_timestamp;

  /*
    A container to hold on Intvar-, Rand-, Uservar- log-events in case
    the slave is configured with table filtering rules.
    The withhold events are executed when their parent Query destiny is
    determined for execution as well.
  */
  Deferred_log_events *deferred_events;

  /*
    State of the container: true stands for IRU events gathering, 
    false does for execution, either deferred or direct.
  */
  bool deferred_events_collecting;

  /*****************************************************************************
    WL#5569 MTS

    legends:
    C  - Coordinator;
    W  - Worker;
    WQ - Worker Queue containing event assignments
  */
  // number's is determined by global slave_parallel_workers
  Slave_worker_array workers;

  HASH mapping_db_to_worker; // To map a database to a worker
  bool inited_hash_workers; //  flag to check if mapping_db_to_worker is inited

  mysql_mutex_t slave_worker_hash_lock; // for mapping_db_to_worker
  mysql_cond_t  slave_worker_hash_cond;// for mapping_db_to_worker

  /*
    For the purpose of reporting the worker status in performance schema table,
    we need to preserve the workers array after worker thread was killed. So, we
    copy this array into the below vector which is used for reporting
    until next init_workers(). Note that we only copy those attributes that
    would be useful in reporting worker status. We only use a few attributes in
    this object as of now but still save the whole object. The idea is
    to be future proof. We will extend performance schema tables in future
    and then we would use a good number of attributes from this object.
  */

  std::vector<Slave_worker*> workers_copy_pfs;

  /*
    This flag is turned ON when the workers array is initialized.
    Before destroying the workers array we check this flag to make sure
    we are not destroying an unitilized array. For the purpose of reporting the
    worker status in performance schema table, we need to preserve the workers
    array after worker thread was killed. So, we copy this array into
    workers_copy_pfs array which is used for reporting until next init_workers().
  */
  bool workers_array_initialized;

  volatile ulong pending_jobs;
  mysql_mutex_t pending_jobs_lock;
  mysql_cond_t pending_jobs_cond;
  mysql_mutex_t exit_count_lock; // mutex of worker exit count
  ulong       mts_slave_worker_queue_len_max;
  ulonglong   mts_pending_jobs_size;      // actual mem usage by WQ:s
  ulonglong   mts_pending_jobs_size_max;  // max of WQ:s size forcing C to wait
  bool    mts_wq_oversize;      // C raises flag to wait some memory's released
  Slave_worker  *last_assigned_worker;// is set to a Worker at assigning a group
  /*
    master-binlog ordered queue of Slave_job_group descriptors of groups
    that are under processing. The queue size is @c checkpoint_group.
  */
  Slave_committed_queue *gaq;
  /*
    Container for references of involved partitions for the current event group
  */
  // CGAP dynarray holds id:s of partitions of the Current being executed Group
  Prealloced_array<db_worker_hash_entry*, 4> curr_group_assigned_parts;
  // deferred array to hold partition-info-free events
  Prealloced_array<Slave_job_item, 8> curr_group_da;

  bool curr_group_seen_gtid;   // current group started with Gtid-event or not
  bool curr_group_seen_begin;   // current group started with B-event or not
  bool curr_group_isolated;     // current group requires execution in isolation
  bool mts_end_group_sets_max_dbs; // flag indicates if partitioning info is discovered
  volatile ulong mts_wq_underrun_w_id;  // Id of a Worker whose queue is getting empty
  /* 
     Ongoing excessive overrun counter to correspond to number of events that
     are being scheduled while a WQ is close to be filled up.
     `Close' is defined as (100 - mts_worker_underrun_level) %.
     The counter is incremented each time a WQ get filled over that level
     and decremented when the level drops below.
     The counter therefore describes level of saturation that Workers 
     are experiencing and is used as a parameter to compute a nap time for
     Coordinator in order to avoid reaching WQ limits.
  */
  volatile long mts_wq_excess_cnt;
  long  mts_worker_underrun_level; // % of WQ size at which W is considered hungry
  ulong mts_coordinator_basic_nap; // C sleeps to avoid WQs overrun
  ulong opt_slave_parallel_workers; // cache for ::opt_slave_parallel_workers
  ulong slave_parallel_workers; // the one slave session time number of workers
  ulong exit_counter; // Number of workers contributed to max updated group index
  ulonglong max_updated_index;
  ulong recovery_parallel_workers; // number of workers while recovering
  uint checkpoint_seqno;  // counter of groups executed after the most recent CP
  uint checkpoint_group;  // cache for ::opt_mts_checkpoint_group
  MY_BITMAP recovery_groups;  // bitmap used during recovery
  bool recovery_groups_inited;
  ulong mts_recovery_group_cnt; // number of groups to execute at recovery
  ulong mts_recovery_index;     // running index of recoverable groups
  bool mts_recovery_group_seen_begin;

  /*
    While distibuting events basing on their properties MTS
    Coordinator changes its mts group status.
    Transition normally flowws to follow `=>' arrows on the diagram:

            +----------------------------+
            V                            |
    MTS_NOT_IN_GROUP =>                  |
        {MTS_IN_GROUP => MTS_END_GROUP --+} while (!killed) => MTS_KILLED_GROUP

    MTS_END_GROUP has `->' loop breaking link to MTS_NOT_IN_GROUP when
    Coordinator synchronizes with Workers by demanding them to
    complete their assignments.
  */
  enum
  {
    /*
       no new events were scheduled after last synchronization,
       includes Single-Threaded-Slave case.
    */
    MTS_NOT_IN_GROUP,

    MTS_IN_GROUP,    /* at least one not-terminal event scheduled to a Worker */
    MTS_END_GROUP,   /* the last scheduled event is a terminal event */
    MTS_KILLED_GROUP /* Coordinator gave up to reach MTS_END_GROUP */
  } mts_group_status;

  /*
    MTS statistics:
  */
  ulonglong mts_events_assigned; // number of events (statements) scheduled
  ulonglong mts_groups_assigned; // number of groups (transactions) scheduled
  volatile ulong mts_wq_overrun_cnt; // counter of all mts_wq_excess_cnt increments
  ulong wq_size_waits_cnt;    // number of times C slept due to WQ:s oversize
  /*
    Counter of how many times Coordinator saw Workers are filled up
    "enough" with assignements. The enough definition depends on
    the scheduler type.
  */
  ulong mts_wq_no_underrun_cnt;
  std::atomic<longlong> mts_total_wait_overlap; // Waiting time corresponding to above
  /*
    Stats to compute Coordinator waiting time for any Worker available,
    applies solely to the Commit-clock scheduler.
  */
  ulonglong mts_total_wait_worker_avail;
  ulong mts_wq_overfill_cnt;  // counter of C waited due to a WQ queue was full
  /*
    Statistics (todo: replace with WL5631) applies to either Coordinator and Worker.
    The exec time in the Coordinator case means scheduling.
    The read time in the Worker case means getting an event out of Worker queue
  */
  ulonglong stats_exec_time;
  ulonglong stats_read_time;
  struct timespec ts_exec[2];  // per event pre- and post- exec timestamp
  struct timespec stats_begin; // applier's bootstrap time

  /*
     A sorted array of the Workers' current assignement numbers to provide
     approximate view on Workers loading.
     The first row of the least occupied Worker is queried at assigning
     a new partition. Is updated at checkpoint commit to the main RLI.
  */
  Prealloced_array<ulong, 16> least_occupied_workers;
  time_t mts_last_online_stat;
  /* end of MTS statistics */

  /**
    Storage for holding newly computed values for the last executed
    event group coordinates while the current group of events is
    being committed, see @c pre_commit, post_commit.
  */
  char new_group_master_log_name[FN_REFLEN];
  my_off_t new_group_master_log_pos;
  char new_group_relay_log_name [FN_REFLEN];
  my_off_t new_group_relay_log_pos;

  /* Returns the number of elements in workers array/vector. */
  inline size_t get_worker_count()
  {
    if (workers_array_initialized)
      return workers.size();
    else
      return workers_copy_pfs.size();
  }

  /*
    Returns a pointer to the worker instance at index n in workers
    array/vector.
  */
  Slave_worker* get_worker(size_t n)
  {
    if (workers_array_initialized)
    {
      if (n >= workers.size())
        return NULL;

      return workers[n];
    }
    else if (workers_copy_pfs.size())
    {
      if (n >= workers_copy_pfs.size())
        return NULL;

      return workers_copy_pfs[n];
    }
    else
      return NULL;
  }

  /**
    The method implements updating a slave info table. It's
    specialized differently for STS and MTS.
  */
  virtual bool commit_positions();

  /*Channel defined mts submode*/
  enum_mts_parallel_type channel_mts_submode;
  /* MTS submode  */
  Mts_submode* current_mts_submode;

  /* most of allocation in the coordinator rli is there */
  void init_workers(ulong);

  /* counterpart of the init */
  void deinit_workers();

  /**
     returns true if there is any gap-group of events to execute
                  at slave starting phase.
  */
  inline bool is_mts_recovery() const
  {
    return mts_recovery_group_cnt != 0;
  }

  inline void clear_mts_recovery_groups()
  {
    if (recovery_groups_inited)
    {
      bitmap_free(&recovery_groups);
      mts_recovery_group_cnt= 0;
      recovery_groups_inited= false;
    }
  }

  /**
     returns true if events are to be executed in parallel
  */
  inline bool is_parallel_exec() const
  {
    bool ret= (slave_parallel_workers > 0) && !is_mts_recovery();

    DBUG_ASSERT(!ret || !workers.empty());

    return ret;
  }

  /**
     returns true if Coordinator is scheduling events belonging to
     the same group and has not reached yet its terminal event.
  */
  inline bool is_mts_in_group()
  {
    return is_parallel_exec() &&
      mts_group_status == MTS_IN_GROUP;
  }

  /**
     While a group is executed by a Worker the relay log can change.
     Coordinator notifies Workers about this event. Worker is supposed
     to commit to the recovery table with the new info.
  */
  void reset_notified_relay_log_change();

  /**
     While a group is executed by a Worker the relay log can change.
     Coordinator notifies Workers about this event. Coordinator and Workers
     maintain a bitmap of executed group that is reset with a new checkpoint. 
  */
  void reset_notified_checkpoint(ulong, time_t, bool);

  /**
     Called when gaps execution is ended so it is crash-safe
     to reset the last session Workers info.
  */
  bool mts_finalize_recovery();
  /*
   * End of MTS section ******************************************************/

  /* The general cleanup that slave applier may need at the end of query. */
  inline void cleanup_after_query()
  {
    if (deferred_events)
      deferred_events->rewind();
  };
  /* The general cleanup that slave applier may need at the end of session. */
  void cleanup_after_session()
  {
    if (deferred_events)
      delete deferred_events;
  };
   
  /**
    Helper function to do after statement completion.

    This function is called from an event to complete the group by
    either stepping the group position, if the "statement" is not
    inside a transaction; or increase the event position, if the
    "statement" is inside a transaction.

    @param event_log_pos
    Master log position of the event. The position is recorded in the
    relay log info and used to produce information for <code>SHOW
    SLAVE STATUS</code>.
  */
  int stmt_done(my_off_t event_log_pos);


  /**
     Set the value of a replication state flag.

     @param flag Flag to set
   */
  void set_flag(enum_state_flag flag)
  {
    m_flags |= (1UL << flag);
  }

  /**
     Get the value of a replication state flag.

     @param flag Flag to get value of

     @return @c true if the flag was set, @c false otherwise.
   */
  bool get_flag(enum_state_flag flag)
  {
    return m_flags & (1UL << flag);
  }

  /**
     Clear the value of a replication state flag.

     @param flag Flag to clear
   */
  void clear_flag(enum_state_flag flag)
  {
    m_flags &= ~(1UL << flag);
  }

private:
  /**
    Auxiliary function used by is_in_group.

    The execute thread is in the middle of a statement in the
    following cases:
    - User_var/Intvar/Rand events have been processed, but the
      corresponding Query_log_event has not been processed.
    - Table_map or Row events have been processed, and the last Row
      event did not have the STMT_END_F set.

    @retval true Replication thread is inside a statement.
    @retval false Replication thread is not inside a statement.
   */
  bool is_in_stmt() const
  {
    bool ret= (m_flags & (1UL << IN_STMT));
    DBUG_PRINT("info", ("is_in_stmt()=%d", ret));
    return ret;
  }
  /**
    Auxiliary function used by is_in_group.

    @retval true The execute thread is inside a statement or a
    transaction, i.e., either a BEGIN has been executed or we are in
    the middle of a statement.
    @retval false The execute thread thread is not inside a statement
    or a transaction.
  */
  bool is_in_trx_or_stmt() const
  {
    bool ret= is_in_stmt() || (info_thd->variables.option_bits & OPTION_BEGIN);
    DBUG_PRINT("info", ("is_in_trx_or_stmt()=%d", ret));
    return ret;
  }
public:
  /**
    A group is defined as the entire range of events that constitute
    a transaction or auto-committed statement. It has one of the
    following forms:

    (Gtid)? Query(BEGIN) ... (Query(COMMIT) | Query(ROLLBACK) | Xid)
    (Gtid)? (Rand | User_var | Int_var)* Query(DDL)

    Thus, to check if the execute thread is in a group, there are
    two cases:

    - If the master generates Gtid events (5.7.5 or later, or 5.6 or
      later with GTID_MODE=ON), then is_in_group is the same as
      info_thd->owned_gtid.sidno != 0, since owned_gtid.sidno is set
      to non-zero by the Gtid_log_event and cleared to zero at commit
      or rollback.

    - If the master does not generate Gtid events (i.e., master is
      pre-5.6, or pre-5.7.5 with GTID_MODE=OFF), then is_in_group is
      the same as is_in_trx_or_stmt().

    @retval true Replication thread is inside a group.
    @retval false Replication thread is not inside a group.
  */
  bool is_in_group() const
  {
    bool ret= is_in_trx_or_stmt() || info_thd->owned_gtid.sidno != 0;
    DBUG_PRINT("info", ("is_in_group()=%d", ret));
    return ret;
  }

  int count_relay_log_space();

  int rli_init_info();
  void end_info();
  int flush_info(bool force= FALSE);
  int flush_current_log();
  void set_master_info(Master_info *info);

  inline ulonglong get_future_event_relay_log_pos() { return future_event_relay_log_pos; }
  inline void set_future_event_relay_log_pos(ulonglong log_pos)
  {
    future_event_relay_log_pos= log_pos;
  }

  inline const char* get_group_master_log_name() { return group_master_log_name; }
  inline ulonglong get_group_master_log_pos() { return group_master_log_pos; }
  inline void set_group_master_log_name(const char *log_file_name)
  {
     strmake(group_master_log_name,log_file_name, sizeof(group_master_log_name)-1);
  }
  inline void set_group_master_log_pos(ulonglong log_pos)
  {
    group_master_log_pos= log_pos;
  }

  inline const char* get_group_relay_log_name() { return group_relay_log_name; }
  inline ulonglong get_group_relay_log_pos() { return group_relay_log_pos; }
  inline void set_group_relay_log_name(const char *log_file_name)
  {
     strmake(group_relay_log_name,log_file_name, sizeof(group_relay_log_name)-1);
  }
  inline void set_group_relay_log_name(const char *log_file_name, size_t len)
  {
     strmake(group_relay_log_name, log_file_name, len);
  }
  inline void set_group_relay_log_pos(ulonglong log_pos)
  {
    group_relay_log_pos= log_pos;
  }

  inline const char* get_event_relay_log_name() { return event_relay_log_name; }
  inline ulonglong get_event_relay_log_pos() { return event_relay_log_pos; }
  inline void set_event_relay_log_name(const char *log_file_name)
  {
    strmake(event_relay_log_name,log_file_name, sizeof(event_relay_log_name)-1);
    set_event_relay_log_number(relay_log_name_to_number(log_file_name));
    notify_relay_log_change();
  }

  uint get_event_relay_log_number() { return event_relay_log_number; }
  void set_event_relay_log_number(uint number)
  {
    event_relay_log_number= number;
  }

  /**
    Given the extension number of the relay log, gets the full
    relay log path. Currently used in Slave_worker::retry_transaction()

    @param [in]   number      extension number of relay log
    @param[in, out] name      The full path of the relay log (per-channel)
                              to be read by the slave worker.
  */
  void relay_log_number_to_name(uint number, char name[FN_REFLEN+1]);
  uint relay_log_name_to_number(const char *name);

  void set_event_start_pos(my_off_t pos) { event_start_pos= pos; }
  my_off_t get_event_start_pos() { return event_start_pos; }

  inline void set_event_relay_log_pos(ulonglong log_pos)
  {
    event_relay_log_pos= log_pos;
  }
  inline const char* get_rpl_log_name()
  {
    return (group_master_log_name[0] ? group_master_log_name : "FIRST");
  }

  static size_t get_number_info_rli_fields();

  /**
    Indicate that a delay starts.

    This does not actually sleep; it only sets the state of this
    Relay_log_info object to delaying so that the correct state can be
    reported by SHOW SLAVE STATUS and SHOW PROCESSLIST.

    Requires rli->data_lock.

    @param delay_end The time when the delay shall end.
  */
  void start_sql_delay(time_t delay_end);

  /* Note that this is cast to uint32 in show_slave_status(). */
  time_t get_sql_delay() { return sql_delay; }
  void set_sql_delay(time_t _sql_delay) { sql_delay= _sql_delay; }
  time_t get_sql_delay_end() { return sql_delay_end; }

  Relay_log_info(bool is_slave_recovery
#ifdef HAVE_PSI_INTERFACE
                 ,PSI_mutex_key *param_key_info_run_lock,
                 PSI_mutex_key *param_key_info_data_lock,
                 PSI_mutex_key *param_key_info_sleep_lock,
                 PSI_mutex_key *param_key_info_thd_lock,
                 PSI_mutex_key *param_key_info_data_cond,
                 PSI_mutex_key *param_key_info_start_cond,
                 PSI_mutex_key *param_key_info_stop_cond,
                 PSI_mutex_key *param_key_info_sleep_cond
#endif
                 , uint param_id, const char* param_channel, bool is_rli_fake
                );
  virtual ~Relay_log_info();

  /*
    Determines if a warning message on unsafe execution was
    already printed out to avoid clutering the error log
    with several warning messages.
  */
  bool reported_unsafe_warning;

  /*
    'sql_thread_kill_accepted is set to TRUE when killed status is recognized.
  */
  bool sql_thread_kill_accepted;

  time_t get_row_stmt_start_timestamp()
  {
    return row_stmt_start_timestamp;
  }

  time_t set_row_stmt_start_timestamp()
  {
    if (row_stmt_start_timestamp == 0)
      row_stmt_start_timestamp= my_time(0);

    return row_stmt_start_timestamp;
  }

  void reset_row_stmt_start_timestamp()
  {
    row_stmt_start_timestamp= 0;
  }

  void set_long_find_row_note_printed()
  {
    long_find_row_note_printed= true;
  }

  void unset_long_find_row_note_printed()
  {
    long_find_row_note_printed= false;
  }

  bool is_long_find_row_note_printed()
  {
    return long_find_row_note_printed;
  }

public:
  /**
    Delete the existing event and set a new one.  This class is
    responsible for freeing the event, the caller should not do that.
  */
  virtual void set_rli_description_event(Format_description_log_event *fdle);

  /**
    Return the current Format_description_log_event.
  */
  Format_description_log_event *get_rli_description_event() const
  {
    return rli_description_event;
  }

  /**
    adaptation for the slave applier to specific master versions.
  */
  ulong adapt_to_master_version(Format_description_log_event *fdle);
  ulong adapt_to_master_version_updown(ulong master_version,
                                       ulong current_version);
  uchar slave_version_split[3]; // bytes of the slave server version
  /*
    relay log info repository should be updated on relay log
    rotate. But when the transaction is split across two relay logs,
    update the repository will cause unexpected results and should
    be postponed till the 'commit' of the transaction is executed.

    A flag that set to 'true' when this type of 'forced flush'(at the
    time of rotate relay log) is postponed due to transaction split
    across the relay logs.
  */
  bool force_flush_postponed_due_to_split_trans;

  Commit_order_manager *get_commit_order_manager()
  {
    return commit_order_mngr;
  }

  void set_commit_order_manager(Commit_order_manager *mngr)
  {
    commit_order_mngr= mngr;
  }

  /*
    Following set function is required to initialize the 'until_option' during
    MTS relay log recovery process.

    Ideally initialization of 'until_option' is done through rli::init_until_option.
    This init_until_option requires the main server thread object and it makes
    use of the thd->lex->mi object to initialize the 'until_option'.

    But MTS relay log recovery process happens before the main server comes
    up at this time the THD object will not be available. Hence the following
    set function does the initialization of 'until_option'.
  */
  void set_until_option(Until_option *option)
  {
    mysql_mutex_lock(&data_lock);
    until_option= option;
    mysql_mutex_unlock(&data_lock);
  }

  void clear_until_option()
  {
    mysql_mutex_lock(&data_lock);
    if (until_option)
    {
      delete until_option;
      until_option= NULL;
    }
    mysql_mutex_unlock(&data_lock);
  }

  bool set_info_search_keys(Rpl_info_handler *to);

  /**
    Get coordinator's RLI. Especially used get the rli from
    a slave thread, like this: thd->rli_slave->get_c_rli();
    thd could be a SQL thread or a worker thread
  */
  virtual Relay_log_info* get_c_rli()
  {
    return this;
  }

  virtual const char* get_for_channel_str(bool upper_case= false) const;

  /**
    Set replication filter for the channel.
  */
  inline void set_filter(Rpl_filter *channel_filter)
  {
    rpl_filter= channel_filter;
  }

protected:
  Format_description_log_event *rli_description_event;

private:
  /*
    Commit order manager to order commits made by its workers. In context of
    Multi Source Replication each worker will be ordered by the coresponding
    corrdinator's order manager.
   */
  Commit_order_manager* commit_order_mngr;

  /**
    Delay slave SQL thread by this amount of seconds.
    The delay is applied per transaction and based on the immediate master's
    commit time. Exceptionally, if a server in the replication chain does not
    support the commit timestamps in Gtid_log_event, the delay is applied per
    event and is based on the event timestamp.
    This is set with CHANGE MASTER TO MASTER_DELAY=X.

    Guarded by data_lock.  Initialized by the client thread executing
    START SLAVE.  Written by client threads executing CHANGE MASTER TO
    MASTER_DELAY=X.  Read by SQL thread and by client threads
    executing SHOW SLAVE STATUS.  Note: must not be written while the
    slave SQL thread is running, since the SQL thread reads it without
    a lock when executing flush_info().
  */
  time_t sql_delay;

  /**
    During a delay, specifies the point in time when the delay ends.

    This is used for the SQL_Remaining_Delay column in SHOW SLAVE STATUS.

    Guarded by data_lock. Written by the sql thread.  Read by client
    threads executing SHOW SLAVE STATUS.
  */
  time_t sql_delay_end;

  uint32 m_flags;

  /*
    Before the MASTER_DELAY parameter was added (WL#344), relay_log.info
    had 4 lines. Now it has 5 lines.
  */
  static const int LINES_IN_RELAY_LOG_INFO_WITH_DELAY= 5;

  /*
    Before the WL#5599, relay_log.info had 5 lines. Now it has 6 lines.
  */
  static const int LINES_IN_RELAY_LOG_INFO_WITH_WORKERS= 6;

  /*
    Before the Id was added (BUG#2334346), relay_log.info
    had 6 lines. Now it has 7 lines.
  */
  static const int LINES_IN_RELAY_LOG_INFO_WITH_ID= 7;

  /*
    Add a channel in the slave relay log info
  */
  static const int LINES_IN_RELAY_LOG_INFO_WITH_CHANNEL= 8;

  bool read_info(Rpl_info_handler *from);
  bool write_info(Rpl_info_handler *to);

  Relay_log_info(const Relay_log_info& info);
  Relay_log_info& operator=(const Relay_log_info& info);

  /*
    Runtime state for printing a note when slave is taking
    too long while processing a row event.
   */
  time_t row_stmt_start_timestamp;
  bool long_find_row_note_printed;


 /**
   sets the suffix required for relay log names
   in multisource replication.
   The extension is "-relay-bin-<channel_name>"
   @param[in, out]  buff       buffer to store the complete relay log file name
   @param[in]       buff_size  size of buffer buff
   @param[in]       base_name  the base name of the relay log file
 */
  const char* add_channel_to_relay_log_name(char *buff, uint buff_size,
                                            const char *base_name);

  /*
    Applier thread InnoDB priority.
    When two transactions conflict inside InnoDB, the one with
    greater priority wins.
    Priority must be set before applier thread start so that all
    executed transactions have the same priority.
  */
  int thd_tx_priority;

  /* The object stores and handles START SLAVE UNTIL option */
  Until_option *until_option;
public:
  /*
    The boolean is set to true when the binlog (rli_fake) or slave
    (rli_slave) applier thread detaches any engine ha_data
    it has dealt with at time of XA START processing.
    The boolean is reset to false at the end of XA PREPARE,
    XA COMMIT ONE PHASE for the binlog applier, and
    at internal rollback of the slave applier at the same time with
    the engine ha_data re-attachment.
  */
  bool is_engine_ha_data_detached;
  /**
    Reference to being applied event. The member is set at event reading
    and gets reset at the end of the event lifetime.
    See more in @c RLI_current_event_raii that provides the main
    interface to the member.
  */
  Log_event *current_event;

  /**
    Raised when slave applies and writes to its binary log statement
    which is not atomic DDL and has no XID assigned. Checked at commit
    time to decide whether it is safe to update slave info table
    within the same transaction as the write to binary log or this
    should be deffered. The deffered scenario applies for not XIDed events
    in which case such update might be lost on recovery.
  */
  bool ddl_not_atomic;

  void set_thd_tx_priority(int priority)
  {
    thd_tx_priority= priority;
  }

  int get_thd_tx_priority()
  {
    return thd_tx_priority;
  }

  const char *get_until_log_name();
  my_off_t get_until_log_pos();
  bool is_until_satisfied_at_start_slave()
  {
    return until_option != NULL && until_option->is_satisfied_at_start_slave();
  }
  bool is_until_satisfied_before_dispatching_event(const Log_event *ev)
  {
    return until_option != NULL &&
      until_option->is_satisfied_before_dispatching_event(ev);
  }
  bool is_until_satisfied_after_dispatching_event()
  {
    return until_option != NULL &&
      until_option->is_satisfied_after_dispatching_event();
  }
  /**
   Intialize until option object when starting slave.

   @param[in] thd The thread object of current session.
   @param[in] master_param the parameters of START SLAVE.

   @return int
     @retval 0      Succeeds to initialize until option object.
     @retval <> 0   A defined error number is return if any error happens.
 */
  int init_until_option(THD *thd, const LEX_MASTER_INFO* master_param);

  /**
    Detaches the engine ha_data from THD. The fact
    is memorized in @c is_engine_ha_detached flag.

    @param  thd a reference to THD
  */

  void detach_engine_ha_data(THD *thd);
  /**
    Drops the engine ha_data flag when it is up.
    The method is run at execution points of the engine ha_data
    re-attachment.

    @return true   when THD has detached the engine ha_data,
            false  otherwise
  */

  bool unflag_detached_engine_ha_data()
  {
    bool rc= false;

    if (is_engine_ha_data_detached)
      rc= !(is_engine_ha_data_detached= false); // return the old value

    return rc;
  }

  /**
    Execute actions at replicated atomic DLL post rollback time.
    This include marking the current atomic DDL query-log-event
    as having processed.
    This measure is necessary to avoid slave info table update execution
    when @c pre_commit() hook is called as part of DDL's eventual
    implicit commit.
  */
  void post_rollback()
  {
    static_cast<Query_log_event*>(current_event)->has_ddl_committed= true;
  }

  /**
    The method implements a pre-commit hook to add up a new statement
    typically to a DDL transaction to update the slave info table.
    Note, in the non-transactional repository case the slave info
    is updated after successful commit of the main transaction.

    @return false as success, otherwise true
  */
  bool pre_commit()
  {
    bool rc= false;

    if (is_transactional())
    {
      static_cast<Query_log_event*>(current_event)->has_ddl_committed= true;
      rc= commit_positions();
    }
    return rc;
  }
  /**
    Cleanup of any side effect that pre_commit() inflicts, including
    restore of the last executed group coordinates in case the current group
    has been destined to rollback, and signaling to possible waiters
    in the positive case.

    @param on_rollback  when true the method carries out rollback action
  */
  virtual void post_commit(bool on_rollback);
};

bool mysql_show_relaylog_events(THD* thd);


/**
   @param  thd a reference to THD
   @return TRUE if thd belongs to a Worker thread and FALSE otherwise.
*/
inline bool is_mts_worker(const THD *thd)
{
  return thd->system_thread == SYSTEM_THREAD_SLAVE_WORKER;
}


/**
 Auxiliary function to check if we have a db partitioned MTS
 */
bool is_mts_db_partitioned(Relay_log_info * rli);

/**
  Checks whether the supplied event encodes a (2pc-aware) DDL
  that has been already committed.

  @param  ev    A reference to Query-log-event
  @return true  when the event is already committed transactional DDL
*/
inline bool is_committed_ddl(Log_event *ev)
{
  return
    ev->get_type_code() == binary_log::QUERY_EVENT &&
    /* has been already committed */
    static_cast<Query_log_event*>(ev)->has_ddl_committed;
}

/**
  Checks whether the transaction identified by the argument
  is executed by a slave applier thread is an atomic DDL
  not yet committed (see @c Query_log_event::has_ddl_committed).
  THD::is_operating_substatement_implicitly filters out intermediate
  commits done by non-atomic DDLs.
  The error-tagged atomic statements are regarded as non-atomic
  therefore this predicate returns negative in such case.

  Note that call to is_atomic_ddl() returns "approximate" outcome in
  this case as it misses information about type of tables used by the DDL.

  This can be a problem for binlogging slave, as updates to slave info
  which happen in the same transaction as write of binary log event
  without XID might be lost on recovery. To avoid this problem
  RLI::ddl_not_atomic flag is employed which is set to true when
  non-atomic DDL without XID is written to the binary log.

  "Approximate" outcome is always fine for non-binlogging slave as in
  this case commit happens using one-phase routine for which recovery
  is always correct.

  @param  thd   a pointer to THD describing the transaction context
  @return true  when a slave applier thread is set to commmit being processed
                DDL query-log-event, otherwise returns false.
*/
inline bool is_atomic_ddl_commit_on_slave(THD *thd)
{
  DBUG_ASSERT(thd);

  Relay_log_info *rli= thd->rli_slave;

  /* Early return is about an error in the SQL thread initialization */
  if (!rli)
    return false;

  return ((thd->system_thread == SYSTEM_THREAD_SLAVE_SQL ||
           thd->system_thread == SYSTEM_THREAD_SLAVE_WORKER) &&
          rli->current_event) ?
    (rli->is_transactional() &&
     /* has not yet committed */
     (rli->current_event->get_type_code() == binary_log::QUERY_EVENT &&
      !static_cast<Query_log_event*>(rli->current_event)->has_ddl_committed) &&
     /* unless slave binlogger identified non-atomic */
     !rli->ddl_not_atomic &&
     /* slave info is not updated when a part of multi-DROP-TABLE commits */
     !thd->is_commit_in_middle_of_statement &&
     (is_atomic_ddl(thd, true) && !thd->is_operating_substatement_implicitly) &&
     /* error-tagged atomic DDL do not update yet slave info */
     static_cast<Query_log_event*>(rli->current_event)->error_code == 0) :
    false;
}


/**
  RAII class to control the slave applier execution context binding
  with a being handled event. The main object of control is Query-log-event
  containing DDL statement.
  The member RLI::current_event is set to refer to an event once it is
  read, e.g by next_event() and is reset to NULL at exiting a
  read-exec loop. Once the event is destroyed RLI::current_event must be reset
  or guaranteed not be accessed anymore.
  In the MTS execution the worker is reliably associated with an event
  only with the latter is not deferred. This includes Query-log-event.
*/
class RLI_current_event_raii
{
  Relay_log_info *m_rli;

public:
  RLI_current_event_raii(Relay_log_info* rli_arg, Log_event *ev) : m_rli(rli_arg)
  {
    m_rli->current_event= ev;
  }
  void set_current_event(Log_event *ev) { m_rli->current_event= ev; }
  ~RLI_current_event_raii() { m_rli->current_event= NULL; }
};


#endif /* RPL_RLI_H */
