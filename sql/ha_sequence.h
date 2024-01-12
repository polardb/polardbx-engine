/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyStore hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyStore.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef HA_SEQUENCE_INCLUDED
#define HA_SEQUENCE_INCLUDED

#include "my_bitmap.h"  // MY_BITMAP
#include "sql/sql_class.h"

#include "sql/sequence_common.h"

class THD;

extern bool opt_only_report_warning_when_skip_sequence;

/* Global sequence engine handlerton variable, inited when plugin_register */
extern handlerton *sequence_hton;

/* Define the field */
#define SF_CURRVAL Sequence_field::FIELD_NUM_CURRVAL
#define SF_NEXTVAL Sequence_field::FIELD_NUM_NEXTVAL
#define SF_MINVALUE Sequence_field::FIELD_NUM_MINVALUE
#define SF_MAXVALUE Sequence_field::FIELD_NUM_MAXVALUE
#define SF_START Sequence_field::FIELD_NUM_START
#define SF_INCREMENT Sequence_field::FIELD_NUM_INCREMENT
#define SF_CACHE Sequence_field::FIELD_NUM_CACHE
#define SF_CYCLE Sequence_field::FIELD_NUM_CYCLE
#define SF_ROUND Sequence_field::FIELD_NUM_ROUND
#define SF_END Sequence_field::FIELD_NUM_END

/** How many rounds then put into sleep */
#define TIMESTAMP_SEQUENCE_ROUND_SLEEP 100
/** Sleep time (milliseconds) */
#define TIMESTAMP_SEQUENCE_SLEEP_TIME 10
/** How many rounds then failed */
#define TIMESTAMP_SEQUENCE_MAX_ROUND 100000

/* Min number of timestamp value that could be reserved in one request */
#define TIMESTAMP_SEQUENCE_MIN_BATCH_SIZE 1
/* Max number of timestamp value that could be reserved in one request */
#define TIMESTAMP_SEQUENCE_MAX_BATCH_SIZE 60000

/*
  Request context

  It's mainly used to tranfer batch size,
  and decide whether nextval to inherit when calc_number_next_round;
*/
class Sequence_request_context {
 public:
  explicit Sequence_request_context(ulonglong size, Sequence_skip skip,
                                    Sequence_operation operation)
      : m_skip(skip),
        m_inherit(false),
        m_nextval(0),
        m_batch(size),
        m_first_skip(false),
        m_operation(operation) {}

  void inherit(ulonglong size) {
    assert(m_inherit == false);
    m_inherit = true;
    m_nextval = size;
  }

  void init(ulonglong size, Sequence_skip skip, Sequence_operation operation) {
    m_skip = skip;
    m_inherit = false;
    m_nextval = 0;
    m_batch = size;
    m_first_skip = false;
    m_operation = operation;
  }

  bool is_inherit() { return m_inherit; }

  ulonglong get_inherit_value() {
    assert(m_inherit);
    return m_nextval;
  }

  Sequence_skip *get_skip_ptr() { return &m_skip; }

  ulonglong batch() { return m_batch; }
  void set_batch(ulonglong batch) { m_batch = batch; }

  void clear_skip() { m_skip.reset(); }

  bool is_first_skip() { return m_first_skip; }
  void set_first_skip() { m_first_skip = true; }

  Sequence_operation get_operation() { return m_operation; }
  void clear_operation() { m_operation.reset(); }

 private:
  Sequence_skip m_skip;
  bool m_inherit;
  ulonglong m_nextval;
  ulonglong m_batch;

  bool m_first_skip;

  Sequence_operation m_operation;
};

typedef class Sequence_request_context SR_ctx;

/**
  The sequence caches class definition, that's allowed to be accessed
  simultaneously while protected by mutex.
*/
class Sequence_share {
 public:
  /**
    Cache data state.

     1) Retrieve the data from cache if cache is valid.
     2) Need to reload the data from base table if cache is invalid.
     3) Loading represent that some thread is loading data, others should wait.
  */
  enum Cache_state {
    CACHE_STATE_INVALID,
    CACHE_STATE_VALID,
    CACHE_STATE_LOADING
  };

  /**
    Cache request result.

     1) Fill data from cache if cache hit
     2) Reload data if cache has run out
     3) Report error if cache has run out and DEF didn't support cycle.
     4) System error.
     5) Retry timeout.
  */
  enum Cache_request {
    CACHE_REQUEST_HIT,
    CACHE_REQUEST_NEED_LOAD,
    CACHE_REQUEST_ROUND_OUT,
    CACHE_REQUEST_ERROR,
    CACHE_REQUEST_RETRY_TIMEOUT,
    CACHE_REQUEST_SKIP_ERROR
  };

  Sequence_share() {}

  ~Sequence_share() {
    DBUG_ENTER("~Sequence_share");
    assert(m_ref_count == 0);
    mysql_mutex_destroy(&m_mutex);
    mysql_cond_destroy(&m_cond);
    if (m_name) {
      my_free(const_cast<char *>(m_name));
      m_name = NULL;
    }
    bitmap_free(&m_read_set);
    bitmap_free(&m_write_set);
    m_initialized = false;
    DBUG_VOID_RETURN;
  }
  /**
    Init all the member variable.

    @param[in]      table_name      db_name + table_name

    @retval         void
  */
  void init(const char *table_name);

  /**
    Get sequence share cache field value pointer

    @param[in]      field_num     The sequence field number

    @retval         field pointer
  */
  ulonglong *get_field_ptr(const Sequence_field field_num);

  Sequence_type get_type() { return m_type; }

  /**
    Reload the sequence value cache.

    @param[in]      table         TABLE object
    @param[out]     changed       Whether values are changed

    @retval         0             Success
    @retval         ~0            Failure
  */
  int reload_cache(TABLE *table, bool *changed, SR_ctx *sr_ctx);
  int calc_timestamp_next_round(ulonglong *durable);
  int calc_digital_next_round(ulonglong *durable, SR_ctx *sr_ctx);

  /**
    Retrieve the nextval from cache directly.

    @param[out]     local_values    Used to store into thd->sequence_last_value
    @param[in]      batch           Number of value requested

    @retval         request         Cache request result
  */
  Cache_request quick_read(ulonglong *local_values, SR_ctx *sr_ctx);
  Cache_request digital_quick_read(ulonglong *local_values, SR_ctx *sr_ctx);
  Cache_request digital_skip_read(ulonglong *local_values, SR_ctx *sr_ctx);
  Cache_request timestamp_quick_read(ulonglong *local_values, SR_ctx *sr_ctx);

  /**
     Show the next value store in cache. It will reload cache if
    current cache has run out.

    Show cache will set local_values to 0 if the sequence has
    run out

    @param[out]     local_values           local value array
    @param[in]       sr_ctx                   sequence request context
    @param[in]       is_run_out             sequence has run out

    @retval            cache request result
  */
  Cache_request show_cache(ulonglong *local_values, SR_ctx *sr_ctx);

  /**
     handle some specific errors:
    1. skip_sequence will raise no error if
        opt_only_report_warning_when_skip_sequence is setted
    2. HA_ERR_SEQUENCE_SKIP_ERROR will not invalidate cache

    3. other errors will make cache invalidate and return input error

    @param[in]       error                     error no

    @retval            error                      error no
  */
  int handle_specific_error(int error, ulonglong *local_values);

  /**
    Validate cache.
  */
  void validate() {
    mysql_mutex_assert_owner(&m_mutex);
    m_cache_state = CACHE_STATE_VALID;
    mysql_cond_broadcast(&m_cond);
  }
  /**
    Invalidate cache.
  */
  void invalidate() {
    mysql_mutex_assert_owner(&m_mutex);
    m_cache_state = CACHE_STATE_INVALID;
    mysql_cond_broadcast(&m_cond);
  }

  /* Broadcast the condition if loading completed or updating happened. */
  void set_state(Cache_state state) {
    mysql_mutex_assert_owner(&m_mutex);
    m_cache_state = state;
    if (m_cache_state == CACHE_STATE_INVALID ||
        m_cache_state == CACHE_STATE_VALID)
      mysql_cond_broadcast(&m_cond);
  }
  /**
    Enter the wait condition until loading complete or error happened.
    @param[in]     thd           User connection

    @retval        0             Success
    @retval        ~0            Failure
  */
  int enter_cond(THD *thd);
  /**
    In order to invalid the THD sequence when sequence is dropped
    or altered
  */
  ulonglong m_version;

  mysql_mutex_t m_mutex;
  mysql_cond_t m_cond;

  /* Protected by m_mutex */
  Cache_state m_cache_state;

  /* Only changed when get_share or close_share,  so didn't need m_mutex */
  uint m_ref_count;
  bool m_initialized;

  /* All setted read/write set. */
  MY_BITMAP m_read_set;
  MY_BITMAP m_write_set;

  /* db_name + table_name */
  const char *m_name;

 private:
  /* Protected by m_mutex */
  ulonglong m_caches[Sequence_field::FIELD_NUM_END];
  ulonglong m_cache_end;

  Sequence_type m_type;
  /** nextval for last time */
  ulonglong m_last_time;
  /** increment counter if timestamp equal with last time */
  ulonglong m_counter;

  /** time nextval must be >= last persisted nextval */
  ulonglong m_low_limit;
};

typedef Sequence_share::Cache_state Sequence_cache_state;
typedef Sequence_share::Cache_request Sequence_cache_request;

/**
  Disable binlog generation helper class
*/
class Disable_binlog_helper {
 public:
  explicit Disable_binlog_helper(THD *thd) : m_thd(thd) {
    m_saved_options = m_thd->variables.option_bits;
    m_thd->variables.option_bits &= ~OPTION_BIN_LOG;
  }

  ~Disable_binlog_helper() { m_thd->variables.option_bits = m_saved_options; }

 private:
  THD *m_thd;
  ulonglong m_saved_options;
};
/**
  Sequence engine handler

  @Note
    Sequence engine is only logical engine, which didn't store any real data.
    The sequence values are stored into the based-table whose engine is InnoDB.

  @Rules
    Sequence_share is used to cache values that's consistent with sequence
    defined:

    1. If hit cache, it can query back sequence nextval directly instead of
       scanning base-table.
    2. When run out of the caches, sequence engine will lanuch autonomous
       transaction to update base-table, and get the new value.
    3. Invalid the caches if any update on base-table.
*/
class ha_sequence : public handler {
 public:
  /**
    Sequence share object mutex helper class
  */
  class Share_locker_helper {
   public:
    explicit Share_locker_helper(Sequence_share *share) : mm_share(share) {
      mysql_mutex_lock(&mm_share->m_mutex);
      m_hold_mutex = true;
    }

    ~Share_locker_helper() {
      if (m_hold_mutex) mysql_mutex_unlock(&mm_share->m_mutex);
    }

    void release() {
      assert(m_hold_mutex);
      mysql_mutex_unlock(&mm_share->m_mutex);
      m_hold_mutex = false;
    }

    void loading() {
      assert(m_hold_mutex);
      mm_share->set_state(Sequence_cache_state::CACHE_STATE_LOADING);
      release();
    }

    void complete_load(int error) {
      assert(!m_hold_mutex);
      lock();
      if (error)
        mm_share->invalidate();
      else
        mm_share->validate();
    }

    void lock() {
      assert(!m_hold_mutex);
      mysql_mutex_lock(&mm_share->m_mutex);
      m_hold_mutex = true;
    }

   private:
    Sequence_share *mm_share;
    bool m_hold_mutex;
  };

  /**
    TABLE read/write bitmap set helper, since maybe update while query nextval.
  */
  class Bitmap_helper {
   public:
    explicit Bitmap_helper(TABLE *table, Sequence_share *share);

    ~Bitmap_helper();

   private:
    TABLE *m_table;
    MY_BITMAP *save_read_set;
    MY_BITMAP *save_write_set;
  };

  ha_sequence(handlerton *hton, TABLE_SHARE *share);

  /* Init handler when CREATE SEQUENCE */
  ha_sequence(handlerton *hton, Sequence_info *info);

  /**
    Initialize sequence handler

    @param[in]    mem_root    memory space

    @retval       false       success
    @retval       true        failure
  */
  bool initialize_sequence(MEM_ROOT *mem_root);

  /**
    Initialize the sequence handler member variable.
  */
  void init_variables();

  /**
     Sequence base table engine setup.
  */
  bool setup_base_engine();

  /**
    Create the base table handler by m_engine.

    @param[in]      mem_root        Memory space

    @retval         false           Success
    @retval         true            Failure
  */
  bool setup_base_handler(MEM_ROOT *mem_root);

  /**
    Clear the locked sequence base table engine and destroy file handler
  */
  void clear_base_handler_file();

  /**
    Setup the sequence base table engine and base file handler.

    @param[in]    name        Sequence table name
    @param[in]    mem_root    Memory space

    @retval       false       success
    @retval       true        failure
  */
  bool get_from_handler_file(const char *, MEM_ROOT *mem_root);

  /**
    Init the sequence base table engine handler by sequence info

    @param[in]    mem_root    memory space

    @retval       false       success
    @retval       true        failure
  */
  bool new_handler_from_sequence_info(MEM_ROOT *mem_root);

  /**
    Unlock the base storage plugin and destroy the handler
  */
  ~ha_sequence() override;

  int rnd_init(bool scan) override;
  int rnd_next(uchar *buf) override;
  int rnd_end() override;
  int rnd_pos(uchar *buf, uchar *pos) override;
  void position(const uchar *record) override;

  /** Store lock */
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override;

  /**
    Open the sequence table, release the resource in ~ha_sequence if any error
    happened.

    @param[in]      name            Sequence table name.
    @param[in]      mode
    @param[in]      test_if_locked
    @param[in]      table_def       DD table definition


    @retval         0               Success
    @retval         ~0              Failure
  */
  int open(const char *name, int mode, uint test_if_locked,
           const dd::Table *) override;

  /**
    Close sequence handler.
    We didn't destroy share although the ref_count == 0,
    the cached values will be lost if we do that.

    @retval         0               Success
    @retval         ~0              Failure
  */
  int close(void) override;

  /** Inherit base table handler function implementation */
  Table_flags table_flags() const override;
  int info(uint) override;
  const char *table_type() const override;
  ulong index_flags(uint inx, uint part, bool all_parts) const override;

  void update_create_info(HA_CREATE_INFO *create_info) override;

  /**
    Add hidden columns and indexes to an InnoDB table definition.

    @param[in,out]	dd_table	      data dictionary cache object

    @retval         error number
    @retval         0               success
  */
  int get_extra_columns_and_keys(const HA_CREATE_INFO *create_info,
                                 const List<Create_field> *create_list,
                                 const KEY *key_info, uint key_count,
                                 dd::Table *dd_table) override;
  /**
    Create sequence table.

    @param[in]      name            Sequence table name.
    @param[in]      form            TABLE object
    @param[in]      create_info     create options
    @param[in]      table_def       dd::Table object that has been created

    @retval         0               success
    @retval         ~0              failure
  */
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info,
             dd::Table *table_def) override;
  /**
    Sequence engine special file extension

    @retval     String array      File extension array
  */
  const char **bas_ext() const;

  /**
    Drop sequence table object

    @param[in]    name        Sequence table name
    @param[in]    table_def   Table DD object

    @retval       0           Success
    @retval       ~0          Failure
  */
  int delete_table(const char *name, const dd::Table *) override;

  /**
    Write sequence row.

    @param[in]      buf       table->record

    @retval         0         Success
    @retval         ~0        Failure
  */
  int write_row(uchar *buf) override;
  int update_row(const uchar *old_data, uchar *new_data) override;
  int delete_row(const uchar *buf) override;
  /**
    External lock

    @param[in]      thd         User connection
    @param[in]      lock_typ    Lock type

    @retval         0         Success
    @retval         ~0        Failure
  */
  int external_lock(THD *thd, int lock_type) override;

  /**
    Scrolling the sequence cache by update the base table through autonomous
    transaction.

    @param[in]      table       TABLE object
    @param[in]      request     Sequence cache request
    @param[in]      helper      Sequence share locker

    @retval         0         Success
    @retval         ~0        Failure
  */
  int scroll_sequence(TABLE *table, Sequence_cache_request request,
                      Share_locker_helper *helper, SR_ctx *sr_ctx);

  /**
    Rename sequence table name.

    @param[in]      from            Old name of sequence table
    @param[in]      to              New name of sequence table
    @param[in]      from_table_def  Old dd::Table object
    @param[in/out]  to_table_def    New dd::Table object

    @retval         0               Success
    @retval         ~0              Failure
  */
  int rename_table(const char *from, const char *to, const dd::Table *,
                   dd::Table *) override;
  /**
    Report sequence error.
  */
  void print_error(int error, myf errflag) override;

  /**
    Bind the table/handler thread to track table i/o.
  */
  void unbind_psi();
  void rebind_psi();

  /**
    Update the base table and flush the caches.

    @param[in]      table           Super TABLE object

    @retval         0               Success
    @retval         ~0              Failure
  */
  int ha_flush_cache(TABLE *, void *ctx) override;

  /**
    Fill values into sequence table fields from iterated local_values

    @param[in]      thd             User connection
    @param[in]      table           TABLE object
    @param[in]      local_values    Temporoary iterated values

    @retval         false           Success
    @retval         true            Failure
  */
  bool fill_into_sequence_fields(THD *thd, TABLE *table,
                                 ulonglong *local_values);

  /**
    Fill values int sequence table fields from thd local Sequence_last_value.

    @param[in]      thd             User connection
    @param[in]      table           TABLE object

    @retval         false           Success
    @retval         true            Failure
  */
  bool fill_sequence_fields_from_thd(THD *thd, TABLE *table);

 private:
  handler *m_file;
  plugin_ref m_engine;
  Sequence_info *m_sequence_info;
  Sequence_share *m_share;
  ulong start_of_scan;

  /* Number of timestamp value requested */
  ulonglong m_batch;

  Sequence_scan_mode m_scan_mode;
  Sequence_iter_mode m_iter_mode;

  Sequence_skip m_skip;
  Sequence_operation m_operation;
};

/**
  Create sequence handler

  @param[in]      sequence_info         Sequence create info
  @param[in]      mem_root              thd->mem_root, handler is allocated from
                                        it.

  @retval         handler               Sequence engine handler object
*/
extern handler *get_ha_sequence(Sequence_info *sequence_info,
                                MEM_ROOT *mem_root);

#endif /* HA_SEQUENCE_INCLUDED */
