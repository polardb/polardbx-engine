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

/**
  @file

  Sequence Engine handler interface and implementation.
*/

#include "my_systime.h"
#include "mysql/components/services/bits/psi_mutex_bits.h"
#include "mysql/components/services/bits/psi_rwlock_bits.h"
#include "mysql/plugin.h"          //st_mysql_storage_engine
#include "mysql/psi/mysql_cond.h"  //mysql_mutex_init
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_memory.h"
#include "mysql/psi/mysql_mutex.h"  //mysql_mutex_init
#include "mysql/service_mysql_alloc.h"
#include "sql/handler.h"
#include "sql/mysqld.h"
#include "sql/psi_memory_key.h"
#include "sql/sequence_transaction.h"
#include "sql/sql_update.h"  //compare_record

#include "sql/ha_sequence.h"

/**
  @addtogroup Sequence Engine

  Implementation of Sequence Engine interface

  @{
*/

#define SEQUENCE_ENABLED_TABLE_FLAGS (HA_FILE_BASED | HA_NO_AUTO_INCREMENT)

#define SEQUENCE_DISABLED_TABLE_FLAGS \
  (HA_CAN_GEOMETRY | HA_CAN_FULLTEXT | HA_DUPLICATE_POS | HA_CAN_SQL_HANDLER)

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key key_LOCK_sequence_share;
static PSI_mutex_key key_LOCK_sequence_open_shares_hash;
static PSI_cond_key key_COND_sequence_share;
static PSI_memory_key key_memory_sequence_share;

static PSI_mutex_info sequence_mutexes[] = {
    {&key_LOCK_sequence_share, "LOCK_sequence_share", 0, 0, PSI_DOCUMENT_ME},
    {&key_LOCK_sequence_open_shares_hash, "LOCK_sequence_hash", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_memory_info sequence_memory[] = {
    {&key_memory_sequence_share, "sequence_share", 0, 0, PSI_DOCUMENT_ME}};

static PSI_cond_info sequence_conds[] = {
    {&key_COND_sequence_share, "sequence_share", 0, 0, PSI_DOCUMENT_ME}};

static void init_sequence_psi_keys() {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(sequence_mutexes));
  mysql_mutex_register(category, sequence_mutexes, count);

  count = static_cast<int>(array_elements(sequence_memory));
  mysql_memory_register(category, sequence_memory, count);

  count = static_cast<int>(array_elements(sequence_conds));
  mysql_cond_register(category, sequence_conds, count);
}
#endif /* HAVE_PSI_INTERFACE */

bool opt_only_report_warning_when_skip_sequence = false;

/* Global sequence engine handlerton */
handlerton *sequence_hton;

static const char sequence_plugin_author[] = "GalaxyStore";
static const char sequence_plugin_name[] = "Sequence";

/* Protect sequence_open_shares map */
static mysql_mutex_t LOCK_sequence_open_shares_hash;
/* Sequence open shares map */
typedef collation_unordered_map<std::string, Sequence_share *>
    Sequence_shares_hash;
static Sequence_shares_hash *sequence_shares_hash;

/* Increment the sequence version */
static ulonglong sequence_global_version = 0;

static bool sequence_engine_inited = false;

static Sequence_share *get_share(const char *name) {
  Sequence_share *share = nullptr;
  Sequence_shares_hash::const_iterator it;
  DBUG_ENTER("get_share");

  /**
    We will hold the lock until the object creation, if the sequence_share
    didn't exist in the map, since the creation has only very low cost.

    Otherwise we should set CREATING flag to release the lock and
    load sequence value from table slowly.
  */
  mysql_mutex_lock(&LOCK_sequence_open_shares_hash);

  it = sequence_shares_hash->find(std::string(name));
  if (it != sequence_shares_hash->end()) {
    share = it->second;
  } else {
    share = new Sequence_share();
    share->init(name);
    share->m_version = sequence_global_version++;
    sequence_shares_hash->insert(
        std::pair<std::string, Sequence_share *>(std::string(name), share));
  }

  if (share) share->m_ref_count++;

  mysql_mutex_unlock(&LOCK_sequence_open_shares_hash);
  DBUG_RETURN(share);
}
/**
  Close the sequence share,
  make sure that sequence handler has been disassociated from it.

  @param[in]      share           Sequence share object

  @retval         void
*/
static void close_share(Sequence_share *share) {
  DBUG_ENTER("close_share");

  mysql_mutex_lock(&LOCK_sequence_open_shares_hash);
#ifndef NDEBUG
  Sequence_shares_hash::const_iterator it =
      sequence_shares_hash->find(std::string(share->m_name));
  assert(it != sequence_shares_hash->end() && it->second == share);
#endif
  assert(share->m_ref_count > 0);
  --share->m_ref_count;
  mysql_mutex_unlock(&LOCK_sequence_open_shares_hash);
  DBUG_VOID_RETURN;
}
/**
  Destroy the sequence_share object.

  @param[in]      name            Destroy the sequence_share object.

  @retval         void
*/
static void destroy_share(const char *name) {
  DBUG_ENTER("destroy_share");
  mysql_mutex_lock(&LOCK_sequence_open_shares_hash);

  Sequence_shares_hash::const_iterator it =
      sequence_shares_hash->find(std::string(name));

  if (it != sequence_shares_hash->end()) {
    delete it->second;
    sequence_shares_hash->erase(it);
  }
  mysql_mutex_unlock(&LOCK_sequence_open_shares_hash);
  DBUG_VOID_RETURN;
}

/**
  Init all the member variable.

  @param[in]      table_name      db_name + table_name

  @retval         void
*/
void Sequence_share::init(const char *table_name) {
  DBUG_ENTER("Sequence_share::init");
  mysql_mutex_init(key_LOCK_sequence_share, &m_mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_sequence_share, &m_cond);
  size_t length = strlen(table_name);
  m_name = my_strndup(key_memory_sequence_share, table_name, length,
                      MYF(MY_FAE | MY_ZEROFILL));

  bitmap_init(&m_read_set, nullptr, SF_END);
  bitmap_init(&m_write_set, nullptr, SF_END);
  bitmap_set_all(&m_read_set);
  bitmap_set_all(&m_write_set);

  m_cache_state = CACHE_STATE_INVALID;
  m_initialized = true;
  m_cache_end = 0;
  m_ref_count = 0;
  memset(m_caches, 0, sizeof(m_caches));

  m_last_time = 0;
  m_counter = 0;
  m_low_limit = 0;
  m_type = Sequence_type::DIGITAL;
  DBUG_VOID_RETURN;
}

/**
  Get sequence share cache field value pointer

  @param[in]      field_num     The sequence field number

  @retval         field pointer
*/
ulonglong *Sequence_share::get_field_ptr(const Sequence_field field_num) {
  DBUG_ENTER("Sequence_share::get_field_ptr");
  assert(field_num < SF_END);
  DBUG_RETURN(&m_caches[field_num]);
}

/**
   Enter the wait condition until loading complete or error happened.
   @param[in]     thd           User connection

   @retval        0             Success
   @retval        ~0            Failure
*/
int Sequence_share::enter_cond(THD *thd) {
  int wait_result = 0;
  struct timespec abs_timeout;

  mysql_mutex_assert_owner(&m_mutex);
  set_timespec(&abs_timeout, thd->variables.lock_wait_timeout);

  while (m_cache_state == CACHE_STATE_LOADING && !thd->is_killed() &&
         !is_timeout(wait_result)) {
    wait_result = mysql_cond_timedwait(&m_cond, &m_mutex, &abs_timeout);
  }

  if (m_cache_state == CACHE_STATE_LOADING) {
    if (thd->is_killed()) {
      thd_set_kill_status(thd);  // set my_error
    } else if (is_timeout(wait_result)) {
      my_error(ER_LOCK_WAIT_TIMEOUT, MYF(0));
    }
    return HA_ERR_SEQUENCE_ACCESS_FAILURE;
  }
  return 0;
}
/**
     handle some specific errors:
    1. skip_sequence will raise no error if
        opt_only_report_warning_when_skip_sequence is setted
    2. HA_ERR_SEQUENCE_SKIP_ERROR will not invalidate cache

    3. other errors will make cache invalidate and return input error

    @param[in]       error                     error no

    @retval            error                      error no
*/
int Sequence_share::handle_specific_error(int error, ulonglong *local_values) {
  DBUG_ENTER("handle_specific_error");

  switch (error) {
    case HA_ERR_SEQUENCE_SKIP_ERROR:
      if (opt_only_report_warning_when_skip_sequence) {
        memcpy(local_values, m_caches, sizeof(m_caches));
        push_warning_printf(
            current_thd, Sql_condition::SL_WARNING, ER_SEQUENCE_SKIP_ERROR,
            "Nextval skipped to is not valid, nothing will be done");
        DBUG_RETURN(0);
      }
      DBUG_RETURN(error);
    default:
      break;
  }
  invalidate();
  DBUG_RETURN(error);
}

Sequence_cache_request Sequence_share::quick_read(ulonglong *local_values,
                                                  SR_ctx *sr_ctx) {
  DBUG_ENTER("quick_read");

  mysql_mutex_assert_owner(&m_mutex);
  assert(m_cache_state != CACHE_STATE_LOADING);

  /* If cache is not valid, need load and flush cache. */
  if (m_cache_state == CACHE_STATE_INVALID) {
    /** If SKIP, CHANGE TO BATCH */
    Sequence_skip *skip = sr_ctx->get_skip_ptr();
    /**
      We didn't know how much range to load since first nextval_skip didn't know
      current nextval, then didn't calc the batch size.
      So delay the batch size calculation to calc_digital_next_round.
    */
    if (skip->is_skip()) {
      sr_ctx->set_first_skip();
      assert(sr_ctx->batch() == 1);
    }
    assert(sr_ctx->is_inherit() == false);

    DBUG_RETURN(CACHE_REQUEST_NEED_LOAD);
  }

  assert(m_cache_state == CACHE_STATE_VALID);

  if (m_type == Sequence_type::DIGITAL) {
    if (sr_ctx->get_skip_ptr()->is_skip()) {
      DBUG_RETURN(digital_skip_read(local_values, sr_ctx));
    } else {
      DBUG_RETURN(digital_quick_read(local_values, sr_ctx));
    }
  } else {
    DBUG_RETURN(timestamp_quick_read(local_values, sr_ctx));
  }
}

/**
  Allocate a new time range [ m_low_limit - m_cache_end] as cached timestamp;

  @param[in/out]    durable     it will be persisted into table.
*/
int Sequence_share::calc_timestamp_next_round(ulonglong *durable) {
  DBUG_ENTER("calc_timestamp_next_round");
  assert(m_type == Sequence_type::TIMESTAMP);

  ulonglong retry_counter = 0;

retry:
  if ((++retry_counter % TIMESTAMP_SEQUENCE_ROUND_SLEEP) == 0) {
    std::chrono::milliseconds sleep_time(TIMESTAMP_SEQUENCE_SLEEP_TIME);
    std::this_thread::sleep_for(sleep_time);

    if (retry_counter > TIMESTAMP_SEQUENCE_MAX_ROUND)
      DBUG_RETURN(HA_ERR_SEQUENCE_RETRY_TIMEOUT);
  }

  DBUG_EXECUTE_IF("sequence_reload_retry_timeout",
                  { DBUG_RETURN(HA_ERR_SEQUENCE_RETRY_TIMEOUT); });

  ulonglong future = time_system_ms();
  future += m_caches[SF_CACHE] * 1000;

  if (future <= m_caches[SF_NEXTVAL]) goto retry;

  durable[SF_NEXTVAL] = future;

  m_cache_end = future;
  m_low_limit = m_caches[SF_NEXTVAL];
  DBUG_RETURN(0);
}

/**
  Quick read timestamp from cache directly;

  @param[in/out]      local_values      it will send to user.
  @param[in]          batch             number of timestamp value requested.
*/
Sequence_cache_request Sequence_share::timestamp_quick_read(
    ulonglong *local_values, SR_ctx *sr_ctx) {
  DBUG_ENTER("Sequence_share::timestamp_quick_read");

  mysql_mutex_assert_owner(&m_mutex);
  ulonglong batch = sr_ctx->batch();
  ulonglong retry_counter = 0;

  /**
    Timestamp sequence will allocate a range timestamp as cache, the range is:
     [m_low_limit, m_cache_end],

     1) if now value is lower than m_low_limit, it's not valid timestamp, since
        the time maybe has been used.
     2) if now value is larger than m_cache_end, it means cache has been
        run out.

     3) if now value is equal with last time, then increment m_counter.
        and if m_counter >= 65536, retry to increment millsecond.

     4) if now value is lower than last time, retry again.
  */
retry:
  if ((++retry_counter % TIMESTAMP_SEQUENCE_ROUND_SLEEP) == 0) {
    std::chrono::milliseconds sleep_time(TIMESTAMP_SEQUENCE_SLEEP_TIME);
    std::this_thread::sleep_for(sleep_time);

    if (retry_counter > TIMESTAMP_SEQUENCE_MAX_ROUND)
      DBUG_RETURN(CACHE_REQUEST_RETRY_TIMEOUT);
  }

  DBUG_EXECUTE_IF("sequence_quick_read_retry_timeout",
                  { DBUG_RETURN(CACHE_REQUEST_RETRY_TIMEOUT); });

  ulonglong now = time_system_ms();

  if (now < m_low_limit) goto retry;

  if (now >= m_cache_end) DBUG_RETURN(CACHE_REQUEST_ROUND_OUT);

  if (sr_ctx->get_operation().is_show_cache()) {
    DBUG_RETURN(show_cache(local_values, sr_ctx));
  }

  if (now == m_last_time) {
    m_counter++;
    /* If remaining value does not satisfy the request, retry */
    if (m_counter + (batch - 1) >= 65536) goto retry;
  } else if (now > m_last_time) {
    m_counter = 0;
  } else {
    goto retry;
  }
  m_last_time = now;

  ulonglong tmp = (now << 22) | (m_counter << 6);
  local_values[SF_CURRVAL] = tmp;
  local_values[SF_NEXTVAL] = tmp;

  /* Remember the value that has been used so far */
  m_counter += (batch - 1);

  DBUG_RETURN(CACHE_REQUEST_HIT);
}

/**
  Quickly nextval_skip operation according to current cache
  [nextval, m_cache_end].
*/
Sequence_cache_request Sequence_share::digital_skip_read(
    ulonglong *local_values, SR_ctx *sr_ctx) {
  ulonglong *nextval_ptr;
  ulonglong *currval_ptr;
  ulonglong *increment_ptr;
  Sequence_skip *skip;
  ulonglong skipped_to;
  DBUG_ENTER("Sequence_share::digital_skip_read");

  assert(sr_ctx->batch() == 1);
  assert(sr_ctx->is_inherit() == false);

  skip = sr_ctx->get_skip_ptr();
  assert(skip->is_skip());
  skipped_to = skip->skipped_to();

  nextval_ptr = &m_caches[SF_NEXTVAL];
  currval_ptr = &m_caches[SF_CURRVAL];
  increment_ptr = &m_caches[SF_INCREMENT];

  /** Skipped value should be in [nextval - max_value] */
  if (skipped_to < *nextval_ptr || skipped_to >= m_caches[SF_MAXVALUE]) {
    DBUG_RETURN(Sequence_cache_request::CACHE_REQUEST_SKIP_ERROR);
  } else if (skipped_to >= *nextval_ptr && skipped_to < m_cache_end) {
    /** directly skip */
    *currval_ptr = *nextval_ptr;
    memcpy(local_values, m_caches, sizeof(m_caches));
    if ((m_cache_end - skipped_to) >= *increment_ptr) {
      *nextval_ptr = (skipped_to + *increment_ptr);
    } else {
      *nextval_ptr = m_cache_end;
    }
    DBUG_RETURN(CACHE_REQUEST_HIT);
  } else {
    /**
      Both batch and skip operation will be changed into batch attributes.
      because calc_next_round only need to know how much will you need.
    */
    ulonglong batch = (skipped_to - *nextval_ptr) / *increment_ptr + 1;
    /** Need load a batch */
    sr_ctx->set_batch(batch);
    sr_ctx->inherit(*nextval_ptr);
    sr_ctx->clear_skip();
    DBUG_RETURN(CACHE_REQUEST_ROUND_OUT);
  }
}

/**
  Retrieve the nextval from cache directly.

  @param[out]     local_values    Used to store into thd->sequence_last_value

  @retval         request         Cache request result
*/
Sequence_cache_request Sequence_share::digital_quick_read(
    ulonglong *local_values, SR_ctx *sr_ctx) {
  ulonglong *nextval_ptr;
  ulonglong *currval_ptr;
  ulonglong *increment_ptr;
  ulonglong batch;
  bool last_round;
  DBUG_ENTER("Sequence_share::quick_read");

  mysql_mutex_assert_owner(&m_mutex);
  assert(m_cache_state != CACHE_STATE_LOADING);

  nextval_ptr = &m_caches[SF_NEXTVAL];
  currval_ptr = &m_caches[SF_CURRVAL];
  increment_ptr = &m_caches[SF_INCREMENT];
  batch = sr_ctx->batch();

  assert(batch > 0);

  /* If cache_end roll upon maxvalue, then it is last round */
  last_round = (m_caches[SF_MAXVALUE] == m_cache_end);

  if (!last_round && ulonglong(*nextval_ptr) >= m_cache_end) {
    DBUG_RETURN(CACHE_REQUEST_ROUND_OUT);
  } else if (last_round) {
    if (*nextval_ptr > m_cache_end) DBUG_RETURN(CACHE_REQUEST_ROUND_OUT);
  }

  if (sr_ctx->get_operation().is_show_cache()) {
    DBUG_RETURN(show_cache(local_values, sr_ctx));
  }

  /** Here, at least still have one available number. */
  longlong left = (m_cache_end - *nextval_ptr) / *increment_ptr - 1;
  if (left <= 0) {
    left = 1;
  } else {
    left = left + 1;
  }

  if (((ulonglong)left) >= batch) {
    /* Retrieve values from cache directly */
    assert(*nextval_ptr <= m_cache_end);
    *currval_ptr = *nextval_ptr;
    memcpy(local_values, m_caches, sizeof(m_caches));
    if ((m_cache_end - *nextval_ptr) >= (*increment_ptr * batch))
      *nextval_ptr += (*increment_ptr * batch);
    else {
      *nextval_ptr = m_cache_end;
      invalidate();
    }
    DBUG_RETURN(CACHE_REQUEST_HIT);
  } else {
    sr_ctx->inherit(*nextval_ptr);
    DBUG_RETURN(CACHE_REQUEST_ROUND_OUT);
  }
}

/**
  param[in/out]   durable   Read from record and calc next round to write
*/
int Sequence_share::calc_digital_next_round(ulonglong *durable,
                                            SR_ctx *sr_ctx) {
  DBUG_ENTER("calc_digital_next_round");
  assert(m_type == Sequence_type::DIGITAL);
  /** We have copy the record from table into m_caches */
  ulonglong begin;

  /* Step 1: decide the begin value */
  if (m_caches[SF_NEXTVAL] == 0) {
    if (m_caches[SF_ROUND] == 0) /* Take start value as the begining */
      begin = m_caches[SF_START];
    else
      /* Next round from minvalue */
      begin = m_caches[SF_MINVALUE];
  } else if (m_caches[SF_NEXTVAL] == m_caches[SF_MAXVALUE])
    /* Run out value when nocycle */
    DBUG_RETURN(HA_ERR_SEQUENCE_RUN_OUT);
  else
    begin = m_caches[SF_NEXTVAL];

  assert(begin <= m_caches[SF_MAXVALUE]);

  if (begin > m_caches[SF_MAXVALUE]) {
    DBUG_RETURN(HA_ERR_SEQUENCE_INVALID);
  }

  ulonglong batch = sr_ctx->batch();

  /** If it's first skip nextval, calc the batch size. */
  if (sr_ctx->is_first_skip()) {
    assert(batch == 1);
    ulonglong skipped_to = sr_ctx->get_skip_ptr()->skipped_to();
    if (skipped_to < begin || skipped_to >= m_caches[SF_MAXVALUE]) {
      DBUG_RETURN(HA_ERR_SEQUENCE_SKIP_ERROR);
    }
    batch = (skipped_to - begin) / m_caches[SF_INCREMENT] + 1;
  }

  /** Bigger cache size and batch size as next round buffer */
  ulonglong cache = m_caches[SF_CACHE] > batch ? m_caches[SF_CACHE] : batch;

  /* Step 3: calc the left counter to cache */
  longlong left = (m_caches[SF_MAXVALUE] - begin) / m_caches[SF_INCREMENT] - 1;

  /* The left counter is less than cache size */
  if (left < 0 || ((ulonglong)left) <= cache) {
    /* If cycle, start again; else will report error! */
    m_cache_end = m_caches[SF_MAXVALUE];

    if (m_caches[SF_CYCLE] > 0) {
      durable[SF_NEXTVAL] = 0;
      durable[SF_ROUND]++;
    } else
      durable[SF_NEXTVAL] = m_caches[SF_MAXVALUE];
  } else {
    m_cache_end = begin + (cache + 1) * m_caches[SF_INCREMENT];
    durable[SF_NEXTVAL] = m_cache_end;
    assert(m_cache_end < m_caches[SF_MAXVALUE]);
  }

  if (sr_ctx->is_inherit()) {
    m_caches[SF_NEXTVAL] = sr_ctx->get_inherit_value();
  } else {
    m_caches[SF_NEXTVAL] = begin;
  }

  DBUG_RETURN(0);
}
/**
     Show the next value store in cache. It will reload cache if
    current cache has run out.

    @param[out]     local_values           local value array
    @param[in]       sr_ctx                    sequence request context

    @retval            cache request
*/
Sequence_cache_request Sequence_share::show_cache(ulonglong *local_values,
                                                  SR_ctx *sr_ctx) {
  DBUG_ENTER("show_cache");

  local_values[SF_NEXTVAL] = m_caches[SF_NEXTVAL];

  sr_ctx->clear_operation();
  DBUG_RETURN(CACHE_REQUEST_HIT);
}

/**
  Reload the sequence value cache.

  @param[in]      table         TABLE object
  @param[out]     changed       Whether values are changed

  @retval         0             Success
  @retval         ~0            Failure
*/
int Sequence_share::reload_cache(TABLE *table, bool *changed, SR_ctx *sr_ctx) {
  int err = 0;
  st_sequence_field_info *field_info;
  Field **field;
  ulonglong durable[SF_END];
  Sequence_field field_num;
  DBUG_ENTER("Sequence_share::reload_cache");

  /* Read the durable values */
  for (field = table->field, field_info = seq_fields; *field;
       field++, field_info++) {
    field_num = field_info->field_num;
    durable[field_num] = (ulonglong)((*field)->val_int());
  }

  m_type = convert_sequence_type_by_increment(durable[SF_INCREMENT]);

  /* If someone update the table directly, need this check again. */
  if (check_sequence_values_valid(m_type, durable))
    DBUG_RETURN(HA_ERR_SEQUENCE_INVALID);

  /* Step 1: overlap the cache using durable values */
  for (field_info = seq_fields; field_info->field_name; field_info++)
    m_caches[field_info->field_num] = durable[field_info->field_num];

  if (m_type == Sequence_type::DIGITAL) {
    err = calc_digital_next_round(durable, sr_ctx);
  } else {
    err = calc_timestamp_next_round(durable);
  }
  if (err) DBUG_RETURN(err);

  /* Step 4: Write back durable values */
  store_record(table, record[1]);
  for (field = table->field, field_info = seq_fields; *field;
       field++, field_info++) {
    (*field)->set_notnull();
    (*field)->store(durable[field_info->field_num], true);
  }
  *changed = compare_records(table);

#ifndef DBUG_OFF
  fprintf(stderr,
          "Sequence will write values: "
          "currval %llu "
          "nextval %llu "
          "minvalue %llu "
          "maxvalue %llu "
          "start %llu "
          "increment %llu "
          "cache %llu "
          "cycle %llu \n",
          durable[SF_CURRVAL], durable[SF_NEXTVAL], durable[SF_MINVALUE],
          durable[SF_MAXVALUE], durable[SF_START], durable[SF_INCREMENT],
          durable[SF_CACHE], durable[SF_CYCLE]);
#endif
  DBUG_RETURN(0);
}

/**
  Update the base table and flush the caches.

  @param[in]      table           Super TABLE object

  @retval         0               Success
  @retval         ~0              Failure
*/
int ha_sequence::ha_flush_cache(TABLE *, void *ctx) {
  int error = 0;
  bool changed;
  DBUG_ENTER("ha_sequence::ha_flush_cache");
  assert(m_file);

  Bitmap_helper helper(table, m_share);

  SR_ctx *sr_ctx = static_cast<SR_ctx *>(ctx);

  if ((error = m_file->ha_rnd_init(true))) goto err;

  if ((error = m_file->ha_rnd_next(table->record[0]))) goto err;

  if ((error = m_share->reload_cache(table, &changed, sr_ctx))) goto err;

  if (!error && changed) {
    if ((error = m_file->ha_update_row(table->record[1], table->record[0])))
      goto err;
  }
err:
  m_file->ha_rnd_end();
  DBUG_RETURN(error);
}

/**
  Create sequence handler

  @param[in]      sequence_info         Sequence create info
  @param[in]      mem_root              thd->mem_root, handler is allocated from
                                        it.

  @retval         handler               Sequence engine handler object
*/
handler *get_ha_sequence(Sequence_info *sequence_info, MEM_ROOT *mem_root) {
  ha_sequence *ha;
  DBUG_ENTER("get_ha_sequence");
  if ((ha = new (mem_root) ha_sequence(sequence_hton, sequence_info))) {
    if (ha->initialize_sequence(mem_root)) {
      destroy(ha);
      ha = nullptr;
    } else
      ha->init();
  } else {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR),
             static_cast<int>(sizeof(ha_sequence)));
  }
  DBUG_RETURN((handler *)ha);
}

/**
  Sequence base table engine setup.
*/
bool ha_sequence::setup_base_engine() {
  handlerton *hton;
  DBUG_ENTER("ha_sequence::setup_base_engine");
  assert((table_share && table_share->sequence_property->is_sequence()) ||
         !table_share);

  if (table_share) {
    hton = table_share->sequence_property->db_type();
    m_engine = ha_lock_engine(nullptr, hton);
  } else {
    m_engine = ha_resolve_sequence_base(nullptr);
  }
  if (!m_engine) goto err;

  DBUG_RETURN(false);
err:
  clear_base_handler_file();
  DBUG_RETURN(true);
}
/**
  Clear the locked sequence base table engine and destroy file handler
*/
void ha_sequence::clear_base_handler_file() {
  DBUG_ENTER("ha_sequence::clear_base_handler_file");
  if (m_engine) {
    plugin_unlock(nullptr, m_engine);
    m_engine = nullptr;
  }
  if (m_file) {
    destroy(m_file);
    m_file = nullptr;
  }
  DBUG_VOID_RETURN;
}

/**
  Create the base table handler by m_engine.

  @param[in]      mem_root        Memory space

  @retval         false           Success
  @retval         true            Failure
*/
bool ha_sequence::setup_base_handler(MEM_ROOT *mem_root) {
  handlerton *hton;

  DBUG_ENTER("ha_sequence::setup_base_handler");
  assert(m_engine);

  hton = plugin_data<handlerton *>(m_engine);
  if (!(m_file = get_new_handler(table_share, false, mem_root, hton))) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR),
             static_cast<int>(sizeof(handler)));
    DBUG_RETURN(true);
  }
  DBUG_RETURN(false);
}
/**
  Setup the sequence base table engine and base file handler.

  @param[in]    name        Sequence table name
  @param[in]    mem_root    Memory space

  @retval       false       success
  @retval       true        failure
*/
bool ha_sequence::get_from_handler_file(const char *, MEM_ROOT *mem_root) {
  DBUG_ENTER("ha_sequence::get_from_handler_file");

  if (m_file) DBUG_RETURN(false);

  if (setup_base_engine() || setup_base_handler(mem_root)) goto err;

  DBUG_RETURN(false);
err:
  clear_base_handler_file();
  DBUG_RETURN(true);
}

/**
  Init the sequence base table engine handler by sequence info

  @param[in]    mem_root    memory space

  @retval       false       success
  @retval       true        failure
*/
bool ha_sequence::new_handler_from_sequence_info(MEM_ROOT *mem_root) {
  DBUG_ENTER("ha_sequence::new_handler_from_sequence_info");
  assert(m_sequence_info);

  if (!(m_file = get_new_handler(table_share, false, mem_root,
                                 m_sequence_info->base_db_type))) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR),
             static_cast<int>(sizeof(handler)));
    DBUG_RETURN(true);
  }
  DBUG_RETURN(false);
}

/**
  Initialize sequence handler

  @param[in]    mem_root    memory space

  @retval       false       success
  @retval       true        failure
*/
bool ha_sequence::initialize_sequence(MEM_ROOT *mem_root) {
  DBUG_ENTER("ha_sequence::initialize_sequence");

  if (m_sequence_info) {
    if (new_handler_from_sequence_info(mem_root)) {
      DBUG_RETURN(true);
    }
  } else if (get_from_handler_file(nullptr, mem_root)) {
    DBUG_RETURN(true);
  }

  DBUG_EXECUTE_IF("sequence_handler_error", {
    my_error(ER_SEQUENCE_ACCESS_FAILURE, MYF(0), nullptr, nullptr);
    DBUG_RETURN(true);
  });

  DBUG_RETURN(false);
}

/**
  Sequence handlerton create interface function.

  @param[in]    hton          sequence hton
  @param[in]    share         TABLE_SHARE object
  @param[in]    partitioned   whether base table is partition table
  @param[in]    mem_root      memory space

  @retval       handler       sequence handler
*/
static handler *sequence_create_handler(handlerton *hton, TABLE_SHARE *share,
                                        bool, MEM_ROOT *mem_root) {
  DBUG_ENTER("sequence_create_handler");
  ha_sequence *file = new (mem_root) ha_sequence(hton, share);
  if (file && file->initialize_sequence(mem_root)) {
    destroy(file);
    file = nullptr;
  }
  DBUG_RETURN(file);
}
/**
  Initialize the sequence handler member variable.
*/
void ha_sequence::init_variables() {
  DBUG_ENTER("ha_sequence::init_variables");
  m_file = nullptr;
  m_engine = nullptr;
  m_sequence_info = nullptr;
  m_share = nullptr;
  m_batch = 1;

  m_skip.reset();
  m_operation.reset();

  start_of_scan = 0;
  DBUG_VOID_RETURN;
}

ha_sequence::ha_sequence(handlerton *hton, TABLE_SHARE *share)
    : handler(hton, share) {
  init_variables();
}

/* Init handler when create sequence */
ha_sequence::ha_sequence(handlerton *hton, Sequence_info *info)
    : handler(hton, 0) {
  init_variables();
  m_sequence_info = info;
}

/**
  Unlock the base storage plugin and destroy the handler
*/
ha_sequence::~ha_sequence() {
  if (m_share) {
    close_share(m_share);
    m_share = nullptr;
  }
  clear_base_handler_file();
}
/**
  Fill values into sequence table fields from iterated local_values

  @param[in]      thd             User connection
  @param[in]      table           TABLE object
  @param[in]      local_values    Temporoary iterated values

  @retval         false           Success
  @retval         true            Failure
*/
bool ha_sequence::fill_into_sequence_fields(THD *thd, TABLE *table_arg,
                                            ulonglong *local_values) {
  Sequence_last_value *entry;
  st_sequence_field_info *field_info;
  Field **field;
  DBUG_ENTER("fill_sequence_fields");

  std::string key(table_arg->s->table_cache_key.str,
                  table_arg->s->table_cache_key.length);
  Sequence_last_value_hash::const_iterator it =
      thd->get_sequence_hash()->find(key);

  if (it != thd->get_sequence_hash()->end()) {
    entry = it->second;
    entry->set_version(m_share->m_version);
  } else {
    entry = new Sequence_last_value();
    entry->set_version(m_share->m_version);
    thd->get_sequence_hash()->insert(
        std::pair<std::string, Sequence_last_value *>(key, entry));
  }

  Bitmap_helper bitmap_helper(table_arg, m_share);

  for (field = table_arg->field, field_info = seq_fields; *field;
       field++, field_info++) {
    assert(!memcmp(field_info->field_name, (*field)->field_name,
                   strlen(field_info->field_name)));

    ulonglong value = local_values[field_info->field_num];
    (*field)->set_notnull();
    (*field)->store(value, true);
    entry->m_values[field_info->field_num] = value;
  }
  DBUG_RETURN(false);
}

/**
  Fill values into sequence table fields from thd local Sequence_last_value.

  @param[in]      thd             User connection
  @param[in]      table           TABLE object

  @retval         false           Success
  @retval         true            Failure
*/
bool ha_sequence::fill_sequence_fields_from_thd(THD *thd, TABLE *table_arg) {
  Sequence_last_value *entry;
  st_sequence_field_info *field_info;
  Field **field;
  DBUG_ENTER("fill_sequence_fields_from_thd");

  std::string key(table_arg->s->table_cache_key.str,
                  table_arg->s->table_cache_key.length);
  Sequence_last_value_hash::const_iterator it =
      thd->get_sequence_hash()->find(key);

  if (it != thd->get_sequence_hash()->end()) {
    entry = it->second;
    if (entry->get_version() != m_share->m_version) {
      thd->get_sequence_hash()->erase(it);
      DBUG_RETURN(true);
    }
  } else {
    DBUG_RETURN(true);
  }

  Bitmap_helper bitmap_helper(table_arg, m_share);

  for (field = table_arg->field, field_info = seq_fields; *field;
       field++, field_info++) {
    assert(!memcmp(field_info->field_name, (*field)->field_name,
                   strlen(field_info->field_name)));
    ulonglong value = entry->m_values[field_info->field_num];
    (*field)->set_notnull();
    (*field)->store(value, true);
  }

  DBUG_RETURN(false);
}

/**
  Sequence full table scan.

  @param[in]      scan
  @retval         ~0              error number
  @retval         0               success
*/
int ha_sequence::rnd_init(bool scan) {
  DBUG_ENTER("ha_sequence::rnd_init");
  assert(m_file);
  assert(m_share);
  assert(table_share && table);

  start_of_scan = 1;

  /* Inherit the sequence scan mode option. */
  m_scan_mode = table->sequence_scan.get();
  m_iter_mode = Sequence_iter_mode::IT_NON;
  m_batch = table->sequence_scan.get_batch();
  m_skip = table->sequence_scan.get_skip();
  m_operation = table->sequence_scan.get_operation();

  assert(m_batch <= TIMESTAMP_SEQUENCE_MAX_BATCH_SIZE);

  if (m_scan_mode == Sequence_scan_mode::ITERATION_SCAN)
    m_iter_mode = sequence_iteration_type(table);

  DBUG_RETURN(m_file->ha_rnd_init(scan));
}

/**
  Sequence engine main logic.
  Embedded into the table scan process.

  Logics:
    1.Skip sequence cache to scan the based table record if
      a. update;
      b. select_from clause;

    2.Only scan the first row that controlled by
      variable 'start_of_scan'

    3.Lock strategy
      a. Only hold MDL_SHARED_READ if cache hit
      b. Hold MDL_SHARE_WRITE, GLOBAL READ LOCK when update, and COMMIT LOCK
          when autonomous transaction commit if cache miss

    4.Transaction
      a. begin a new autonomous transaction when updating base table.
*/
int ha_sequence::rnd_next(uchar *buf) {
  int error = 0;
  int retry_time = 2;
  Sequence_cache_request cache_request;
  ulonglong local_values[SF_END];
  DBUG_ENTER("ha_sequence::rnd_next");

  assert(m_file && m_share && ha_thd() && table_share && table);

  if (m_scan_mode == Sequence_scan_mode::ORIGINAL_SCAN ||
      ha_thd()->variables.sequence_read_skip_cache) {
    DBUG_RETURN(m_file->ha_rnd_next(buf));
  }

  if (start_of_scan) {
    start_of_scan = 0;

    /**
      Get the currval from THD local sequence_last_value directly if only query
      currval.
    */
    if (m_iter_mode == Sequence_iter_mode::IT_NON_NEXTVAL) {
      if (fill_sequence_fields_from_thd(ha_thd(), table))
        DBUG_RETURN(HA_ERR_SEQUENCE_NOT_DEFINED);
      else
        DBUG_RETURN(0);
    }

    assert(m_iter_mode == Sequence_iter_mode::IT_NEXTVAL);

    SR_ctx sr_ctx(m_batch, m_skip, m_operation);
    Share_locker_helper share_locker(m_share);

  retry_once:
    retry_time--;
    /**
      Enter the condition:
       1. Wait if other thread is loading the cache.
       2. Report error if timeout.
       3. Return if thd->killed.
    */
    if ((error = m_share->enter_cond(ha_thd()))) {
      DBUG_RETURN(error);
    }

    /** Set sequence request context */
    sr_ctx.init(m_batch, m_skip, m_operation);
    cache_request = m_share->quick_read(local_values, &sr_ctx);
    switch (cache_request) {
      case Sequence_cache_request::CACHE_REQUEST_HIT: {
        share_locker.release();
        goto end;
      }
      case Sequence_cache_request::CACHE_REQUEST_SKIP_ERROR: {
        error = HA_ERR_SEQUENCE_SKIP_ERROR;
        break;
      }
      case Sequence_cache_request::CACHE_REQUEST_ERROR: {
        error = HA_ERR_SEQUENCE_ACCESS_FAILURE;
        break;
      }
      case Sequence_cache_request::CACHE_REQUEST_RETRY_TIMEOUT: {
        error = HA_ERR_SEQUENCE_RETRY_TIMEOUT;
        break;
      }

      case Sequence_cache_request::CACHE_REQUEST_NEED_LOAD:
      case Sequence_cache_request::CACHE_REQUEST_ROUND_OUT: {
        if (retry_time > 0) {
          error = scroll_sequence(table, cache_request, &share_locker, &sr_ctx);
          share_locker.complete_load(error);
          if (error) {
            break;
          } else {
            /** Didn't allowed to run out if timestamp sequence */
            if (m_share->get_type() == Sequence_type::TIMESTAMP) retry_time++;
            goto retry_once;
          }
        } else {
          error = HA_ERR_SEQUENCE_RUN_OUT;
          break;
        }
      }
    } /* switch end */

    error = m_share->handle_specific_error(error, local_values);
    DBUG_RETURN(error);
  } else
    DBUG_RETURN(HA_ERR_END_OF_FILE); /* if (start_of_scan) end */

end:

  /* Fill the Sequence_last_value object.*/
  if (fill_into_sequence_fields(ha_thd(), table, local_values))
    DBUG_RETURN(HA_ERR_SEQUENCE_ACCESS_FAILURE);
  DBUG_RETURN(0);
}

int ha_sequence::rnd_end() {
  DBUG_ENTER("ha_sequence::rnd_end");
  assert(m_file && m_share);
  assert(table_share && table);
  DBUG_RETURN(m_file->ha_rnd_end());
}

int ha_sequence::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_ENTER("ha_sequence::rnd_pos");
  assert(m_file);
  DBUG_RETURN(m_file->ha_rnd_pos(buf, pos));
}

void ha_sequence::position(const uchar *record) {
  DBUG_ENTER("ha_sequence::positioin");
  assert(m_file);
  m_file->position(record);
}

void ha_sequence::update_create_info(HA_CREATE_INFO *create_info) {
  if (m_file) m_file->update_create_info(create_info);
}

int ha_sequence::info(uint) {
  DBUG_ENTER("ha_sequence::info");
  DBUG_RETURN(false);
}

/**
  Add hidden columns and indexes to an InnoDB table definition.

  @param[in,out]	dd_table	      data dictionary cache object

  @retval         error number
  @retval         0               success
*/
int ha_sequence::get_extra_columns_and_keys(
    const HA_CREATE_INFO *create_info, const List<Create_field> *create_list,
    const KEY *key_info, uint key_count, dd::Table *dd_table) {
  DBUG_ENTER("ha_sequence::get_extra_columns_and_keys");
  DBUG_RETURN(m_file->get_extra_columns_and_keys(
      create_info, create_list, key_info, key_count, dd_table));
}

const char *ha_sequence::table_type() const {
  DBUG_ENTER("ha_sequence::table_type");
  DBUG_RETURN(sequence_plugin_name);
}

ulong ha_sequence::index_flags(uint inx, uint part, bool all_parts) const {
  DBUG_ENTER("ha_sequence::index_flags");
  DBUG_RETURN(m_file->index_flags(inx, part, all_parts));
}
/**
  Store lock
*/
THR_LOCK_DATA **ha_sequence::store_lock(THD *thd, THR_LOCK_DATA **to,
                                        enum thr_lock_type lock_type) {
  DBUG_ENTER("ha_sequence::store_lock");
  DBUG_RETURN(m_file->store_lock(thd, to, lock_type));
}
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
int ha_sequence::open(const char *name, int mode, uint test_if_locked,
                      const dd::Table *table_def) {
  int error;
  DBUG_ENTER("ha_sequence::open");
  assert(table->s == table_share);
  error = HA_ERR_INITIALIZATION;

  if (!(m_share = get_share(name))) DBUG_RETURN(error);

  if (get_from_handler_file(name, &table->mem_root)) DBUG_RETURN(error);

  assert(m_engine && m_file);

  DBUG_RETURN(
      (error = m_file->ha_open(table, name, mode, test_if_locked, table_def)));
}

/**
  Close sequence handler.
  We didn't destroy share although the ref_count == 0,
  the cached values will be lost if we do that.

  @retval         0               Success
  @retval         ~0              Failure
*/
int ha_sequence::close(void) {
  DBUG_ENTER("ha_sequence::close");
  close_share(m_share);
  m_share = nullptr;
  DBUG_RETURN(m_file->ha_close());
}

ulonglong ha_sequence::table_flags() const {
  DBUG_ENTER("ha_sequence::table_flags");
  if (!m_file) {
    DBUG_RETURN(SEQUENCE_ENABLED_TABLE_FLAGS);
  }
  DBUG_RETURN((m_file->ha_table_flags() | HA_NO_AUTO_INCREMENT) &
              ~(HA_STATS_RECORDS_IS_EXACT | HA_REQUIRE_PRIMARY_KEY));
}

/**
  Create sequence table.

  @param[in]      name            Sequence table name.
  @param[in]      form            TABLE object
  @param[in]      create_info     create options
  @param[in]      table_def       dd::Table object that has been created

  @retval         0               success
  @retval         ~0              failure
*/
int ha_sequence::create(const char *name, TABLE *form,
                        HA_CREATE_INFO *create_info, dd::Table *table_def) {
  int error;
  DBUG_ENTER("ha_sequence::create");

  if (get_from_handler_file(name, ha_thd()->mem_root)) DBUG_RETURN(true);

  assert(m_engine && m_file);
  if ((error = m_file->ha_create(name, form, create_info, table_def))) goto err;

  DBUG_RETURN(false);

err:
  m_file->ha_delete_table(name, table_def);

  /* Delete the special file for sequence engine. */
  handler::delete_table(name, table_def);
  DBUG_RETURN(error);
}

static const char *ha_sequence_ext[] = {NullS};

/**
  Sequence engine special file extension

  @retval     String array      File extension array
*/
const char **ha_sequence::bas_ext() const {
  DBUG_ENTER("ha_sequence::bas_ext");
  DBUG_RETURN(ha_sequence_ext);
}

/**
  Drop sequence table object

  @param[in]    name        Sequence table name
  @param[in]    table_def   Table DD object

  @retval       0           Success
  @retval       ~0          Failure
*/
int ha_sequence::delete_table(const char *name, const dd::Table *table_def) {
  DBUG_ENTER("ha_sequence::delete_table");

  if (get_from_handler_file(name, ha_thd()->mem_root)) DBUG_RETURN(true);

  destroy_share(name);
  DBUG_RETURN(m_file->ha_delete_table(name, table_def));
}

/**
  Write sequence row.

  @param[in]      buf       table->record

  @retval         0         Success
  @retval         ~0        Failure
*/
int ha_sequence::write_row(uchar *buf) {
  int error;
  DBUG_ENTER("ha_sequence::write_row");
  assert(m_file && m_share);

  Share_locker_helper share_locker(m_share);
  Disable_binlog_helper disable_binlog(ha_thd());
  if ((error = m_share->enter_cond(ha_thd()))) DBUG_RETURN(error);
  m_share->invalidate();
  error = m_file->ha_write_row(buf);

  DBUG_EXECUTE_IF("sequence_write_error",
                  { error = HA_ERR_SEQUENCE_ACCESS_FAILURE; });

  DBUG_RETURN(error);
}

int ha_sequence::update_row(const uchar *old_data, uchar *new_data) {
  int error;
  DBUG_ENTER("ha_sequence::update_row");
  assert(m_file && m_share);

  /* Binlog will decided by m_file engine. so disable here */
  Share_locker_helper share_locker(m_share);
  Disable_binlog_helper disable_binlog(ha_thd());
  if ((error = m_share->enter_cond(ha_thd()))) DBUG_RETURN(error);
  m_share->invalidate();
  DBUG_RETURN(m_file->ha_update_row(old_data, new_data));
}

int ha_sequence::delete_row(const uchar *buf) {
  int error;
  DBUG_ENTER("ha_sequence::update_row");
  assert(m_file && m_share);

  /* Binlog will decided by m_file engine. so disable here */
  Share_locker_helper share_locker(m_share);
  Disable_binlog_helper disable_binlog(ha_thd());
  if ((error = m_share->enter_cond(ha_thd()))) DBUG_RETURN(error);
  m_share->invalidate();
  DBUG_RETURN(m_file->ha_delete_row(buf));
}

/**
  External lock

  @param[in]      thd         User connection
  @param[in]      lock_typ    Lock type

  @retval         0         Success
  @retval         ~0        Failure
*/
int ha_sequence::external_lock(THD *thd, int lock_type) {
  DBUG_ENTER("ha_sequence::external_lock");
  assert(m_file);
  DBUG_RETURN(m_file->ha_external_lock(thd, lock_type));
}

/**
  Scrolling the sequence cache by update the base table through autonomous
  transaction.

  @param[in]      table       TABLE object
  @param[in]      state       Sequence cache state
  @param[in]      helper      Sequence share locker

  @retval         0         Success
  @retval         ~0        Failure
*/
int ha_sequence::scroll_sequence(TABLE *table_arg,
                                 Sequence_cache_request cache_request,
                                 Share_locker_helper *helper, SR_ctx *sr_ctx) {
  DBUG_ENTER("ha_sequence::scroll_sequence");
  assert(cache_request == Sequence_cache_request::CACHE_REQUEST_NEED_LOAD ||
         cache_request == Sequence_cache_request::CACHE_REQUEST_ROUND_OUT);
  assert(m_share->m_cache_state != Sequence_cache_state::CACHE_STATE_LOADING);
  (void)(cache_request);
  helper->loading();

  /* Sequence transaction do the reload */
  Reload_sequence_cache_ctx ctx(ha_thd(), table_share);
  DBUG_RETURN(ctx.reload_sequence_cache(table_arg, (void *)sr_ctx));
}

/**
  Rename sequence table name.

  @param[in]      from            Old name of sequence table
  @param[in]      to              New name of sequence table
  @param[in]      from_table_def  Old dd::Table object
  @param[in/out]  to_table_def    New dd::Table object

  @retval         0               Success
  @retval         ~0              Failure
*/
int ha_sequence::rename_table(const char *from, const char *to,
                              const dd::Table *from_table_def,
                              dd::Table *to_table_def) {
  DBUG_ENTER("ha_sequence::rename_table");
  if (get_from_handler_file(from, ha_thd()->mem_root)) DBUG_RETURN(true);

  destroy_share(from);
  DBUG_RETURN(m_file->ha_rename_table(from, to, from_table_def, to_table_def));
}

/**
  Construtor of Bitmap_helper, backup current read/write bitmap set.
*/
ha_sequence::Bitmap_helper::Bitmap_helper(TABLE *table, Sequence_share *share)
    : m_table(table) {
  save_read_set = table->read_set;
  save_write_set = table->write_set;
  table->read_set = &(share->m_read_set);
  table->write_set = &(share->m_write_set);
}

/**
  Destrutor of Bitmap_helper, restore the read/write bitmap set.
*/
ha_sequence::Bitmap_helper::~Bitmap_helper() {
  m_table->read_set = save_read_set;
  m_table->write_set = save_write_set;
}

/**
  Report sequence error.
*/
void ha_sequence::print_error(int error, myf errflag) {
  THD *thd = ha_thd();
  const char *sequence_db = const_cast<char *>("???");
  const char *sequence_name = const_cast<char *>("???");
  DBUG_ENTER("ha_sequence::print_error");

  if (table_share) {
    sequence_db = table_share->db.str;
    sequence_name = table_share->table_name.str;
  }
  switch (error) {
    case HA_ERR_SEQUENCE_INVALID: {
      my_error(ER_SEQUENCE_INVALID, MYF(0), sequence_db, sequence_name);
      DBUG_VOID_RETURN;
    }
    case HA_ERR_SEQUENCE_RUN_OUT: {
      my_error(ER_SEQUENCE_RUN_OUT, MYF(0), sequence_db, sequence_name);
      DBUG_VOID_RETURN;
    }
    case HA_ERR_SEQUENCE_NOT_DEFINED: {
      my_error(ER_SEQUENCE_NOT_DEFINED, MYF(0), sequence_db, sequence_name);
      DBUG_VOID_RETURN;
    }
    case HA_ERR_SEQUENCE_SKIP_ERROR: {
      my_error(ER_SEQUENCE_SKIP_ERROR, MYF(0), sequence_db, sequence_name);
      DBUG_VOID_RETURN;
    }
    /*
      We has reported error using my_error, so this unkown error
      is used to prevent from repeating error definition
     */
    case HA_ERR_SEQUENCE_ACCESS_FAILURE: {
      if (thd->is_error()) DBUG_VOID_RETURN;

      my_error(ER_SEQUENCE_ACCESS_FAILURE, MYF(0), sequence_db, sequence_name);
      DBUG_VOID_RETURN;
    }
    case HA_ERR_SEQUENCE_RETRY_TIMEOUT: {
      my_error(ER_SEQUENCE_RETRY_TIMEOUT, MYF(0), sequence_db, sequence_name);
      DBUG_VOID_RETURN;
    }
  }
  if (m_file)
    m_file->print_error(error, errflag);
  else
    handler::print_error(error, errflag);

  DBUG_VOID_RETURN;
}

void ha_sequence::unbind_psi() {
  DBUG_ENTER("ha_sequence::unbind_psi");
  handler::unbind_psi();

  assert(m_file != nullptr);
  m_file->unbind_psi();
  DBUG_VOID_RETURN;
}

void ha_sequence::rebind_psi() {
  DBUG_ENTER("ha_sequence::rebind_psi");
  handler::rebind_psi();

  assert(m_file != nullptr);
  m_file->rebind_psi();
  DBUG_VOID_RETURN;
}

/**
  Sequence engine end.

  @param[in]    p         engine handlerton
  @param[in]    type      panic type

  @retval       0         success
  @retval       >0        failure
*/
static int sequence_end(handlerton *,
                        ha_panic_function type __attribute__((unused))) {
  DBUG_ENTER("sequence_end");
  if (sequence_engine_inited) {
    destroy_hash<std::string, Sequence_share *>(sequence_shares_hash);
    sequence_shares_hash = nullptr;
    mysql_mutex_destroy(&LOCK_sequence_open_shares_hash);
  }
  sequence_engine_inited = false;
  DBUG_RETURN(0);
}

/**
  Sequence support the atomic ddl by base engine.

  @param[in]    thd       User connection
*/
static void sequence_post_ddl(THD *thd) {
  handlerton *hton;
  plugin_ref plugin;
  DBUG_ENTER("sequence_post_ddl");
  if ((plugin = ha_resolve_sequence_base(nullptr)) &&
      (hton = plugin_data<handlerton *>(plugin))) {
    hton->post_ddl(thd);
  }
  if (plugin) plugin_unlock(nullptr, plugin);
  DBUG_VOID_RETURN;
}
/**
  Sequence engine init function.

  @param[in]    p         engine handlerton

  @retval       0         success
  @retval       >0        failure
*/
static int sequence_initialize(void *p) {
  handlerton *sequence_hton;
  DBUG_ENTER("sequence_initialize");

#ifdef HAVE_PSI_INTERFACE
  init_sequence_psi_keys();
#endif

  sequence_hton = (handlerton *)p;
  // TODO: functions

  sequence_hton->panic = sequence_end;
  sequence_hton->db_type = DB_TYPE_SEQUENCE_DB;
  sequence_hton->create = sequence_create_handler;
  sequence_hton->post_ddl = sequence_post_ddl;
  sequence_hton->flags = HTON_SUPPORTS_ATOMIC_DDL;
  mysql_mutex_init(key_LOCK_sequence_open_shares_hash,
                   &LOCK_sequence_open_shares_hash, MY_MUTEX_INIT_FAST);
  sequence_shares_hash =
      new Sequence_shares_hash(system_charset_info, key_memory_sequence_share);

  sequence_engine_inited = true;
  DBUG_RETURN(0);
}

/** Sequence storage engine declaration */
static struct st_mysql_storage_engine sequence_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

mysql_declare_plugin(sequence){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &sequence_storage_engine,
    sequence_plugin_name,
    sequence_plugin_author,
    "Sequence Storage Engine Helper",
    PLUGIN_LICENSE_GPL,
    sequence_initialize, /* Plugin Init */
    nullptr,             /* Plugin Check uninstall */
    nullptr,             /* Plugin Deinit */
    0x0100,              /* 1.0 */
    nullptr,             /* status variables */
    nullptr,             /* system variables */
    nullptr,             /* config options */
    0,                   /* flags */
} mysql_declare_plugin_end;

/// @} (end of group Sequence Engine)
