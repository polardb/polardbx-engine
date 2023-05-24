/* Copyright (c) 2000, 2018, Alibaba and/or its affiliates. All rights reserved.
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

/**
  @file

  Performance point plugin IO_STATISITICS table implementation.

*/
#include "plugin/performance_point/pps_table_iostat.h"

#include "my_sys.h"  // my_micro_time
#include "my_systime.h"

/**
  @addtogroup Performance Point plugin

  Implementation of Performance Point plugin.

  @{
*/

/* The max array size of IO statisitcs */
ulong performance_point_iostat_volume_size = 10000;
/* The time interval of aggregating IO statistics */
ulong performance_point_iostat_interval = 2;
/* Lock retry count that find correct position within iostat array */
ulonglong performance_point_iostat_retry_count = 0;

/* The global singleton instance of table IO_STATISTICS */
PPS_table_iostat *global_table_iostat_instance = nullptr;

/* Init global iostat instance  */
void register_table_iostat() {
  global_table_iostat_instance =
      new PPS_table_iostat(performance_point_iostat_volume_size);
}

void unregister_table_iostat() {
  assert(global_table_iostat_instance != nullptr);
  delete global_table_iostat_instance;
  global_table_iostat_instance = nullptr;
}

/**
  Init the io statistics array.

  @retval       0           Success
  @retval       ~0          Failure
*/
int PPS_table_iostat::init() {
  if (m_inited || m_array_size == 0) return 0;

  Safe_iostat_array m_array(
      allocate_raw_array<PPS_row_iostat>(m_array_size, MYF(MY_ZEROFILL)));

  if (m_array.get() == nullptr) return 1;

  m_rows = m_array.release();

  for (uint index = 0; index < m_array_size; index++) {
    m_rows[index].reset_data();
  }

  uint32 m_now = my_micro_time() / 1000000;
  m_lock.reset_data(0, m_now);
  m_rows[0].init_time(m_now);

  m_inited = true;
  return 0;
}

/* The destructor */
PPS_table_iostat::~PPS_table_iostat() {
  if (!m_inited) return;

  assert(m_rows);

  m_inited = false;
  destroy_raw_array(m_rows);
  m_rows = nullptr;
}

/**
  Retrieve the data by array index.

  @param[out]     stat        PPI_iostat_data
  @param[in]      index       Array index
*/
bool PPS_table_iostat::get_data(PPI_iostat_data *stat, int index) {
  if (!m_inited) return true;

  if (index >= 0 && (size_t)index < m_array_size) {
    m_rows[index].copy_statistics_to(stat);
    return false;
  }
  return true;
}

/**
  Find the correct position according to the end_time, and aggregate the
  IO operation.

  @param[in]      io_type       Data file read/write
  @param[in]      count         IO operation count
  @param[in]      elapsed_time  IO operation time by us
  @param[in]      bytes         IO operation volume
  @param[in]      end_time      IO operation end time
*/
void PPS_table_iostat::find_and_aggregate(PPI_IO_TYPE io_type, ulonglong count,
                                          longlong elapsed_time,
                                          ulonglong bytes, uint32 end_time) {
  PPS_iostat_dirty_data current_state;
  uint64 dirty_value;
  int retry_count = -1;
  ulonglong time_delta = performance_point_iostat_interval;

  if (!m_inited || elapsed_time < 0) return;

retry:
  retry_count++;
  if (retry_count > MAX_RETRY) {
    performance_point_iostat_retry_count += retry_count;
    return;
  }

  m_lock.get_data(&current_state, &dirty_value);

  /* The IO operation is in current time interval */
  if (current_state.time >= end_time) {
    if (io_type == PPI_IO_DATAFILE_READ)
      m_rows[current_state.index].aggregate_data_read(
          count, (ulonglong)elapsed_time, bytes);
    else if (io_type == PPI_IO_DATAFILE_WRITE)
      m_rows[current_state.index].aggregate_data_write(
          count, (ulonglong)elapsed_time, bytes);

    performance_point_iostat_retry_count += retry_count;
    return;

  } else {
    /* Iterate next position */
    uint32 index = current_state.index;
    uint32 time = current_state.time;

    uint32 new_time =
        ((((end_time - time) / time_delta) + 1) * time_delta) + time;

    uint32 new_index = (index + 1) % m_array_size;

    uint64 new_value = ((uint64)new_index << INDEX_POS) + new_time;

    bool pass = atomic_compare_exchange_strong(&m_lock.m_state, &dirty_value,
                                               new_value);

    if (pass) {
      assert(m_rows[new_index].get_time() == 0);
      m_rows[new_index].init_time(new_time);
      /* Reset next slot in advance */
      m_rows[(new_index + 1) % m_array_size].reset_data();
    }
    goto retry;
  }
}

/// @} (end of group Performance Point plugin)
