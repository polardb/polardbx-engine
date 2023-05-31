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

#ifndef PP_PPS_TABLE_IOSTAT_H_INCLUDED
#define PP_PPS_TABLE_IOSTAT_H_INCLUDED

#include "plugin/performance_point/pps.h"
#include "plugin/performance_point/pps_stat.h"

#include "my_inttypes.h"
#include "ppi/ppi_stat.h"

#include <atomic>
#include <memory>

#define INDEX_MASK (0xFFFFFFFF00000000)

#define TIME_MASK (~INDEX_MASK)

#define INDEX_POS (32)

#define MAX_RETRY (10)

extern ulonglong performance_point_iostat_retry_count;

/* The time lock data structure */
struct PPS_iostat_dirty_data {
  /* Array index */
  uint32 index;
  /* Time by second */
  uint32 time;
};

/**
  The atomic lock:

  Format:
    High 32 bits: The array index
    Low  32 bits: The time by second
*/
struct PPS_time_lock {
  std::atomic<uint64> m_state;

  PPS_time_lock() { reset_data(0, 0); }

  /**
    Reset state by index and time.

    @param[in]      index     Array index
    @param[in]      time      Time by second
  */
  void reset_data(size_t index, uint32 time) {
    assert(index < TIME_MASK);
    assert(time < TIME_MASK);

    uint64 value = ((uint64)index << INDEX_POS) + time;

    m_state.store(value);
  }

  /**
    Get state data

    @param[out]     data        Lock data structure
    @param[out]     dirty_value State value
  */
  void get_data(PPS_iostat_dirty_data *data, uint64 *dirty_value) {
    ulonglong value = m_state.load();
    data->time = value & TIME_MASK;
    data->index = value >> INDEX_POS;
    *dirty_value = value;
  }
};

/**
  Statistics include bytes.
*/
class PPS_bytes_stat : public PPS_base_stat {
 public:
  explicit PPS_bytes_stat() : PPS_base_stat(), m_bytes(0) {}
  void reset() {
    PPS_base_stat::reset();
    m_bytes = 0;
  }
  /* Aggregate the items */
  void aggregate_value(ulonglong count, ulonglong elapsed_time,
                       ulonglong bytes) {
    PPS_base_stat::aggregate_value(count, elapsed_time);
    m_bytes += bytes;
  }
  ulonglong get_bytes() { return m_bytes; }

  ulonglong get_max() { return PPS_base_stat::get_max(); }

 private:
  /* Volume by bytes. */
  ulonglong m_bytes;
};

/**
  Table IO_STATISTICS row properties.
*/
struct PPS_row_iostat {
  /* Interval time end (second) */
  uint32 end_time;
  PPS_bytes_stat data_file_read;
  PPS_bytes_stat data_file_write;

  void reset_data() {
    end_time = 0;
    data_file_read.reset();
    data_file_write.reset();
  }

  /* Aggregate data file read */
  void aggregate_data_read(ulonglong count, ulonglong elapsed_time,
                           ulonglong bytes) {
    data_file_read.aggregate_value(count, elapsed_time, bytes);
  }

  /* Aggregate data file write */
  void aggregate_data_write(ulonglong count, ulonglong elapsed_time,
                            ulonglong bytes) {
    data_file_write.aggregate_value(count, elapsed_time, bytes);
  }

  void init_time(uint32 time) { end_time = time; }

  uint32 get_time() { return end_time; }

  /* Copy data into the PPI_iostat_data structure */
  void copy_statistics_to(PPI_iostat_data *stat) {
    stat->time = end_time;
    stat->data_read = data_file_read.get_count();
    stat->data_read_time = data_file_read.get_sum();
    stat->data_read_max_time = data_file_read.get_max();
    stat->data_read_bytes = data_file_read.get_bytes();

    stat->data_write = data_file_write.get_count();
    stat->data_write_time = data_file_write.get_sum();
    stat->data_write_max_time = data_file_write.get_max();
    stat->data_write_bytes = data_file_write.get_bytes();
  }
};

/**
  TABLE IO_STATISTICS
*/
class PPS_table_iostat {
 public:
  explicit PPS_table_iostat(ulong volume_size)
      : m_inited(false), m_lock(), m_array_size(volume_size), m_rows(nullptr) {
    init();
  }

  /**
    Init the io statistics array.

    @retval       0           Success
    @retval       ~0          Failure
  */
  int init();
  /**
    Find the correct position according to the end_time, and aggregate the
    IO operation.

    @param[in]      io_type       Data file read/write
    @param[in]      count         IO operation count
    @param[in]      elapsed_time  IO operation time by us
    @param[in]      bytes         IO operation volume
    @param[in]      end_time      IO operation end time
  */
  void find_and_aggregate(PPI_IO_TYPE io_type, ulonglong count,
                          longlong elapsed_time, ulonglong bytes,
                          uint32 end_time);

  virtual ~PPS_table_iostat();

  bool enabled() { return m_inited; }

  /**
    Retrieve the data by array index.

    @param[out]     stat        PPI_iostat_data
    @param[in]      index       Array index
  */
  bool get_data(PPI_iostat_data *stat, int index);

 private:
  bool m_inited;

  /* The atomic lock */
  PPS_time_lock m_lock;

  /* IO statistic array */
  std::size_t m_array_size;
  PPS_row_iostat *m_rows;
};

typedef std::unique_ptr<PPS_row_iostat, PPS_raw_array_deleter<PPS_row_iostat>>
    Safe_iostat_array;

/* Register the table io_statistics */
extern void register_table_iostat();
extern void unregister_table_iostat();

/* Global table iostat variable */
extern PPS_table_iostat *global_table_iostat_instance;
#endif
