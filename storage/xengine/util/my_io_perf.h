/* Copyright (c) 2016, Percona and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef _io_perf_h_
#define _io_perf_h_

#include "atomic_stat.h"
#include <algorithm>

#ifdef	__cplusplus
extern "C" {
#endif

/* Per-table operation and IO statistics */

/* Struct used for IO performance counters within a single thread */
struct my_io_perf_struct {
  ulonglong bytes;
  ulonglong requests;
  ulonglong svc_time; /*!< time to do read or write operation */
  ulonglong svc_time_max;
  ulonglong wait_time; /*!< total time in the request array */
  ulonglong wait_time_max;
  ulonglong slow_ios; /*!< requests that take too long */

  /* Initialize a my_io_perf_t struct. */
  inline void init() {
    memset(this, 0, sizeof(*this));
  }

  /* Sets this to a - b in diff */
  inline void diff(const my_io_perf_struct& a, const my_io_perf_struct& b) {
    if (a.bytes > b.bytes)
      bytes = a.bytes - b.bytes;
    else
      bytes = 0;

    if (a.requests > b.requests)
      requests = a.requests - b.requests;
    else
      requests = 0;

    if (a.svc_time > b.svc_time)
      svc_time = a.svc_time - b.svc_time;
    else
      svc_time = 0;

    if (a.wait_time > b.wait_time)
      wait_time = a.wait_time - b.wait_time;
    else
      wait_time = 0;

    if (a.slow_ios > b.slow_ios)
      slow_ios = a.slow_ios - b.slow_ios;
    else
      slow_ios = 0;

    svc_time_max = std::max(a.svc_time_max, b.svc_time_max);
    wait_time_max = std::max(a.wait_time_max, b.wait_time_max);
  }

  /* Accumulates io perf values */
  inline void sum(const my_io_perf_struct& that) {
    bytes += that.bytes;
    requests += that.requests;
    svc_time += that.svc_time;
    svc_time_max = std::max(svc_time_max, that.svc_time_max);
    wait_time += that.wait_time;
    wait_time_max = std::max(wait_time_max, that.wait_time_max);
    slow_ios += that.slow_ios;
  }
};
typedef struct my_io_perf_struct my_io_perf_t;

/* Struct used for IO performance counters, shared among multiple threads */
struct my_io_perf_atomic_struct {
  atomic_stat<ulonglong> bytes;
  atomic_stat<ulonglong> requests;
  atomic_stat<ulonglong> svc_time; /*!< time to do read or write operation */
  atomic_stat<ulonglong> svc_time_max;
  atomic_stat<ulonglong> wait_time; /*!< total time in the request array */
  atomic_stat<ulonglong> wait_time_max;
  atomic_stat<ulonglong> slow_ios; /*!< requests that take too long */

  /* Initialize an my_io_perf_atomic_t struct. */
  inline void init() {
    bytes.clear();
    requests.clear();
    svc_time.clear();
    svc_time_max.clear();
    wait_time.clear();
    wait_time_max.clear();
    slow_ios.clear();
  }

  /* Accumulates io perf values using atomic operations */
  inline void sum(const my_io_perf_struct& that) {
    bytes.inc(that.bytes);
    requests.inc(that.requests);

    svc_time.inc(that.svc_time);
    wait_time.inc(that.wait_time);

    // In the unlikely case that two threads attempt to update the max
    // value at the same time, only the first will succeed.  It's possible
    // that the second thread would have set a larger max value, but we
    // would rather error on the side of simplicity and avoid looping the
    // compare-and-swap.
    svc_time_max.set_max_maybe(that.svc_time);
    wait_time_max.set_max_maybe(that.wait_time);

    slow_ios.inc(that.slow_ios);
  }

  /* These assignments allow for races. That is OK. */
  inline void set_maybe(const my_io_perf_struct& that) {
    bytes.set_maybe(that.bytes);
    requests.set_maybe(that.requests);
    svc_time.set_maybe(that.svc_time);
    svc_time_max.set_maybe(that.svc_time_max);
    wait_time.set_maybe(that.wait_time);
    wait_time_max.set_maybe(that.wait_time_max);
    slow_ios.set_maybe(that.slow_ios);
  }
};
typedef struct my_io_perf_atomic_struct my_io_perf_atomic_t;

/* struct used in per page type stats in IS.table_stats */
struct page_stats_struct {
  /*!< number read operations of all pages at given space*/
  ulong n_pages_read;

  /*!< number read operations of FIL_PAGE_INDEX pages at given space*/
  ulong n_pages_read_index;

  /*!< number read operations FIL_PAGE_TYPE_BLOB and FIL_PAGE_TYPE_ZBLOB
       and FIL_PAGE_TYPE_ZBLOB2 pages at given space*/
  ulong n_pages_read_blob;

  /*!< number write operations of all pages at given space*/
  ulong n_pages_written;

  /*!< number write operations of FIL_PAGE_INDEX pages at given space*/
  ulong n_pages_written_index;

  /*!< number write operations FIL_PAGE_TYPE_BLOB and FIL_PAGE_TYPE_ZBLOB
       and FIL_PAGE_TYPE_ZBLOB2 pages at given space*/
  ulong n_pages_written_blob;
};
typedef struct page_stats_struct page_stats_t;

/* struct used in per page type stats in IS.table_stats, atomic version */
struct page_stats_atomic_struct {
  atomic_stat<ulong> n_pages_read;
  atomic_stat<ulong> n_pages_read_index;
  atomic_stat<ulong> n_pages_read_blob;
  atomic_stat<ulong> n_pages_written;
  atomic_stat<ulong> n_pages_written_index;
  atomic_stat<ulong> n_pages_written_blob;
};
typedef struct page_stats_atomic_struct page_stats_atomic_t;

/** Compression statistics for a fil_space */
struct comp_stats_struct {
  /** Size of the compressed data on the page */
  int page_size;
  /** Current padding for compression */
  int padding;
  /** Number of page compressions */
  ulonglong compressed;
  /** Number of successful page compressions */
  ulonglong compressed_ok;
  /** Number of compressions in primary index */
  ulonglong compressed_primary;
  /** Number of successful compressions in primary index */
  ulonglong compressed_primary_ok;
  /** Number of page decompressions */
  ulonglong decompressed;
  /** Duration of page compressions */
  ulonglong compressed_time;
  /** Duration of successful page compressions */
  ulonglong compressed_ok_time;
  /** Duration of page decompressions */
  ulonglong decompressed_time;
  /** Duration of primary index page compressions */
  ulonglong compressed_primary_time;
  /** Duration of successful primary index page compressions */
  ulonglong compressed_primary_ok_time;
};

/** Compression statistics */
typedef struct comp_stats_struct comp_stats_t;

/** Compression statistics for a fil_space */
struct comp_stats_atomic_struct {
  /** Size of the compressed data on the page */
  atomic_stat<int> page_size;
  /** Current padding for compression */
  atomic_stat<int> padding;
  /** Number of page compressions */
  atomic_stat<ulonglong> compressed;
  /** Number of successful page compressions */
  atomic_stat<ulonglong> compressed_ok;
  /** Number of compressions in primary index */
  atomic_stat<ulonglong> compressed_primary;
  /** Number of successful compressions in primary index */
  atomic_stat<ulonglong> compressed_primary_ok;
  /** Number of page decompressions */
  atomic_stat<ulonglong> decompressed;
  /** Duration of page compressions */
  atomic_stat<ulonglong> compressed_time;
  /** Duration of successful page compressions */
  atomic_stat<ulonglong> compressed_ok_time;
  /** Duration of page decompressions */
  atomic_stat<ulonglong> decompressed_time;
  /** Duration of primary index page compressions */
  atomic_stat<ulonglong> compressed_primary_time;
  /** Duration of successful primary index page compressions */
  atomic_stat<ulonglong> compressed_primary_ok_time;
};

/** Compression statistics, atomic */
typedef struct comp_stats_atomic_struct comp_stats_atomic_t;

#ifdef	__cplusplus
}
#endif
#endif
