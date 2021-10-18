/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2016, Facebook, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#pragma once

/* MySQL header files */
#include "sql/log.h"
#include "handler.h"   /* handler */
//#include "./my_global.h" /* ulonglong */

/* C++ standard header files */
#include <queue>
#include <set>
#include <vector>

/* XENGINE header files */
#include "xengine/db.h"

/* MyX header files */
#include "./xdb_comparator.h"

namespace myx {

/*
  Length of delimiters used during inplace index creation.
*/
#define XDB_MERGE_CHUNK_LEN sizeof(size_t)
#define XDB_MERGE_REC_DELIMITER sizeof(size_t)
#define XDB_MERGE_KEY_DELIMITER XDB_MERGE_REC_DELIMITER
#define XDB_MERGE_VAL_DELIMITER XDB_MERGE_REC_DELIMITER

class Xdb_key_def;
class Xdb_tbl_def;

class Xdb_index_merge {

  Xdb_index_merge(const Xdb_index_merge &p) = delete;
  Xdb_index_merge &operator=(const Xdb_index_merge &p) = delete;

public:
  /* Information about temporary files used in external merge sort */
  struct merge_file_info {
    File fd = -1;           /* file descriptor */
    ulong num_sort_buffers; /* number of sort buffers in temp file */
  };

  /* Buffer for sorting in main memory. */
  struct merge_buf_info {
    /* heap memory allocated for main memory sort/merge  */
    //std::unique_ptr<uchar[]> block;
    uchar* block;
    const ulonglong
        block_len;         /* amount of data bytes allocated for block above */
    ulonglong curr_offset; /* offset of the record pointer for the block */
    ulonglong disk_start_offset; /* where the chunk starts on disk */
    ulonglong disk_curr_offset;  /* current offset on disk */
    ulonglong total_size;        /* total # of data bytes in chunk */

    void store_key_value(const xengine::common::Slice &key, const xengine::common::Slice &val)
        MY_ATTRIBUTE((__nonnull__));

    void store_slice(const xengine::common::Slice &slice) MY_ATTRIBUTE((__nonnull__));

    size_t prepare(File fd, ulonglong f_offset) MY_ATTRIBUTE((__nonnull__));

    int read_next_chunk_from_disk(File fd)
        MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

    inline bool is_chunk_finished() const {
      return curr_offset + disk_curr_offset - disk_start_offset == total_size;
    }

    inline bool has_space(uint64 needed) const {
      return curr_offset + needed <= block_len;
    }

    explicit merge_buf_info(const ulonglong merge_block_size)
        : block(nullptr), block_len(merge_block_size), curr_offset(0),
          disk_start_offset(0), disk_curr_offset(0),
          total_size(merge_block_size) {
      /* Will throw an exception if it runs out of memory here */
      //block = std::unique_ptr<uchar[]>(new uchar[merge_block_size]);
      block = static_cast<uchar *>(xengine::memory::base_malloc(merge_block_size, xengine::memory::ModId::kDDLSort));

      /* Initialize entire buffer to 0 to avoid valgrind errors */
      memset(block, 0, merge_block_size);
    }

    ~merge_buf_info()
    {
      xengine::memory::base_free(block);
      block = nullptr;
    }

    void clear() {
      curr_offset = 0;
      disk_start_offset = 0;
      disk_curr_offset = 0;
      total_size = block_len;
    }
  };

  /* Represents an entry in the heap during merge phase of external sort */
  struct merge_heap_entry {
    std::shared_ptr<merge_buf_info> chunk_info; /* pointer to buffer info */
    uchar *block; /* pointer to heap memory where record is stored */
    const xengine::util::Comparator *const comparator;
    xengine::common::Slice key; /* current key pointed to by block ptr */
    xengine::common::Slice val;

    size_t prepare(File fd, ulonglong f_offset, ulonglong chunk_size)
        MY_ATTRIBUTE((__nonnull__));

    int read_next_chunk_from_disk(File fd)
        MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

    int read_rec(xengine::common::Slice *const key, xengine::common::Slice *const val)
        MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

    int read_slice(xengine::common::Slice *const slice, const uchar **block_ptr)
        MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

    explicit merge_heap_entry(const xengine::util::Comparator *const comparator)
        : chunk_info(nullptr), block(nullptr), comparator(comparator) {}
  };

  struct merge_heap_comparator {
    bool operator()(const std::shared_ptr<merge_heap_entry> &lhs,
                    const std::shared_ptr<merge_heap_entry> &rhs) {
      return lhs->comparator->Compare(rhs->key, lhs->key) < 0;
    }
  };

  struct merge_record {
    uchar *block; /* points to offset of key in sort buffer */
//    const xengine::util::Comparator *const comparator;

//    bool operator<(const merge_record &record) const {
//      return merge_record_compare(this->block, record.block, comparator) < 0;
//    }

    merge_record(uchar *const block/*,
                 const xengine::util::Comparator *const comparator*/)
        : block(block)/*, comparator(comparator) */{}
  };

  class Bg_merge {
   private:
    std::shared_ptr<Xdb_index_merge> m_xdb_merge;
    std::vector<struct merge_file_info> m_sorted_files;
    merge_buf_info m_buf;
    std::vector<merge_heap_entry> m_entries;

    std::vector<ulong> m_read_sort_buffers;
    std::atomic<bool> m_interrupt{false};
    const ulonglong m_buf_size;
    size_t m_curr_part = 0;
    bool m_exit_interrupt = false;

    int write_buf();
    int xdb_merge_sort(std::vector<xengine::common::Slice>& sample,
                       THD *mysql_thd);
    int prepare_entry(size_t part_id);

   public:
    Bg_merge(const std::shared_ptr<Xdb_index_merge>& xdb_merge,
             size_t max_partition_num) : m_xdb_merge(xdb_merge),
                                         m_sorted_files(max_partition_num),
                                         m_buf(xdb_merge->m_merge_buf_size),
                                         m_read_sort_buffers(max_partition_num, 0),
                                         m_buf_size(xdb_merge->m_merge_buf_size) {
      for (size_t i = 0; i < max_partition_num; i++) {
        m_entries.emplace_back(xdb_merge->m_comparator);
      }
    }
    ~Bg_merge();
    int init() MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
    int merge(std::vector<xengine::common::Slice>& sample, THD *mysql_thd);
    int next(xengine::common::Slice *const key,
             xengine::common::Slice *const val, size_t part_id);
    void set_interrupt() { m_interrupt = true; }
    bool get_interrupt() { return m_exit_interrupt; }
  };

private:
  const char *m_tmpfile_path;
  const ulonglong m_merge_buf_size;
  const ulonglong m_merge_combine_read_size;
  const xengine::util::Comparator *m_comparator;
  struct merge_file_info m_merge_file;
  std::shared_ptr<merge_buf_info> m_rec_buf_unsorted;
  std::shared_ptr<merge_buf_info> m_output_buf;
  std::vector<merge_record> m_offset_array;
  std::priority_queue<std::shared_ptr<merge_heap_entry>,
                      std::vector<std::shared_ptr<merge_heap_entry>>,
                      merge_heap_comparator>
      m_merge_min_heap;
  bool m_buf_sorted = false;
  xengine::common::Slice m_dup_key;
  xengine::common::Slice m_dup_val;
  bool m_has_dup_key = false;
  // used for sample key deep_copy
  std::shared_ptr<xengine::util::Arena> m_arena;
  std::vector<xengine::common::Slice> m_sample;
  std::function<bool(const struct merge_record&, const struct merge_record&)>
      merge_record_compare_check_dup;
  int m_point_per_block;
  const size_t m_sample_mem_limit;

  static inline void merge_store_uint64(uchar *const dst, uint64 n) {
    memcpy(dst, &n, sizeof(n));
  }

  static inline void merge_read_uint64(const uchar **buf_ptr,
                                       uint64 *const dst) {
    DBUG_ASSERT(buf_ptr != nullptr);
    memcpy(dst, *buf_ptr, sizeof(uint64));
    *buf_ptr += sizeof(uint64);
  }

  static inline xengine::common::Slice as_slice(const uchar *block) {
    uint64 len;
    merge_read_uint64(&block, &len);

    return xengine::common::Slice(reinterpret_cast<const char *>(block), len);
  }

  static int merge_record_compare(const uchar *a_block, const uchar *b_block,
                                  const xengine::util::Comparator *const comparator)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  void merge_read_rec(const uchar *const block, xengine::common::Slice *const key,
                      xengine::common::Slice *const val) MY_ATTRIBUTE((__nonnull__));

  void read_slice(xengine::common::Slice *slice, const uchar *block_ptr)
      MY_ATTRIBUTE((__nonnull__));

public:
  Xdb_index_merge(const char *const tmpfile_path, const ulonglong merge_buf_size,
                  const ulonglong merge_combine_read_size,
                  const xengine::util::Comparator *const comparator,
                  int point_per_block = 0, const size_t sample_mem_limit = 0);
  ~Xdb_index_merge();

  int init() MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int merge_file_create(struct merge_file_info& file_info)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int add(const xengine::common::Slice &key, const xengine::common::Slice &val, bool &inserted)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int merge_buf_write() MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int next(xengine::common::Slice *const key, xengine::common::Slice *const val)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int merge_heap_prepare() MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  void merge_heap_top(xengine::common::Slice *key, xengine::common::Slice *val)
      MY_ATTRIBUTE((__nonnull__));

  int merge_heap_pop_and_get_next(xengine::common::Slice *const key,
                                  xengine::common::Slice *const val)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  void merge_reset();
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  void set_collect_sample(int point_per_block) {
    m_point_per_block = point_per_block;}

  void get_sample(std::vector<xengine::common::Slice>& sample);

  xengine::common::Slice get_dup_key() { return m_dup_key; }
  xengine::common::Slice get_dup_val() { return m_dup_val; }

};

} // namespace myx
