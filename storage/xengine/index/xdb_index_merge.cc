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

/* This C++ file's header file */
#include "./xdb_index_merge.h"

/* MySQL header files */
#include "sql_class.h"
#include "mysql/plugin.h"
#include "mysql/psi/mysql_file.h"
#include "sql_thd_internal_api.h"

/* MyX header files */
#include "./ha_xengine.h"
#include "./xdb_datadic.h"

namespace myx {

Xdb_index_merge::Xdb_index_merge(const char *const tmpfile_path,
                                 const ulonglong merge_buf_size,
                                 const ulonglong merge_combine_read_size,
                                 const xengine::util::Comparator *const comparator,
                                 int point_per_block, const size_t sample_mem_limit)
    : m_tmpfile_path(tmpfile_path), m_merge_buf_size(merge_buf_size),
      m_merge_combine_read_size(merge_combine_read_size),
      m_comparator(comparator), m_rec_buf_unsorted(nullptr),
      m_output_buf(nullptr),
      m_arena(new xengine::util::Arena()),
      merge_record_compare_check_dup ([this](const struct merge_record& lhs,
                                            const struct merge_record& rhs) {
        int res = this->merge_record_compare(lhs.block, rhs.block, m_comparator);
        if (res == 0) {
          this->merge_read_rec(lhs.block, &(this->m_dup_key), &(this->m_dup_val));
          this->m_has_dup_key = true;
        }
        return res < 0;
      }),
      m_point_per_block(point_per_block),
      m_sample_mem_limit(sample_mem_limit)
{}

Xdb_index_merge::~Xdb_index_merge() {
  /*
    Close tmp file, we don't need to worry about deletion, mysql handles it.
  */
  my_close(m_merge_file.fd, MYF(MY_WME));
}

int Xdb_index_merge::init() {
  /*
    Create a temporary merge file on disk to store sorted chunks during
    inplace index creation.
  */
  if (merge_file_create(m_merge_file)) {
    return HA_ERR_INTERNAL_ERROR;
  }


  /*
    Then, allocate buffer to store unsorted records before they are written
    to disk. They will be written to disk sorted. A sorted tree is used to
    keep track of the offset of each record within the unsorted buffer.
  */
  m_rec_buf_unsorted =
      std::shared_ptr<merge_buf_info>(new merge_buf_info(m_merge_buf_size));

  /*
    Allocate output buffer that will contain sorted block that is written to
    disk.
  */
  m_output_buf =
      std::shared_ptr<merge_buf_info>(new merge_buf_info(m_merge_buf_size));

  return HA_EXIT_SUCCESS;
}

/**
  Create a merge file in the given location.
*/
int Xdb_index_merge::merge_file_create(struct merge_file_info& file_info) {
//  DBUG_ASSERT(m_merge_file.fd == -1);

  int fd;
  /* If no path set for tmpfile, use mysql_tmpdir by default */
  if (m_tmpfile_path == nullptr) {
    fd = mysql_tmpfile("myx");
  } else {
    fd = mysql_tmpfile_path(m_tmpfile_path, "myx");
  }

  if (fd < 0) {
    return HA_ERR_INTERNAL_ERROR;
  }

  file_info.fd = fd;
  file_info.num_sort_buffers = 0;

  return HA_EXIT_SUCCESS;
}

/**
  Add record to offset tree (and unsorted merge buffer) in preparation for
  writing out to disk in sorted chunks.

  If buffer in memory is full, write the buffer out to disk sorted using the
  offset tree, and clear the tree. (Happens in merge_buf_write)
*/
int Xdb_index_merge::add(const xengine::common::Slice &key, const xengine::common::Slice &val, bool &inserted) {
  /* Adding a record after heap is already created results in error */
  DBUG_ASSERT(m_merge_min_heap.empty());

  /*
    Check if sort buffer is going to be out of space, if so write it
    out to disk in sorted order using offset tree.
  */
  const uint total_offset = XDB_MERGE_CHUNK_LEN +
                            m_rec_buf_unsorted->curr_offset +
                            XDB_MERGE_KEY_DELIMITER + XDB_MERGE_VAL_DELIMITER +
                            key.size() + val.size();
  if (total_offset >= m_rec_buf_unsorted->total_size) {
    /*
      If the offset tree is empty here, that means that the proposed key to
      add is too large for the buffer.
    */
    if (m_offset_array.empty()) {
      return HA_ERR_XENGINE_OUT_OF_SORTMEMORY;
    }

    if (merge_buf_write()) {
      XHANDLER_LOG(ERROR, "Error writing sort buffer to disk.");
      return HA_ERR_INTERNAL_ERROR;
    }
  }

  const ulonglong rec_offset = m_rec_buf_unsorted->curr_offset;

  if (m_has_dup_key) {
    inserted = false;
    return HA_EXIT_SUCCESS;
  }
  /*
    Store key and value in temporary unsorted in memory buffer pointed to by
    offset tree.
  */
  m_rec_buf_unsorted->store_key_value(key, val);

  m_offset_array.emplace_back(m_rec_buf_unsorted->block + rec_offset/*,
                             m_comparator*/);
  inserted = true;

  return HA_EXIT_SUCCESS;
}

/**
  Sort + write merge buffer chunk out to disk.
*/
int Xdb_index_merge::merge_buf_write() {
  DBUG_ASSERT(m_merge_file.fd != -1);
  DBUG_ASSERT(m_rec_buf_unsorted != nullptr);
  DBUG_ASSERT(m_output_buf != nullptr);
  DBUG_ASSERT(!m_offset_array.empty());

  /* Write actual chunk size to first 8 bytes of the merge buffer */
  merge_store_uint64(m_output_buf->block,
                     m_rec_buf_unsorted->curr_offset + XDB_MERGE_CHUNK_LEN);
  m_output_buf->curr_offset += XDB_MERGE_CHUNK_LEN;

  /*
    Iterate through the offset tree.  Should be ordered by the secondary key
    at this point.
  */
  // before iter, sort vector
  std::sort(m_offset_array.begin(), m_offset_array.end(),
            merge_record_compare_check_dup);

  // oversampling ratio is 4*4
  if (m_point_per_block > 0 &&
      m_arena->MemoryAllocatedBytes() < m_sample_mem_limit) {
    for (size_t i = 0; i < static_cast<size_t>(m_point_per_block); i++) {
      xengine::common::Slice key;
      xengine::common::Slice val;
      auto it = m_offset_array.begin() +
                (m_offset_array.size() / m_point_per_block) * i;
      merge_read_rec(it->block, &key, &val);
      m_sample.push_back(key.deep_copy(*m_arena));
    }
  }

  for (const auto &rec : m_offset_array) {
    DBUG_ASSERT(m_output_buf->curr_offset <= m_merge_buf_size);

    /* Read record from offset (should never fail) */
    xengine::common::Slice key;
    xengine::common::Slice val;
    merge_read_rec(rec.block, &key, &val);

    /* Store key and value into sorted output buffer */
    m_output_buf->store_key_value(key, val);
  }

  DBUG_ASSERT(m_output_buf->curr_offset <= m_output_buf->total_size);

  /*
    Write output buffer to disk.

    Need to position cursor to the chunk it needs to be at on filesystem
    then write into the respective merge buffer.
  */
  if (my_seek(m_merge_file.fd, m_merge_file.num_sort_buffers * m_merge_buf_size,
              SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
    // NO_LINT_DEBUG
    sql_print_error("Error seeking to location in merge file on disk.");
    return HA_ERR_INTERNAL_ERROR;
  }

  /*
    Add a file sync call here to flush the data out. Otherwise, the filesystem
    cache can flush out all of the files at the same time, causing a write
    burst.
  */
  if (my_write(m_merge_file.fd, m_output_buf->block,
               m_output_buf->total_size, MYF(MY_WME | MY_NABP)) ||
      mysql_file_sync(m_merge_file.fd, MYF(MY_WME))) {
    // NO_LINT_DEBUG
    sql_print_error("Error writing sorted merge buffer to disk.");
    return HA_ERR_INTERNAL_ERROR;
  }

  /* Increment merge file offset to track number of merge buffers written */
  m_merge_file.num_sort_buffers += 1;

  /* Reset everything for next run */
  merge_reset();

  return HA_EXIT_SUCCESS;
}

/**
  Prepare n-way merge of n sorted buffers on disk, using a heap sorted by
  secondary key records.
*/
int Xdb_index_merge::merge_heap_prepare() {
  DBUG_ASSERT(m_merge_min_heap.empty());

  /*
    If the offset tree is not empty, there are still some records that need to
    be written to disk. Write them out now.
  */
  if (!m_offset_array.empty() && merge_buf_write()) {
    return HA_ERR_INTERNAL_ERROR;
  }

  DBUG_ASSERT(m_merge_file.num_sort_buffers > 0);

  /*
    For an n-way merge, we need to read chunks of each merge file
    simultaneously.
  */
  ulonglong chunk_size =
      m_merge_combine_read_size / m_merge_file.num_sort_buffers;
  if (chunk_size >= m_merge_buf_size) {
    chunk_size = m_merge_buf_size;
  }

  /* Allocate buffers for each chunk */
  for (ulonglong i = 0; i < m_merge_file.num_sort_buffers; i++) {
    const auto entry = std::make_shared<merge_heap_entry>(m_comparator);

    /*
      Read chunk_size bytes from each chunk on disk, and place inside
      respective chunk buffer.
    */
    const size_t total_size =
        entry->prepare(m_merge_file.fd, i * m_merge_buf_size, chunk_size);

    if (total_size == (size_t)-1) {
      return HA_ERR_INTERNAL_ERROR;
    }

    /* Can reach this condition if an index was added on table w/ no rows */
    if (total_size - XDB_MERGE_CHUNK_LEN == 0) {
      break;
    }

    /* Read the first record from each buffer to initially populate the heap */
    if (entry->read_rec(&entry->key, &entry->val)) {
      // NO_LINT_DEBUG
      return HA_ERR_XENGINE_OUT_OF_SORTMEMORY;
    }

    m_merge_min_heap.push(std::move(entry));
  }

  return HA_EXIT_SUCCESS;
}

/**
  Create and/or iterate through keys in the merge heap.
*/
int Xdb_index_merge::next(xengine::common::Slice *const key,
                          xengine::common::Slice *const val) {
  /*
    If table fits in one sort buffer, we can optimize by writing
    the sort buffer directly through to the sstfilewriter instead of
    needing to create tmp files/heap to merge the sort buffers.

    If there are no sort buffer records (alters on empty tables),
    also exit here.
  */
  if (m_merge_file.num_sort_buffers == 0) {
    if (m_offset_array.empty()) {
      return HA_ERR_END_OF_FILE;
    }
    if (!m_buf_sorted) {
      std::sort(m_offset_array.begin(), m_offset_array.end(),
                merge_record_compare_check_dup);
      m_buf_sorted = true;
    }

    const auto rec = m_offset_array.begin();

    /* Read record from offset */
    merge_read_rec(rec->block, key, val);

    m_offset_array.erase(rec);
    return HA_EXIT_SUCCESS;
  }

  int res;

  /*
    If heap and heap chunk info are empty, we must be beginning the merge phase
    of the external sort. Populate the heap with initial values from each
    disk chunk.
  */
  if (m_merge_min_heap.empty()) {
    if ((res = merge_heap_prepare())) {
      // NO_LINT_DEBUG
      XHANDLER_LOG(ERROR, "Error during preparation of heap.");
      return res;
    }

    /*
      Return the first top record without popping, as we haven't put this
      inside the SST file yet.
    */
    merge_heap_top(key, val);
    return HA_EXIT_SUCCESS;
  }

  DBUG_ASSERT(!m_merge_min_heap.empty());
  return merge_heap_pop_and_get_next(key, val);
}

/**
  Get current top record from the heap.
*/
void Xdb_index_merge::merge_heap_top(xengine::common::Slice *const key,
                                     xengine::common::Slice *const val) {
  DBUG_ASSERT(!m_merge_min_heap.empty());

  const std::shared_ptr<merge_heap_entry> &entry = m_merge_min_heap.top();
  *key = entry->key;
  *val = entry->val;
}

/**
  Pops the top record, and uses it to read next record from the
  corresponding sort buffer and push onto the heap.

*/
int Xdb_index_merge::merge_heap_pop_and_get_next(xengine::common::Slice *const key,
                                                 xengine::common::Slice *const val) {
  /*
    Make a new reference to shared ptr so it doesn't get destroyed
    during pop(). We are going to push this entry back onto the heap.
  */
  const std::shared_ptr<merge_heap_entry> entry = m_merge_min_heap.top();
  m_merge_min_heap.pop();

  /*
    We are finished w/ current chunk if:
    current_offset + disk_offset == total_size

    Return without adding entry back onto heap.
    If heap is also empty, we must be finished with merge.
  */
  if (entry->chunk_info->is_chunk_finished()) {
    if (m_merge_min_heap.empty()) {
      return HA_ERR_END_OF_FILE;
    }

    merge_heap_top(key, val);
    return HA_EXIT_SUCCESS;
  }

  /*
    Make sure we haven't reached the end of the chunk.
  */
  DBUG_ASSERT(!entry->chunk_info->is_chunk_finished());

  /*
    If merge_read_rec fails, it means the either the chunk was cut off
    or we've reached the end of the respective chunk.
  */
  if (entry->read_rec(&entry->key, &entry->val)) {
    if (entry->read_next_chunk_from_disk(m_merge_file.fd)) {
      return HA_ERR_INTERNAL_ERROR;
    }

    /* Try reading record again, should never fail. */
    if (entry->read_rec(&entry->key, &entry->val)) {
      XHANDLER_LOG(ERROR, "chunk size is too small to read record.");
      return HA_ERR_XENGINE_OUT_OF_SORTMEMORY;
    }
  }

  /* Push entry back on to the heap w/ updated buffer + offset ptr */
  m_merge_min_heap.push(std::move(entry));

  /* Return the current top record on heap */
  merge_heap_top(key, val);
  return HA_EXIT_SUCCESS;
}

int Xdb_index_merge::merge_heap_entry::read_next_chunk_from_disk(File fd) {
  if (chunk_info->read_next_chunk_from_disk(fd)) {
    return HA_EXIT_FAILURE;
  }

  block = chunk_info->block;
  return HA_EXIT_SUCCESS;
}

int Xdb_index_merge::merge_buf_info::read_next_chunk_from_disk(File fd) {
  disk_curr_offset += curr_offset;

  if (my_seek(fd, disk_curr_offset, SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
    // NO_LINT_DEBUG
    sql_print_error("Error seeking to location in merge file on disk.");
    return HA_EXIT_FAILURE;
  }

  /* Overwrite the old block */
  const size_t bytes_read = my_read(fd, block, block_len, MYF(MY_WME));
  if (bytes_read == (size_t)-1) {
    // NO_LINT_DEBUG
    sql_print_error("Error reading merge file from disk.");
    return HA_EXIT_FAILURE;
  }

  curr_offset = 0;
  return HA_EXIT_SUCCESS;
}

/**
  Get records from offset within sort buffer and compare them.
  Sort by least to greatest.
*/
int Xdb_index_merge::merge_record_compare(
    const uchar *const a_block, const uchar *const b_block,
    const xengine::util::Comparator *const comparator) {
  return comparator->Compare(as_slice(a_block), as_slice(b_block));
}

/**
  Given an offset in a merge sort buffer, read out the keys + values.
  After this, block will point to the next record in the buffer.
**/
void Xdb_index_merge::merge_read_rec(const uchar *const block,
                                     xengine::common::Slice *const key,
                                     xengine::common::Slice *const val) {
  /* Read key at block offset into key slice and the value into value slice*/
  read_slice(key, block);
  read_slice(val, block + XDB_MERGE_REC_DELIMITER + key->size());
}

void Xdb_index_merge::read_slice(xengine::common::Slice *slice,
                                 const uchar *block_ptr) {
  uint64 slice_len;
  merge_read_uint64(&block_ptr, &slice_len);

  *slice = xengine::common::Slice(reinterpret_cast<const char *>(block_ptr), slice_len);
}

int Xdb_index_merge::merge_heap_entry::read_rec(xengine::common::Slice *const key,
                                                xengine::common::Slice *const val) {
  const uchar *block_ptr = block;
  const auto orig_offset = chunk_info->curr_offset;
  const auto orig_block = block;

  /* Read key at block offset into key slice and the value into value slice*/
  if (read_slice(key, &block_ptr) != 0) {
    return HA_EXIT_FAILURE;
  }

  chunk_info->curr_offset += (uintptr_t)block_ptr - (uintptr_t)block;
  block += (uintptr_t)block_ptr - (uintptr_t)block;

  if (read_slice(val, &block_ptr) != 0) {
    chunk_info->curr_offset = orig_offset;
    block = orig_block;
    return HA_EXIT_FAILURE;
  }

  chunk_info->curr_offset += (uintptr_t)block_ptr - (uintptr_t)block;
  block += (uintptr_t)block_ptr - (uintptr_t)block;

  return HA_EXIT_SUCCESS;
}

int Xdb_index_merge::merge_heap_entry::read_slice(xengine::common::Slice *const slice,
                                                  const uchar **block_ptr) {
  if (!chunk_info->has_space(XDB_MERGE_REC_DELIMITER)) {
    return HA_EXIT_FAILURE;
  }

  uint64 slice_len;
  merge_read_uint64(block_ptr, &slice_len);
  if (!chunk_info->has_space(XDB_MERGE_REC_DELIMITER + slice_len)) {
    return HA_EXIT_FAILURE;
  }

  *slice =
      xengine::common::Slice(reinterpret_cast<const char *>(*block_ptr), slice_len);
  *block_ptr += slice_len;
  return HA_EXIT_SUCCESS;
}

size_t Xdb_index_merge::merge_heap_entry::prepare(File fd, ulonglong f_offset,
                                                  ulonglong chunk_size) {
  chunk_info = std::make_shared<merge_buf_info>(chunk_size);
  const size_t res = chunk_info->prepare(fd, f_offset);
  if (res != (size_t)-1) {
    block = chunk_info->block + XDB_MERGE_CHUNK_LEN;
  }

  return res;
}

size_t Xdb_index_merge::merge_buf_info::prepare(File fd, ulonglong f_offset) {
  disk_start_offset = f_offset;
  disk_curr_offset = f_offset;

  /*
    Need to position cursor to the chunk it needs to be at on filesystem
    then read 'chunk_size' bytes into the respective chunk buffer.
  */
  if (my_seek(fd, f_offset, SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
    // NO_LINT_DEBUG
    sql_print_error("Error seeking to location in merge file on disk.");
    return (size_t)-1;
  }

  const size_t bytes_read = my_read(fd, block, total_size, MYF(MY_WME));
  if (bytes_read == (size_t)-1) {
    // NO_LINT_DEBUG
    sql_print_error("Error reading merge file from disk.");
    return (size_t)-1;
  }

  /*
    Read the first 8 bytes of each chunk, this gives us the actual
    size of each chunk.
  */
  const uchar *block_ptr = block;
  merge_read_uint64(&block_ptr, (uint64*)&total_size);
  curr_offset += XDB_MERGE_CHUNK_LEN;
  return total_size;
}

/* Store key and value w/ their respective delimiters at the given offset */
void Xdb_index_merge::merge_buf_info::store_key_value(
    const xengine::common::Slice &key, const xengine::common::Slice &val) {
  store_slice(key);
  store_slice(val);
}

void Xdb_index_merge::merge_buf_info::store_slice(const xengine::common::Slice &slice) {
  /* Store length delimiter */
  merge_store_uint64(&block[curr_offset], slice.size());

  /* Store slice data */
  memcpy(&block[curr_offset + XDB_MERGE_REC_DELIMITER], slice.data(),
         slice.size());

  curr_offset += slice.size() + XDB_MERGE_REC_DELIMITER;
}

void Xdb_index_merge::merge_reset() {
  /*
    Either error, or all values in the sort buffer have been written to disk,
    so we need to clear the offset tree.
  */
  m_offset_array.clear();

  /* Reset sort buffer block */
  if (m_rec_buf_unsorted && m_rec_buf_unsorted->block) {
    m_rec_buf_unsorted->curr_offset = 0;
  }

  /* Reset output buf */
  if (m_output_buf && m_output_buf->block) {
    m_output_buf->curr_offset = 0;
  }

}

void Xdb_index_merge::get_sample(std::vector<xengine::common::Slice>& sample) {
  sample.insert(sample.end(), m_sample.begin(), m_sample.end());
}

Xdb_index_merge::Bg_merge::~Bg_merge() {
  /*
    Close tmp file, we don't need to worry about deletion, mysql handles it.
  */
  for (size_t i = 0; i < m_sorted_files.size(); i++) {
    my_close(m_sorted_files[i].fd, MYF(MY_WME));
  }
}

int Xdb_index_merge::Bg_merge::init() {
  for (size_t i = 0; i < m_sorted_files.size(); i++) {
    if (m_xdb_merge->merge_file_create(m_sorted_files[i])) {
      return HA_ERR_INTERNAL_ERROR;
    }
  }

//  m_buf =
//      std::shared_ptr<merge_buf_info>(new merge_buf_info(m_buf_size));
  return HA_EXIT_SUCCESS;
}

int Xdb_index_merge::Bg_merge::prepare_entry(size_t part_id) {
  if (m_interrupt) {
    m_exit_interrupt = true;
    return HA_ERR_END_OF_FILE;
  }
  if (m_sorted_files[part_id].num_sort_buffers == m_read_sort_buffers[part_id])
    return HA_ERR_END_OF_FILE;

  const size_t total_size = m_entries[part_id].prepare(
      m_sorted_files[part_id].fd, m_read_sort_buffers[part_id]++ * m_buf_size,
      m_xdb_merge->m_merge_combine_read_size / (m_curr_part + 1));
  if (total_size == (size_t)-1) {
    return HA_ERR_INTERNAL_ERROR;
  }
  return HA_EXIT_SUCCESS;
}

int Xdb_index_merge::Bg_merge::next(xengine::common::Slice *const key,
                                    xengine::common::Slice *const val,
                                    size_t part_id) {
  if (part_id > m_curr_part) {
    return HA_ERR_END_OF_FILE;
  }

  int res = HA_EXIT_SUCCESS;
  if (m_entries[part_id].block == nullptr ||
      m_entries[part_id].chunk_info->is_chunk_finished()) {
    if ((res = prepare_entry(part_id)) != 0)
      return res;
  }

  if (m_entries[part_id].read_rec(&m_entries[part_id].key,
                                   &m_entries[part_id].val)) {
    if (m_entries[part_id].read_next_chunk_from_disk(m_sorted_files[part_id].fd)) {
      return HA_ERR_INTERNAL_ERROR;
    }

    /* Try reading record again, should never fail. */
    if (m_entries[part_id].read_rec(&m_entries[part_id].key,
                                     &m_entries[part_id].val)) {
      XHANDLER_LOG(ERROR, "chunk size is too small to read record.");
      return HA_ERR_XENGINE_OUT_OF_SORTMEMORY;
    }
  }

  *key = m_entries[part_id].key;
  *val = m_entries[part_id].val;

  return HA_EXIT_SUCCESS;
}

int Xdb_index_merge::Bg_merge::write_buf() {
  if (m_buf.curr_offset > XDB_MERGE_CHUNK_LEN) {
    /* Write actual chunk size to first 8 bytes of the merge buffer */
    merge_store_uint64(m_buf.block, m_buf.curr_offset);

    DBUG_ASSERT(m_buf.curr_offset <= m_buf.total_size);

    /*
      Write output buffer to disk.

      Need to position cursor to the chunk it needs to be at on filesystem
      then write into the respective merge buffer.
    */
    if (my_seek(m_sorted_files[m_curr_part].fd,
                m_sorted_files[m_curr_part].num_sort_buffers * m_buf_size,
                SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
      // NO_LINT_DEBUG
      sql_print_error("Error seeking to location in merge file on disk.");
      return HA_ERR_INTERNAL_ERROR;
    }

    /*
      Add a file sync call here to flush the data out. Otherwise, the filesystem
      cache can flush out all of the files at the same time, causing a write
      burst.
    */
    if (my_write(m_sorted_files[m_curr_part].fd, m_buf.block, m_buf.total_size, MYF(MY_WME | MY_NABP)) ||
        mysql_file_sync(m_sorted_files[m_curr_part].fd, MYF(MY_WME))) {
      // NO_LINT_DEBUG
      sql_print_error("Error writing sorted merge buffer to disk.");
      return HA_ERR_INTERNAL_ERROR;
    }

    /* Increment merge file offset to track number of merge buffers written */
    m_sorted_files[m_curr_part].num_sort_buffers += 1;

  }

  return HA_EXIT_SUCCESS;
}

int Xdb_index_merge::Bg_merge::xdb_merge_sort(std::vector<xengine::common::Slice>& sample,
                                              THD *mysql_thd) {

  DBUG_ASSERT(m_buf.curr_offset == 0);

  xengine::common::Slice merge_key;
  xengine::common::Slice merge_val;
  m_buf.curr_offset = XDB_MERGE_CHUNK_LEN;
  int res = HA_EXIT_SUCCESS;
  while ((res = m_xdb_merge->next(&merge_key, &merge_val)) == 0) {
    const uint total_offset = m_buf.curr_offset +
                              XDB_MERGE_KEY_DELIMITER + XDB_MERGE_VAL_DELIMITER +
                              merge_key.size() + merge_val.size();
    if (total_offset >= m_buf.total_size ||
        (m_curr_part < sample.size() &&
         m_xdb_merge->m_comparator->Compare(sample[m_curr_part], merge_key) < 0)) {
      if (m_interrupt) {
        m_exit_interrupt = true;
        return res;
      }

      if (write_buf()) {
        XHANDLER_LOG(ERROR, "Error writing sort buffer to disk.");
        return HA_ERR_INTERNAL_ERROR;
      }
      while (m_curr_part < sample.size() &&
             m_xdb_merge->m_comparator->Compare(sample[m_curr_part], merge_key) < 0) {
        m_curr_part++;
      }

      m_buf.curr_offset = XDB_MERGE_CHUNK_LEN;
    }

    m_buf.store_key_value(merge_key, merge_val);

    if (mysql_thd && mysql_thd->killed) {
      res = HA_ERR_QUERY_INTERRUPTED;
      break;
    }
  }

  if (res != HA_ERR_END_OF_FILE) {
    return res;
  }

  // write last sorted buf out
  if (write_buf()) {
    XHANDLER_LOG(ERROR, "Error writing sort buffer to disk.");
    return HA_ERR_INTERNAL_ERROR;
  }

  return HA_EXIT_SUCCESS;
}

int Xdb_index_merge::Bg_merge::merge(std::vector<xengine::common::Slice>& sample,
                                     THD *mysql_thd) {
  int res = xdb_merge_sort(sample, mysql_thd);
  return res;
}

} // namespace myx
