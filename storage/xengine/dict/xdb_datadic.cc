/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2012,2013 Monty Program Ab

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

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

/* This C++ file's header file */
#include "./xdb_datadic.h"
#include "handler_alter.h"

/* C++ standard header files */
#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <set>
#include <utility>
#include <vector>

/* MySQL header files */
#include "my_dir.h"
#include "m_ctype.h"
#include "my_bit.h"
#include "my_bitmap.h"
#include "my_stacktrace.h"
#include "myisampack.h"
#include "sql/field.h"
#include "sql/key.h"
#include "sql/mysqld.h"
#include "sql/sql_table.h"
#include "sql/table.h"
#include "sql/system_variables.h"
#include "sql/sql_class.h"
#include "sql/thd_raii.h"      // Disable_autocommit_guard
#include "sql/transaction.h"   // trans_commit

/* XENGINE includes */
#include "./port/likely.h"

/* XEngine handler header files */
#include "./ha_xengine_proto.h"
#include "./my_stacktrace.h"
#include "./xdb_cf_manager.h"
#include "./xdb_utils.h"
#include "./log_ddl.h"
#include "handler/xdb_i_s.h"

namespace myx {
extern ulong purge_acquire_lock_timeout;
extern Xdb_cf_manager cf_manager;
extern xengine::util::TransactionDB *xdb;

void get_mem_comparable_space(const CHARSET_INFO *cs,
                              const std::vector<uchar> **xfrm, size_t *xfrm_len,
                              size_t *mb_len);

/*
  Xdb_key_def class implementation
*/

Xdb_key_def::Xdb_key_def(uint indexnr_arg, uint keyno_arg,
                         xengine::db::ColumnFamilyHandle *cf_handle_arg,
                         uint16_t index_dict_version_arg, uchar index_type_arg,
                         uint16_t kv_format_version_arg, bool is_reverse_cf_arg,
                         bool is_auto_cf_arg, const std::string &name,
                         Xdb_index_stats _stats)
    : m_index_number(indexnr_arg), m_cf_handle(cf_handle_arg),
      m_index_dict_version(index_dict_version_arg),
      m_index_type(index_type_arg), m_kv_format_version(kv_format_version_arg),
      m_is_reverse_cf(is_reverse_cf_arg), m_is_auto_cf(is_auto_cf_arg),
      m_name(name), m_stats(_stats), m_pk_part_no(nullptr),
      m_pack_info(nullptr), m_keyno(keyno_arg), m_key_parts(0),
      m_prefix_extractor(nullptr), m_maxlength(0) // means 'not intialized'
{
  mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
  xdb_netbuf_store_index(m_index_number_storage_form, m_index_number);
  DBUG_ASSERT(m_cf_handle != nullptr);
}

Xdb_key_def::Xdb_key_def(const Xdb_key_def &k)
    : m_index_number(k.m_index_number), m_cf_handle(k.m_cf_handle),
      m_is_reverse_cf(k.m_is_reverse_cf), m_is_auto_cf(k.m_is_auto_cf),
      m_name(k.m_name), m_stats(k.m_stats), m_pk_part_no(k.m_pk_part_no),
      m_pack_info(k.m_pack_info), m_keyno(k.m_keyno),
      m_key_parts(k.m_key_parts), m_prefix_extractor(k.m_prefix_extractor),
      m_maxlength(k.m_maxlength) {
  mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
  xdb_netbuf_store_index(m_index_number_storage_form, m_index_number);
  if (k.m_pack_info) {
    const size_t size = sizeof(Xdb_field_packing) * k.m_key_parts;
    m_pack_info =
        reinterpret_cast<Xdb_field_packing *>(my_malloc(PSI_NOT_INSTRUMENTED, size, MYF(0)));
    memcpy(m_pack_info, k.m_pack_info, size);
  }

  if (k.m_pk_part_no) {
    const size_t size = sizeof(uint) * m_key_parts;
    m_pk_part_no = reinterpret_cast<uint *>(my_malloc(PSI_NOT_INSTRUMENTED, size, MYF(0)));
    memcpy(m_pk_part_no, k.m_pk_part_no, size);
  }
}

Xdb_key_def::~Xdb_key_def() {
  mysql_mutex_destroy(&m_mutex);

  my_free(m_pk_part_no);
  m_pk_part_no = nullptr;

  my_free(m_pack_info);
  m_pack_info = nullptr;
}

void Xdb_key_def::setup(const TABLE *const tbl,
                        const Xdb_tbl_def *const tbl_def) {
  DBUG_ASSERT(tbl != nullptr);
  DBUG_ASSERT(tbl_def != nullptr);

  /*
    Set max_length based on the table.  This can be called concurrently from
    multiple threads, so there is a mutex to protect this code.
  */
  const bool is_hidden_pk = (m_index_type == INDEX_TYPE_HIDDEN_PRIMARY);
  const bool hidden_pk_exists = table_has_hidden_pk(tbl);
  const bool secondary_key = (m_index_type == INDEX_TYPE_SECONDARY);
  if (!m_maxlength) {
    XDB_MUTEX_LOCK_CHECK(m_mutex);
    if (m_maxlength != 0) {
      XDB_MUTEX_UNLOCK_CHECK(m_mutex);
      return;
    }

    KEY *key_info = nullptr;
    KEY *pk_info = nullptr;
    if (!is_hidden_pk) {
      key_info = &tbl->key_info[m_keyno];
      if (!hidden_pk_exists)
        pk_info = &tbl->key_info[tbl->s->primary_key];
      m_name = std::string(key_info->name);
    } else {
      m_name = HIDDEN_PK_NAME;
    }

    if (secondary_key)
      m_pk_key_parts = hidden_pk_exists ? 1 : pk_info->actual_key_parts;
    else {
      pk_info = nullptr;
      m_pk_key_parts = 0;
    }

    // "unique" secondary keys support:
    m_key_parts = is_hidden_pk ? 1 : key_info->actual_key_parts;

    if (secondary_key) {
      /*
        In most cases, SQL layer puts PK columns as invisible suffix at the
        end of secondary key. There are cases where this doesn't happen:
        - unique secondary indexes.
        - partitioned tables.

        Internally, we always need PK columns as suffix (and InnoDB does,
        too, if you were wondering).

        The loop below will attempt to put all PK columns at the end of key
        definition.  Columns that are already included in the index (either
        by the user or by "extended keys" feature) are not included for the
        second time.
      */
      m_key_parts += m_pk_key_parts;
    }

    if (secondary_key)
      m_pk_part_no = reinterpret_cast<uint *>(
          my_malloc(PSI_NOT_INSTRUMENTED, sizeof(uint) * m_key_parts, MYF(0)));
    else
      m_pk_part_no = nullptr;

    const size_t size = sizeof(Xdb_field_packing) * m_key_parts;
    m_pack_info =
        reinterpret_cast<Xdb_field_packing *>(my_malloc(PSI_NOT_INSTRUMENTED, size, MYF(0)));

    size_t max_len = INDEX_NUMBER_SIZE;
    int unpack_len = 0;
    int max_part_len = 0;
    bool simulating_extkey = false;
    uint dst_i = 0;

    uint keyno_to_set = m_keyno;
    uint keypart_to_set = 0;
    bool can_extract_from_index = true;

    if (is_hidden_pk) {
      Field *field = nullptr;
      m_pack_info[dst_i].setup(this, field, keyno_to_set, 0, 0);
      m_pack_info[dst_i].m_unpack_data_offset = unpack_len;
      max_len += m_pack_info[dst_i].m_max_image_len;
      max_part_len = std::max(max_part_len, m_pack_info[dst_i].m_max_image_len);
      dst_i++;
    } else {
      KEY_PART_INFO *key_part = key_info->key_part;

      /* this loop also loops over the 'extended key' tail */
      for (uint src_i = 0; src_i < m_key_parts; src_i++, keypart_to_set++) {
        Field *const field = key_part ? key_part->field : nullptr;

        if (simulating_extkey && !hidden_pk_exists) {
          DBUG_ASSERT(secondary_key);
          /* Check if this field is already present in the key definition */
          bool found = false;
          for (uint j = 0; j < key_info->actual_key_parts; j++) {
            if (field->field_index ==
                    key_info->key_part[j].field->field_index &&
                key_part->length == key_info->key_part[j].length) {
              found = true;
              break;
            }
          }

          if (found) {
            key_part++;
            continue;
          }
        }

        if (field && field->real_maybe_null())
          max_len += 1; // NULL-byte

        if (!m_pack_info[dst_i].setup(this, field, keyno_to_set, keypart_to_set,
                                 key_part ? key_part->length : 0)) {
          can_extract_from_index = false;
        }

        m_pack_info[dst_i].m_unpack_data_offset = unpack_len;

        if (pk_info) {
          m_pk_part_no[dst_i] = -1;
          for (uint j = 0; j < m_pk_key_parts; j++) {
            if (field->field_index == pk_info->key_part[j].field->field_index) {
              m_pk_part_no[dst_i] = j;
              break;
            }
          }
        } else if (secondary_key && hidden_pk_exists) {
          /*
            The hidden pk can never be part of the sk.  So it is always
            appended to the end of the sk.
          */
          m_pk_part_no[dst_i] = -1;
          if (simulating_extkey)
            m_pk_part_no[dst_i] = 0;
        }

        max_len += m_pack_info[dst_i].m_max_image_len;

        max_part_len =
            std::max(max_part_len, m_pack_info[dst_i].m_max_image_len);

        key_part++;
        /*
          For "unique" secondary indexes, pretend they have
          "index extensions"
         */
        if (secondary_key && src_i + 1 == key_info->actual_key_parts) {
          simulating_extkey = true;
          if (!hidden_pk_exists) {
            keyno_to_set = tbl->s->primary_key;
            key_part = pk_info->key_part;
            keypart_to_set = (uint)-1;
          } else {
            keyno_to_set = tbl_def->m_key_count - 1;
            key_part = nullptr;
            keypart_to_set = 0;
          }
        }

        dst_i++;
      }
    }

    m_is_support_icp = can_extract_from_index;
    m_key_parts = dst_i;

    /* Initialize the memory needed by the stats structure */
    m_stats.m_distinct_keys_per_prefix.resize(get_key_parts());

    /* Cache prefix extractor for bloom filter usage later */
    xengine::common::Options opt = xdb_get_xengine_db()->GetOptions(get_cf());
    m_prefix_extractor = opt.prefix_extractor;

    /*
      This should be the last member variable set before releasing the mutex
      so that other threads can't see the object partially set up.
     */
    m_maxlength = max_len;

    XDB_MUTEX_UNLOCK_CHECK(m_mutex);
  }
}

/**
  Read a memcmp key part from a slice using the passed in reader.

  Returns -1 if field was null, 1 if error, 0 otherwise.
*/
int Xdb_key_def::read_memcmp_key_part(const TABLE *table_arg,
                                      Xdb_string_reader *reader,
                                      const uint part_num) const {
  /* It is impossible to unpack the column. Skip it. */
  if (m_pack_info[part_num].m_maybe_null) {
    const char *nullp;
    if (!(nullp = reader->read(1)))
      return 1;
    if (*nullp == 0) {
      /* This is a NULL value */
      return -1;
    } else {
      /* If NULL marker is not '0', it can be only '1'  */
      if (*nullp != 1)
        return 1;
    }
  }

  Xdb_field_packing *fpi = &m_pack_info[part_num];
  DBUG_ASSERT(table_arg->s != nullptr);

  bool is_hidden_pk_part = (part_num + 1 == m_key_parts) &&
                           (table_arg->s->primary_key == MAX_INDEXES);
  Field *field = nullptr;
  if (!is_hidden_pk_part)
    field = fpi->get_field_in_table(table_arg);
  if (fpi->m_skip_func(fpi, field, reader))
    return 1;

  return 0;
}

/**
  Get a mem-comparable form of Primary Key from mem-comparable form of this key

  @param
    pk_descr        Primary Key descriptor
    key             Index tuple from this key in mem-comparable form
    pk_buffer  OUT  Put here mem-comparable form of the Primary Key.

  @note
    It may or may not be possible to restore primary key columns to their
    mem-comparable form.  To handle all cases, this function copies mem-
    comparable forms directly.

    XEngine SE supports "Extended keys". This means that PK columns are present
    at the end of every key.  If the key already includes PK columns, then
    these columns are not present at the end of the key.

    Because of the above, we copy each primary key column.

  @todo
    If we checked crc32 checksums in this function, we would catch some CRC
    violations that we currently don't. On the other hand, there is a broader
    set of queries for which we would check the checksum twice.
*/
uint Xdb_key_def::get_primary_key_tuple(const TABLE *const table,
                                        const Xdb_key_def &pk_descr,
                                        const xengine::common::Slice *const key,
                                        uchar *const pk_buffer) const {
  DBUG_ASSERT(table != nullptr);
  DBUG_ASSERT(key != nullptr);
  DBUG_ASSERT(pk_buffer);

  uint size = 0;
  uchar *buf = pk_buffer;
  DBUG_ASSERT(m_pk_key_parts);

  /* Put the PK number */
  xdb_netbuf_store_index(buf, pk_descr.m_index_number);
  buf += INDEX_NUMBER_SIZE;
  size += INDEX_NUMBER_SIZE;

  const char *start_offs[MAX_REF_PARTS];
  const char *end_offs[MAX_REF_PARTS];
  int pk_key_part;
  uint i;
  Xdb_string_reader reader(key);

  // Skip the index number
  if ((!reader.read(INDEX_NUMBER_SIZE)))
    return XDB_INVALID_KEY_LEN;

  for (i = 0; i < m_key_parts; i++) {
    if ((pk_key_part = m_pk_part_no[i]) != -1) {
      start_offs[pk_key_part] = reader.get_current_ptr();
    }

    if (UNLIKELY(read_memcmp_key_part(table, &reader, i) > 0)) {
      return XDB_INVALID_KEY_LEN;
    }

    if (pk_key_part != -1) {
      end_offs[pk_key_part] = reader.get_current_ptr();
    }
  }

  for (i = 0; i < m_pk_key_parts; i++) {
    const uint part_size = end_offs[i] - start_offs[i];
    memcpy(buf, start_offs[i], end_offs[i] - start_offs[i]);
    buf += part_size;
    size += part_size;
  }

  return size;
}

uint Xdb_key_def::get_memcmp_sk_size(const TABLE *table,
                                     const xengine::common::Slice &key,
                                     uint *n_null_fields) const {
  DBUG_ASSERT(table != nullptr);
  DBUG_ASSERT(n_null_fields != nullptr);
  DBUG_ASSERT(m_keyno != table->s->primary_key);

  int res;
  Xdb_string_reader reader(&key);
  const char *start = reader.get_current_ptr();

  // Skip the index number
  if ((!reader.read(INDEX_NUMBER_SIZE)))
    return XDB_INVALID_KEY_LEN;

  for (uint i = 0; i < table->key_info[m_keyno].user_defined_key_parts; i++) {
    if ((res = read_memcmp_key_part(table, &reader, i)) > 0) {
      return XDB_INVALID_KEY_LEN;
    } else if (res == -1) {
      (*n_null_fields)++;
    }
  }

  return (reader.get_current_ptr() - start);
}

/**
  Get a mem-comparable form of Secondary Key from mem-comparable form of this
  key, without the extended primary key tail.

  @param
    key                Index tuple from this key in mem-comparable form
    sk_buffer     OUT  Put here mem-comparable form of the Secondary Key.
    n_null_fields OUT  Put number of null fields contained within sk entry
*/
uint Xdb_key_def::get_memcmp_sk_parts(const TABLE *table,
                                      const xengine::common::Slice &key,
                                      uchar *sk_buffer,
                                      uint *n_null_fields) const {
  DBUG_ASSERT(table != nullptr);
  DBUG_ASSERT(sk_buffer != nullptr);
  DBUG_ASSERT(n_null_fields != nullptr);
  DBUG_ASSERT(m_keyno != table->s->primary_key);

  uchar *buf = sk_buffer;
  const char *start = key.data();

  uint sk_memcmp_len = get_memcmp_sk_size(table, key, n_null_fields);
  if (sk_memcmp_len == XDB_INVALID_KEY_LEN) {
    return XDB_INVALID_KEY_LEN;
  }

  memcpy(buf, start, sk_memcmp_len);

  return sk_memcmp_len;
}

/**
  Convert index tuple into storage (i.e. mem-comparable) format

  @detail
    Currently this is done by unpacking into table->record[0] and then
    packing index columns into storage format.

  @param pack_buffer Temporary area for packing varchar columns. Its
                     size is at least max_storage_fmt_length() bytes.
*/

uint Xdb_key_def::pack_index_tuple(TABLE *tbl, uchar *pack_buffer,
                                   uchar *packed_tuple,
                                   const uchar *key_tuple,
                                   key_part_map &keypart_map) const{
  DBUG_ASSERT(tbl != nullptr);
  DBUG_ASSERT(pack_buffer != nullptr);
  DBUG_ASSERT(packed_tuple != nullptr);
  DBUG_ASSERT(key_tuple != nullptr);

  uchar* key_tuple_local = const_cast<uchar*>(key_tuple);

  /* We were given a record in KeyTupleFormat. First, save it to record */
  uint key_len = calculate_key_len(tbl, m_keyno, keypart_map);
  key_restore(tbl->record[0], key_tuple_local, &tbl->key_info[m_keyno], key_len);

  uint n_used_parts = my_count_bits(keypart_map);
  if (keypart_map == HA_WHOLE_KEY)
    n_used_parts = 0; // Full key is used

  /* Then, convert the record into a mem-comparable form */
  return pack_record(tbl, pack_buffer, tbl->record[0], packed_tuple, nullptr,
                     false, 0, n_used_parts);
}

/**
  @brief
    Check if "unpack info" data includes checksum.

  @detail
    This is used only by CHECK TABLE to count the number of rows that have
    checksums.
*/

bool Xdb_key_def::unpack_info_has_checksum(const xengine::common::Slice &unpack_info) {
  const uchar *ptr = (const uchar *)unpack_info.data();
  size_t size = unpack_info.size();

  // Skip unpack info if present.
  if (size >= XDB_UNPACK_HEADER_SIZE && ptr[0] == XDB_UNPACK_DATA_TAG) {
    const uint16 skip_len = xdb_netbuf_to_uint16(ptr + 1);
    SHIP_ASSERT(size >= skip_len);

    size -= skip_len;
    ptr += skip_len;
  }

  return (size == XDB_CHECKSUM_CHUNK_SIZE && ptr[0] == XDB_CHECKSUM_DATA_TAG);
}

/*
  @return Number of bytes that were changed
*/
int Xdb_key_def::successor(uchar *const packed_tuple, const uint &len) {
  DBUG_ASSERT(packed_tuple != nullptr);

  int changed = 0;
  uchar *p = packed_tuple + len - 1;
  for (; p > packed_tuple; p--) {
    changed++;
    if (*p != uchar(0xFF)) {
      *p = *p + 1;
      break;
    }
    *p = '\0';
  }
  return changed;
}

int Xdb_key_def::pack_field(
  Field *const             field,
  Xdb_field_packing       *pack_info,
  uchar *                  &tuple,
  uchar *const             packed_tuple,
  uchar *const             pack_buffer,
  Xdb_string_writer *const unpack_info,
  uint *const              n_null_fields) const
{
  if (pack_info->m_maybe_null) {
    DBUG_ASSERT(is_storage_available(tuple - packed_tuple, 1));
    if (field->is_real_null()) {
      /* NULL value. store '\0' so that it sorts before non-NULL values */
      *tuple++ = 0;
      /* That's it, don't store anything else */
      if (n_null_fields)
        (*n_null_fields)++;
      return HA_EXIT_SUCCESS;
    } else {
      /* Not a NULL value. Store '1' */
      *tuple++ = 1;
    }
  } else {
    /*
     * field->real_maybe_null() may be not correct, if a nullable column
     * be a part of new table pk after rebuild ddl. In that case,
     * field in old_table is nullable, while field in new_table is not nullable.
     * When we use old_field to pack memcmp key, there will be wrong.
     */
    if (field->is_real_null()) {
//      my_error(ER_INVALID_USE_OF_NULL, MYF(0));
      return HA_ERR_INVALID_NULL_ERROR;
    }
  }

  const bool create_unpack_info =
      (unpack_info &&  // we were requested to generate unpack_info
       pack_info->uses_unpack_info());  // and this keypart uses it
  Xdb_pack_field_context pack_ctx(unpack_info);

  // Set the offset for methods which do not take an offset as an argument
  DBUG_ASSERT(is_storage_available(tuple - packed_tuple,
                                   pack_info->m_max_image_len));

  pack_info->m_pack_func(pack_info, field, pack_buffer, &tuple, &pack_ctx);

  /* Make "unpack info" to be stored in the value */
  if (create_unpack_info) {
    pack_info->m_make_unpack_info_func(pack_info->m_charset_codec, field,
                                       &pack_ctx);
  }

  return HA_EXIT_SUCCESS;
}

/**
  Get index columns from the record and pack them into mem-comparable form.

  @param
    tbl                   Table we're working on
    record           IN   Record buffer with fields in table->record format
    pack_buffer      IN   Temporary area for packing varchars. The size is
                          at least max_storage_fmt_length() bytes.
    packed_tuple     OUT  Key in the mem-comparable form
    unpack_info      OUT  Unpack data
    unpack_info_len  OUT  Unpack data length
    n_key_parts           Number of keyparts to process. 0 means all of them.
    n_null_fields    OUT  Number of key fields with NULL value.

  @detail
    Some callers do not need the unpack information, they can pass
    unpack_info=nullptr, unpack_info_len=nullptr.

  @return
    Length of the packed tuple
*/

uint Xdb_key_def::pack_record(const TABLE *const tbl, uchar *const pack_buffer,
                              const uchar *const record,
                              uchar *const packed_tuple,
                              Xdb_string_writer *const unpack_info,
                              const bool &should_store_row_debug_checksums,
                              const longlong &hidden_pk_id, uint n_key_parts,
                              uint *const n_null_fields,
                              const TABLE *const altered_table) const {
  DBUG_ASSERT(tbl != nullptr);
  DBUG_ASSERT(pack_buffer != nullptr);
  DBUG_ASSERT(record != nullptr);
  DBUG_ASSERT(packed_tuple != nullptr);
  // Checksums for PKs are made when record is packed.
  // We should never attempt to make checksum just from PK values
  DBUG_ASSERT_IMP(should_store_row_debug_checksums,
                  (m_index_type == INDEX_TYPE_SECONDARY));

  uchar *tuple = packed_tuple;
  size_t unpack_len_pos = size_t(-1);
  const bool hidden_pk_exists = table_has_hidden_pk(tbl);

  xdb_netbuf_store_index(tuple, m_index_number);
  tuple += INDEX_NUMBER_SIZE;

  // If n_key_parts is 0, it means all columns.
  // The following includes the 'extended key' tail.
  // The 'extended key' includes primary key. This is done to 'uniqify'
  // non-unique indexes
  const bool use_all_columns = n_key_parts == 0 || n_key_parts == MAX_REF_PARTS;

  // If hidden pk exists, but hidden pk wasnt passed in, we can't pack the
  // hidden key part.  So we skip it (its always 1 part).
  if (hidden_pk_exists && !hidden_pk_id && use_all_columns)
    n_key_parts = m_key_parts - 1;
  else if (use_all_columns)
    n_key_parts = m_key_parts;

  if (n_null_fields)
    *n_null_fields = 0;

  if (unpack_info) {
    unpack_info->clear();
    unpack_info->write_uint8(XDB_UNPACK_DATA_TAG);
    unpack_len_pos = unpack_info->get_current_pos();
    // we don't know the total length yet, so write a zero
    unpack_info->write_uint16(0);
  }

  int ret = HA_EXIT_SUCCESS;
  for (uint i = 0; i < n_key_parts; i++) {
    // Fill hidden pk id into the last key part for secondary keys for tables
    // with no pk
    if (hidden_pk_exists && hidden_pk_id && i + 1 == n_key_parts) {
      m_pack_info[i].fill_hidden_pk_val(&tuple, hidden_pk_id);
      break;
    }

    Field *field = nullptr;
    uint field_index = 0;
    if (altered_table != nullptr) {
      field_index = m_pack_info[i].get_field_index_in_table(altered_table);
      field = tbl->field[field_index];
    } else {
      field = m_pack_info[i].get_field_in_table(tbl);
    }

    DBUG_ASSERT(field != nullptr);

    uint field_offset = field->ptr - tbl->record[0];
    uint null_offset = field->null_offset(tbl->record[0]);
    bool maybe_null = field->real_maybe_null();
    field->move_field(const_cast<uchar*>(record) + field_offset,
        maybe_null ? const_cast<uchar*>(record) + null_offset : nullptr,
        field->null_bit);
    // WARNING! Don't return without restoring field->ptr and field->null_ptr

    ret = pack_field(field, &m_pack_info[i], tuple, packed_tuple, pack_buffer,
                       unpack_info, n_null_fields);

    if (ret != HA_ERR_INVALID_NULL_ERROR) {
      DBUG_ASSERT(ret == HA_EXIT_SUCCESS);
    }

    // Restore field->ptr and field->null_ptr
    field->move_field(tbl->record[0] + field_offset,
                      maybe_null ? tbl->record[0] + null_offset : nullptr,
                      field->null_bit);
  }

  if (unpack_info) {
    const size_t len = unpack_info->get_current_pos();
    DBUG_ASSERT(len <= std::numeric_limits<uint16_t>::max());

    // Don't store the unpack_info if it has only the header (that is, there's
    // no meaningful content).
    // Primary Keys are special: for them, store the unpack_info even if it's
    // empty (provided m_maybe_unpack_info==true, see
    // ha_xengine::convert_record_to_storage_format)
    if (len == XDB_UNPACK_HEADER_SIZE &&
        m_index_type != Xdb_key_def::INDEX_TYPE_PRIMARY) {
      unpack_info->clear();
    } else {
      unpack_info->write_uint16_at(unpack_len_pos, len);
    }

    //
    // Secondary keys have key and value checksums in the value part
    // Primary key is a special case (the value part has non-indexed columns),
    // so the checksums are computed and stored by
    // ha_xengine::convert_record_to_storage_format
    //
    if (should_store_row_debug_checksums) {
      const uint32_t key_crc32 = crc32(0, packed_tuple, tuple - packed_tuple);
      const uint32_t val_crc32 =
          crc32(0, unpack_info->ptr(), unpack_info->get_current_pos());

      unpack_info->write_uint8(XDB_CHECKSUM_DATA_TAG);
      unpack_info->write_uint32(key_crc32);
      unpack_info->write_uint32(val_crc32);
    }
  }

  DBUG_ASSERT(is_storage_available(tuple - packed_tuple, 0));

  return tuple - packed_tuple;
}



/**
  Get index columns from the old table record and added default column,
  pack them into new table xengine mem-comparable key form.

  @param
    old_tbl               old Table we're working on
    record           IN   old Record buffer with fields in old_table->record format
    pack_buffer      IN   Temporary area for packing varchars. The size is
                          at least max_storage_fmt_length() bytes.
    packed_tuple     OUT  Key in the mem-comparable form
    unpack_info      OUT  Unpack data
    unpack_info_len  OUT  Unpack data length
    n_key_parts           Number of keyparts to process. 0 means all of them.
    n_null_fields    OUT  Number of key fields with NULL value.
    altered_table    IN   new table
    dict_info        IN   added new columns

  @detail
    Some callers do not need the unpack information, they can pass
    unpack_info=nullptr, unpack_info_len=nullptr.

  @return
    Length of the packed tuple
*/

int Xdb_key_def::pack_new_record(const TABLE *const old_tbl, uchar *const pack_buffer,
                              const uchar *const record,
                              uchar *const packed_tuple,
                              Xdb_string_writer *const unpack_info,
                              const bool &should_store_row_debug_checksums,
                              const longlong &hidden_pk_id, uint n_key_parts,
                              uint *const n_null_fields,
                              const TABLE *const altered_table,
                              const std::shared_ptr<Xdb_inplace_ddl_dict_info> dict_info, uint &size) const {
  DBUG_ASSERT(old_tbl != nullptr);
  DBUG_ASSERT(pack_buffer != nullptr);
  DBUG_ASSERT(record != nullptr);
  DBUG_ASSERT(packed_tuple != nullptr);
  // Checksums for PKs are made when record is packed.
  // We should never attempt to make checksum just from PK values
  DBUG_ASSERT_IMP(should_store_row_debug_checksums,
                  (m_index_type == INDEX_TYPE_SECONDARY));

  int ret = HA_EXIT_SUCCESS;

  uchar *tuple = packed_tuple;
  size_t unpack_len_pos = size_t(-1);
  const bool hidden_pk_exists = table_has_hidden_pk(altered_table);

  xdb_netbuf_store_index(tuple, m_index_number);
  tuple += INDEX_NUMBER_SIZE;

  // If n_key_parts is 0, it means all columns.
  // The following includes the 'extended key' tail.
  // The 'extended key' includes primary key. This is done to 'uniqify'
  // non-unique indexes
  const bool use_all_columns = n_key_parts == 0 || n_key_parts == MAX_REF_PARTS;

  // If hidden pk exists, but hidden pk wasnt passed in, we can't pack the
  // hidden key part.  So we skip it (its always 1 part).
  if (hidden_pk_exists && !hidden_pk_id && use_all_columns)
    n_key_parts = m_key_parts - 1;
  else if (use_all_columns)
    n_key_parts = m_key_parts;

  if (n_null_fields)
    *n_null_fields = 0;

  if (unpack_info) {
    unpack_info->clear();
    unpack_info->write_uint8(XDB_UNPACK_DATA_TAG);
    unpack_len_pos = unpack_info->get_current_pos();
    // we don't know the total length yet, so write a zero
    unpack_info->write_uint16(0);
  }

  // check field is/not old table field
  Xdb_field_encoder *encoder_arr = dict_info->m_encoder_arr;
  uint *col_map = dict_info->m_col_map;
  uint old_col_index = 0;
  uint add_col_index = 0;
  Field *new_field = nullptr;

  for (uint i = 0; i < n_key_parts; i++) {
    // Fill hidden pk id into the last key part for secondary keys for tables
    // with no pk
    if (hidden_pk_exists && hidden_pk_id && i + 1 == n_key_parts) {
      m_pack_info[i].fill_hidden_pk_val(&tuple, hidden_pk_id);
      break;
    }

    new_field = m_pack_info[i].get_field_in_table(altered_table);
    DBUG_ASSERT(new_field != nullptr);

    uint col_index = new_field->field_index;
    old_col_index = col_map[col_index];

    // field that maybe come from old table or new table
    Field *field = nullptr;
    if (old_col_index == uint(-1)) {
      /* column is from new added columns, we can use default value */
      ret = pack_field(new_field, &m_pack_info[i], tuple, packed_tuple,
                       pack_buffer, unpack_info, n_null_fields);
      if (ret && ret != HA_ERR_INVALID_NULL_ERROR) {
        __XHANDLER_LOG(ERROR, "pack field failed, error code: %d", ret);
        return ret;
      }
    } else {
      field = old_tbl->field[old_col_index];
      uint field_offset = field->ptr - old_tbl->record[0];
      uint null_offset = field->null_offset(old_tbl->record[0]);
      bool maybe_null = field->real_maybe_null();
      field->move_field(
          const_cast<uchar *>(record) + field_offset,
          maybe_null ? const_cast<uchar *>(record) + null_offset : nullptr,
          field->null_bit);
      // WARNING! Don't return without restoring field->ptr and field->null_ptr

      ret = pack_field(field, &m_pack_info[i], tuple, packed_tuple, pack_buffer,
                       unpack_info, n_null_fields);
      if (ret && ret != HA_ERR_INVALID_NULL_ERROR) {
        __XHANDLER_LOG(ERROR, "pack field failed, error code: %d", ret);
        return ret;
      }

      // Restore field->ptr and field->null_ptr
      field->move_field(old_tbl->record[0] + field_offset,
                        maybe_null ? old_tbl->record[0] + null_offset : nullptr,
                        field->null_bit);
    }
  }

  if (unpack_info) {
    const size_t len = unpack_info->get_current_pos();
    DBUG_ASSERT(len <= std::numeric_limits<uint16_t>::max());

    // Don't store the unpack_info if it has only the header (that is, there's
    // no meaningful content).
    // Primary Keys are special: for them, store the unpack_info even if it's
    // empty (provided m_maybe_unpack_info==true, see
    // ha_xengine::convert_record_to_storage_format)
    if (len == XDB_UNPACK_HEADER_SIZE &&
        m_index_type != Xdb_key_def::INDEX_TYPE_PRIMARY) {
      unpack_info->clear();
    } else {
      unpack_info->write_uint16_at(unpack_len_pos, len);
    }

    //
    // Secondary keys have key and value checksums in the value part
    // Primary key is a special case (the value part has non-indexed columns),
    // so the checksums are computed and stored by
    // ha_xengine::convert_record_to_storage_format
    //
    if (should_store_row_debug_checksums) {
      const uint32_t key_crc32 = crc32(0, packed_tuple, tuple - packed_tuple);
      const uint32_t val_crc32 =
          crc32(0, unpack_info->ptr(), unpack_info->get_current_pos());

      unpack_info->write_uint8(XDB_CHECKSUM_DATA_TAG);
      unpack_info->write_uint32(key_crc32);
      unpack_info->write_uint32(val_crc32);
    }
  }

  DBUG_ASSERT(is_storage_available(tuple - packed_tuple, 0));

  size = tuple - packed_tuple;

  return HA_EXIT_SUCCESS;
}


/**
  Pack the hidden primary key into mem-comparable form.

  @param
    tbl                   Table we're working on
    hidden_pk_id     IN   New value to be packed into key
    packed_tuple     OUT  Key in the mem-comparable form

  @return
    Length of the packed tuple
*/

uint Xdb_key_def::pack_hidden_pk(const longlong &hidden_pk_id,
                                 uchar *const packed_tuple) const {
  DBUG_ASSERT(packed_tuple != nullptr);

  uchar *tuple = packed_tuple;
  xdb_netbuf_store_index(tuple, m_index_number);
  tuple += INDEX_NUMBER_SIZE;
  DBUG_ASSERT(m_key_parts == 1);
  DBUG_ASSERT(is_storage_available(tuple - packed_tuple,
                                   m_pack_info[0].m_max_image_len));

  m_pack_info[0].fill_hidden_pk_val(&tuple, hidden_pk_id);

  DBUG_ASSERT(is_storage_available(tuple - packed_tuple, 0));
  return tuple - packed_tuple;
}

bool Xdb_key_def::write_dd_index(dd::Index *dd_index, uint64_t table_id) const
{
  if (nullptr != dd_index) {
    // If there is no user-defined primary key, while there is one user-defined
    // unique key on non-nullable column(s), the key will be treated as PRIMARY
    // key, but type of coresponding dd::Index object isn't dd::Index::IT_PRIMARY.
    // To recover key type correctly, we need persist index_type as metadata.
    int index_type = m_index_type;
    dd::Properties &p = dd_index->se_private_data();
    return p.set(dd_index_key_strings[DD_INDEX_TABLE_ID], table_id) ||
           p.set(dd_index_key_strings[DD_INDEX_SUBTABLE_ID], m_index_number) ||
           p.set(dd_index_key_strings[DD_INDEX_TYPE], index_type) ||
           write_dd_index_ext(p);
  }
  return true;
}

bool Xdb_key_def::write_dd_index_ext(dd::Properties& prop) const
{
  int index_version = m_index_dict_version, kv_version = m_kv_format_version;
  int flags = 0;
  if (m_is_reverse_cf) flags |= REVERSE_CF_FLAG;
  if (m_is_auto_cf) flags |= AUTO_CF_FLAG;

  return prop.set(dd_index_key_strings[DD_INDEX_VERSION_ID], index_version) ||
         prop.set(dd_index_key_strings[DD_INDEX_KV_VERSION], kv_version) ||
         prop.set(dd_index_key_strings[DD_INDEX_FLAGS], flags);
}


bool Xdb_key_def::verify_dd_index(const dd::Index *dd_index, uint64_t table_id)
{
  if (nullptr == dd_index)  return true;

  // check primary key name
  if ((dd_index->type() == dd::Index::IT_PRIMARY) !=
      !my_strcasecmp(system_charset_info,
                 dd_index->name().c_str(), primary_key_name))
    return true;

  const dd::Properties& p = dd_index->se_private_data();
  uint64_t table_id_in_dd = dd::INVALID_OBJECT_ID;
  uint32_t subtable_id=0;
  int index_type = 0;

  return !p.exists(dd_index_key_strings[DD_INDEX_TABLE_ID]) ||
         !p.exists(dd_index_key_strings[DD_INDEX_SUBTABLE_ID]) ||
         !p.exists(dd_index_key_strings[DD_INDEX_TYPE]) ||
         p.get(dd_index_key_strings[DD_INDEX_TABLE_ID], &table_id_in_dd) ||
         p.get(dd_index_key_strings[DD_INDEX_SUBTABLE_ID], &subtable_id) ||
         p.get(dd_index_key_strings[DD_INDEX_TYPE], &index_type) ||
         (table_id_in_dd == dd::INVALID_OBJECT_ID) ||
         (table_id != table_id_in_dd) ||
         verify_dd_index_ext(p) ||
         (nullptr == cf_manager.get_cf(subtable_id));
}

bool Xdb_key_def::verify_dd_index_ext(const dd::Properties& prop)
{
  int index_version_id = 0, kv_version = 0, flags = 0;
  return !prop.exists(dd_index_key_strings[DD_INDEX_VERSION_ID]) ||
         !prop.exists(dd_index_key_strings[DD_INDEX_KV_VERSION]) ||
         !prop.exists(dd_index_key_strings[DD_INDEX_FLAGS]) ||
         prop.get(dd_index_key_strings[DD_INDEX_VERSION_ID], &index_version_id) ||
         prop.get(dd_index_key_strings[DD_INDEX_KV_VERSION], &kv_version) ||
         prop.get(dd_index_key_strings[DD_INDEX_FLAGS], &flags);
}

/*
 * From Field_blob::make_sort_key in MySQL 5.7
 */
void xdb_pack_blob(Xdb_field_packing *const fpi, Field *const field,
    uchar *const buf MY_ATTRIBUTE((__unused__)), uchar **dst,
    Xdb_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__))) {
  DBUG_ASSERT(fpi != nullptr);
  DBUG_ASSERT(field != nullptr);
  DBUG_ASSERT(dst != nullptr);
  DBUG_ASSERT(*dst != nullptr);
  DBUG_ASSERT(field->real_type() == MYSQL_TYPE_TINY_BLOB ||
              field->real_type() == MYSQL_TYPE_MEDIUM_BLOB ||
              field->real_type() == MYSQL_TYPE_LONG_BLOB ||
              field->real_type() == MYSQL_TYPE_BLOB ||
              field->real_type() == MYSQL_TYPE_JSON);

  int64_t length = fpi->m_max_image_len;
  Field_blob* const field_blob = static_cast<Field_blob *const>(field);
  const CHARSET_INFO *field_charset = field_blob->charset();

  uchar *blob = nullptr;
  int64_t blob_length = field_blob->get_length();

  if (!blob_length && field_charset->pad_char == 0) {
    memset(*dst, 0, length);
  } else {
    if (field_charset == &my_charset_bin) {
      /*
        Store length of blob last in blob to shorter blobs before longer blobs
      */
      length -= field_blob->pack_length_no_ptr();
      uchar *pos = *dst + length;
      int64_t key_length = blob_length < length ? blob_length : length;

      switch (field_blob->pack_length_no_ptr()) {
      case 1:
        *pos = (char)key_length;
        break;
      case 2:
        mi_int2store(pos, key_length);
        break;
      case 3:
        mi_int3store(pos, key_length);
        break;
      case 4:
        mi_int4store(pos, key_length);
        break;
      }
    }

    // Copy a ptr
    memcpy(&blob, field->ptr + field_blob->pack_length_no_ptr(), sizeof(char *));

    //weights for utf8mb4_900 and gbk are not same length for diffent characters
    //for utf8mb4_0900_ai_ci prefix text index, see also xdb_pack_with_varchar_encoding
    if (fpi->m_prefix_index_flag &&
        (field_charset == &my_charset_utf8mb4_0900_ai_ci ||
         field_charset == &my_charset_gbk_chinese_ci ||
         field_charset == &my_charset_gbk_bin)) {
      DBUG_ASSERT(fpi->m_weights > 0);
      int input_length = std::min<size_t>(
          blob_length,
          field_charset->cset->charpos(
              field_charset, pointer_cast<const char *>(blob),
              pointer_cast<const char *>(blob) + blob_length, fpi->m_weights));

      blob_length = blob_length <= input_length ? blob_length : input_length;
    }

    if (blob == nullptr) {
      //in case, for sql_mode is not in the strict mode, if doing nullable to not null
      //ddl-change, field null value will change to empty string, but blob address is nullptr.
      //so, we use dummy address for padding
      blob = pointer_cast<uchar*>(&blob);
      blob_length = 0;
    }

    blob_length =
        field_charset->coll->strnxfrm(field_charset, *dst, length, length, blob,
                                      blob_length, MY_STRXFRM_PAD_TO_MAXLEN);
    DBUG_ASSERT(blob_length == length);
  }

  *dst += fpi->m_max_image_len;
}

#if !defined(DBL_EXP_DIG)
#define DBL_EXP_DIG (sizeof(double) * 8 - DBL_MANT_DIG)
#endif

static void change_double_for_sort(double nr, uchar *to) {
  uchar *tmp = to;
  if (nr == 0.0) { /* Change to zero string */
    tmp[0] = (uchar)128;
    memset(tmp + 1, 0, sizeof(nr) - 1);
  } else {
#ifdef WORDS_BIGENDIAN
    memcpy(tmp, &nr, sizeof(nr));
#else
    {
      uchar *ptr = (uchar *)&nr;
#if defined(__FLOAT_WORD_ORDER) && (__FLOAT_WORD_ORDER == __BIG_ENDIAN)
      tmp[0] = ptr[3];
      tmp[1] = ptr[2];
      tmp[2] = ptr[1];
      tmp[3] = ptr[0];
      tmp[4] = ptr[7];
      tmp[5] = ptr[6];
      tmp[6] = ptr[5];
      tmp[7] = ptr[4];
#else
      tmp[0] = ptr[7];
      tmp[1] = ptr[6];
      tmp[2] = ptr[5];
      tmp[3] = ptr[4];
      tmp[4] = ptr[3];
      tmp[5] = ptr[2];
      tmp[6] = ptr[1];
      tmp[7] = ptr[0];
#endif
    }
#endif
    if (tmp[0] & 128) /* Negative */
    {                 /* make complement */
      uint i;
      for (i = 0; i < sizeof(nr); i++)
        tmp[i] = tmp[i] ^ (uchar)255;
    } else { /* Set high and move exponent one up */
      ushort exp_part =
          (((ushort)tmp[0] << 8) | (ushort)tmp[1] | (ushort)32768);
      exp_part += (ushort)1 << (16 - 1 - DBL_EXP_DIG);
      tmp[0] = (uchar)(exp_part >> 8);
      tmp[1] = (uchar)exp_part;
    }
  }
}

void xdb_pack_double(Xdb_field_packing *const fpi, Field *const field,
    uchar *const buf MY_ATTRIBUTE((__unused__)), uchar **dst,
    Xdb_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__))) {
  DBUG_ASSERT(fpi != nullptr);
  DBUG_ASSERT(field != nullptr);
  DBUG_ASSERT(dst != nullptr);
  DBUG_ASSERT(*dst != nullptr);
  DBUG_ASSERT(field->real_type() == MYSQL_TYPE_DOUBLE);

  const size_t length = fpi->m_max_image_len;
  const uchar *ptr = field->ptr;
  uchar *to = *dst;

  double nr;
#ifdef WORDS_BIGENDIAN
  if (field->table->s->db_low_byte_first) {
    float8get(&nr, ptr);
  } else
#endif
    doubleget(&nr, ptr);
  if (length < 8) {
    uchar buff[8];
    change_double_for_sort(nr, buff);
    memcpy(to, buff, length);
  } else
    change_double_for_sort(nr, to);

  *dst += length;
}

#if !defined(FLT_EXP_DIG)
#define FLT_EXP_DIG (sizeof(float) * 8 - FLT_MANT_DIG)
#endif

void xdb_pack_float(Xdb_field_packing *const fpi, Field *const field,
    uchar *const buf MY_ATTRIBUTE((__unused__)), uchar **dst,
    Xdb_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__))) {
  DBUG_ASSERT(fpi != nullptr);
  DBUG_ASSERT(field != nullptr);
  DBUG_ASSERT(dst != nullptr);
  DBUG_ASSERT(*dst != nullptr);
  DBUG_ASSERT(field->real_type() == MYSQL_TYPE_FLOAT);

  const size_t length = fpi->m_max_image_len;
  const uchar *ptr = field->ptr;
  uchar *to = *dst;

  DBUG_ASSERT(length == sizeof(float));
  float nr;

#ifdef WORDS_BIGENDIAN
  if (field->table->s->db_low_byte_first) {
    float4get(&nr, ptr);
  } else
#endif
    memcpy(&nr, ptr, length < sizeof(float) ? length : sizeof(float));

  uchar *tmp = to;
  if (nr == (float)0.0) { /* Change to zero string */
    tmp[0] = (uchar)128;
    memset(tmp + 1, 0, length < sizeof(nr) - 1 ? length : sizeof(nr) - 1);
  } else {
#ifdef WORDS_BIGENDIAN
    memcpy(tmp, &nr, sizeof(nr));
#else
    tmp[0] = ptr[3];
    tmp[1] = ptr[2];
    tmp[2] = ptr[1];
    tmp[3] = ptr[0];
#endif
    if (tmp[0] & 128) /* Negative */
    {                 /* make complement */
      uint i;
      for (i = 0; i < sizeof(nr); i++)
        tmp[i] = (uchar)(tmp[i] ^ (uchar)255);
    } else {
      ushort exp_part =
          (((ushort)tmp[0] << 8) | (ushort)tmp[1] | (ushort)32768);
      exp_part += (ushort)1 << (16 - 1 - FLT_EXP_DIG);
      tmp[0] = (uchar)(exp_part >> 8);
      tmp[1] = (uchar)exp_part;
    }
  }

  *dst += length;
}

/*
  Function of type xdb_index_field_pack_t
*/

void xdb_pack_with_make_sort_key(
    Xdb_field_packing *const fpi, Field *const field,
    uchar *const buf MY_ATTRIBUTE((__unused__)), uchar **dst,
    Xdb_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__))) {
  DBUG_ASSERT(fpi != nullptr);
  DBUG_ASSERT(field != nullptr);
  DBUG_ASSERT(dst != nullptr);
  DBUG_ASSERT(*dst != nullptr);

  const int max_len = fpi->m_max_image_len;
  field->make_sort_key(*dst, max_len);
  *dst += max_len;
}


/** Function of type xdb_index_field_pack_t for utf8mb4_0900_aici MYSQL_TYPE_STRING
 *  for that m_max_image_len is max size of weights, it's not suitable for utf8mb4_0900_ai_ci, every character maybe occupy different bytes for weight.
 *  see also Field_string::make_sort_key
 */
void xdb_pack_with_make_sort_key_utf8mb4_0900(Xdb_field_packing *fpi, Field *field,
                                 uchar *buf __attribute__((__unused__)),
                                 uchar **dst,
                                 Xdb_pack_field_context *pack_ctx
                                 __attribute__((__unused__)))
{
  DBUG_ASSERT(fpi != nullptr);
  DBUG_ASSERT(field != nullptr);
  DBUG_ASSERT(dst != nullptr);
  DBUG_ASSERT(*dst != nullptr);

  const CHARSET_INFO *cs= fpi->m_charset;
  const int max_len= fpi->m_max_image_len;

  DBUG_ASSERT(fpi->m_weights > 0);
  DBUG_ASSERT(fpi->m_weights <= field->char_length());

  if (fpi->m_prefix_index_flag == false) {
    field->make_sort_key(*dst, max_len);
  } else {
    uint nweights = fpi->m_weights;

    uint field_length = field->field_length;
    size_t input_length = std::min<size_t>(
        field_length,
        cs->cset->charpos(cs, pointer_cast<const char *>(field->ptr),
                          pointer_cast<const char *>(field->ptr) + field_length,
                          nweights));

    TABLE *table = ((Field_string *)field)->table;
    if (cs->pad_attribute == NO_PAD &&
        !(table->in_use->variables.sql_mode & MODE_PAD_CHAR_TO_FULL_LENGTH)) {
      /*
        Our CHAR default behavior is to strip spaces. For PAD SPACE collations,
        this doesn't matter, for but NO PAD, we need to do it ourselves here.
      */
      input_length =
          cs->cset->lengthsp(cs, (const char *)(field->ptr), input_length);
    }

    int xfrm_len = cs->coll->strnxfrm(cs, *dst, fpi->m_max_strnxfrm_len,
                                      field->char_length(), field->ptr,
                                      input_length, MY_STRXFRM_PAD_TO_MAXLEN);
    DBUG_ASSERT(xfrm_len <= max_len);
  }

  *dst += max_len;
}

/*
  Function of type xdb_index_field_pack_t for gbk MYSQL_TYPE_STRING
*/
void xdb_pack_with_make_sort_key_gbk(Xdb_field_packing *fpi, Field *field,
                                 uchar *buf __attribute__((__unused__)),
                                 uchar **dst,
                                 Xdb_pack_field_context *pack_ctx
                                 __attribute__((__unused__)))
{
  DBUG_ASSERT(fpi != nullptr);
  DBUG_ASSERT(field != nullptr);
  DBUG_ASSERT(dst != nullptr);
  DBUG_ASSERT(*dst != nullptr);

  const CHARSET_INFO *cs= fpi->m_charset;
  const int max_len= fpi->m_max_image_len;
  uchar *src= field->ptr;
  uint srclen= field->max_display_length();
  uchar *se= src + srclen;
  uchar *dst_buf= *dst;
  uchar *de= *dst + max_len;
  size_t xfrm_len_total= 0;
  uint nweights = field->char_length();

  DBUG_ASSERT(fpi->m_weights > 0);
  DBUG_ASSERT(fpi->m_weights <= nweights);

  if (fpi->m_prefix_index_flag == true) {
    nweights= fpi->m_weights;
  }

  for (; dst_buf < de && src < se && nweights; nweights--)
  {
    if (cs->cset->ismbchar(cs, (const char*)src, (const char*)se))
    {
      cs->coll->strnxfrm(cs, dst_buf, 2, 1, src, 2, MY_STRXFRM_NOPAD_WITH_SPACE);
      src += 2;
      srclen -= 2;
    }
    else
    {
      cs->coll->strnxfrm(cs, dst_buf, 1, 1, src, 1, MY_STRXFRM_NOPAD_WITH_SPACE);
      src += 1;
      srclen -= 1;

      /* add 0x20 for 2bytes comparebytes */
      *(dst_buf + 1)= *dst_buf;
      *dst_buf= 0x20;
    }

    dst_buf += 2;
    xfrm_len_total += 2;
  }

  int tmp __attribute__((unused))=
    cs->coll->strnxfrm(cs,
                       dst_buf, max_len-xfrm_len_total, nweights,
                       src, srclen,
                       MY_STRXFRM_PAD_TO_MAXLEN);

  DBUG_ASSERT(((int)xfrm_len_total + tmp) == max_len);

  *dst += max_len;
}

/*
  Compares two keys without unpacking

  @detail
  @return
    0 - Ok. column_index is the index of the first column which is different.
          -1 if two kes are equal
    1 - Data format error.
*/
int Xdb_key_def::compare_keys(const xengine::common::Slice *key1,
                              const xengine::common::Slice *key2,
                              std::size_t *const column_index) const {
  DBUG_ASSERT(key1 != nullptr);
  DBUG_ASSERT(key2 != nullptr);
  DBUG_ASSERT(column_index != nullptr);

  // the caller should check the return value and
  // not rely on column_index being valid
  *column_index = 0xbadf00d;

  Xdb_string_reader reader1(key1);
  Xdb_string_reader reader2(key2);

  // Skip the index number
  if ((!reader1.read(INDEX_NUMBER_SIZE)))
    return HA_EXIT_FAILURE;

  if ((!reader2.read(INDEX_NUMBER_SIZE)))
    return HA_EXIT_FAILURE;

  for (uint i = 0; i < m_key_parts; i++) {
    const Xdb_field_packing *const fpi = &m_pack_info[i];
    if (fpi->m_maybe_null) {
      const auto nullp1 = reader1.read(1);
      const auto nullp2 = reader2.read(1);

      if (nullp1 == nullptr || nullp2 == nullptr) {
        return HA_EXIT_FAILURE;
      }

      if (*nullp1 != *nullp2) {
        *column_index = i;
        return HA_EXIT_SUCCESS;
      }

      if (*nullp1 == 0) {
        /* This is a NULL value */
        continue;
      }
    }

    const auto before_skip1 = reader1.get_current_ptr();
    const auto before_skip2 = reader2.get_current_ptr();
    DBUG_ASSERT(fpi->m_skip_func);
    if (fpi->m_skip_func(fpi, nullptr, &reader1))
      return HA_EXIT_FAILURE;
    if (fpi->m_skip_func(fpi, nullptr, &reader2))
      return HA_EXIT_FAILURE;
    const auto size1 = reader1.get_current_ptr() - before_skip1;
    const auto size2 = reader2.get_current_ptr() - before_skip2;
    if (size1 != size2) {
      *column_index = i;
      return HA_EXIT_SUCCESS;
    }

    if (memcmp(before_skip1, before_skip2, size1) != 0) {
      *column_index = i;
      return HA_EXIT_SUCCESS;
    }
  }

  *column_index = m_key_parts;
  return HA_EXIT_SUCCESS;
}

/*
  @brief
    Given a zero-padded key, determine its real key length

  @detail
    Fixed-size skip functions just read.
*/

size_t Xdb_key_def::key_length(const TABLE *const table,
                               const xengine::common::Slice &key) const {
  DBUG_ASSERT(table != nullptr);

  Xdb_string_reader reader(&key);

  if ((!reader.read(INDEX_NUMBER_SIZE)))
    return size_t(-1);

  for (uint i = 0; i < m_key_parts; i++) {
    const Xdb_field_packing *fpi = &m_pack_info[i];
    const Field *field = nullptr;
    if (m_index_type != INDEX_TYPE_HIDDEN_PRIMARY)
      field = fpi->get_field_in_table(table);
    if (fpi->m_skip_func(fpi, field, &reader))
      return size_t(-1);
  }
  return key.size() - reader.remaining_bytes();
}

int Xdb_key_def::unpack_field(
    Xdb_field_packing *const fpi,
    Field *const             field,
    Xdb_string_reader*       reader,
    const uchar *const       default_value,
    Xdb_string_reader*       unp_reader) const
{
  if (fpi->m_maybe_null) {
    const char *nullp;
    if (!(nullp = reader->read(1))) {
      return HA_EXIT_FAILURE;
    }

    if (*nullp == 0) {
      /* Set the NULL-bit of this field */
      field->set_null();
      /* Also set the field to its default value */
      memcpy(field->ptr, default_value, field->pack_length());
      return HA_EXIT_SUCCESS;
    } else if (*nullp == 1) {
      field->set_notnull();
    } else {
      return HA_EXIT_FAILURE;
    }
  }

  return fpi->m_unpack_func(fpi, field, field->ptr, reader, unp_reader);
}

bool Xdb_key_def::table_has_unpack_info(TABLE *const table) const {
    bool has_unpack = false;

    for (uint i = 0; i < m_key_parts; i++) {
        if (m_pack_info[i].uses_unpack_info()) {
            has_unpack = true;
            break;
        }
    }

    return has_unpack;
}


/*
  Take mem-comparable form and unpack it to Table->record
  This is a fast unpacking for record with unpack_info is null

  @detail
    not all indexes support this

  @return
    UNPACK_SUCCESS - Ok
    UNPACK_FAILURE - Data format error.
*/

int Xdb_key_def::unpack_record_1(TABLE *const table, uchar *const buf,
                               const xengine::common::Slice *const packed_key,
                               const xengine::common::Slice *const unpack_info,
                               const bool &verify_row_debug_checksums) const {
  Xdb_string_reader reader(packed_key);

  const bool is_hidden_pk = (m_index_type == INDEX_TYPE_HIDDEN_PRIMARY);
  const bool hidden_pk_exists = table_has_hidden_pk(table);
  const bool secondary_key = (m_index_type == INDEX_TYPE_SECONDARY);
  // There is no checksuming data after unpack_info for primary keys, because
  // the layout there is different. The checksum is verified in
  // ha_xengine::convert_record_from_storage_format instead.
  DBUG_ASSERT_IMP(!secondary_key, !verify_row_debug_checksums);

  DBUG_ASSERT(!unpack_info);

  // Skip the index number
  if ((!reader.read(INDEX_NUMBER_SIZE))) {
    return HA_EXIT_FAILURE;
  }

  for (uint i = 0; i < m_key_parts; i++) {
    Xdb_field_packing *const fpi = &m_pack_info[i];

    /*
      Hidden pk field is packed at the end of the secondary keys, but the SQL
      layer does not know about it. Skip retrieving field if hidden pk.
    */
    if ((secondary_key && hidden_pk_exists && i + 1 == m_key_parts) ||
        is_hidden_pk) {
      DBUG_ASSERT(fpi->m_unpack_func);
      if (fpi->m_skip_func(fpi, nullptr, &reader)) {
        return HA_EXIT_FAILURE;
      }
      continue;
    }

    Field *const field = fpi->get_field_in_table(table);

    if (fpi->m_unpack_func) {
      /* It is possible to unpack this column. Do it. */

      uint field_offset = field->ptr - table->record[0];
      uint null_offset = field->null_offset();
      bool maybe_null = field->real_maybe_null();
      field->move_field(buf + field_offset,
                        maybe_null ? buf + null_offset : nullptr,
                        field->null_bit);
      // WARNING! Don't return without restoring field->ptr and field->null_ptr
      int res = unpack_field(fpi, field, &reader,
                             table->s->default_values + field_offset,
                             nullptr);

      // Restore field->ptr and field->null_ptr
      field->move_field(table->record[0] + field_offset,
                        maybe_null ? table->record[0] + null_offset : nullptr,
                        field->null_bit);

      if (res) {
        return res;
      }
    } else {
      /* It is impossible to unpack the column. Skip it. */
      if (fpi->m_maybe_null) {
        const char *nullp;
        if (!(nullp = reader.read(1)))
          return HA_EXIT_FAILURE;
        if (*nullp == 0) {
          /* This is a NULL value */
          continue;
        }
        /* If NULL marker is not '0', it can be only '1'  */
        if (*nullp != 1)
          return HA_EXIT_FAILURE;
      }
      if (fpi->m_skip_func(fpi, field, &reader))
        return HA_EXIT_FAILURE;
    }
  }

  if (reader.remaining_bytes())
    return HA_EXIT_FAILURE;

  return HA_EXIT_SUCCESS;
}

/** used to unpack new-pk record during onlineDDL, new_pk_record format
    is set in convert_new_record_from_old_record.
    see also unpack_record, which used to unpack secondary index
@param[in] table, new table object
@param[in/out] buf, new table record
@param[in] packed_key, xengine-format key
@param[in] packed_value, xengine-format value
@param[in] dict_info, new xdb_table_def
@return  success/failure
*/

int Xdb_key_def::unpack_record_pk(
    TABLE *const table, uchar *const buf,
    const xengine::common::Slice *const packed_key,
    const xengine::common::Slice *const packed_value,
    const std::shared_ptr<Xdb_inplace_ddl_dict_info> &dict_info) const
{
  Xdb_string_reader reader(packed_key);
  Xdb_string_reader value_reader =
      Xdb_string_reader::read_or_empty(packed_value);

  // Skip the index number
  if ((!reader.read(INDEX_NUMBER_SIZE))) {
    __XHANDLER_LOG(ERROR, "unexpected error record,table_name:%s",
                   table->s->table_name.str);
    return HA_ERR_INTERNAL_ERROR;
  }

  // Skip instant columns attribute
  value_reader.read(XENGINE_RECORD_HEADER_LENGTH);

  // Skip null-bytes
  uint32_t null_bytes_in_rec = dict_info->m_null_bytes_in_rec;
  const char *null_bytes = nullptr;
  if (null_bytes_in_rec &&
      !(null_bytes = value_reader.read(null_bytes_in_rec))) {
    __XHANDLER_LOG(ERROR, "unexpected error record,table_name:%s",
                   table->s->table_name.str);
    return HA_ERR_INTERNAL_ERROR;
  }

  const char *unpack_info = nullptr;
  uint16 unpack_info_len = 0;
  xengine::common::Slice unpack_slice;
  if (dict_info->m_maybe_unpack_info) {
    unpack_info = value_reader.read(XDB_UNPACK_HEADER_SIZE);
    if (!unpack_info || unpack_info[0] != XDB_UNPACK_DATA_TAG) {
      __XHANDLER_LOG(ERROR, "unexpected error record,table_name:%s",
                     table->s->table_name.str);
      return HA_ERR_INTERNAL_ERROR;
    }

    unpack_info_len =
        xdb_netbuf_to_uint16(reinterpret_cast<const uchar *>(unpack_info + 1));
    unpack_slice = xengine::common::Slice(unpack_info, unpack_info_len);

    value_reader.read(unpack_info_len - XDB_UNPACK_HEADER_SIZE);
  }

  if (!unpack_info && !table_has_unpack_info(table)) {
    if (this->unpack_record_1(table, buf, packed_key, nullptr,
                              false /* verify_checksum */)) {
      __XHANDLER_LOG(ERROR, "unexpected error record,table_name:%s",
                     table->s->table_name.str);
      return HA_ERR_INTERNAL_ERROR;
    }
  } else if (this->unpack_record(table, buf, packed_key,
                                 unpack_info ? &unpack_slice : nullptr,
                                 false /* verify_checksum */)) {
    __XHANDLER_LOG(ERROR, "unexpected error record,table_name:%s",
                   table->s->table_name.str);
    return HA_ERR_INTERNAL_ERROR;
  }

  return HA_EXIT_SUCCESS;
}


/*
  Take mem-comparable form and unpack_info and unpack it to Table->record

  @detail
    not all indexes support this

  @return
    UNPACK_SUCCESS - Ok
    UNPACK_FAILURE - Data format error.
*/

int Xdb_key_def::unpack_record(TABLE *const table, uchar *const buf,
                               const xengine::common::Slice *const packed_key,
                               const xengine::common::Slice *const unpack_info,
                               const bool &verify_row_debug_checksums) const {
  Xdb_string_reader reader(packed_key);
  Xdb_string_reader unp_reader = Xdb_string_reader::read_or_empty(unpack_info);

  const bool is_hidden_pk = (m_index_type == INDEX_TYPE_HIDDEN_PRIMARY);
  const bool hidden_pk_exists = table_has_hidden_pk(table);
  const bool secondary_key = (m_index_type == INDEX_TYPE_SECONDARY);
  // There is no checksuming data after unpack_info for primary keys, because
  // the layout there is different. The checksum is verified in
  // ha_xengine::convert_record_from_storage_format instead.
  DBUG_ASSERT_IMP(!secondary_key, !verify_row_debug_checksums);

  // Skip the index number
  if ((!reader.read(INDEX_NUMBER_SIZE))) {
    __XHANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }

  // For secondary keys, we expect the value field to contain unpack data and
  // checksum data in that order. One or both can be missing, but they cannot
  // be reordered.
  const bool has_unpack_info =
      unp_reader.remaining_bytes() &&
      *unp_reader.get_current_ptr() == XDB_UNPACK_DATA_TAG;
  if (has_unpack_info && !unp_reader.read(XDB_UNPACK_HEADER_SIZE)) {
    __XHANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }

  for (uint i = 0; i < m_key_parts; i++) {
    Xdb_field_packing *const fpi = &m_pack_info[i];

    /*
      Hidden pk field is packed at the end of the secondary keys, but the SQL
      layer does not know about it. Skip retrieving field if hidden pk.
    */
    if ((secondary_key && hidden_pk_exists && i + 1 == m_key_parts) ||
        is_hidden_pk) {
      DBUG_ASSERT(fpi->m_unpack_func);
      if (fpi->m_skip_func(fpi, nullptr, &reader)) {
        __XHANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
        return HA_EXIT_FAILURE;
      }
      continue;
    }

    Field *const field = fpi->get_field_in_table(table);

    if (fpi->m_unpack_func) {
      /* It is possible to unpack this column. Do it. */

      uint field_offset = field->ptr - table->record[0];
      uint null_offset = field->null_offset();
      bool maybe_null = field->real_maybe_null();
      field->move_field(buf + field_offset,
                        maybe_null ? buf + null_offset : nullptr,
                        field->null_bit);
      // WARNING! Don't return without restoring field->ptr and field->null_ptr

      // If we need unpack info, but there is none, tell the unpack function
      // this by passing unp_reader as nullptr. If we never read unpack_info
      // during unpacking anyway, then there won't an error.
      const bool maybe_missing_unpack =
          !has_unpack_info && fpi->uses_unpack_info();
      int res = unpack_field(fpi, field, &reader,
                             table->s->default_values + field_offset,
                             maybe_missing_unpack ? nullptr : &unp_reader);

      // Restore field->ptr and field->null_ptr
      field->move_field(table->record[0] + field_offset,
                        maybe_null ? table->record[0] + null_offset : nullptr,
                        field->null_bit);

      if (res) {
        __XHANDLER_LOG(ERROR, "unexpected error record, code:%d, table_name:%s", res, table->s->table_name.str);
        return res;
      }
    } else {
      /* It is impossible to unpack the column. Skip it. */
      if (fpi->m_maybe_null) {
        const char *nullp;
        if (!(nullp = reader.read(1))) {
          __XHANDLER_LOG(ERROR, "unexpected error record, table_name:%s", table->s->table_name.str);
          return HA_EXIT_FAILURE;
        }
        if (*nullp == 0) {
          /* This is a NULL value */
          continue;
        }
        /* If NULL marker is not '0', it can be only '1'  */
        if (*nullp != 1) {
          __XHANDLER_LOG(ERROR, "unexpected error record, table_name:%s", table->s->table_name.str);
          return HA_EXIT_FAILURE;
        }
      }
      if (fpi->m_skip_func(fpi, field, &reader)) {
        __XHANDLER_LOG(ERROR, "unexpected error record, table_name:%s", table->s->table_name.str);
        return HA_EXIT_FAILURE;
      }
    }
  }

  /*
    Check checksum values if present
  */
  const char *ptr;
  if ((ptr = unp_reader.read(1)) && *ptr == XDB_CHECKSUM_DATA_TAG) {
    if (verify_row_debug_checksums) {
      uint32_t stored_key_chksum = xdb_netbuf_to_uint32(
          (const uchar *)unp_reader.read(XDB_CHECKSUM_SIZE));
      const uint32_t stored_val_chksum = xdb_netbuf_to_uint32(
          (const uchar *)unp_reader.read(XDB_CHECKSUM_SIZE));

      const uint32_t computed_key_chksum =
          crc32(0, (const uchar *)packed_key->data(), packed_key->size());
      const uint32_t computed_val_chksum =
          crc32(0, (const uchar *)unpack_info->data(),
                unpack_info->size() - XDB_CHECKSUM_CHUNK_SIZE);

      DBUG_EXECUTE_IF("myx_simulate_bad_key_checksum1",
                      stored_key_chksum++;);

      if (stored_key_chksum != computed_key_chksum) {
        report_checksum_mismatch(true, packed_key->data(), packed_key->size());
        return HA_EXIT_FAILURE;
      }

      if (stored_val_chksum != computed_val_chksum) {
        report_checksum_mismatch(false, unpack_info->data(),
                                 unpack_info->size() - XDB_CHECKSUM_CHUNK_SIZE);
        return HA_EXIT_FAILURE;
      }
    } else {
      /* The checksums are present but we are not checking checksums */
    }
  }

  if (reader.remaining_bytes()) {
    __XHANDLER_LOG(ERROR, "unexpected error record, table_name:%s", table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }

  return HA_EXIT_SUCCESS;
}

bool Xdb_key_def::table_has_hidden_pk(const TABLE *const table) {
  return table->s->primary_key == MAX_INDEXES;
}

void Xdb_key_def::report_checksum_mismatch(const bool &is_key,
                                           const char *const data,
                                           const size_t data_size) const {
  // NO_LINT_DEBUG
  sql_print_error("Checksum mismatch in %s of key-value pair for index 0x%x",
                  is_key ? "key" : "value", get_index_number());

  const std::string buf = xdb_hexdump(data, data_size, XDB_MAX_HEXDUMP_LEN);
  // NO_LINT_DEBUG
  // sql_print_error("Data with incorrect checksum (%" PRIu64 " bytes): %s",
  sql_print_error("Data with incorrect checksum %lu bytes): %s",
                  (uint64_t)data_size, buf.c_str());

  my_error(ER_INTERNAL_ERROR, MYF(0), "Record checksum mismatch");
}

///////////////////////////////////////////////////////////////////////////////////////////
// Xdb_field_packing
///////////////////////////////////////////////////////////////////////////////////////////

/*
  Function of type xdb_index_field_skip_t
*/

int xdb_skip_max_length(const Xdb_field_packing *const fpi,
                        const Field *const field MY_ATTRIBUTE((__unused__)),
                        Xdb_string_reader *const reader) {
  if (UNLIKELY(!reader->read(fpi->m_max_image_len)))
    return HA_EXIT_FAILURE;
  return HA_EXIT_SUCCESS;
}

/*
  (XDB_ESCAPE_LENGTH-1) must be an even number so that pieces of lines are not
  split in the middle of an UTF-8 character. See the implementation of
  xdb_unpack_binary_or_utf8_varchar.
*/
const uint XDB_UTF8MB4_LENGTH= 21; //for utf8mb4_general_ci, unpack_info use 21 bits.
const uint XDB_ESCAPE_LENGTH = 9;
static_assert((XDB_ESCAPE_LENGTH - 1) % 2 == 0,
              "XDB_ESCAPE_LENGTH-1 must be even.");

/*
  Function of type xdb_index_field_skip_t
*/

static int xdb_skip_variable_length(
    const Xdb_field_packing *const fpi MY_ATTRIBUTE((__unused__)),
    const Field *const field, Xdb_string_reader *const reader) {
  const uchar *ptr;
  bool finished = false;

  size_t dst_len; /* How much data can be there */
  if (field) {
    const Field_varstring *const field_var =
        static_cast<const Field_varstring *>(field);
    dst_len = field_var->pack_length() - field_var->length_bytes;
  } else {
    dst_len = UINT_MAX;
  }

  /* Decode the length-emitted encoding here */
  while ((ptr = (const uchar *)reader->read(XDB_ESCAPE_LENGTH))) {
    /* See xdb_pack_with_varchar_encoding. */
    const uchar pad =
        255 - ptr[XDB_ESCAPE_LENGTH - 1]; // number of padding bytes
    const uchar used_bytes = XDB_ESCAPE_LENGTH - 1 - pad;

    if (used_bytes > XDB_ESCAPE_LENGTH - 1 || used_bytes > dst_len) {
      return HA_EXIT_FAILURE; /* cannot store that much, invalid data */
    }

    if (used_bytes < XDB_ESCAPE_LENGTH - 1) {
      finished = true;
      break;
    }
    dst_len -= used_bytes;
  }

  if (!finished) {
    return HA_EXIT_FAILURE;
  }

  return HA_EXIT_SUCCESS;
}

const int VARCHAR_CMP_LESS_THAN_SPACES = 1;
const int VARCHAR_CMP_EQUAL_TO_SPACES = 2;
const int VARCHAR_CMP_GREATER_THAN_SPACES = 3;

/*
  Skip a keypart that uses Variable-Length Space-Padded encoding
*/

static int xdb_skip_variable_space_pad(const Xdb_field_packing *const fpi,
                                       const Field *const field,
                                       Xdb_string_reader *const reader) {
  const uchar *ptr;
  bool finished = false;

  size_t dst_len = UINT_MAX; /* How much data can be there */

  if (field) {
    const Field_varstring *const field_var =
        static_cast<const Field_varstring *>(field);
    dst_len = field_var->pack_length() - field_var->length_bytes;
  }

  /* Decode the length-emitted encoding here */
  while ((ptr = (const uchar *)reader->read(fpi->m_segment_size))) {
    // See xdb_pack_with_varchar_space_pad
    const uchar c = ptr[fpi->m_segment_size - 1];
    if (c == VARCHAR_CMP_EQUAL_TO_SPACES) {
      // This is the last segment
      finished = true;
      break;
    } else if (c == VARCHAR_CMP_LESS_THAN_SPACES ||
               c == VARCHAR_CMP_GREATER_THAN_SPACES) {
      // This is not the last segment
      if ((fpi->m_segment_size - 1) > dst_len) {
        // The segment is full of data but the table field can't hold that
        // much! This must be data corruption.
        return HA_EXIT_FAILURE;
      }
      dst_len -= (fpi->m_segment_size - 1);
    } else {
      // Encountered a value that's none of the VARCHAR_CMP* constants
      // It's data corruption.
      return HA_EXIT_FAILURE;
    }
  }
  return finished ? HA_EXIT_SUCCESS : HA_EXIT_FAILURE;
}

/*
  Function of type xdb_index_field_unpack_t
*/

int xdb_unpack_integer(Xdb_field_packing *const fpi, Field *const field,
                       uchar *const to, Xdb_string_reader *const reader,
                       Xdb_string_reader *const unp_reader
                           MY_ATTRIBUTE((__unused__))) {
  const int length = fpi->m_max_image_len;

  const uchar *from;
  if (!(from = (const uchar *)reader->read(length)))
    return UNPACK_FAILURE; /* Mem-comparable image doesn't have enough bytes */

#ifdef WORDS_BIGENDIAN
  {
    if (((Field_num *)field)->unsigned_flag)
      to[0] = from[0];
    else
      to[0] = (char)(from[0] ^ 128); // Reverse the sign bit.
    memcpy(to + 1, from + 1, length - 1);
  }
#else
  {
    const int sign_byte = from[0];
    if (((Field_num *)field)->unsigned_flag)
      to[length - 1] = sign_byte;
    else
      to[length - 1] =
          static_cast<char>(sign_byte ^ 128); // Reverse the sign bit.
    for (int i = 0, j = length - 1; i < length - 1; ++i, --j)
      to[i] = from[j];
  }
#endif
  return UNPACK_SUCCESS;
}

#if !defined(WORDS_BIGENDIAN)
static void xdb_swap_double_bytes(uchar *const dst, const uchar *const src) {
#if defined(__FLOAT_WORD_ORDER) && (__FLOAT_WORD_ORDER == __BIG_ENDIAN)
  // A few systems store the most-significant _word_ first on little-endian
  dst[0] = src[3];
  dst[1] = src[2];
  dst[2] = src[1];
  dst[3] = src[0];
  dst[4] = src[7];
  dst[5] = src[6];
  dst[6] = src[5];
  dst[7] = src[4];
#else
  dst[0] = src[7];
  dst[1] = src[6];
  dst[2] = src[5];
  dst[3] = src[4];
  dst[4] = src[3];
  dst[5] = src[2];
  dst[6] = src[1];
  dst[7] = src[0];
#endif
}

static void xdb_swap_float_bytes(uchar *const dst, const uchar *const src) {
  dst[0] = src[3];
  dst[1] = src[2];
  dst[2] = src[1];
  dst[3] = src[0];
}
#else
#define xdb_swap_double_bytes nullptr
#define xdb_swap_float_bytes nullptr
#endif

static int xdb_unpack_floating_point(
    uchar *const dst, Xdb_string_reader *const reader, const size_t &size,
    const int &exp_digit, const uchar *const zero_pattern,
    const uchar *const zero_val, void (*swap_func)(uchar *, const uchar *)) {
  const uchar *const from = (const uchar *)reader->read(size);
  if (from == nullptr)
    return UNPACK_FAILURE; /* Mem-comparable image doesn't have enough bytes */

  /* Check to see if the value is zero */
  if (memcmp(from, zero_pattern, size) == 0) {
    memcpy(dst, zero_val, size);
    return UNPACK_SUCCESS;
  }

#if defined(WORDS_BIGENDIAN)
  // On big-endian, output can go directly into result
  uchar *const tmp = dst;
#else
  // Otherwise use a temporary buffer to make byte-swapping easier later
  uchar tmp[8];
#endif

  memcpy(tmp, from, size);

  if (tmp[0] & 0x80) {
    // If the high bit is set the original value was positive so
    // remove the high bit and subtract one from the exponent.
    ushort exp_part = ((ushort)tmp[0] << 8) | (ushort)tmp[1];
    exp_part &= 0x7FFF;                            // clear high bit;
    exp_part -= (ushort)1 << (16 - 1 - exp_digit); // subtract from exponent
    tmp[0] = (uchar)(exp_part >> 8);
    tmp[1] = (uchar)exp_part;
  } else {
    // Otherwise the original value was negative and all bytes have been
    // negated.
    for (size_t ii = 0; ii < size; ii++)
      tmp[ii] ^= 0xFF;
  }

#if !defined(WORDS_BIGENDIAN)
  // On little-endian, swap the bytes around
  swap_func(dst, tmp);
#else
  static_assert(swap_func == nullptr, "Assuming that no swapping is needed.");
#endif

  return UNPACK_SUCCESS;
}

#if !defined(DBL_EXP_DIG)
#define DBL_EXP_DIG (sizeof(double) * 8 - DBL_MANT_DIG)
#endif

/*
  Function of type xdb_index_field_unpack_t

  Unpack a double by doing the reverse action of change_double_for_sort
  (sql/filesort.cc).  Note that this only works on IEEE values.
  Note also that this code assumes that NaN and +/-Infinity are never
  allowed in the database.
*/
static int xdb_unpack_double(
    Xdb_field_packing *const fpi MY_ATTRIBUTE((__unused__)),
    Field *const field MY_ATTRIBUTE((__unused__)), uchar *const field_ptr,
    Xdb_string_reader *const reader,
    Xdb_string_reader *const unp_reader MY_ATTRIBUTE((__unused__))) {
  static double zero_val = 0.0;
  static const uchar zero_pattern[8] = {128, 0, 0, 0, 0, 0, 0, 0};

  return xdb_unpack_floating_point(
      field_ptr, reader, sizeof(double), DBL_EXP_DIG, zero_pattern,
      (const uchar *)&zero_val, xdb_swap_double_bytes);
}

#if !defined(FLT_EXP_DIG)
#define FLT_EXP_DIG (sizeof(float) * 8 - FLT_MANT_DIG)
#endif

/*
  Function of type xdb_index_field_unpack_t

  Unpack a float by doing the reverse action of Field_float::make_sort_key
  (sql/field.cc).  Note that this only works on IEEE values.
  Note also that this code assumes that NaN and +/-Infinity are never
  allowed in the database.
*/
static int xdb_unpack_float(
    Xdb_field_packing *const, Field *const field MY_ATTRIBUTE((__unused__)),
    uchar *const field_ptr, Xdb_string_reader *const reader,
    Xdb_string_reader *const unp_reader MY_ATTRIBUTE((__unused__))) {
  static float zero_val = 0.0;
  static const uchar zero_pattern[4] = {128, 0, 0, 0};

  return xdb_unpack_floating_point(
      field_ptr, reader, sizeof(float), FLT_EXP_DIG, zero_pattern,
      (const uchar *)&zero_val, xdb_swap_float_bytes);
}

/*
  Function of type xdb_index_field_unpack_t used to
  Unpack by doing the reverse action to Field_newdate::make_sort_key.
*/

int xdb_unpack_newdate(Xdb_field_packing *const fpi, Field *constfield,
                       uchar *const field_ptr, Xdb_string_reader *const reader,
                       Xdb_string_reader *const unp_reader
                           MY_ATTRIBUTE((__unused__))) {
  const char *from;
  DBUG_ASSERT(fpi->m_max_image_len == 3);

  if (!(from = reader->read(3)))
    return UNPACK_FAILURE; /* Mem-comparable image doesn't have enough bytes */

  field_ptr[0] = from[2];
  field_ptr[1] = from[1];
  field_ptr[2] = from[0];
  return UNPACK_SUCCESS;
}

/*
  Function of type xdb_index_field_unpack_t, used to
  Unpack the string by copying it over.
  This is for BINARY(n) where the value occupies the whole length.
*/

static int xdb_unpack_binary_str(
    Xdb_field_packing *const fpi, Field *const field, uchar *const to,
    Xdb_string_reader *const reader,
    Xdb_string_reader *const unp_reader MY_ATTRIBUTE((__unused__))) {
  const char *from;
  if (!(from = reader->read(fpi->m_max_image_len)))
    return UNPACK_FAILURE; /* Mem-comparable image doesn't have enough bytes */

  memcpy(to, from, fpi->m_max_image_len);
  return UNPACK_SUCCESS;
}

/*
  Function of type xdb_index_field_unpack_t.
  For UTF-8, we need to convert 2-byte wide-character entities back into
  UTF8 sequences.
*/
static int xdb_unpack_bin_str(
    Xdb_field_packing *fpi, Field *field,
    uchar *dst,
    Xdb_string_reader *reader,
    Xdb_string_reader *unp_reader __attribute__((__unused__)))
{
  my_core::CHARSET_INFO *cset= (my_core::CHARSET_INFO*)field->charset();
  const uchar *src;
  if (!(src= (const uchar*)reader->read(fpi->m_max_image_len)))
    return UNPACK_FAILURE; /* Mem-comparable image doesn't have enough bytes */

  const uchar *src_end= src + fpi->m_max_image_len;
  uchar *dst_end= dst + field->pack_length();
  int res;

  if (cset == &my_charset_utf8_bin)
  {
    while (src < src_end)
    {
      my_wc_t wc= (src[0] <<8) | src[1];
      src += 2;
      res= cset->cset->wc_mb(cset, wc, dst, dst_end);
      DBUG_ASSERT(res > 0 && res <=3);
      if (res < 0)
        return UNPACK_FAILURE;
      dst += res;
    }
  }
  else if (cset == &my_charset_gbk_bin)
  {
    while (src < src_end)
    {
      if (src[0] == 0x20)
      {
        /* src[0] is not used */
        dst[0]= src[1];
        res= 1;
      }
      else
      {
        dst[0]= src[0];
        dst[1]= src[1];
        res= 2;
      }

      src += 2;
      dst += res;
    }
  }
  else if (cset == &my_charset_utf8mb4_bin)
  {
    while (src < src_end)
    {
      my_wc_t wc= (src[0] <<16 | src[1] <<8) | src[2];
      src += 3;
      int res= cset->cset->wc_mb(cset, wc, dst, dst_end);
      DBUG_ASSERT(res > 0 && res <=4);
      if (res < 0)
        return UNPACK_FAILURE;
      dst += res;
    }
  }

  cset->cset->fill(cset, reinterpret_cast<char *>(dst),
                   dst_end - dst, cset->pad_char);
  return UNPACK_SUCCESS;
}

/*
  Function of type xdb_index_field_pack_t
*/

static void xdb_pack_with_varchar_encoding(
    Xdb_field_packing *const fpi, Field *const field, uchar *buf, uchar **dst,
    Xdb_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__))) {
  /*
    Use a flag byte every Nth byte. Set it to (255 - #pad) where #pad is 0
    when the var length field filled all N-1 previous bytes and #pad is
    otherwise the number of padding bytes used.

    If N=8 and the field is:
    * 3 bytes (1, 2, 3) this is encoded as: 1, 2, 3, 0, 0, 0, 0, 251
    * 4 bytes (1, 2, 3, 0) this is encoded as: 1, 2, 3, 0, 0, 0, 0, 252
    And the 4 byte string compares as greater than the 3 byte string
  */
  const CHARSET_INFO *const charset = field->charset();
  Field_varstring *const field_var = (Field_varstring *)field;


  //actual length of data column
  size_t value_length = (field_var->length_bytes == 1) ?
                        (uint)*field->ptr : uint2korr(field->ptr);

  // if prefix-column index is less than value length, use prefix-column-length;
  // otherwise use value length.
  if (fpi->m_prefix_index_flag == true && charset == &my_charset_utf8mb4_0900_ai_ci) {
    DBUG_ASSERT(fpi->m_weights > 0);
    int length_bytes = field_var->length_bytes;
    size_t input_length = std::min<size_t>(
        value_length,
        charset->cset->charpos(
            charset, pointer_cast<const char *>(field->ptr + length_bytes),
            pointer_cast<const char *>(field->ptr + length_bytes) +
                value_length,
            fpi->m_weights));

    value_length = value_length <= input_length ? value_length : input_length;
  }

  size_t xfrm_len = charset->coll->strnxfrm(
      charset, buf, fpi->m_max_strnxfrm_len, field_var->char_length(),
      field_var->ptr + field_var->length_bytes, value_length,
      MY_STRXFRM_NOPAD_WITH_SPACE);

  /* Got a mem-comparable image in 'buf'. Now, produce varlength encoding */

  size_t encoded_size = 0;
  uchar *ptr = *dst;
  while (1) {
    const size_t copy_len = std::min((size_t)XDB_ESCAPE_LENGTH - 1, xfrm_len);
    const size_t padding_bytes = XDB_ESCAPE_LENGTH - 1 - copy_len;
    memcpy(ptr, buf, copy_len);
    ptr += copy_len;
    buf += copy_len;
    // pad with zeros if necessary;
    for (size_t idx = 0; idx < padding_bytes; idx++)
      *(ptr++) = 0;
    *(ptr++) = 255 - padding_bytes;

    xfrm_len -= copy_len;
    encoded_size += XDB_ESCAPE_LENGTH;
    if (padding_bytes != 0)
      break;
  }
  *dst += encoded_size;
}

/*
  Compare the string in [buf..buf_end) with a string that is an infinite
  sequence of strings in space_xfrm
*/

static int
xdb_compare_string_with_spaces(const uchar *buf, const uchar *const buf_end,
                               const std::vector<uchar> *const space_xfrm) {
  int cmp = 0;
  while (buf < buf_end) {
    size_t bytes = std::min((size_t)(buf_end - buf), space_xfrm->size());
    if ((cmp = memcmp(buf, space_xfrm->data(), bytes)) != 0)
      break;
    buf += bytes;
  }
  return cmp;
}

static const int XDB_TRIMMED_CHARS_OFFSET = 8;
/*
  Pack the data with Variable-Length Space-Padded Encoding.

  The encoding is there to meet two goals:

  Goal#1. Comparison. The SQL standard says

    " If the collation for the comparison has the PAD SPACE characteristic,
    for the purposes of the comparison, the shorter value is effectively
    extended to the length of the longer by concatenation of <space>s on the
    right.

  At the moment, all MySQL collations except one have the PAD SPACE
  characteristic.  The exception is the "binary" collation that is used by
  [VAR]BINARY columns. (Note that binary collations for specific charsets,
  like utf8_bin or latin1_bin are not the same as "binary" collation, they have
  the PAD SPACE characteristic).

  Goal#2 is to preserve the number of trailing spaces in the original value.

  This is achieved by using the following encoding:
  The key part:
  - Stores mem-comparable image of the column
  - It is stored in chunks of fpi->m_segment_size bytes (*)
    = If the remainder of the chunk is not occupied, it is padded with mem-
      comparable image of the space character (cs->pad_char to be precise).
  - The last byte of the chunk shows how the rest of column's mem-comparable
    image would compare to mem-comparable image of the column extended with
    spaces. There are three possible values.
     - VARCHAR_CMP_LESS_THAN_SPACES,
     - VARCHAR_CMP_EQUAL_TO_SPACES
     - VARCHAR_CMP_GREATER_THAN_SPACES

  VARCHAR_CMP_EQUAL_TO_SPACES means that this chunk is the last one (the rest
  is spaces, or something that sorts as spaces, so there is no reason to store
  it).

  Example: if fpi->m_segment_size=5, and the collation is latin1_bin:

   'abcd\0'   => [ 'abcd' <VARCHAR_CMP_LESS> ]['\0    ' <VARCHAR_CMP_EQUAL> ]
   'abcd'     => [ 'abcd' <VARCHAR_CMP_EQUAL>]
   'abcd   '  => [ 'abcd' <VARCHAR_CMP_EQUAL>]
   'abcdZZZZ' => [ 'abcd' <VARCHAR_CMP_GREATER>][ 'ZZZZ' <VARCHAR_CMP_EQUAL>]

  As mentioned above, the last chunk is padded with mem-comparable images of
  cs->pad_char. It can be 1-byte long (latin1), 2 (utf8_bin), 3 (utf8mb4), etc.

  fpi->m_segment_size depends on the used collation. It is chosen to be such
  that no mem-comparable image of space will ever stretch across the segments
  (see get_segment_size_from_collation).

  == The value part (aka unpack_info) ==
  The value part stores the number of space characters that one needs to add
  when unpacking the string.
  - If the number is positive, it means add this many spaces at the end
  - If the number is negative, it means padding has added extra spaces which
    must be removed.

  Storage considerations
  - depending on column's max size, the number may occupy 1 or 2 bytes
  - the number of spaces that need to be removed is not more than
    XDB_TRIMMED_CHARS_OFFSET=8, so we offset the number by that value and
    then store it as unsigned.

  @seealso
    xdb_unpack_binary_varchar_space_pad
    xdb_unpack_simple_varchar_space_pad
    xdb_dummy_make_unpack_info
    xdb_skip_variable_space_pad
*/

static void xdb_pack_with_varchar_space_pad(
    Xdb_field_packing *fpi, Field *field, uchar *buf, uchar **dst,
    Xdb_pack_field_context *pack_ctx)
{
  Xdb_string_writer *unpack_info= pack_ctx->writer;
  const CHARSET_INFO *charset= field->charset();
  auto field_var= static_cast<Field_varstring *>(field);

  size_t value_length= (field_var->length_bytes == 1) ?
                       (uint) *field->ptr : uint2korr(field->ptr);

  size_t trimmed_len=
    charset->cset->lengthsp(charset,
                            (const char*)field_var->ptr +
                                         field_var->length_bytes,
                            value_length);

  uchar *buf_end;
  if (charset == &my_charset_gbk_chinese_ci || charset == &my_charset_gbk_bin)
  {
    uchar *src= field_var->ptr + field_var->length_bytes;
    uchar *se= src + trimmed_len;
    uchar *dst_buf= buf;
    DBUG_ASSERT(fpi->m_weights > 0);

    uint nweights = trimmed_len;
    if (fpi->m_prefix_index_flag == true) {
      nweights= fpi->m_weights <= trimmed_len ? fpi->m_weights : trimmed_len;
    }

    for (; src < se && nweights; nweights--)
    {
      if (charset->cset->ismbchar(charset, (const char*)src, (const char*)se))
      {
        charset->coll->strnxfrm(charset, dst_buf, 2, 1, src, 2, MY_STRXFRM_NOPAD_WITH_SPACE);
        src += 2;
      }
      else
      {
        charset->coll->strnxfrm(charset, dst_buf, 1, 1, src, 1, MY_STRXFRM_NOPAD_WITH_SPACE);
        src += 1;

        /* add 0x20 for 2bytes comparebytes */
        *(dst_buf + 1)= *dst_buf;
        *dst_buf= 0x20;
      }

      dst_buf += 2;
    }

    buf_end= dst_buf;
  }
  else
  {
   /* for utf8 and utf8mb4 */
    size_t xfrm_len;
    xfrm_len= charset->coll->strnxfrm(charset,
                                      buf, fpi->m_max_strnxfrm_len,
                                      field_var->char_length(),
                                      field_var->ptr + field_var->length_bytes,
                                      trimmed_len,
                                      MY_STRXFRM_NOPAD_WITH_SPACE);

    /* Got a mem-comparable image in 'buf'. Now, produce varlength encoding */
    buf_end= buf + xfrm_len;
  }

  size_t encoded_size= 0;
  uchar *ptr= *dst;
  size_t padding_bytes;
  while (true)
  {
    size_t copy_len= std::min<size_t>(fpi->m_segment_size-1, buf_end - buf);
    padding_bytes= fpi->m_segment_size - 1 - copy_len;

    memcpy(ptr, buf, copy_len);
    ptr += copy_len;
    buf += copy_len;

    if (padding_bytes)
    {
      memcpy(ptr, fpi->space_xfrm->data(), padding_bytes);
      ptr+= padding_bytes;
      *ptr= VARCHAR_CMP_EQUAL_TO_SPACES;  // last segment
    }
    else
    {
      // Compare the string suffix with a hypothetical infinite string of
      // spaces. It could be that the first difference is beyond the end of
      // current chunk.
      int cmp= xdb_compare_string_with_spaces(buf, buf_end, fpi->space_xfrm);

      if (cmp < 0)
        *ptr= VARCHAR_CMP_LESS_THAN_SPACES;
      else if (cmp > 0)
        *ptr= VARCHAR_CMP_GREATER_THAN_SPACES;
      else
      {
        // It turns out all the rest are spaces.
        *ptr= VARCHAR_CMP_EQUAL_TO_SPACES;
      }
    }
    encoded_size += fpi->m_segment_size;

    if (*(ptr++) == VARCHAR_CMP_EQUAL_TO_SPACES)
      break;
  }

  // m_unpack_info_stores_value means unpack_info stores the whole original
  // value. There is no need to store the number of trimmed/padded endspaces
  // in that case.
  if (unpack_info && !fpi->m_unpack_info_stores_value)
  {
    // (value_length - trimmed_len) is the number of trimmed space *characters*
    // then, padding_bytes is the number of *bytes* added as padding
    // then, we add 8, because we don't store negative values.
    DBUG_ASSERT(padding_bytes % fpi->space_xfrm_len == 0);
    DBUG_ASSERT((value_length - trimmed_len)% fpi->space_mb_len == 0);
    size_t removed_chars= XDB_TRIMMED_CHARS_OFFSET +
                          (value_length - trimmed_len) / fpi->space_mb_len -
                          padding_bytes/fpi->space_xfrm_len;

    if (fpi->m_unpack_info_uses_two_bytes)
    {
      unpack_info->write_uint16(removed_chars);
    }
    else
    {
      DBUG_ASSERT(removed_chars < 0x100);
      unpack_info->write_uint8(removed_chars);
    }
  }

  *dst += encoded_size;
}

/*
  Function of type xdb_index_field_unpack_t
*/

static int xdb_unpack_binary_or_utf8_varchar(
    Xdb_field_packing *fpi, Field *field,
    uchar *dst,
    Xdb_string_reader *reader,
    Xdb_string_reader *unp_reader __attribute__((__unused__)))
{
  const uchar *ptr;
  size_t len= 0;
  bool finished= false;
  uchar *d0= dst;
  Field_varstring* field_var= (Field_varstring*)field;
  dst += field_var->length_bytes;
  // How much we can unpack
  size_t dst_len= field_var->pack_length() - field_var->length_bytes;
  uchar *dst_end= dst + dst_len;

  /* Decode the length-emitted encoding here */
  while ((ptr= (const uchar*)reader->read(XDB_ESCAPE_LENGTH)))
  {
    /* See xdb_pack_with_varchar_encoding. */
    uchar pad= 255 - ptr[XDB_ESCAPE_LENGTH - 1];  // number of padding bytes
    uchar used_bytes= XDB_ESCAPE_LENGTH - 1 - pad;

    if (used_bytes > XDB_ESCAPE_LENGTH - 1)
    {
      return UNPACK_FAILURE; /* cannot store that much, invalid data */
    }

    if (dst_len < used_bytes)
    {
      /* Encoded index tuple is longer than the size in the record buffer? */
      return UNPACK_FAILURE;
    }

    /*
      Now, we need to decode used_bytes of data and append them to the value.
    */
    if (fpi->m_charset == &my_charset_utf8_bin)
    {
      if (used_bytes & 1)
      {
        /*
          UTF-8 characters are encoded into two-byte entities. There is no way
          we can have an odd number of bytes after encoding.
        */
        return UNPACK_FAILURE;
      }

      const uchar *src= ptr;
      const uchar *src_end= ptr + used_bytes;
      while (src < src_end)
      {
        my_wc_t wc= (src[0] <<8) | src[1];
        src += 2;
        const CHARSET_INFO *cset= fpi->m_charset;
        int res= cset->cset->wc_mb(cset, wc, dst, dst_end);
        DBUG_ASSERT(res > 0 && res <=3);
        if (res < 0)
          return UNPACK_FAILURE;
        dst += res;
        len += res;
        dst_len -= res;
      }
    }
    else
    {
      memcpy(dst, ptr, used_bytes);
      dst += used_bytes;
      dst_len -= used_bytes;
      len += used_bytes;
    }

    if (used_bytes < XDB_ESCAPE_LENGTH - 1)
    {
      finished= true;
      break;
    }
  }

  if (!finished)
    return UNPACK_FAILURE;

  /* Save the length */
  if (field_var->length_bytes == 1)
  {
    d0[0]= len;
  }
  else
  {
    DBUG_ASSERT(field_var->length_bytes == 2);
    int2store(d0, len);
  }
  return UNPACK_SUCCESS;
}

/*
  @seealso
    xdb_pack_with_varchar_space_pad - packing function
    xdb_unpack_simple_varchar_space_pad - unpacking function for 'simple'
    charsets.
    xdb_skip_variable_space_pad - skip function
*/
static int xdb_unpack_binary_varchar_space_pad(
    Xdb_field_packing *fpi, Field *field,
    uchar *dst,
    Xdb_string_reader *reader,
    Xdb_string_reader *unp_reader)
{
  const uchar *ptr;
  size_t len= 0;
  bool finished= false;
  Field_varstring* field_var= static_cast<Field_varstring *>(field);
  uchar *d0= dst;
  uchar *dst_end= dst + field_var->pack_length();
  dst += field_var->length_bytes;

  uint space_padding_bytes= 0;
  uint extra_spaces;
  if (!unp_reader)
  {
    return UNPACK_INFO_MISSING;
  }

  if ((fpi->m_unpack_info_uses_two_bytes?
       unp_reader->read_uint16(&extra_spaces):
       unp_reader->read_uint8(&extra_spaces)))
  {
    return UNPACK_FAILURE;
  }

  if (extra_spaces <= XDB_TRIMMED_CHARS_OFFSET)
  {
    space_padding_bytes= -(static_cast<int>(extra_spaces) -
                           XDB_TRIMMED_CHARS_OFFSET);
    extra_spaces= 0;
  }
  else
    extra_spaces -= XDB_TRIMMED_CHARS_OFFSET;

  space_padding_bytes *= fpi->space_xfrm_len;

  /* Decode the length-emitted encoding here */
  while ((ptr= (const uchar*)reader->read(fpi->m_segment_size)))
  {
    char last_byte= ptr[fpi->m_segment_size - 1];
    size_t used_bytes;
    if (last_byte == VARCHAR_CMP_EQUAL_TO_SPACES)  // this is the last segment
    {
      if (space_padding_bytes > (fpi->m_segment_size-1))
        return UNPACK_FAILURE;  // Cannot happen, corrupted data
      used_bytes= (fpi->m_segment_size-1) - space_padding_bytes;
      finished= true;
    }
    else
    {
      if (last_byte != VARCHAR_CMP_LESS_THAN_SPACES &&
          last_byte != VARCHAR_CMP_GREATER_THAN_SPACES)
      {
        return UNPACK_FAILURE;  // Invalid value
      }
      used_bytes= fpi->m_segment_size-1;
    }

    // Now, need to decode used_bytes of data and append them to the value.
    if (fpi->m_charset == &my_charset_utf8_bin)
    {
      if (used_bytes & 1)
      {
        /*
          UTF-8 characters are encoded into two-byte entities. There is no way
          we can have an odd number of bytes after encoding.
        */
        return UNPACK_FAILURE;
      }

      const uchar *src= ptr;
      const uchar *src_end= ptr + used_bytes;
      while (src < src_end)
      {
        my_wc_t wc= (src[0] <<8) | src[1];
        src += 2;
        const CHARSET_INFO *cset= fpi->m_charset;
        int res= cset->cset->wc_mb(cset, wc, dst, dst_end);
        DBUG_ASSERT(res <=3);
        if (res <= 0)
          return UNPACK_FAILURE;
        dst += res;
        len += res;
      }
    }
    else if (fpi->m_charset == &my_charset_gbk_bin)
    {
      if (used_bytes & 1)
      {
        /*
          GBK characters are encoded into two-byte entities. There is no way
          we can have an odd number of bytes after encoding.
        */
        return UNPACK_FAILURE;
      }

      const uchar *src= ptr;
      const uchar *src_end= ptr + used_bytes;
      int res;
      while (src < src_end)
      {
        if (src[0] == 0x20)
        {
          /* src[0] is not used */
          dst[0]= src[1];
          res= 1;
        }
        else
        {
          dst[0]= src[0];
          dst[1]= src[1];
          res= 2;
        }

        src += 2;
        dst += res;
        len += res;
      }
    }
    else if (fpi->m_charset == &my_charset_utf8mb4_bin)
    {
      if ((used_bytes % 3) != 0)
      {
        /*
          UTF-8 characters are encoded into three-byte entities. There is no way
          we can an odd number of bytes after encoding.
        */
        return UNPACK_FAILURE;
      }

      const uchar *src= ptr;
      const uchar *src_end= ptr + used_bytes;
      while (src < src_end)
      {
        my_wc_t wc= (src[0] <<16) | src[1] <<8 | src[2];
        src += 3;
        const CHARSET_INFO *cset= fpi->m_charset;
        int res= cset->cset->wc_mb(cset, wc, dst, dst_end);
        DBUG_ASSERT(res > 0 && res <=4);
        if (res < 0)
          return UNPACK_FAILURE;
        dst += res;
        len += res;
      }
    }
    else
    {
      if (dst + used_bytes > dst_end)
        return UNPACK_FAILURE;
      memcpy(dst, ptr, used_bytes);
      dst += used_bytes;
      len += used_bytes;
    }

    if (finished)
    {
      if (extra_spaces)
      {
        // Both binary and UTF-8 charset store space as ' ',
        // so the following is ok:
        if (dst + extra_spaces > dst_end)
          return UNPACK_FAILURE;
        memset(dst, fpi->m_charset->pad_char, extra_spaces);
        len += extra_spaces;
      }
      break;
    }
  }

  if (!finished)
    return UNPACK_FAILURE;

  /* Save the length */
  if (field_var->length_bytes == 1)
  {
    d0[0]= len;
  }
  else
  {
    DBUG_ASSERT(field_var->length_bytes == 2);
    int2store(d0, len);
  }
  return UNPACK_SUCCESS;
}


/////////////////////////////////////////////////////////////////////////

/*
  Function of type xdb_make_unpack_info_t
*/

static void xdb_make_unpack_unknown(
    const Xdb_collation_codec *codec MY_ATTRIBUTE((__unused__)),
    Field *const field, Xdb_pack_field_context *const pack_ctx) {
  pack_ctx->writer->write(field->ptr, field->pack_length());
}

/*
  This point of this function is only to indicate that unpack_info is
  available.

  The actual unpack_info data is produced by the function that packs the key,
  that is, xdb_pack_with_varchar_space_pad.
*/

static void xdb_dummy_make_unpack_info(
    const Xdb_collation_codec *codec MY_ATTRIBUTE((__unused__)),
    Field *field MY_ATTRIBUTE((__unused__)),
    Xdb_pack_field_context *pack_ctx MY_ATTRIBUTE((__unused__))) {}

/*
  Function of type xdb_index_field_unpack_t
*/

static int xdb_unpack_unknown(Xdb_field_packing *const fpi, Field *const field,
                              uchar *const dst, Xdb_string_reader *const reader,
                              Xdb_string_reader *const unp_reader) {
  const uchar *ptr;
  const uint len = fpi->m_unpack_data_len;
  // We don't use anything from the key, so skip over it.
  if (xdb_skip_max_length(fpi, field, reader)) {
    return UNPACK_FAILURE;
  }

  DBUG_ASSERT_IMP(len > 0, unp_reader != nullptr);

  if ((ptr = (const uchar *)unp_reader->read(len))) {
    memcpy(dst, ptr, len);
    return UNPACK_SUCCESS;
  }
  return UNPACK_FAILURE;
}

/*
  Function of type xdb_make_unpack_info_t
*/

static void xdb_make_unpack_unknown_varchar(
    const Xdb_collation_codec *const codec MY_ATTRIBUTE((__unused__)),
    Field *const field, Xdb_pack_field_context *const pack_ctx) {
  const auto f = static_cast<const Field_varstring *>(field);
  uint len = f->length_bytes == 1 ? (uint)*f->ptr : uint2korr(f->ptr);
  len += f->length_bytes;
  pack_ctx->writer->write(field->ptr, len);
}

/*
  Function of type xdb_index_field_unpack_t

  @detail
  Unpack a key part in an "unknown" collation from its
  (mem_comparable_form, unpack_info) form.

  "Unknown" means we have no clue about how mem_comparable_form is made from
  the original string, so we keep the whole original string in the unpack_info.

  @seealso
    xdb_make_unpack_unknown, xdb_unpack_unknown
*/

static int xdb_unpack_unknown_varchar(Xdb_field_packing *const fpi,
                                      Field *const field, uchar *dst,
                                      Xdb_string_reader *const reader,
                                      Xdb_string_reader *const unp_reader) {
  const uchar *ptr;
  uchar *const d0 = dst;
  const auto f = static_cast<Field_varstring *>(field);
  dst += f->length_bytes;
  const uint len_bytes = f->length_bytes;
  // We don't use anything from the key, so skip over it.
  if (fpi->m_skip_func(fpi, field, reader)) {
    return UNPACK_FAILURE;
  }

  DBUG_ASSERT(len_bytes > 0);
  DBUG_ASSERT(unp_reader != nullptr);

  if ((ptr = (const uchar *)unp_reader->read(len_bytes))) {
    memcpy(d0, ptr, len_bytes);
    const uint len = len_bytes == 1 ? (uint)*ptr : uint2korr(ptr);
    if ((ptr = (const uchar *)unp_reader->read(len))) {
      memcpy(dst, ptr, len);
      return UNPACK_SUCCESS;
    }
  }
  return UNPACK_FAILURE;
}

/*
  Write unpack_data for a "simple" collation
*/
static void xdb_write_unpack_simple(Xdb_bit_writer *const writer,
                                    const Xdb_collation_codec *const codec,
                                    const uchar *const src,
                                    const size_t src_len) {
  const auto c= static_cast<const Xdb_collation_codec_simple*>(codec);
  for (uint i = 0; i < src_len; i++) {
    writer->write(c->m_enc_size[src[i]], c->m_enc_idx[src[i]]);
  }
}

static uint xdb_read_unpack_simple(Xdb_bit_reader *reader,
                                   const Xdb_collation_codec *codec,
                                   const uchar *src, uchar src_len,
                                   uchar *dst)
{
  const auto c= static_cast<const Xdb_collation_codec_simple*>(codec);
  for (uint i= 0; i < src_len; i++)
  {
    if (c->m_dec_size[src[i]] > 0)
    {
      uint *ret;
      // Unpack info is needed but none available.
      if (reader == nullptr)
      {
        return UNPACK_INFO_MISSING;
      }

      if ((ret= reader->read(c->m_dec_size[src[i]])) == nullptr)
      {
        return UNPACK_FAILURE;
      }
      dst[i]= c->m_dec_idx[*ret][src[i]];
    }
    else
    {
      dst[i]= c->m_dec_idx[0][src[i]];
    }
  }

  return UNPACK_SUCCESS;
}

/*
  Function of type xdb_make_unpack_info_t

  @detail
    Make unpack_data for VARCHAR(n) in a "simple" charset.
*/

void
xdb_make_unpack_simple_varchar(const Xdb_collation_codec *const codec,
                               Field * field,
                               Xdb_pack_field_context *const pack_ctx){
  auto f = static_cast<Field_varstring *>(field);
  uchar *const src = f->ptr + f->length_bytes;
  const size_t src_len =
      f->length_bytes == 1 ? (uint)*f->ptr : uint2korr(f->ptr);
  Xdb_bit_writer bit_writer(pack_ctx->writer);
  // The std::min compares characters with bytes, but for simple collations,
  // mbmaxlen = 1.
  xdb_write_unpack_simple(&bit_writer, codec, src,
                          std::min((size_t)f->char_length(), src_len));
}

/*
  Function of type xdb_index_field_unpack_t

  @seealso
    xdb_pack_with_varchar_space_pad - packing function
    xdb_unpack_binary_or_utf8_varchar_space_pad - a similar unpacking function
*/

int xdb_unpack_simple_varchar_space_pad(Xdb_field_packing *const fpi,
                                        Field *const field, uchar *dst,
                                        Xdb_string_reader *const reader,
                                        Xdb_string_reader *const unp_reader) {
  const uchar *ptr;
  size_t len = 0;
  bool finished = false;
  uchar *d0 = dst;
  Field_varstring *const field_var =
      static_cast<Field_varstring *>(field);
  // For simple collations, char_length is also number of bytes.
  DBUG_ASSERT((size_t)fpi->m_max_image_len >= field_var->char_length());
  uchar *dst_end = dst + field_var->pack_length();
  dst += field_var->length_bytes;
  Xdb_bit_reader bit_reader(unp_reader);

  uint space_padding_bytes = 0;
  uint extra_spaces;
  DBUG_ASSERT(unp_reader != nullptr);

  if ((fpi->m_unpack_info_uses_two_bytes
           ? unp_reader->read_uint16(&extra_spaces)
           : unp_reader->read_uint8(&extra_spaces))) {
    return UNPACK_FAILURE;
  }

  if (extra_spaces <= 8) {
    space_padding_bytes = -(static_cast<int>(extra_spaces) - 8);
    extra_spaces = 0;
  } else
    extra_spaces -= 8;

  space_padding_bytes *= fpi->space_xfrm_len;

  /* Decode the length-emitted encoding here */
  while ((ptr = (const uchar *)reader->read(fpi->m_segment_size))) {
    const char last_byte =
        ptr[fpi->m_segment_size - 1]; // number of padding bytes
    size_t used_bytes;
    if (last_byte == VARCHAR_CMP_EQUAL_TO_SPACES) {
      // this is the last one
      if (space_padding_bytes > (fpi->m_segment_size - 1))
        return UNPACK_FAILURE; // Cannot happen, corrupted data
      used_bytes = (fpi->m_segment_size - 1) - space_padding_bytes;
      finished = true;
    } else {
      if (last_byte != VARCHAR_CMP_LESS_THAN_SPACES &&
          last_byte != VARCHAR_CMP_GREATER_THAN_SPACES) {
        return UNPACK_FAILURE;
      }
      used_bytes = fpi->m_segment_size - 1;
    }

    if (dst + used_bytes > dst_end) {
      // The value on disk is longer than the field definition allows?
      return UNPACK_FAILURE;
    }

    uint ret;
    if ((ret = xdb_read_unpack_simple(&bit_reader, fpi->m_charset_codec, ptr,
                                      used_bytes, dst)) != UNPACK_SUCCESS) {
      return ret;
    }

    dst += used_bytes;
    len += used_bytes;

    if (finished) {
      if (extra_spaces) {
        if (dst + extra_spaces > dst_end)
          return UNPACK_FAILURE;
        // pad_char has a 1-byte form in all charsets that
        // are handled by xdb_init_collation_mapping.
        memset(dst, field_var->charset()->pad_char, extra_spaces);
        len += extra_spaces;
      }
      break;
    }
  }

  if (!finished)
    return UNPACK_FAILURE;

  /* Save the length */
  if (field_var->length_bytes == 1) {
    d0[0] = len;
  } else {
    DBUG_ASSERT(field_var->length_bytes == 2);
    int2store(d0, len);
  }
  return UNPACK_SUCCESS;
}

/*
  Function of type xdb_make_unpack_info_t

  @detail
    Make unpack_data for CHAR(n) value in a "simple" charset.
    It is CHAR(N), so SQL layer has padded the value with spaces up to N chars.

  @seealso
    The VARCHAR variant is in xdb_make_unpack_simple_varchar
*/

static void xdb_make_unpack_simple(const Xdb_collation_codec *const codec,
                                   Field *const field,
                                   Xdb_pack_field_context *const pack_ctx) {
  const uchar *const src = field->ptr;
  Xdb_bit_writer bit_writer(pack_ctx->writer);
  xdb_write_unpack_simple(&bit_writer, codec, src, field->pack_length());
}

/*
  Function of type xdb_index_field_unpack_t
*/

static int xdb_unpack_simple(Xdb_field_packing *const fpi,
                             Field *const field MY_ATTRIBUTE((__unused__)),
                             uchar *const dst, Xdb_string_reader *const reader,
                             Xdb_string_reader *const unp_reader) {
  const uchar *ptr;
  const uint len = fpi->m_max_image_len;
  Xdb_bit_reader bit_reader(unp_reader);

  if (!(ptr = (const uchar *)reader->read(len))) {
    return UNPACK_FAILURE;
  }

  return xdb_read_unpack_simple(unp_reader ? &bit_reader : nullptr,
                                fpi->m_charset_codec, ptr, len, dst);
}

/*
  Write unpack_data for a utf8 collation
*/
static void xdb_write_unpack_utf8(Xdb_bit_writer *writer,
                                  const Xdb_collation_codec *codec,
                                  const uchar *src, size_t src_len,
                                  bool is_varchar)
{
  const auto c= static_cast<const Xdb_collation_codec_utf8*>(codec);
  my_wc_t src_char;
  uint i;
  uint max_bytes= 0; //for char
  for (i= 0; i < src_len && max_bytes < src_len;)
  {
    int res= c->m_cs->cset->mb_wc(c->m_cs, &src_char, src + i, src + src_len);
    DBUG_ASSERT(res > 0 && res <= 3);
    writer->write(c->m_enc_size[src_char], c->m_enc_idx[src_char]);

    i+= res;
    if (!is_varchar)
    {
      max_bytes+= 3; //max bytes for every utf8 character.
    }
  }
  DBUG_ASSERT(i <= src_len);
}

/*
  Write unpack_data for a utf8mb4 collation
*/
static void xdb_write_unpack_utf8mb4(Xdb_bit_writer *writer,
                                  const Xdb_collation_codec *codec,
                                  const uchar *src, size_t src_len,
                                  bool is_varchar)
{
  auto *c= static_cast<const Xdb_collation_codec_utf8mb4 *>(codec);
  uint i;
  uint max_bytes= 0; //for char
  for (i= 0; i < src_len && max_bytes < src_len;)
  {
    my_wc_t wc;
    int res;
    uchar weight_buf[2];

    res= c->m_cs->cset->mb_wc(c->m_cs, &wc, src + i, src + src_len);
    DBUG_ASSERT(res > 0 && res <= 4);

    c->m_cs->coll->strnxfrm(c->m_cs, weight_buf, 2, 1, src + i, src_len, MY_STRXFRM_NOPAD_WITH_SPACE);
    /* for utf8mb4 characters, write src into unpack_info, others use codec->m_enc_idx[wc] */
    if (weight_buf[0] == 0xFF && weight_buf[1] == 0xFD)
    {
      writer->write(XDB_UTF8MB4_LENGTH, wc);
    }
    else
    {
      writer->write(c->m_enc_size[wc], c->m_enc_idx[wc]);
    }

    i+= res;
    if (!is_varchar)
    {
      max_bytes+= 4; //max bytes for every utf8 character.
    }
  }

  DBUG_ASSERT(i <= src_len);
}

/*
  Write unpack_data for a gbk collation
*/
static void xdb_write_unpack_gbk(Xdb_bit_writer *writer,
                                  const Xdb_collation_codec *codec,
                                  const uchar *src, size_t src_len,
                                  bool is_varchar)
{
  const auto c= static_cast<const Xdb_collation_codec_gbk*>(codec);
  uint i;
  uint max_bytes = 0;
  for (i= 0; i < src_len && max_bytes < src_len;)
  {
    my_wc_t wc;
    int res;

    res= c->m_cs->cset->mbcharlen(c->m_cs, (uint)(src[i]));
    if (res == 1)
    {
      wc= src[i];
    }
    else
    {
      wc= src[i] << 8;
      wc= wc | src[i+1];
    }

    writer->write(c->m_enc_size[wc], c->m_enc_idx[wc]);

    i+= res;
    if (!is_varchar)
    {
      max_bytes+= 2; //max bytes for every utf8 character.
    }
  }
  DBUG_ASSERT(i <= src_len);
}

static uint xdb_read_unpack_utf8(Xdb_bit_reader *reader,
                                 const Xdb_collation_codec *codec,
                                 const uchar *src, size_t src_len,
                                 uchar *dst, size_t *dst_len)
{
  const auto c= static_cast<const Xdb_collation_codec_utf8*>(codec);
  int len= 0;
  DBUG_ASSERT(src_len % 2 == 0);
  for (uint i= 0; i < src_len; i+=2)
  {
    my_wc_t src_char, dst_char;
    src_char = src[i] << 8 | src[i+1];
    if (c->m_dec_size[src_char] > 0)
    {
      uint *ret;
      // Unpack info is needed but none available.
      if (reader == nullptr)
      {
        return UNPACK_INFO_MISSING;
      }

      if ((ret= reader->read(c->m_dec_size[src_char])) == nullptr)
      {
        return UNPACK_FAILURE;
      }

      if (*ret < c->level)
      {
        dst_char= c->m_dec_idx[*ret][src_char];
      }
      else
      {
        dst_char= c->m_dec_idx_ext[src_char][*ret - c->level];
      }
    }
    else
    {
      dst_char= c->m_dec_idx[0][src_char];
    }

    len= c->m_cs->cset->wc_mb(c->m_cs, dst_char, dst, dst + *dst_len);
    DBUG_ASSERT(*dst_len >= (size_t)len);
    *dst_len-= len;
    dst+= len;
  }

  return UNPACK_SUCCESS;
}

static uint xdb_read_unpack_utf8mb4(Xdb_bit_reader *reader,
                                 const Xdb_collation_codec *codec,
                                 const uchar *src, size_t src_len,
                                 uchar *dst, size_t *dst_len)
{
  const auto c= static_cast<const Xdb_collation_codec_utf8*>(codec);
  int len= 0;
  DBUG_ASSERT(src_len % 2 == 0);
  for (uint i= 0; i < src_len; i+=2)
  {
    my_wc_t src_char, dst_char;
    uint *ret;
    src_char = src[i] << 8 | src[i+1];
    if (src_char == 0xFFFD)
    {
      if ((ret= reader->read(XDB_UTF8MB4_LENGTH)) == nullptr)
      {
        return UNPACK_FAILURE;
      }
      dst_char= *ret;
    }
    else
    {
      if (c->m_dec_size[src_char] > 0)
      {
        uint *ret;
        // Unpack info is needed but none available.
        if (reader == nullptr)
        {
          return UNPACK_INFO_MISSING;
        }

        if ((ret= reader->read(c->m_dec_size[src_char])) == nullptr)
        {
          return UNPACK_FAILURE;
        }
        if (*ret < c->level)
        {
          dst_char= c->m_dec_idx[*ret][src_char];
        }
        else
        {
          dst_char= c->m_dec_idx_ext[src_char][*ret - c->level];
        }
      }
      else
      {
        dst_char= c->m_dec_idx[0][src_char];
      }
    }

    len= c->m_cs->cset->wc_mb(c->m_cs, dst_char, dst, dst + *dst_len);
    DBUG_ASSERT(*dst_len >= (size_t)len);
    *dst_len-= len;
    dst+= len;
  }

  return UNPACK_SUCCESS;
}


static uint xdb_read_unpack_gbk(Xdb_bit_reader *reader,
                                 const Xdb_collation_codec *codec,
                                 const uchar *src, size_t src_len,
                                 uchar *dst, size_t *dst_len)
{
  const auto c= static_cast<const Xdb_collation_codec_gbk*>(codec);
  int len= 0;
  DBUG_ASSERT(src_len % 2 == 0);
  for (uint i= 0; i < src_len; i+=2)
  {
    my_wc_t src_char, dst_char, gbk_high;
    // prefix for gbk 1 byte character
    if (src[i] == 0x20)
    {
      src_char= src[i+1];
    }
    else
    {
      src_char= src[i] << 8 | src[i+1];
    }
    if (c->m_dec_size[src_char] > 0)
    {
      uint *ret;
      // Unpack info is needed but none available.
      if (reader == nullptr)
      {
        return UNPACK_INFO_MISSING;
      }

      if ((ret= reader->read(c->m_dec_size[src_char])) == nullptr)
      {
        return UNPACK_FAILURE;
      }

      if (*ret < c->level)
      {
        dst_char= c->m_dec_idx[*ret][src_char];
      }
      else
      {
        dst_char= c->m_dec_idx_ext[src_char][*ret - c->level];
      }
    }
    else
    {
      dst_char= c->m_dec_idx[0][src_char];
    }

    gbk_high= dst_char >> 8;
    len= c->m_cs->cset->mbcharlen(c->m_cs, gbk_high);
    DBUG_ASSERT(len > 0 && len <= 2);
    if (len == 1)
    {
      *dst= dst_char;
    }
    else
    {
      *dst= dst_char >> 8;
      *(dst+1)= dst_char & 0xFF;
    }

    dst += len;
    if (dst_len)
    {
      *dst_len-= len;
    }
  }

  return UNPACK_SUCCESS;
}

/*
  Function of type xdb_make_unpack_info_t

  @detail
    Make unpack_data for VARCHAR(n) in a specific charset.
*/

static void
xdb_make_unpack_varchar(const Xdb_collation_codec* codec,
                             Field *field,
                             Xdb_pack_field_context *pack_ctx)
{
  auto f= static_cast<const Field_varstring *>(field);
  uchar *src= f->ptr + f->length_bytes;
  size_t src_len= f->length_bytes == 1 ? (uint) *f->ptr : uint2korr(f->ptr);

  Xdb_bit_writer bit_writer(pack_ctx->writer);

  //if column-prefix is part of key, we don't support index-only-scan
  //so, src_len is ok for write unpack_info.
  if (codec->m_cs == &my_charset_utf8_general_ci)
  {
    xdb_write_unpack_utf8(&bit_writer, codec, src, src_len, true);
  }
  else if (codec->m_cs == &my_charset_gbk_chinese_ci)
  {
    xdb_write_unpack_gbk(&bit_writer, codec, src, src_len, true);
  }
  else if (codec->m_cs == &my_charset_utf8mb4_general_ci)
  {
    xdb_write_unpack_utf8mb4(&bit_writer, codec, src, src_len, true);
  }
}

/*
  Function of type xdb_index_field_unpack_t

  @seealso
    xdb_pack_with_varchar_space_pad - packing function
    xdb_unpack_binary_varchar_space_pad - a similar unpacking function
*/

int
xdb_unpack_varchar_space_pad(Xdb_field_packing *fpi, Field *field,
                                  uchar *dst,
                                  Xdb_string_reader *reader,
                                  Xdb_string_reader *unp_reader)
{
  const uchar *ptr;
  size_t len= 0;
  bool finished= false;
  uchar *d0= dst;
  Field_varstring* field_var= static_cast<Field_varstring*>(field);
  uchar *dst_end= dst + field_var->pack_length();
  size_t dst_len = field_var->pack_length() - field_var->length_bytes;
  dst += field_var->length_bytes;
  Xdb_bit_reader bit_reader(unp_reader);

  uint space_padding_bytes= 0;
  uint extra_spaces;
  if (!unp_reader)
  {
    return UNPACK_INFO_MISSING;
  }

  if ((fpi->m_unpack_info_uses_two_bytes?
       unp_reader->read_uint16(&extra_spaces):
       unp_reader->read_uint8(&extra_spaces)))
  {
    return UNPACK_FAILURE;
  }

  if (extra_spaces <= 8)
  {
    space_padding_bytes= -(static_cast<int>(extra_spaces) - 8);
    extra_spaces= 0;
  }
  else
    extra_spaces -= 8;

  space_padding_bytes *= fpi->space_xfrm_len;

  /* Decode the length-emitted encoding here */
  while ((ptr= (const uchar*)reader->read(fpi->m_segment_size)))
  {
    char last_byte= ptr[fpi->m_segment_size - 1];  // number of padding bytes
    size_t used_bytes;
    if (last_byte == VARCHAR_CMP_EQUAL_TO_SPACES)
    {
      // this is the last one
      if (space_padding_bytes > (fpi->m_segment_size-1))
        return UNPACK_FAILURE;  // Cannot happen, corrupted data
      used_bytes= (fpi->m_segment_size-1) - space_padding_bytes;
      finished= true;
    }
    else
    {
      if (last_byte != VARCHAR_CMP_LESS_THAN_SPACES &&
          last_byte != VARCHAR_CMP_GREATER_THAN_SPACES)
      {
        return UNPACK_FAILURE;
      }
      used_bytes= fpi->m_segment_size-1;
    }

    if (dst + used_bytes > dst_end)
    {
      // The value on disk is longer than the field definition allows?
      return UNPACK_FAILURE;
    }

    uint ret;
    auto cset= fpi->m_charset_codec->m_cs;
    if (cset == &my_charset_gbk_chinese_ci)
    {
      if ((ret= xdb_read_unpack_gbk(&bit_reader,
                                   fpi->m_charset_codec, ptr, used_bytes,
                                   dst, &dst_len)) != UNPACK_SUCCESS)
      {
        return ret;
      }
    }
    else if (cset == &my_charset_utf8_general_ci)
    {
      if ((ret= xdb_read_unpack_utf8(&bit_reader,
                                   fpi->m_charset_codec, ptr, used_bytes,
                                   dst, &dst_len)) != UNPACK_SUCCESS)
      {
        return ret;
      }
    }
    else if (cset == &my_charset_utf8mb4_general_ci)
    {
      if ((ret= xdb_read_unpack_utf8mb4(&bit_reader,
                                   fpi->m_charset_codec, ptr, used_bytes,
                                   dst, &dst_len)) != UNPACK_SUCCESS)
      {
        return ret;
      }
    }

    dst = dst_end - dst_len;
    len = dst_end - d0 - dst_len - field_var->length_bytes;

    if (finished)
    {
      if (extra_spaces)
      {
        if (dst + extra_spaces > dst_end)
          return UNPACK_FAILURE;
        // pad_char has a 1-byte form in all charsets that
        // are handled by xdb_init_collation_mapping.
        memset(dst, field_var->charset()->pad_char, extra_spaces);
        len += extra_spaces;
      }
      break;
    }
  }

  if (!finished)
    return UNPACK_FAILURE;

  /* Save the length */
  if (field_var->length_bytes == 1)
  {
    d0[0]= len;
  }
  else
  {
    DBUG_ASSERT(field_var->length_bytes == 2);
    int2store(d0, len);
  }
  return UNPACK_SUCCESS;
}


/*
  Function of type xdb_make_unpack_info_t

  @detail
    Make unpack_data for CHAR(n) value in a specific charset.
    It is CHAR(N), so SQL layer has padded the value with spaces up to N chars.

  @seealso
    The VARCHAR variant is in xdb_make_unpack_varchar
*/

static void xdb_make_unpack_char(const Xdb_collation_codec *codec,
                                 Field *field,
                                 Xdb_pack_field_context *pack_ctx)
{
  uchar *src= field->ptr;
  Xdb_bit_writer bit_writer(pack_ctx->writer);
  if (codec->m_cs == &my_charset_utf8_general_ci)
  {
    xdb_write_unpack_utf8(&bit_writer, codec, src, field->pack_length(), false);
  }
  else if (codec->m_cs == &my_charset_gbk_chinese_ci)
  {
    xdb_write_unpack_gbk(&bit_writer, codec, src, field->pack_length(), false);
  }
  else if (codec->m_cs == &my_charset_utf8mb4_general_ci)
  {
    xdb_write_unpack_utf8mb4(&bit_writer, codec, src, field->pack_length(), false);
  }
}

/*
  Function of type xdb_index_field_unpack_t
*/

static int xdb_unpack_char(Xdb_field_packing *fpi,
                           Field *field __attribute__((__unused__)),
                           uchar *dst,
                           Xdb_string_reader *reader,
                           Xdb_string_reader *unp_reader)
{
  const uchar *ptr;
  int ret= UNPACK_SUCCESS;
  uint len = fpi->m_max_image_len;
  uchar *dst_end= dst + field->pack_length();
  size_t dst_len= field->pack_length();
  Xdb_bit_reader bit_reader(unp_reader);
  auto cset= fpi->m_charset_codec->m_cs;

  if (!(ptr= (const uchar*)reader->read(len)))
  {
    return UNPACK_FAILURE;
  }

  if (cset == &my_charset_utf8_general_ci)
  {
    ret = xdb_read_unpack_utf8(unp_reader ? &bit_reader : nullptr,
                             fpi->m_charset_codec, ptr, len, dst, &dst_len);
  }
  else if (cset == &my_charset_gbk_chinese_ci)
  {
    ret = xdb_read_unpack_gbk(unp_reader ? &bit_reader : nullptr,
                             fpi->m_charset_codec, ptr, len, dst, &dst_len);
  }
  else if (cset == &my_charset_utf8mb4_general_ci)
  {
    ret = xdb_read_unpack_utf8mb4(unp_reader ? &bit_reader : nullptr,
                             fpi->m_charset_codec, ptr, len, dst, &dst_len);
  }

  if (ret == UNPACK_SUCCESS)
  {
    cset->cset->fill(cset, reinterpret_cast<char *>(dst_end - dst_len),
                     dst_len, cset->pad_char);
  }
  return ret;
}




// See Xdb_charset_space_info::spaces_xfrm
const int XDB_SPACE_XFRM_SIZE = 32;

// A class holding information about how space character is represented in a
// charset.
class Xdb_charset_space_info {
public:
  Xdb_charset_space_info(const Xdb_charset_space_info &) = delete;
  Xdb_charset_space_info &operator=(const Xdb_charset_space_info &) = delete;
  Xdb_charset_space_info() = default;

  // A few strxfrm'ed space characters, at least XDB_SPACE_XFRM_SIZE bytes
  std::vector<uchar> spaces_xfrm;

  // length(strxfrm(' '))
  size_t space_xfrm_len;

  // length of the space character itself
  // Typically space is just 0x20 (length=1) but in ucs2 it is 0x00 0x20
  // (length=2)
  size_t space_mb_len;
};

static std::array<std::unique_ptr<Xdb_charset_space_info>, MY_ALL_CHARSETS_SIZE>
    xdb_mem_comparable_space;

/*
  @brief
  For a given charset, get
   - strxfrm('    '), a sample that is at least XDB_SPACE_XFRM_SIZE bytes long.
   - length of strxfrm(charset, ' ')
   - length of the space character in the charset

  @param cs  IN    Charset to get the space for
  @param ptr OUT   A few space characters
  @param len OUT   Return length of the space (in bytes)

  @detail
    It is tempting to pre-generate mem-comparable form of space character for
    every charset on server startup.
    One can't do that: some charsets are not initialized until somebody
    attempts to use them (e.g. create or open a table that has a field that
    uses the charset).
*/

static void xdb_get_mem_comparable_space(const CHARSET_INFO *const cs,
                                         const std::vector<uchar> **xfrm,
                                         size_t *const xfrm_len,
                                         size_t *const mb_len) {
  DBUG_ASSERT(cs->number < MY_ALL_CHARSETS_SIZE);
  if (!xdb_mem_comparable_space[cs->number].get()) {
    XDB_MUTEX_LOCK_CHECK(xdb_mem_cmp_space_mutex);
    if (!xdb_mem_comparable_space[cs->number].get()) {
      // Upper bound of how many bytes can be occupied by multi-byte form of a
      // character in any charset.
      const int MAX_MULTI_BYTE_CHAR_SIZE = 4;
      DBUG_ASSERT(cs->mbmaxlen <= MAX_MULTI_BYTE_CHAR_SIZE);

      // multi-byte form of the ' ' (space) character
      uchar space_mb[MAX_MULTI_BYTE_CHAR_SIZE];

      const size_t space_mb_len = cs->cset->wc_mb(
          cs, (my_wc_t)cs->pad_char, space_mb, space_mb + sizeof(space_mb));

      uchar space[20]; // mem-comparable image of the space character

      const size_t space_len = cs->coll->strnxfrm(cs, space, sizeof(space), 1,
                                                  space_mb, space_mb_len, MY_STRXFRM_NOPAD_WITH_SPACE);
      Xdb_charset_space_info *const info = new Xdb_charset_space_info;
      info->space_xfrm_len = space_len;
      info->space_mb_len = space_mb_len;
      while (info->spaces_xfrm.size() < XDB_SPACE_XFRM_SIZE) {
        info->spaces_xfrm.insert(info->spaces_xfrm.end(), space,
                                 space + space_len);
      }
      xdb_mem_comparable_space[cs->number].reset(info);
    }
    XDB_MUTEX_UNLOCK_CHECK(xdb_mem_cmp_space_mutex);
  }

  *xfrm = &xdb_mem_comparable_space[cs->number]->spaces_xfrm;
  *xfrm_len = xdb_mem_comparable_space[cs->number]->space_xfrm_len;
  *mb_len = xdb_mem_comparable_space[cs->number]->space_mb_len;

  if (cs == &my_charset_gbk_chinese_ci || cs == &my_charset_gbk_bin)
  {
    *xfrm_len= 2;
  }
}

mysql_mutex_t xdb_mem_cmp_space_mutex;

std::array<const Xdb_collation_codec *, MY_ALL_CHARSETS_SIZE>
    xdb_collation_data;
mysql_mutex_t xdb_collation_data_mutex;

static bool xdb_is_collation_supported(const my_core::CHARSET_INFO * cs)
{
  return (cs->coll == &my_collation_8bit_simple_ci_handler ||
          cs == &my_charset_utf8_general_ci ||
          cs == &my_charset_gbk_chinese_ci ||
          cs == &my_charset_utf8mb4_general_ci);
}

template <typename C, uint S>
static C *xdb_compute_lookup_values(const my_core::CHARSET_INFO *cs)
{
  using T = typename C::character_type;
  auto cur= new C;

  std::map<T, std::vector<T>> rev_map;
  size_t max_conflict_size= 0;
  std::array<uchar, S> srcbuf;
  std::array<uchar, S * XDB_MAX_XFRM_MULTIPLY> dstbuf;
  for (int src = 0; src < (1 << (8 * sizeof(T))); src++)
  {
    uint srclen= 0;
    if (cs == &my_charset_utf8_general_ci ||
        cs == &my_charset_utf8mb4_general_ci)
    {
      srclen= cs->cset->wc_mb(cs, src, srcbuf.data(), srcbuf.data() + srcbuf.size());
      DBUG_ASSERT(srclen > 0 && srclen <= 3);
    }
    else if (cs == &my_charset_gbk_chinese_ci)
    {
      if (src <= 0xFF)
      {
        /* for 1byte src, use 0x00 makeup 2btyes */
        srcbuf[0]= 0;
        srcbuf[1]= src & 0xFF;
      }
      else
      {
        /* gbk code */
        srcbuf[0]= src >> 8;
        srcbuf[1]= src & 0xFF;
      }
      srclen= 2;
    }
    size_t xfrm_len __attribute__((__unused__))= cs->coll->strnxfrm(cs, dstbuf.data(), dstbuf.size(),
                                                 dstbuf.size(), srcbuf.data(), srclen, MY_STRXFRM_NOPAD_WITH_SPACE);
    T dst;
    if (xfrm_len > 0) {
      dst= dstbuf[0] << 8 | dstbuf[1];
      DBUG_ASSERT(xfrm_len >= sizeof(dst));
    } else {
     /*
      According to RFC 3629, UTF-8 should prohibit characters between
      U+D800 and U+DFFF, which are reserved for surrogate pairs and do
      not directly represent characters.
     */
     dst = src;
    }

    rev_map[dst].push_back(src);
    max_conflict_size= std::max(max_conflict_size, rev_map[dst].size());
  }
  cur->m_dec_idx.resize(cur->level);

  for (const auto &p : rev_map)
  {
    T dst= p.first;
    for (T idx = 0; idx < p.second.size(); idx++)
    {
      auto src= p.second[idx];
      uchar bits=
        my_bit_log2(my_round_up_to_next_power(p.second.size()));
      cur->m_enc_idx[src]= idx;
      cur->m_enc_size[src]= bits;
      cur->m_dec_size[dst]= bits;
      if (cur->level == 0 || idx < cur->level)
      {
        cur->m_dec_idx[idx][dst]= src;
      }
      else
      {
        cur->m_dec_idx_ext[dst].push_back(src);
      }
    }
  }

  cur->m_cs= cs;

  return cur;
}

static const Xdb_collation_codec *xdb_init_collation_mapping(
    const my_core::CHARSET_INFO *cs)
{
  DBUG_ASSERT(cs && cs->state & MY_CS_AVAILABLE);
  const Xdb_collation_codec *codec= xdb_collation_data[cs->number];

  if (codec == nullptr && xdb_is_collation_supported(cs))
  {
    mysql_mutex_lock(&xdb_collation_data_mutex);
    codec= xdb_collation_data[cs->number];
    if (codec == nullptr)
    {
      // Compute reverse mapping for simple collations.
      if (cs->coll == &my_collation_8bit_simple_ci_handler)
      {
        Xdb_collation_codec_simple *cur= new Xdb_collation_codec_simple();
        std::map<uchar, std::vector<uchar>> rev_map;
        size_t max_conflict_size= 0;
        for (int src = 0; src < 256; src++)
        {
          uchar dst= cs->sort_order[src];
          rev_map[dst].push_back(src);
          max_conflict_size= std::max(max_conflict_size, rev_map[dst].size());
        }
        cur->m_dec_idx.resize(max_conflict_size);

        for (auto const &p : rev_map)
        {
          uchar dst= p.first;
          for (uint idx = 0; idx < p.second.size(); idx++)
          {
            uchar src= p.second[idx];
            uchar bits= my_bit_log2(my_round_up_to_next_power(p.second.size()));
            cur->m_enc_idx[src]= idx;
            cur->m_enc_size[src]= bits;
            cur->m_dec_size[dst]= bits;
            cur->m_dec_idx[idx][dst]= src;
          }
        }

        cur->m_make_unpack_info_func=
          {{xdb_make_unpack_simple_varchar, xdb_make_unpack_simple}};

        cur->m_unpack_func=
          {{ xdb_unpack_simple_varchar_space_pad, xdb_unpack_simple }};

        cur->m_cs= cs;
        codec= cur;
      }
      else if (cs == &my_charset_utf8_general_ci) {
        Xdb_collation_codec_utf8 *cur;
        cur= xdb_compute_lookup_values<Xdb_collation_codec_utf8, 3>(cs);
        cur->m_make_unpack_info_func=
          {{ xdb_make_unpack_varchar, xdb_make_unpack_char }};
        cur->m_unpack_func=
          {{ xdb_unpack_varchar_space_pad, xdb_unpack_char }};
        codec= cur;
      }
      // The gbk_chinese_ci collation is similar to utf8 collations, for 1 byte weight character
      // extended to 2 bytes. for example:
      // a -> 0x61->(0x2061)
      // b -> 0x62->(0x2062)
      // after that, every character in gbk get 2bytes weight.
      else if (cs == &my_charset_gbk_chinese_ci){
        Xdb_collation_codec_gbk *cur;
        cur= xdb_compute_lookup_values<Xdb_collation_codec_gbk, 2>(cs);
        cur->m_make_unpack_info_func=
          {{ xdb_make_unpack_varchar, xdb_make_unpack_char }};
        cur->m_unpack_func=
          {{ xdb_unpack_varchar_space_pad, xdb_unpack_char }};
        codec= cur;
      }
      // The utf8mb4 collation is similar to utf8 collations. For 4 bytes characters, the weight generated by
      // my_strnxfrm_unicode is 2bytes and weight is 0xFFFD. So these characters,we just store character itself in
      // unpack_info,every character use 21 bits.
      else if (cs == &my_charset_utf8mb4_general_ci){
        Xdb_collation_codec_utf8mb4 *cur;
        cur= xdb_compute_lookup_values<Xdb_collation_codec_utf8mb4, 4>(cs);
        cur->m_make_unpack_info_func=
          {{ xdb_make_unpack_varchar, xdb_make_unpack_char }};
        cur->m_unpack_func=
          {{ xdb_unpack_varchar_space_pad, xdb_unpack_char }};
        codec= cur;
      } else {
        // utf8mb4_0900_ai_ci
        // Out of luck for now.
      }

      if (codec != nullptr)
      {
        xdb_collation_data[cs->number]= codec;
      }
    }
    mysql_mutex_unlock(&xdb_collation_data_mutex);
  }

  return codec;
}

static int get_segment_size_from_collation(const CHARSET_INFO *const cs) {
  int ret;
  if (cs == &my_charset_utf8mb4_bin || cs == &my_charset_utf16_bin ||
      cs == &my_charset_utf16le_bin || cs == &my_charset_utf32_bin) {
    /*
      In these collations, a character produces one weight, which is 3 bytes.
      Segment has 3 characters, add one byte for VARCHAR_CMP_* marker, and we
      get 3*3+1=10
    */
    ret = 10;
  } else {
    /*
      All other collations. There are two classes:
      - Unicode-based, except for collations mentioned in the if-condition.
        For these all weights are 2 bytes long, a character may produce 0..8
        weights.
        in any case, 8 bytes of payload in the segment guarantee that the last
        space character won't span across segments.

      - Collations not based on unicode. These have length(strxfrm(' '))=1,
        there nothing to worry about.

      In both cases, take 8 bytes payload + 1 byte for VARCHAR_CMP* marker.
    */
    ret = 9;
  }
  DBUG_ASSERT(ret < XDB_SPACE_XFRM_SIZE);
  return ret;
}

/*
  @brief
    Setup packing of index field into its mem-comparable form

  @detail
    - It is possible produce mem-comparable form for any datatype.
    - Some datatypes also allow to unpack the original value from its
      mem-comparable form.
      = Some of these require extra information to be stored in "unpack_info".
        unpack_info is not a part of mem-comparable form, it is only used to
        restore the original value

  @param
    field  IN  field to be packed/un-packed

  @return
    TRUE  -  Field can be read with index-only reads
    FALSE -  Otherwise
*/

bool Xdb_field_packing::setup(const Xdb_key_def *const key_descr,
                              const Field *const field, const uint &keynr_arg,
                              const uint &key_part_arg,
                              const uint16 &key_length) {
  int res = false;
  enum_field_types type = field ? field->real_type() : MYSQL_TYPE_LONGLONG;

  m_keynr = keynr_arg;
  m_key_part = key_part_arg;

  m_maybe_null = field ? field->real_maybe_null() : false;
  m_unpack_func = nullptr;
  m_make_unpack_info_func = nullptr;
  m_unpack_data_len = 0;
  space_xfrm = nullptr; // safety
  m_weights = 0;  // only used for varchar/char
  m_prefix_index_flag = false;

  /* Calculate image length. By default, is is pack_length() */
  m_max_image_len =
      field ? field->pack_length() : XENGINE_SIZEOF_HIDDEN_PK_COLUMN;

  m_skip_func = xdb_skip_max_length;
  m_pack_func = xdb_pack_with_make_sort_key;

  switch (type) {
  case MYSQL_TYPE_LONGLONG:
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_TINY:
    m_unpack_func = xdb_unpack_integer;
    return true;

  case MYSQL_TYPE_DOUBLE:
    m_pack_func = xdb_pack_double;
    m_unpack_func = xdb_unpack_double;
    return true;

  case MYSQL_TYPE_FLOAT:
    m_pack_func = xdb_pack_float;
    m_unpack_func = xdb_unpack_float;
    return true;

  case MYSQL_TYPE_NEWDECIMAL:
  /*
    Decimal is packed with Field_new_decimal::make_sort_key, which just
    does memcpy.
    Unpacking decimal values was supported only after fix for issue#253,
    because of that ha_xengine::get_storage_type() handles decimal values
    in a special way.
  */
  case MYSQL_TYPE_DATETIME2:
  case MYSQL_TYPE_TIMESTAMP2:
  /* These are packed with Field_temporal_with_date_and_timef::make_sort_key */
  case MYSQL_TYPE_TIME2: /* TIME is packed with Field_timef::make_sort_key */
  case MYSQL_TYPE_YEAR:  /* YEAR is packed with  Field_tiny::make_sort_key */
    /* Everything that comes here is packed with just a memcpy(). */
    m_unpack_func = xdb_unpack_binary_str;
    return true;

  case MYSQL_TYPE_NEWDATE:
    /*
      This is packed by Field_newdate::make_sort_key. It assumes the data is
      3 bytes, and packing is done by swapping the byte order (for both big-
      and little-endian)
    */
    m_unpack_func = xdb_unpack_newdate;
    return true;
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_JSON:
  case MYSQL_TYPE_BLOB: {
    m_pack_func = xdb_pack_blob;
    if (key_descr) {
      const CHARSET_INFO *cs = field->charset();
      if (cs == &my_charset_bin) {  //for blob type
        // The my_charset_bin collation is special in that it will consider
        // shorter strings sorting as less than longer strings.
        //
        // See Field_blob::make_sort_key for details.
        m_max_image_len =
            key_length + (field->charset() == &my_charset_bin
                              ? reinterpret_cast<const Field_blob *>(field)
                                    ->pack_length_no_ptr()
                              : 0);

      } else { // for text type
        if (cs == &my_charset_utf8mb4_0900_ai_ci ||
            cs == &my_charset_utf8mb4_general_ci ||
            cs == &my_charset_utf8mb4_bin) {
          // m_weights is number of characters, key_length is length in bytes
          // for utf8mb4_0900_ai_ci for diffent weight length,from 2bytes to 32
          // bytes, only m_max_image_len is not enougth.
          m_weights = key_length > 0 ? key_length / 4 : 0;
        } else if (cs == &my_charset_utf8_general_ci ||
                   cs == &my_charset_utf8_bin) {
          m_weights = key_length > 0 ? key_length / 3 : 0;
        } else if (cs == &my_charset_gbk_chinese_ci ||
                   cs == &my_charset_gbk_bin) {
          m_weights = key_length > 0 ? key_length / 2 : 0;
        } else if (cs == &my_charset_latin1_bin) {
          m_weights = key_length > 0 ? key_length : 0;
        } else {
          // other collation is not supported by xengine-index, later will report error by create_cfs
        }

        m_max_image_len = cs->coll->strnxfrmlen(cs, key_length);
      }
      // Return false because indexes on text/blob will always require
      // a prefix. With a prefix, the optimizer will not be able to do an
      // index-only scan since there may be content occuring after the prefix
      // length.
      m_prefix_index_flag = true;
      return false;
    }
  }
  default:
    break;
  }

  m_unpack_info_stores_value = false;
  /* Handle [VAR](CHAR|BINARY) */

  if (type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_STRING) {
    /*
      For CHAR-based columns, check how strxfrm image will take.
      field->field_length = field->char_length() * cs->mbmaxlen.
    */
    const CHARSET_INFO *cs = field->charset();
    m_max_image_len = cs->coll->strnxfrmlen(cs, field->field_length);
    m_max_strnxfrm_len = m_max_image_len;
    m_charset = cs;
    m_weights = (const_cast<Field*>(field))->char_length();
  }
  const bool is_varchar = (type == MYSQL_TYPE_VARCHAR);
  const CHARSET_INFO *cs = field->charset();
  // max_image_len before chunking is taken into account
  const int max_image_len_before_chunks = m_max_image_len;

  if (is_varchar) {
    // The default for varchar is variable-length, without space-padding for
    // comparisons
    m_skip_func = xdb_skip_variable_length;
    m_pack_func = xdb_pack_with_varchar_encoding;
    m_max_image_len =
        (m_max_image_len / (XDB_ESCAPE_LENGTH - 1) + 1) * XDB_ESCAPE_LENGTH;

    const auto field_var = static_cast<const Field_varstring *>(field);
    m_unpack_info_uses_two_bytes = (field_var->field_length + 8 >= 0x100);
  }

  if (type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_STRING) {
    // See http://dev.mysql.com/doc/refman/5.7/en/string-types.html for
    // information about character-based datatypes are compared.
    bool use_unknown_collation = false;
    DBUG_EXECUTE_IF("myx_enable_unknown_collation_index_only_scans",
                    use_unknown_collation = true;);

    if (cs == &my_charset_bin) {
      // - SQL layer pads BINARY(N) so that it always is N bytes long.
      // - For VARBINARY(N), values may have different lengths, so we're using
      //   variable-length encoding. This is also the only charset where the
      //   values are not space-padded for comparison.
      m_unpack_func = is_varchar ? xdb_unpack_binary_or_utf8_varchar
                                 : xdb_unpack_binary_str;
      res = true;
    } else if (cs == &my_charset_latin1_bin || cs == &my_charset_utf8_bin ||
               cs == &my_charset_gbk_bin || cs == &my_charset_utf8mb4_bin) {
      // For _bin collations, mem-comparable form of the string is the string
      // itself.
      if (is_varchar) {
        // VARCHARs
        // - are compared as if they were space-padded
        // - but are not actually space-padded (reading the value back
        //   produces the original value, without the padding)
        m_unpack_func = xdb_unpack_binary_varchar_space_pad;
        m_skip_func = xdb_skip_variable_space_pad;
        m_pack_func = xdb_pack_with_varchar_space_pad;
        m_make_unpack_info_func = xdb_dummy_make_unpack_info;
        m_segment_size = get_segment_size_from_collation(cs);
        m_max_image_len =
            (max_image_len_before_chunks / (m_segment_size - 1) + 1) *
            m_segment_size;
        xdb_get_mem_comparable_space(cs, &space_xfrm, &space_xfrm_len,
                                     &space_mb_len);
      } else {
        if (cs == &my_charset_gbk_bin) {
          m_pack_func = xdb_pack_with_make_sort_key_gbk;
        }
        // SQL layer pads CHAR(N) values to their maximum length.
        // We just store that and restore it back.
        m_unpack_func= (cs == &my_charset_latin1_bin)? xdb_unpack_binary_str:
                                 xdb_unpack_bin_str;
      }

      res = true;
    } else {
      // This is [VAR]CHAR(n) and the collation is not $(charset_name)_bin

      res = true; // index-only scans are possible
      m_unpack_data_len = is_varchar ? 0 : field->field_length;
      const uint idx = is_varchar ? 0 : 1;
      const Xdb_collation_codec *codec = nullptr;

      if (is_varchar) {
        // VARCHAR requires space-padding for doing comparisons
        //
        // The check for cs->levels_for_order is to catch
        // latin2_czech_cs and cp1250_czech_cs - multi-level collations
        // that Variable-Length Space Padded Encoding can't handle.
        // It is not expected to work for any other multi-level collations,
        // either.
        // 8.0 removes levels_for_order but leaves levels_for_compare which
        // seems to be identical in value and extremely similar in
        // purpose/indication for our needs here.
        if (cs->levels_for_compare != 1)
        {
          //  NO_LINT_DEBUG
          sql_print_warning("XEngine: you're trying to create an index "
                            "with a multi-level collation %s", cs->name);
          //  NO_LINT_DEBUG
          sql_print_warning("XEngine will handle this collation internally "
                            " as if it had a NO_PAD attribute.");

          m_pack_func = xdb_pack_with_varchar_encoding;
          m_skip_func = xdb_skip_variable_length;
        }
        else if (cs->pad_attribute == PAD_SPACE)
        {
          m_pack_func= xdb_pack_with_varchar_space_pad;
          m_skip_func= xdb_skip_variable_space_pad;
          m_segment_size= get_segment_size_from_collation(cs);
          m_max_image_len=
              (max_image_len_before_chunks/(m_segment_size-1) + 1) *
              m_segment_size;
          xdb_get_mem_comparable_space(cs, &space_xfrm, &space_xfrm_len,
                                       &space_mb_len);
        }
        else if (cs->pad_attribute == NO_PAD) //utf8mb4_0900_ai_ci
        {
          m_pack_func= xdb_pack_with_varchar_encoding;
          m_skip_func= xdb_skip_variable_length;
          m_segment_size= get_segment_size_from_collation(cs);
          m_max_image_len=
              (max_image_len_before_chunks/(m_segment_size-1) + 1) *
              m_segment_size;
          res = false;  //now, we just not support index-only-scan for collation utf8mb4_0900_ai_ci
        }
      }
      else //string CHAR[N]
      {
        if (cs == &my_charset_gbk_chinese_ci)
        {
          m_pack_func= xdb_pack_with_make_sort_key_gbk;
        } else if (cs == &my_charset_utf8mb4_0900_ai_ci) {
          m_pack_func = xdb_pack_with_make_sort_key_utf8mb4_0900;
        }
      }

      if ((codec = xdb_init_collation_mapping(cs)) != nullptr) {
        // The collation allows to store extra information in the unpack_info
        // which can be used to restore the original value from the
        // mem-comparable form.
        m_make_unpack_info_func = codec->m_make_unpack_info_func[idx];
        m_unpack_func = codec->m_unpack_func[idx];
        m_charset_codec = codec;
      } else if (cs == &my_charset_utf8mb4_0900_ai_ci) {
        //now, we only support value->mem-cmpkey form, but don't know how to
        //retore value from mem-comparable form.
        DBUG_ASSERT(m_unpack_func == nullptr);
        DBUG_ASSERT(m_make_unpack_info_func == nullptr);
        m_unpack_info_stores_value = false;
        res = false;  // Indicate that index-only reads are not possible
      } else if (use_unknown_collation) {
        // We have no clue about how this collation produces mem-comparable
        // form. Our way of restoring the original value is to keep a copy of
        // the original value in unpack_info.
        m_unpack_info_stores_value = true;
        m_make_unpack_info_func = is_varchar ? xdb_make_unpack_unknown_varchar
                                             : xdb_make_unpack_unknown;
        m_unpack_func =
            is_varchar ? xdb_unpack_unknown_varchar : xdb_unpack_unknown;
      } else {
        // Same as above: we don't know how to restore the value from its
        // mem-comparable form.
        // Here, we just indicate to the SQL layer we can't do it.
        DBUG_ASSERT(m_unpack_func == nullptr);
        m_unpack_info_stores_value = false;
        res = false; // Indicate that index-only reads are not possible
      }
    }

    // Make an adjustment: unpacking partially covered columns is not
    // possible. field->table is populated when called through
    // Xdb_key_def::setup, but not during ha_xengine::index_flags.
    if (field->table) {
      // Get the original Field object and compare lengths. If this key part is
      // a prefix of a column, then we can't do index-only scans.
      if (field->table->field[field->field_index]->field_length != key_length) {
        m_unpack_func = nullptr;
        m_make_unpack_info_func = nullptr;
        m_unpack_info_stores_value = true;
        m_prefix_index_flag = true;
        res = false;
      }
    } else {
      if (field->field_length != key_length) {
        m_unpack_func = nullptr;
        m_make_unpack_info_func = nullptr;
        m_unpack_info_stores_value = true;
        res = false;
      }
    }
  }
  return res;
}

/*
  get Field from table, if online-inplace-norebuild ddl, new key is only in the
  altered_table, so we need get field index from altered_table->key_info.

*/
uint Xdb_field_packing::get_field_index_in_table(
    const TABLE *const altered_table) const
{
  KEY *key_info = &altered_table->key_info[m_keynr];

  Field *field = key_info->key_part[m_key_part].field;
  DBUG_ASSERT(field != nullptr);

  return field->field_index;
}


Field *Xdb_field_packing::get_field_in_table(const TABLE *const tbl) const {
  return tbl->key_info[m_keynr].key_part[m_key_part].field;
}

void Xdb_field_packing::fill_hidden_pk_val(uchar **dst,
                                           const longlong &hidden_pk_id) const {
  DBUG_ASSERT(m_max_image_len == 8);

  String to;
  xdb_netstr_append_uint64(&to, hidden_pk_id);
  memcpy(*dst, to.ptr(), m_max_image_len);

  *dst += m_max_image_len;
}

///////////////////////////////////////////////////////////////////////////////////////////
// Xdb_ddl_manager
///////////////////////////////////////////////////////////////////////////////////////////

Xdb_tbl_def::~Xdb_tbl_def() {
  /* Don't free key definitions */
  if (m_key_descr_arr) {
    for (uint i = 0; i < m_key_count; i++) {
      m_key_descr_arr[i] = nullptr;
    }

    delete[] m_key_descr_arr;
    m_key_descr_arr = nullptr;
  }

  m_inplace_new_tdef = nullptr;

  m_dict_info = nullptr;
}

/*
  Put table definition DDL entry. Actual write is done at
  Xdb_dict_manager::commit.

  We write
    dbname.tablename -> version + {key_entry, key_entry, key_entry, ... }

  Where key entries are a tuple of
    ( cf_id, index_nr )
*/

#if 0
bool Xdb_tbl_def::put_dict(Xdb_dict_manager *const dict,
                           xengine::db::WriteBatch *const batch) {
#if 0
                           xengine::db::WriteBatch *const batch, uchar *const key,
                           const size_t &keylen) {
  StringBuffer<8 * Xdb_key_def::PACKED_SIZE> indexes;
  indexes.alloc(Xdb_key_def::VERSION_SIZE +
                m_key_count * Xdb_key_def::PACKED_SIZE * 2);
  xdb_netstr_append_uint16(&indexes, Xdb_key_def::DDL_ENTRY_INDEX_VERSION);
#endif

  for (uint i = 0; i < m_key_count; i++) {
    const Xdb_key_def &kd = *m_key_descr_arr[i];

    const uchar flags =
        (kd.m_is_reverse_cf ? Xdb_key_def::REVERSE_CF_FLAG : 0) |
        (kd.m_is_auto_cf ? Xdb_key_def::AUTO_CF_FLAG : 0);

    const uint cf_id = kd.get_cf()->GetID();
    /*
      If cf_id already exists, cf_flags must be the same.
      To prevent race condition, reading/modifying/committing CF flags
      need to be protected by mutex (dict_manager->lock()).
      When XEngine supports transaction with pessimistic concurrency
      control, we can switch to use it and removing mutex.
    */
    uint existing_cf_flags;
    if (dict->get_cf_flags(cf_id, &existing_cf_flags)) {
      if (existing_cf_flags != flags) {
        my_printf_error(ER_UNKNOWN_ERROR,
                        "XEngine sub table flag is different from existing flag. "
                        "Assign a new flag, or do not change existing flag.",
                        MYF(0));
        return true;
      }
    } else {
      dict->add_cf_flags(batch, cf_id, flags);
    }

#if 0
    xdb_netstr_append_uint32(&indexes, cf_id);
    xdb_netstr_append_uint32(&indexes, kd.m_index_number);
#endif
    dict->add_or_update_index_cf_mapping(batch, kd.m_index_type,
                                         kd.m_kv_format_version,
                                         kd.m_index_number, cf_id);
  }

#if 0
  const xengine::common::Slice skey((char *)key, keylen);
  const xengine::common::Slice svalue(indexes.c_ptr(), indexes.length());

  dict->put_key(batch, skey, svalue);
#endif
  return false;
}
#endif

void Xdb_tbl_def::check_if_is_mysql_system_table() {
  static const char *const system_dbs[] = {
      "mysql", "performance_schema", "information_schema",
  };

  m_is_mysql_system_table = false;
  for (uint ii = 0; ii < array_elements(system_dbs); ii++) {
    if (strcmp(m_dbname.c_str(), system_dbs[ii]) == 0) {
      m_is_mysql_system_table = true;
      break;
    }
  }
}

void Xdb_tbl_def::set_name(const std::string &name) {
  int err MY_ATTRIBUTE((__unused__));

  m_dbname_tablename = name;
  err = xdb_split_normalized_tablename(name, &m_dbname, &m_tablename,
                                       &m_partition);
  DBUG_ASSERT(err == 0);

  check_if_is_mysql_system_table();
}

int Xdb_tbl_def::clear_keys_for_ddl() {
  clear_added_key();
  clear_new_keys_info();

  m_inplace_new_tdef = nullptr;

  m_dict_info = nullptr;

  return 0;
}

/* for online build-index */
int Xdb_tbl_def::create_added_key(std::shared_ptr<Xdb_key_def> added_key,
                                  Added_key_info info) {
  m_added_key.emplace(added_key.get(), info);
  m_added_key_ref.push_back(added_key);
  return 0;
}

int Xdb_tbl_def::clear_added_key() {
  m_added_key.clear();

  for (auto kd : m_added_key_ref) {
    kd = nullptr;
  }

  m_added_key_ref.clear();
  return 0;
}

/* for online inplace rebuild table */
int Xdb_tbl_def::create_new_keys_info(std::shared_ptr<Xdb_key_def> added_key,
                                  Added_key_info info) {
  m_inplace_new_keys.emplace(added_key.get(), info);
  m_inplace_new_keys_ref.push_back(added_key);
  return 0;
}

int Xdb_tbl_def::clear_new_keys_info() {
  m_inplace_new_keys.clear();

  for (auto kd : m_inplace_new_keys_ref) {
    kd = nullptr;
  }

  m_inplace_new_keys_ref.clear();

  return 0;
}

bool Xdb_tbl_def::verify_dd_table(const dd::Table *dd_table, uint32_t &hidden_pk)
{
  if (nullptr != dd_table) {
    auto table_id = dd_table->se_private_id();
    if (dd::INVALID_OBJECT_ID == table_id) return true;

    // verify all user defined indexes
    for (auto index : dd_table->indexes())
      if (Xdb_key_def::verify_dd_index(index, table_id))
        return true;

    uint32_t hidden_pk_id = DD_SUBTABLE_ID_INVALID;
    // verify metadata for hidden primary key created by XEngine if exists
    auto &p = dd_table->se_private_data();
    if (p.exists(dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID]) &&
        (p.get(dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID], &hidden_pk_id) ||
         Xdb_key_def::verify_dd_index_ext(p) ||
         (DD_SUBTABLE_ID_INVALID == hidden_pk_id) ||
         (nullptr == cf_manager.get_cf(hidden_pk_id))))
      return true;

    hidden_pk = hidden_pk_id;
    return false;
  }
  return true;
}

bool Xdb_tbl_def::init_table_id(Xdb_ddl_manager& ddl_manager)
{
  uint64_t table_id = dd::INVALID_OBJECT_ID;
  if (ddl_manager.get_table_id(table_id) || (dd::INVALID_OBJECT_ID == table_id))
    return true;

  m_table_id = table_id;
  return false;
}

bool Xdb_tbl_def::write_dd_table(dd::Table* dd_table) const
{
  DBUG_ASSERT (nullptr != dd_table);
  DBUG_ASSERT (dd::INVALID_OBJECT_ID != m_table_id);

  dd_table->set_se_private_id(m_table_id);
  size_t key_no = 0;
  size_t dd_index_size = dd_table->indexes()->size();
  for (auto dd_index : *dd_table->indexes()) {
    if (m_key_descr_arr[key_no]->write_dd_index(dd_index, m_table_id)) {
      XHANDLER_LOG(ERROR, "write_dd_index failed", "index_name",
                   dd_index->name().c_str(), K(m_dbname_tablename));
      return true;
    }
    ++key_no;
  }

  if (m_key_count > dd_index_size) {
    // table should have a hidden primary key created by XEngine
    if (m_key_count != (dd_index_size + 1)) {
      XHANDLER_LOG(ERROR, "number of keys mismatch", K(m_dbname_tablename),
                   K(m_key_count), K(dd_index_size));
      return true;
    }

    auto& hidden_pk = m_key_descr_arr[dd_index_size];
    uint32_t hidden_pk_id = hidden_pk->get_index_number();
    if (!hidden_pk->is_hidden_primary_key()) {
      XHANDLER_LOG(ERROR, "Invalid primary key definition",
                   K(m_dbname_tablename), K(hidden_pk_id));
      return true;
    // persist subtable_id for hidden primary key created by XEngine
    } else if (dd_table->se_private_data().set(
                   dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID], hidden_pk_id)) {
      XHANDLER_LOG(ERROR, "failed to set hidden_pk_id in se_private_data",
                   K(hidden_pk_id), K(m_dbname_tablename));
      return true;
    } else if (hidden_pk->write_dd_index_ext(dd_table->se_private_data())) {
      XHANDLER_LOG(ERROR,
                   "failed to set metadata for hidden pk in se_private_data",
                   K(hidden_pk_id), K(m_dbname_tablename));
      return true;
    }
  }

  for (auto dd_column : *dd_table->columns()) {
    if (dd_column->se_private_data().set(
            dd_index_key_strings[DD_INDEX_TABLE_ID], m_table_id)) {
      XHANDLER_LOG(ERROR, "failed to set table id in se_private_data",
                   "column", dd_column->name().c_str(), K(m_dbname_tablename));
      return true;
    }
  }

  return false;
}

void Xdb_ddl_manager::erase_index_num(const GL_INDEX_ID &gl_index_id) {
  m_index_num_to_keydef.erase(gl_index_id);
}

void Xdb_ddl_manager::add_uncommitted_keydefs(
    const std::unordered_set<std::shared_ptr<Xdb_key_def>> &indexes) {
  mysql_rwlock_wrlock(&m_rwlock);
  for (const auto &index : indexes) {
    m_index_num_to_uncommitted_keydef[index->get_gl_index_id()] = index;
  }
  mysql_rwlock_unlock(&m_rwlock);
}

void Xdb_ddl_manager::remove_uncommitted_keydefs(
    const std::unordered_set<std::shared_ptr<Xdb_key_def>> &indexes) {
  mysql_rwlock_wrlock(&m_rwlock);
  for (const auto &index : indexes) {
    m_index_num_to_uncommitted_keydef.erase(index->get_gl_index_id());
  }
  mysql_rwlock_unlock(&m_rwlock);
}

// Check whethter we can safely purge given subtable.
// We only can safely purge it if
//   (1) no related Xdb_key_def object in uncommited hash table;
//   (2) no associated dd::Table object from global data dictionary.
bool Xdb_ddl_manager::can_purge_subtable(THD* thd, const GL_INDEX_ID& gl_index_id)
{
  if (m_index_num_to_uncommitted_keydef.find(gl_index_id) !=
      m_index_num_to_uncommitted_keydef.end())
    return false;

  uint32_t subtable_id = gl_index_id.index_id;
  auto it = m_index_num_to_keydef.find(gl_index_id);
  if (it != m_index_num_to_keydef.end()) {
    auto& tbl_name = it->second.first;
    uint keyno = it->second.second;
    // Normally, Xdb_key_def object always exists with Xdb_tbl_def object together
    std::shared_ptr<Xdb_tbl_def> table_def = find_tbl_from_cache(tbl_name);
    if (!table_def) {
      /* Exception case 1:
       *   Xdb_tbl_def object with the name is removed but index cache entry of
       *   Xdb_key_def in the Xdb_tbl_def is kept.
       *   The Xdb_tbl_def object may be loaded again later.
       */
      XHANDLER_LOG(ERROR,
                   "XEngine: unexpected error, key cache entry exists without "
                   "table cache entry", "table_name", tbl_name);
      return false;
    } else if (!table_def->m_key_descr_arr || keyno > table_def->m_key_count ||
               !table_def->m_key_descr_arr[keyno] ||
               subtable_id != table_def->m_key_descr_arr[keyno]->get_index_number()) {
      /* Exception case 2:
       *   old Xdb_tbl_def object with the name is removed and new
       *   Xdb_tbl_def object is added but index cache entry of Xdb_key_def in
       *   old Xdb_tbl_def is kept
       */
      XHANDLER_LOG(ERROR,
                   "XEngine: found invalid index_id in m_index_num_to_keydef "
                   "without related Xdb_tbl_def/Xdb_key_def object",
                   K(subtable_id));
      m_index_num_to_keydef.erase(it);
      return true;
    }

    // Xdb_key_def object is still present with Xdb_tbl_def object in cache
    // name string in Xdb_tbl_def is from filename format, to acquire dd object
    // we should use table name format which uses different charset
    char schema_name[FN_REFLEN+1], table_name[FN_REFLEN+1];
    filename_to_tablename(table_def->base_dbname().c_str(), schema_name, sizeof(schema_name));
    filename_to_tablename(table_def->base_tablename().c_str(), table_name, sizeof(table_name));
    dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
    const dd::Table *dd_table = nullptr;
    MDL_ticket *tbl_ticket = nullptr;
    // try to acquire and check existence of dd::Table object
    if (Xdb_dd_helper::acquire_xengine_table(
            thd, purge_acquire_lock_timeout, schema_name,
            table_name, dd_table, tbl_ticket)) {
      // if failed to acquire dd::Table object we treat as lock wait timeout
      // and related dd::Table is locked by other thread
      return false;
    } else if (nullptr != dd_table) {
      XHANDLER_LOG(WARN,
                   "XEngine: subtable associated table is still present in "
                   "global data dictionary!",
                   K(subtable_id), K(schema_name), K(table_name));
      if (tbl_ticket) dd::release_mdl(thd, tbl_ticket);
      return false;
    } else {
      // Xdb_tbl_def is only present in cache
      // remove cache entry
      remove_cache(tbl_name);
      return true;
    }
  }

  return true;
}

bool Xdb_ddl_manager::init(THD *const thd, Xdb_dict_manager *const dict_arg) {
  m_dict = dict_arg;
  mysql_rwlock_init(0, &m_rwlock);

  uint max_index_id_in_dict = 0;
  m_dict->get_max_index_id(&max_index_id_in_dict);
  // index ids used by applications should not conflict with
  // data dictionary index ids
  if (max_index_id_in_dict < Xdb_key_def::END_DICT_INDEX_ID) {
    max_index_id_in_dict = Xdb_key_def::END_DICT_INDEX_ID;
  }

  m_next_table_id = DD_TABLE_ID_END + 1;
  mysql_mutex_init(0, &m_next_table_id_mutex, MY_MUTEX_INIT_FAST);

  // get value of system_cf_version
  uint16_t system_cf_version_in_dict;
  if (!m_dict->get_system_cf_version(&system_cf_version_in_dict)) {
    system_cf_version_in_dict = Xdb_key_def::SYSTEM_CF_VERSION::VERSION_0;
  }
  // if we can't get value for system_cf_version from dictionary or we get older
  // version, we do upgrade on XEngine dictionary.
  switch (system_cf_version_in_dict) {
    // For upgrading to VERSION_1, all existing XEngine tables are upgraded by
    // 1) load all existing tables according to DDL_ENTRY_INDEX_START_NUMBER
    //    records in system cf
    // 2) try to aquire dd::Table object for each of them and set se_private_id
    //    and se_private_data in dd::Table/dd::Index
    // 3) persist dd::Table/dd:Index object to DDSE
    // 4) set max_table_id in system cf even there is no user defined table
    // 5) set VERSION_1 as system_cf_version in dictionary
    case Xdb_key_def::SYSTEM_CF_VERSION::VERSION_0: {
      if (load_existing_tables(max_index_id_in_dict)) {
        XHANDLER_LOG(ERROR, "XEngine: failed to load existing tables!");
        return true;
      } else if (upgrade_system_cf_version1(thd)) {
        XHANDLER_LOG(ERROR, "XEngine: failed to upgrade system_cf to VERSION1");
        return true;
      } else if (upgrade_system_cf_version2()) {
        XHANDLER_LOG(ERROR, "XEngine: failed to upgrade system_cf to VERSION2");
        return true;
      }
      break;
    }
    case Xdb_key_def::SYSTEM_CF_VERSION::VERSION_1:
    case Xdb_key_def::SYSTEM_CF_VERSION::VERSION_2: {
      uint64_t max_table_id_in_dict = 0;
      if (!m_dict->get_max_table_id(&max_table_id_in_dict)) {
        XHANDLER_LOG(ERROR, "XEngine: failed to get max_table_id from dictionary");
        return true;
      }
/** there is OOM risk when populating all tables if table count is very large
      else if (!thd || populate_existing_tables(thd)) {
        XHANDLER_LOG(ERROR, "XEngine: failed to populate existing tables!");
        return true;
      }
*/

#ifndef NDEBUG
      XHANDLER_LOG(INFO, "ddl_manager init get max table id from dictionary",
                   K(max_table_id_in_dict));
#endif
      if (max_table_id_in_dict > DD_TABLE_ID_END)
        m_next_table_id = max_table_id_in_dict + 1;

      if (Xdb_key_def::SYSTEM_CF_VERSION::VERSION_1 == system_cf_version_in_dict
          && upgrade_system_cf_version2()) {
        XHANDLER_LOG(ERROR, "XEngine: failed to upgrade system_cf to VERSION2");
        return true;
      }
      break;
    }
    default: {
      XHANDLER_LOG(ERROR, "XEngine: unexpected value for system_cf_version",
                   K(system_cf_version_in_dict));
      return true;
    }
  }

  m_sequence.init(max_index_id_in_dict + 1);
  return false;
}

bool Xdb_ddl_manager::upgrade_system_cf_version1(THD* thd)
{
  if (!m_ddl_hash.empty()) {
    if (nullptr == thd) {
      XHANDLER_LOG(ERROR, "XEngine: unable to upgrade existing tables!");
      return true;
    } else if (upgrade_existing_tables(thd)) {
      XHANDLER_LOG(ERROR, "XEngine: failed to upgrade existing tables!");
      return true;
    }
  }

  uint16_t target_version = Xdb_key_def::SYSTEM_CF_VERSION::VERSION_1;
  if (update_max_table_id(m_next_table_id - 1)) {
    XHANDLER_LOG(ERROR, "XEngine: failed to set max_table_id in system cf!");
    return true;
  } else if (update_system_cf_version(target_version)) {
    XHANDLER_LOG(ERROR, "XEngine: failed to set system_cf_version as VERSION1!");
    return true;
  } else {
#ifndef NDEBUG
    XHANDLER_LOG(INFO, "XEngine: successfully upgrade system_cf to VERSION1");
#endif
    return false;
  }
}

bool Xdb_ddl_manager::upgrade_system_cf_version2()
{
  auto wb = m_dict->begin();
  if (!wb) {
    XHANDLER_LOG(ERROR, "XEgnine: failed to begin wrtiebatch");
    return true;
  }
  auto write_batch = wb.get();

  auto dd_type = Xdb_key_def::DDL_DROP_INDEX_ONGOING;
  std::unordered_set<GL_INDEX_ID> gl_index_ids;
  m_dict->get_ongoing_index_operation(&gl_index_ids, dd_type);
  for (auto& gl_index_id : gl_index_ids) {
    m_dict->delete_with_prefix(write_batch, dd_type, gl_index_id);
    m_dict->delete_index_info(write_batch, gl_index_id);
    cf_manager.drop_cf(xdb, gl_index_id.index_id);
  }
  gl_index_ids.clear();

  dd_type = Xdb_key_def::DDL_CREATE_INDEX_ONGOING;
  m_dict->get_ongoing_index_operation(&gl_index_ids, dd_type);
  for (auto& gl_index_id : gl_index_ids) {
    m_dict->delete_with_prefix(write_batch, dd_type, gl_index_id);
    m_dict->delete_index_info(write_batch, gl_index_id);
    cf_manager.drop_cf(xdb, gl_index_id.index_id);
  }

  uint16_t target_version = Xdb_key_def::SYSTEM_CF_VERSION::VERSION_2;
  if (m_dict->update_system_cf_version(write_batch, target_version)) {
    XHANDLER_LOG(ERROR, "XEngine: failed to set system_cf_version as VERSION2!");
    return true;
  } else if (m_dict->commit(write_batch)) {
    XHANDLER_LOG(ERROR, "XEngine: failed commit into dictionary!");
    return true;
  } else {
#ifndef NDEBUG
    XHANDLER_LOG(INFO, "XEngine: successfully upgrade system_cf to VERSION2");
#endif
    return false;
  }
}

bool Xdb_ddl_manager::load_existing_tables(uint max_index_id_in_dict)
{
  /* Read the data dictionary and populate the hash */
  uchar ddl_entry[Xdb_key_def::INDEX_NUMBER_SIZE];
  xdb_netbuf_store_index(ddl_entry, Xdb_key_def::DDL_ENTRY_INDEX_START_NUMBER);
  const xengine::common::Slice ddl_entry_slice((char *)ddl_entry,
                                               Xdb_key_def::INDEX_NUMBER_SIZE);

  /* Reading data dictionary should always skip bloom filter */
  xengine::db::Iterator *it = m_dict->new_iterator();
  int i = 0;
  for (it->Seek(ddl_entry_slice); it->Valid(); it->Next()) {
    const uchar *ptr;
    const uchar *ptr_end;
    const xengine::common::Slice key = it->key();
    const xengine::common::Slice val = it->value();

    if (key.size() >= Xdb_key_def::INDEX_NUMBER_SIZE &&
        memcmp(key.data(), ddl_entry, Xdb_key_def::INDEX_NUMBER_SIZE))
      break;

    if (key.size() <= Xdb_key_def::INDEX_NUMBER_SIZE) {
      XHANDLER_LOG(ERROR, "XEngine: unexepected key size(corruption?)",
                   "key_size", key.size());
      return true;
    }

    auto tdef = std::make_shared<Xdb_tbl_def>(key, Xdb_key_def::INDEX_NUMBER_SIZE);

    auto& table_name = tdef->full_tablename();
    // Now, read the DDLs.
    const int d_PACKED_SIZE = Xdb_key_def::PACKED_SIZE * 2;
    const int real_val_size = val.size() - Xdb_key_def::VERSION_SIZE;
    if (real_val_size % d_PACKED_SIZE) {
      XHANDLER_LOG(ERROR, "XEngine: invalid keylist from dictionary",
                   K(table_name));
      return true;
    }
    tdef->m_key_count = real_val_size / d_PACKED_SIZE;
    tdef->m_key_descr_arr = new std::shared_ptr<Xdb_key_def>[tdef->m_key_count];

    ptr = reinterpret_cast<const uchar *>(val.data());
    const int version = xdb_netbuf_read_uint16(&ptr);
    const int exp_version = Xdb_key_def::DDL_ENTRY_INDEX_VERSION;
    if (version != exp_version) {
      XHANDLER_LOG(ERROR, "XEngine: DDL ENTRY Version mismatch",
                   "expected", exp_version, "actual", version);
      return true;
    }
    ptr_end = ptr + real_val_size;
    for (uint keyno = 0; ptr < ptr_end; keyno++) {
      GL_INDEX_ID gl_index_id;
      xdb_netbuf_read_gl_index(&ptr, &gl_index_id);
      uint32_t subtable_id = gl_index_id.cf_id;
      uint16 index_dict_version = 0;
      uchar index_type = 0;
      uint16 kv_version = 0;
      uint flags = 0;
      xengine::db::ColumnFamilyHandle* cfh;

      if (get_index_dict(table_name, gl_index_id, max_index_id_in_dict,
                         index_dict_version, index_type, kv_version, flags)) {
        XHANDLER_LOG(ERROR,
                     "XEngine: failed to get index information from dictionary.",
                     K(table_name));
        return true;
      } else if (nullptr == (cfh = cf_manager.get_cf(subtable_id))) {
        XHANDLER_LOG(ERROR, "XEngine: failed to find subtable",
                     K(table_name), K(subtable_id));
        return true;
      }

      DBUG_ASSERT(cfh != nullptr);
      tdef->space_id = (reinterpret_cast<xengine::db::ColumnFamilyHandleImpl *const>(cfh))->cfd()->get_table_space_id();

      /*
        We can't fully initialize Xdb_key_def object here, because full
        initialization requires that there is an open TABLE* where we could
        look at Field* objects and set max_length and other attributes
      */
      tdef->m_key_descr_arr[keyno] = std::make_shared<Xdb_key_def>(
          subtable_id, keyno, cfh, index_dict_version, index_type, kv_version,
          flags & Xdb_key_def::REVERSE_CF_FLAG,
          flags & Xdb_key_def::AUTO_CF_FLAG, "",
          m_dict->get_stats(gl_index_id));
    }
    put(tdef);
    i++;
  }

  if (!it->status().ok()) {
    XHANDLER_LOG(ERROR, "XEngine: failed to iterate dictioanry",
                 "status", it->status().ToString());
    return true;
  }
  MOD_DELETE_OBJECT(Iterator, it);
  XHANDLER_LOG(INFO, "XEngine: loaded DDL data for tables", "count", i);
  return false;
}

bool Xdb_ddl_manager::upgrade_existing_tables(THD *const thd)
{
  if (m_ddl_hash.empty()) return false;

  bool error = false;
  DBUG_ASSERT (dd::INVALID_OBJECT_ID != m_next_table_id);
  dd::cache::Dictionary_client* dc = thd->dd_client();
  dd::cache::Dictionary_client::Auto_releaser releaser(dc);
  // Disable autocommit option
  Disable_autocommit_guard autocommit_guard(thd);
  for (auto &kv: m_ddl_hash) {
    std::shared_ptr<Xdb_tbl_def> &tbl_def = kv.second;
    tbl_def->set_table_id(m_next_table_id);
    // name string in Xdb_tbl_def is from filename format, to acquire dd object
    // we should use table name format which uses different charset
    char schema_name[FN_REFLEN+1], table_name[FN_REFLEN+1];
    filename_to_tablename(tbl_def->base_dbname().c_str(), schema_name, sizeof(schema_name));
    filename_to_tablename(tbl_def->base_tablename().c_str(), table_name, sizeof(table_name));
    if (NULL != strstr(table_name, tmp_file_prefix)) {
      XHANDLER_LOG(WARN, "XEngine: found trashy temporary table, skip it during upgrading",
                   K(schema_name), K(table_name));
      continue;
    }

    const dd::Schema *db_sch = nullptr;
    dd::Table *dd_table = nullptr;
    MDL_request sch_mdl_request;
    MDL_REQUEST_INIT(&sch_mdl_request, MDL_key::SCHEMA, schema_name, "",
                     MDL_INTENTION_EXCLUSIVE, MDL_TRANSACTION);
    MDL_request tbl_mdl_request;
    MDL_REQUEST_INIT(&tbl_mdl_request, MDL_key::TABLE, schema_name,
                     table_name, MDL_EXCLUSIVE, MDL_TRANSACTION);
    // acquire intention_exclusive lock on dd::Schema if needed
    if (!thd->mdl_context.owns_equal_or_stronger_lock(
            MDL_key::SCHEMA, schema_name, "", MDL_INTENTION_EXCLUSIVE) &&
        thd->mdl_context.acquire_lock(&sch_mdl_request,
                                      thd->variables.lock_wait_timeout)) {
      XHANDLER_LOG(ERROR, "XEngine: failed to acquire lock for dd::Schema",
                   K(schema_name));
      error = true;
    // acquire dd::Schema object
    } else if (dc->acquire(schema_name, &db_sch) || (nullptr == db_sch)) {
      XHANDLER_LOG(ERROR, "XEngine: failed to acquire dd::Schema object",
                   K(schema_name));
      error = true;
    // acquire exclusive lock on dd::Table if needed
    } else if (!dd::has_exclusive_table_mdl(thd, schema_name, table_name) &&
               thd->mdl_context.acquire_lock(
                   &tbl_mdl_request, thd->variables.lock_wait_timeout)) {
      XHANDLER_LOG(ERROR, "XEngine: failed to acquire exclusive lock for dd::Table",
                   K(schema_name), K(table_name));
      error = true;
    // acquire dd::Table object to modify
    } else if (dc->acquire_for_modification(schema_name, table_name, &dd_table) ||
               (nullptr == dd_table) || dd_table->engine() != xengine_hton_name) {
      XHANDLER_LOG(ERROR, "XEngine: failed to acquire dd::Table object",
                   K(schema_name), K(table_name));
      error = true;
    // set se_private_id and se_private_data
    } else if (tbl_def->write_dd_table(dd_table)) {
      XHANDLER_LOG(ERROR, "XEngine: failed to update dd_table",
                   K(schema_name), K(table_name));
      error = true;
    // persist se_private_id and se_private_data of dd::Table/dd::Index
    } else if (dc->update(dd_table)) {
      XHANDLER_LOG(ERROR, "XEngine: failed to persist dd::Table",
                   K(schema_name), K(table_name));
      error = true;
    } else {
#ifndef NDEBUG
      XHANDLER_LOG(INFO, "XEngine: successfully upgrade dd::Table",
                   K(schema_name), K(table_name));
#endif
      ++m_next_table_id;
      error = false;
    }
    if (error)
      break;
  }

  if (error) {
    trans_rollback_stmt(thd);
    trans_rollback(thd);
    return true;
  } else {
    return (trans_commit_stmt(thd) || trans_commit(thd));
  }
}

bool Xdb_ddl_manager::populate_existing_tables(THD *const thd)
{
  DBUG_ASSERT(nullptr != thd);
  bool error = Xdb_dd_helper::traverse_all_xengine_tables(
      thd, true, 0,
      [&](const dd::Schema *dd_schema, const dd::Table *dd_table) -> bool {
        // always use filename format in Xdb_tbl_def
        char db_filename[FN_REFLEN + 1], table_filename[FN_REFLEN + 1];
        auto schema_name = dd_schema->name().c_str();
        auto table_name = dd_table->name().c_str();
        memset(db_filename, 0, sizeof(db_filename));
        memset(table_filename, 0, sizeof(table_filename));
        tablename_to_filename(schema_name, db_filename, sizeof(db_filename));
        tablename_to_filename(table_name, table_filename,
                              sizeof(table_filename));
        std::ostringstream oss;
        oss << db_filename << '.' << table_filename;
        Xdb_tbl_def *tbl_def = restore_table_from_dd(dd_table, oss.str());
        if (nullptr == tbl_def) {
          XHANDLER_LOG(ERROR, "XEngine: failed to restore table from dd::Table",
                       K(schema_name), K(table_name));
          return true;
        } else {
#ifndef NDEBUG
          XHANDLER_LOG(INFO,
                       "XEngine: successfully populate table from dd::Table",
                       K(schema_name), K(table_name));
#endif
          put(std::shared_ptr<Xdb_tbl_def>(tbl_def));
          return false;
        }
      });

  if (error) {
    XHANDLER_LOG(
        ERROR,
        "XEngine: failed to populate all XENGINE tables from data dictionary");
#ifndef NDEBUG
  } else {
    XHANDLER_LOG(INFO,
                 "XEngine: successfully populate all XENGINE tables from data "
                 "dictionary");
#endif
  }
  return error;
}

bool Xdb_ddl_manager::update_max_table_id(uint64_t table_id)
{
  auto write_batch = m_dict->begin();
  if (nullptr == write_batch) {
    XHANDLER_LOG(ERROR, "XEgnine: failed to begin wrtiebatch");
    return true;
  }

  bool res = false;
  m_dict->update_max_table_id(write_batch.get(), table_id);
  if (m_dict->commit(write_batch.get())) {
    XHANDLER_LOG(ERROR, "XEngine: failed to update max_table_id", K(table_id));
    res = true;
  }
  return res;
}

bool Xdb_ddl_manager::update_system_cf_version(uint16_t system_cf_version)
{
  auto write_batch = m_dict->begin();
  if (!write_batch) {
    XHANDLER_LOG(ERROR, "XEgnine: failed to begin wrtiebatch");
    return true;
  }

  bool res = false;
  m_dict->update_system_cf_version(write_batch.get(), system_cf_version);
  if (m_dict->commit(write_batch.get())) {
    XHANDLER_LOG(ERROR, "XEngine: failed to update system_cf_version",
                 "version", system_cf_version);
    res = true;
  }
  return res;
}

std::shared_ptr<Xdb_tbl_def> Xdb_ddl_manager::find(
    const std::string &table_name, bool* from_dict, bool lock/* = true*/)
{
  DBUG_ASSERT(!table_name.empty() && nullptr != from_dict);
  std::shared_ptr<Xdb_tbl_def> tbl = find_tbl_from_cache(table_name, lock);
  *from_dict = false;
  // for rename_cache during ha_post_recover, current_thd is nullptr
  if (nullptr == tbl && nullptr != current_thd) {
    // Xdb_tbl_def* tbl_def = find_tbl_from_dict(table_name);
    // try to acquire dd::Table object from dictionary and
    // restore Xdb_tbl_def from dd::Table
    Xdb_tbl_def* tbl_def = restore_table_from_dd(current_thd, table_name);
    if (nullptr != tbl_def) {
      *from_dict = true;
      tbl.reset(tbl_def);
    }
  }

  return tbl;
}

std::shared_ptr<Xdb_tbl_def> Xdb_ddl_manager::find(const char *table_name,
    int64_t name_len, bool* from_dict, bool lock/* = true*/)
{
  return find(std::string(table_name, name_len), from_dict, lock);
}

std::shared_ptr<Xdb_tbl_def> Xdb_ddl_manager::find(const dd::Table* dd_table,
    const std::string& table_name, bool* from_dict, bool lock/* = true*/)
{
  DBUG_ASSERT(!table_name.empty() && nullptr != from_dict);
  std::shared_ptr<Xdb_tbl_def> tbl = find_tbl_from_cache(table_name, lock);
  *from_dict = false;
  if (nullptr == tbl) {
    DBUG_ASSERT(nullptr != dd_table);
    // restore Xdb_tbl_def from dd::Table
    Xdb_tbl_def* tbl_def = restore_table_from_dd(dd_table, table_name);
    if (nullptr != tbl_def) {
      *from_dict = true;
      tbl.reset(tbl_def);
    }
  }
  return tbl;
}

std::shared_ptr<Xdb_tbl_def> Xdb_ddl_manager::find(THD* thd,
    const std::string& table_name, bool* from_dict, bool lock/* = true*/)
{
  DBUG_ASSERT(!table_name.empty() && nullptr != from_dict);
  std::shared_ptr<Xdb_tbl_def> tbl = find_tbl_from_cache(table_name, lock);
  *from_dict = false;
  if (nullptr == tbl) {
    DBUG_ASSERT(nullptr != thd);
    // try to acquire dd::Table object from dictionary and
    // restore Xdb_tbl_def from dd::Table
    Xdb_tbl_def* tbl_def = restore_table_from_dd(thd, table_name);
    if (nullptr != tbl_def) {
      *from_dict = true;
      tbl.reset(tbl_def);
    }
  }
  return tbl;
}

std::shared_ptr<Xdb_tbl_def> Xdb_ddl_manager::find_tbl_from_cache(
    const std::string &table_name, bool lock/* = true*/)
{
  std::shared_ptr<Xdb_tbl_def> tbl;
  if (lock) {
    mysql_rwlock_rdlock(&m_rwlock);
  }

  const auto &it = m_ddl_hash.find(table_name);
  if (it != m_ddl_hash.end())
    tbl = it->second;

  if (lock) {
    mysql_rwlock_unlock(&m_rwlock);
  }

  return tbl;
}

int Xdb_ddl_manager::get_index_dict(const std::string& table_name,
                                    const GL_INDEX_ID &gl_index_id,
                                    uint max_index_id_in_dict,
                                    uint16 &index_dict_version,
                                    uchar &index_type, uint16 &kv_version,
                                    uint &flags) {
  int ret = false;
  index_dict_version = 0;
  index_type = 0;
  kv_version = 0;
  flags = 0;

  if (!m_dict->get_index_info(gl_index_id, &index_dict_version, &index_type,
                              &kv_version)) {
    sql_print_error("XEngine: Could not get index information "
                    "for Index Number (%u,%u), table %s",
                    gl_index_id.cf_id, gl_index_id.index_id, table_name.c_str());
    return true;
  }

  if (max_index_id_in_dict < gl_index_id.index_id) {
    sql_print_error("XEngine: Found max index id %u from data dictionary "
                    "but also found larger index id %u from dictionary. "
                    "This should never happen and possibly a bug.",
                    max_index_id_in_dict, gl_index_id.index_id);
    return true;
  }

  if (!m_dict->get_cf_flags(gl_index_id.cf_id, &flags)) {
    sql_print_error("XEngine: Could not get Column Family Flags "
                    "for CF Number %d, table %s",
                    gl_index_id.cf_id, table_name.c_str());
    return true;
  }

  return ret;
}

#if 0
Xdb_tbl_def *Xdb_ddl_manager::find_tbl_from_dict(const std::string &table_name) {
  uchar buf[FN_LEN * 2 + Xdb_key_def::INDEX_NUMBER_SIZE];
  uint pos = 0;

  xdb_netbuf_store_index(buf, Xdb_key_def::DDL_ENTRY_INDEX_START_NUMBER);
  pos += Xdb_key_def::INDEX_NUMBER_SIZE;

  memcpy(buf + pos, table_name.c_str(), table_name.size());
  pos += table_name.size();

  const xengine::common::Slice skey(reinterpret_cast<char*>(buf), pos);
  std::string val;

  Xdb_tbl_def *tdef = nullptr;
  xengine::common::Status status = m_dict->get_value(skey, &val);
  if (status.ok()) {
    tdef = new Xdb_tbl_def(skey, Xdb_key_def::INDEX_NUMBER_SIZE);
    const int d_PACKED_SIZE = Xdb_key_def::PACKED_SIZE * 2;
    const int real_val_size = val.size() - Xdb_key_def::VERSION_SIZE;
    if (real_val_size % d_PACKED_SIZE) {
      sql_print_error("XEngine: Table_store: invalid keylist for table %s",
                      tdef->full_tablename().c_str());
      return nullptr;
    }
    tdef->m_key_count = real_val_size / d_PACKED_SIZE;
    tdef->m_key_descr_arr = new std::shared_ptr<Xdb_key_def>[tdef->m_key_count];

    const uchar *ptr;
    const uchar *ptr_end;
    ptr = reinterpret_cast<const uchar *>(val.data());
    const int version = xdb_netbuf_read_uint16(&ptr);
    if (version != Xdb_key_def::DDL_ENTRY_INDEX_VERSION) {
      sql_print_error("XEngine: DDL ENTRY Version was not expected."
                      "Expected: %d, Actual: %d",
                      Xdb_key_def::DDL_ENTRY_INDEX_VERSION, version);
      return nullptr;
    }
    ptr_end = ptr + real_val_size;

    uint max_index_id_in_dict = 0;
    m_dict->get_max_index_id(&max_index_id_in_dict);
    for (uint keyno = 0; ptr < ptr_end; keyno++) {
      GL_INDEX_ID gl_index_id;
      xdb_netbuf_read_gl_index(&ptr, &gl_index_id);
      uint16 index_dict_version = 0;
      uchar index_type = 0;
      uint16 kv_version = 0;
      uint flags = 0;

      if (get_index_dict(tdef->full_tablename(), gl_index_id,
                         max_index_id_in_dict, index_dict_version, index_type,
                         kv_version, flags)) {
        sql_print_error("XEngine: Get index information from dictionary error."
                        "table_name: %s", tdef->full_tablename().c_str());
        return nullptr;
      }

      xengine::db::ColumnFamilyHandle *const cfh =
          cf_manager.get_cf(gl_index_id.cf_id);
      DBUG_ASSERT(cfh != nullptr);

      /*
        We can't fully initialize Xdb_key_def object here, because full
        initialization requires that there is an open TABLE* where we could
        look at Field* objects and set max_length and other attributes
      */
      tdef->m_key_descr_arr[keyno] = std::make_shared<Xdb_key_def>(
          gl_index_id.index_id, keyno, cfh, index_dict_version, index_type,
          kv_version, flags & Xdb_key_def::REVERSE_CF_FLAG,
          flags & Xdb_key_def::AUTO_CF_FLAG, "",
          m_dict->get_stats(gl_index_id));
    }
  }

  return tdef;
}
#endif

// this is a safe version of the find() function below.  It acquires a read
// lock on m_rwlock to make sure the Xdb_key_def is not discarded while we
// are finding it.  Copying it into 'ret' increments the count making sure
// that the object will not be discarded until we are finished with it.
std::shared_ptr<const Xdb_key_def>
Xdb_ddl_manager::safe_find(GL_INDEX_ID gl_index_id) {
  std::shared_ptr<const Xdb_key_def> ret(nullptr);

  mysql_rwlock_rdlock(&m_rwlock);

  auto it = m_index_num_to_keydef.find(gl_index_id);
  if (it != m_index_num_to_keydef.end()) {
    bool from_dict = false;
    const auto table_def = find(it->second.first, &from_dict, false);
    if (table_def && it->second.second < table_def->m_key_count) {
      const auto &kd = table_def->m_key_descr_arr[it->second.second];
      if (kd->max_storage_fmt_length() != 0) {
        ret = kd;
      }
      if (from_dict) {
        // put into cache need write lock
        mysql_rwlock_unlock(&m_rwlock);
        put(table_def);
        mysql_rwlock_rdlock(&m_rwlock);
      }
    }
  } else {
    auto it = m_index_num_to_uncommitted_keydef.find(gl_index_id);
    if (it != m_index_num_to_uncommitted_keydef.end()) {
      const auto &kd = it->second;
      if (kd->max_storage_fmt_length() != 0) {
        ret = kd;
      }
    }
  }

  mysql_rwlock_unlock(&m_rwlock);

  return ret;
}

// this method assumes write lock on m_rwlock
const std::shared_ptr<Xdb_key_def> &
Xdb_ddl_manager::find(GL_INDEX_ID gl_index_id) {
  auto it = m_index_num_to_keydef.find(gl_index_id);
  if (it != m_index_num_to_keydef.end()) {
    bool from_dict = false;
    auto table_def = find(it->second.first, &from_dict, false);
    if (table_def) {
      if (from_dict) put(table_def, false);

      if (it->second.second < table_def->m_key_count) {
        return table_def->m_key_descr_arr[it->second.second];
      }
    }
  } else {
    auto it = m_index_num_to_uncommitted_keydef.find(gl_index_id);
    if (it != m_index_num_to_uncommitted_keydef.end()) {
      return it->second;
    }
  }

  static std::shared_ptr<Xdb_key_def> empty = nullptr;

  return empty;
}

void Xdb_ddl_manager::set_stats(
    const std::unordered_map<GL_INDEX_ID, Xdb_index_stats> &stats) {
  mysql_rwlock_wrlock(&m_rwlock);
  for (auto src : stats) {
    const auto &keydef = find(src.second.m_gl_index_id);
    if (keydef) {
      keydef->m_stats = src.second;
      m_stats2store[keydef->m_stats.m_gl_index_id] = keydef->m_stats;
    }
  }
  mysql_rwlock_unlock(&m_rwlock);
}

void Xdb_ddl_manager::adjust_stats2(
    Xdb_index_stats stats,
    const bool increment) {
  mysql_rwlock_wrlock(&m_rwlock);
  const auto &keydef = find(stats.m_gl_index_id);
  if (keydef) {
    keydef->m_stats.m_distinct_keys_per_prefix.resize(
       keydef->get_key_parts());
    keydef->m_stats.merge(stats, increment, keydef->max_storage_fmt_length());
    m_stats2store[keydef->m_stats.m_gl_index_id] = keydef->m_stats;
  }

  const bool should_save_stats = !m_stats2store.empty();
  mysql_rwlock_unlock(&m_rwlock);
  if (should_save_stats) {
    // Queue an async persist_stats(false) call to the background thread.
    xdb_queue_save_stats_request();
  }
}

void Xdb_ddl_manager::adjust_stats(Xdb_index_stats stats)
{
  mysql_rwlock_wrlock(&m_rwlock);
  const auto &keydef = find(stats.m_gl_index_id);
  if (keydef) {
    keydef->m_stats.m_distinct_keys_per_prefix.resize(keydef->get_key_parts());
    keydef->m_stats.update(stats, keydef->max_storage_fmt_length());
    m_stats2store[keydef->m_stats.m_gl_index_id] = keydef->m_stats;
  }

  const bool should_save_stats = !m_stats2store.empty();
  mysql_rwlock_unlock(&m_rwlock);
  if (should_save_stats) {
    // Queue an async persist_stats(false) call to the background thread.
    xdb_queue_save_stats_request();
  }
}

void Xdb_ddl_manager::persist_stats(const bool &sync) {
  mysql_rwlock_wrlock(&m_rwlock);
  const auto local_stats2store = std::move(m_stats2store);
  m_stats2store.clear();
  mysql_rwlock_unlock(&m_rwlock);

  // Persist stats
  const std::unique_ptr<xengine::db::WriteBatch> wb = m_dict->begin();
  std::vector<Xdb_index_stats> stats;
  std::transform(local_stats2store.begin(), local_stats2store.end(),
                 std::back_inserter(stats),
                 [](const std::pair<GL_INDEX_ID, Xdb_index_stats> &s) {
                   return s.second;
                 });
  m_dict->add_stats(wb.get(), stats);
  m_dict->commit(wb.get(), sync);
}

/**
  Put table definition of `tbl` into the mapping, and also write it to the
  on-disk data dictionary.

  @param tbl, xdb_tbl_def put to cache
  @param batch, transaction buffer on current session
  @param ddl_log_manager
  @param thread_id, session thread_id
  @param write_ddl_log, for create-table, we need write remove_cache log to remove rubbish, for alter-table, we just invalid cache, make sure cache is consistent with dictionary.
*/

int Xdb_ddl_manager::put_and_write(const std::shared_ptr<Xdb_tbl_def>& tbl,
                                   xengine::db::WriteBatch *const batch,
                                   Xdb_ddl_log_manager *const ddl_log_manager,
                                   ulong thread_id, bool write_ddl_log) {
  const std::string &dbname_tablename = tbl->full_tablename();
#if 0
  uchar buf[FN_LEN * 2 + Xdb_key_def::INDEX_NUMBER_SIZE];
  uint pos = 0;

  xdb_netbuf_store_index(buf, Xdb_key_def::DDL_ENTRY_INDEX_START_NUMBER);
  pos += Xdb_key_def::INDEX_NUMBER_SIZE;

  memcpy(buf + pos, dbname_tablename.c_str(), dbname_tablename.size());
  pos += dbname_tablename.size();

  int res;
  if ((res = tbl->put_dict(m_dict, batch/*, buf, pos*/))) {
    sql_print_error("put dictionary error, table_name(%s), thread_id(%d)",
                    dbname_tablename.c_str(), thread_id);
    return res;
  }

  DBUG_EXECUTE_IF("crash_after_put_dict_log", DBUG_SUICIDE(););
#endif

  if (write_ddl_log) {
    if (ddl_log_manager->write_remove_cache_log(batch, dbname_tablename,
                                                      thread_id)) {
      sql_print_error(
          "write remove cache ddl_log error, table_name(%s), thread_id(%d)",
          dbname_tablename.c_str(), thread_id);
      return HA_EXIT_FAILURE;
    } else {
      put(tbl);
    }
  } else {
    remove_cache(dbname_tablename);
  }

  return HA_EXIT_SUCCESS;
}

/* TODO:
  This function modifies m_ddl_hash and m_index_num_to_keydef.
  However, these changes need to be reversed if dict_manager.commit fails
  See the discussion here: https://reviews.facebook.net/D35925#inline-259167
  Tracked by https://github.com/facebook/mysql-5.6/issues/33
*/
void Xdb_ddl_manager::put(const std::shared_ptr<Xdb_tbl_def>& tbl, bool lock/* = true*/) {
  const std::string &dbname_tablename = tbl->full_tablename();

  if (lock)
    mysql_rwlock_wrlock(&m_rwlock);

  // We have to do this find because 'tbl' is not yet in the list.  We need
  // to find the one we are replacing ('rec')
  const auto &it = m_ddl_hash.find(dbname_tablename);
  if (it != m_ddl_hash.end()) {
    m_ddl_hash.erase(it);
  }
  m_ddl_hash.insert({dbname_tablename, tbl});

  for (uint keyno = 0; keyno < tbl->m_key_count; keyno++) {
    m_index_num_to_keydef[tbl->m_key_descr_arr[keyno]->get_gl_index_id()] =
        std::make_pair(dbname_tablename, keyno);
  }

  if (lock)
    mysql_rwlock_unlock(&m_rwlock);
}

void Xdb_ddl_manager::remove_cache(const std::string &dbname_tablename,
                                   bool lock/* = true */) {
  if (lock) {
    mysql_rwlock_wrlock(&m_rwlock);
  }

  const auto &it = m_ddl_hash.find(dbname_tablename);
  if (it != m_ddl_hash.end()) {
    // m_index_num_to_keydef is inserted during put
    // we should remove here not other place
    if (it->second && it->second->m_key_descr_arr) {
      for (uint keyno = 0; keyno < it->second->m_key_count; keyno++) {
        auto kd = it->second->m_key_descr_arr[keyno];
        DBUG_ASSERT(kd);
        m_index_num_to_keydef.erase(kd->get_gl_index_id());
      }
    }
    m_ddl_hash.erase(it);
  }

  if (lock) {
    mysql_rwlock_unlock(&m_rwlock);
  }
}

#if 0
void Xdb_ddl_manager::remove_dict(const std::string &dbname_tablename,
                                  xengine::db::WriteBatch *const batch) {
  uchar buf[FN_LEN * 2 + Xdb_key_def::INDEX_NUMBER_SIZE];
  uint pos = 0;

  xdb_netbuf_store_index(buf, Xdb_key_def::DDL_ENTRY_INDEX_START_NUMBER);
  pos += Xdb_key_def::INDEX_NUMBER_SIZE;

  memcpy(buf + pos, dbname_tablename.c_str(), dbname_tablename.size());
  pos += dbname_tablename.size();

  const xengine::common::Slice tkey((char *)buf, pos);
  m_dict->delete_key(batch, tkey);
}

void Xdb_ddl_manager::remove(const std::string &dbname_tablename,
                             xengine::db::WriteBatch *const batch,
                             bool lock/* = true*/) {
  /** remove dictionary record */
  remove_dict(dbname_tablename, batch);

  /** remove the object in the cache */
  remove_cache(dbname_tablename, lock);
}
#endif

bool Xdb_ddl_manager::rename_cache(const std::string &from, const std::string &to) {
  std::shared_ptr<Xdb_tbl_def> tbl;
  bool from_dict = false;
  if (!(tbl = find(from, &from_dict))) {
    /** if not found, that's ok for we may executed many times */
    XHANDLER_LOG(WARN, "Table doesn't exist when rename_cache", K(from), K(to));
    return false;
  }

  return rename_cache(tbl.get(), to);
}

bool Xdb_ddl_manager::rename_cache(Xdb_tbl_def* tbl, const std::string &to)
{
  DBUG_ASSERT(nullptr != tbl);
  auto new_tbl = std::make_shared<Xdb_tbl_def>(to);

  new_tbl->m_key_count = tbl->m_key_count;
  new_tbl->m_auto_incr_val =
      tbl->m_auto_incr_val.load(std::memory_order_relaxed);
  new_tbl->m_key_descr_arr = tbl->m_key_descr_arr;
  // so that it's not free'd when deleting the old rec
  tbl->m_key_descr_arr = nullptr;

  mysql_rwlock_wrlock(&m_rwlock);
  /** update dictionary cache */
  put(new_tbl, false);
  remove_cache(tbl->full_tablename(), false);

  mysql_rwlock_unlock(&m_rwlock);

  DBUG_EXECUTE_IF("ddl_log_inject_rollback_rename_process",{return true;});
  return false;
}

#if 0
bool Xdb_ddl_manager::rename(Xdb_tbl_def *const tbl, const std::string &to,
                             xengine::db::WriteBatch *const batch) {
  DBUG_ASSERT(nullptr != tbl);
  uchar new_buf[FN_LEN * 2 + Xdb_key_def::INDEX_NUMBER_SIZE];
  uint new_pos = 0;

  auto new_tbl = std::make_shared<Xdb_tbl_def>(to);

  new_tbl->m_key_count = tbl->m_key_count;
  new_tbl->m_auto_incr_val =
      tbl->m_auto_incr_val.load(std::memory_order_relaxed);
  new_tbl->m_key_descr_arr = tbl->m_key_descr_arr;
  // so that it's not free'd when deleting the old rec
  tbl->m_key_descr_arr = nullptr;

  // Create a new key
  xdb_netbuf_store_index(new_buf, Xdb_key_def::DDL_ENTRY_INDEX_START_NUMBER);
  new_pos += Xdb_key_def::INDEX_NUMBER_SIZE;

  const std::string &from = tbl->full_tablename();
  const std::string &dbname_tablename = new_tbl->full_tablename();
  memcpy(new_buf + new_pos, dbname_tablename.c_str(), dbname_tablename.size());
  new_pos += dbname_tablename.size();

  // Create a key to add
  DBUG_EXECUTE_IF("sleep_before_rename_write_storage_engine",
                  { sleep(20); fprintf(stdout, "sleep 20s before write storage engine\n"); });

  mysql_rwlock_wrlock(&m_rwlock);
  if (new_tbl->put_dict(m_dict, batch, new_buf, new_pos)) {
    sql_print_error(
        "put newtable to dictionary failed, table_name(%s), dst_table_name(%d)",
        from.c_str(), to.c_str());
    mysql_rwlock_unlock(&m_rwlock);
    return true;
  }

  remove(from, batch, false);

  /** update dictionary cache */
  put(new_tbl, false);

  DBUG_EXECUTE_IF("ddl_log_inject_rollback_rename_process",
                  {mysql_rwlock_unlock(&m_rwlock); return true; });

  mysql_rwlock_unlock(&m_rwlock);
  return false;
}
#endif

void Xdb_ddl_manager::cleanup() {
  m_ddl_hash.clear();
  mysql_rwlock_destroy(&m_rwlock);
  m_sequence.cleanup();
}

int Xdb_ddl_manager::scan_for_tables(Xdb_tables_scanner *const tables_scanner) {
  int i, ret;
  Xdb_tbl_def *rec;

  DBUG_ASSERT(tables_scanner != nullptr);

  mysql_rwlock_rdlock(&m_rwlock);

  ret = 0;
  i = 0;

  for (const auto &it : m_ddl_hash) {
    ret = tables_scanner->add_table(it.second.get());
    if (ret)
      break;
    i++;
  }

  mysql_rwlock_unlock(&m_rwlock);
  return ret;
}

bool Xdb_ddl_manager::get_table_id(uint64_t &table_id)
{
  bool res = true;
  XDB_MUTEX_LOCK_CHECK(m_next_table_id_mutex);
  if (!(res = update_max_table_id(m_next_table_id))) {
    table_id = m_next_table_id++;
  }
  XDB_MUTEX_UNLOCK_CHECK(m_next_table_id_mutex);
  return res;
}

Xdb_key_def* Xdb_ddl_manager::restore_index_from_dd(
    const dd::Properties& prop, const std::string &table_name,
    const std::string &index_name, uint32_t subtable_id,
    uint keyno, int index_type)
{
  int index_version_id = 0, kv_version = 0, key_flags = 0;
  xengine::db::ColumnFamilyHandle* cfh = nullptr;
  if (prop.get(dd_index_key_strings[DD_INDEX_VERSION_ID], &index_version_id) ||
      prop.get(dd_index_key_strings[DD_INDEX_KV_VERSION], &kv_version) ||
      prop.get(dd_index_key_strings[DD_INDEX_FLAGS], &key_flags)) {
    XHANDLER_LOG(ERROR, "XEngine: failed to get index metadata from se_private_data",
                 K(index_name), K(subtable_id), K(table_name));
    return nullptr;
  } else if (!Xdb_dict_manager::is_valid_index_version(index_version_id)) {
    XHANDLER_LOG(ERROR, "XEngine: get invalid index version from se_private_data",
                 K(index_version_id), K(index_name), K(subtable_id), K(table_name));
    return nullptr;
  } else if (!Xdb_dict_manager::is_valid_kv_version(index_type, kv_version)) {
    XHANDLER_LOG(ERROR, "XEngine: get invalid kv_version from se_private_data",
                 K(kv_version), K(index_name), K(subtable_id), K(table_name));
    return nullptr;
  } else if (nullptr == (cfh = cf_manager.get_cf(subtable_id))) {
    XHANDLER_LOG(ERROR, "XEngine: failed to get column family handle for index",
                 K(index_name), K(subtable_id), K(table_name));
    return nullptr;
  } else {
    return new Xdb_key_def(subtable_id, keyno, cfh, index_version_id, index_type,
                           kv_version, key_flags & Xdb_key_def::REVERSE_CF_FLAG,
                           key_flags & Xdb_key_def::AUTO_CF_FLAG, index_name,
                           m_dict->get_stats({subtable_id, subtable_id}));
  }
}

Xdb_tbl_def* Xdb_ddl_manager::restore_table_from_dd(
    const dd::Table* dd_table, const std::string& table_name)
{
  Xdb_tbl_def* tbl_def = nullptr;
  uint32_t hidden_subtable_id = DD_SUBTABLE_ID_INVALID;
  if (nullptr != dd_table && !table_name.empty() &&
      !Xdb_tbl_def::verify_dd_table(dd_table, hidden_subtable_id)) {
    uint64_t table_id = dd_table->se_private_id();
    uint max_index_id_in_dict = 0;
    m_dict->get_max_index_id(&max_index_id_in_dict);
    if (max_index_id_in_dict < Xdb_key_def::END_DICT_INDEX_ID) {
      max_index_id_in_dict = Xdb_key_def::END_DICT_INDEX_ID;
    }

    tbl_def = new Xdb_tbl_def(table_name);
    tbl_def->set_table_id(table_id);
    tbl_def->m_key_count = dd_table->indexes().size();
    if (DD_SUBTABLE_ID_INVALID != hidden_subtable_id) {
      ++tbl_def->m_key_count;
    }

    bool error = false;
    // construct Xdb_key_def for all user defiend indexes
    tbl_def->m_key_descr_arr = new std::shared_ptr<Xdb_key_def>[tbl_def->m_key_count];
    uint keyno = 0;
    for (auto dd_index : dd_table->indexes()) {
      std::string index_name(dd_index->name().c_str());
      uint32_t subtable_id = DD_SUBTABLE_ID_INVALID;
      int index_type;
      Xdb_key_def* kd = nullptr;
      const dd::Properties &p = dd_index->se_private_data();
      if (p.get(dd_index_key_strings[DD_INDEX_SUBTABLE_ID], &subtable_id) ||
          DD_SUBTABLE_ID_INVALID == subtable_id) {
        XHANDLER_LOG(ERROR, "XEngine: failed to get subtable_id from "
                            "se_private_data of dd::Index",
                     K(index_name), K(table_name));
        error = true;
        break;
      } else if (max_index_id_in_dict < subtable_id) {
        XHANDLER_LOG(ERROR, "XEngine: got invalid subtable id which larger than"
                            "maximum id recored in dictioanry",
                     K(max_index_id_in_dict), K(subtable_id),
                     K(index_name), K(table_name));
        error = true;
        break;
      } else if (p.get(dd_index_key_strings[DD_INDEX_TYPE], &index_type) ||
                 (index_type != Xdb_key_def::INDEX_TYPE_PRIMARY &&
                  index_type != Xdb_key_def::INDEX_TYPE_SECONDARY)) {
        XHANDLER_LOG(ERROR, "XEngine: failed to get index_type from "
                            "se_private_data of dd::Index",
                     K(index_type), K(subtable_id), K(index_name), K(table_name));
        error = true;
        break;
      } else if (nullptr == (kd = restore_index_from_dd(p, table_name,
            index_name, subtable_id, keyno, index_type))) {
        error = true;
        break;
      } else {
        tbl_def->m_key_descr_arr[keyno++].reset(kd);
      }
    }

    if (!error && DD_SUBTABLE_ID_INVALID != hidden_subtable_id) {
      // construct Xdb_key_def for hidden primary key added by XEngine
      int index_type = Xdb_key_def::INDEX_TYPE::INDEX_TYPE_HIDDEN_PRIMARY;
      Xdb_key_def* kd = nullptr;
      if (max_index_id_in_dict < hidden_subtable_id) {
        XHANDLER_LOG(ERROR, "XEngine: got invalid hidden_subtable_id which "
                            "larger than maximum id recored in dictioanry",
                     K(max_index_id_in_dict), K(hidden_subtable_id), K(table_name));
        error = true;
      } else if (nullptr == (kd = restore_index_from_dd(
           dd_table->se_private_data(), table_name, HIDDEN_PK_NAME,
           hidden_subtable_id, keyno, index_type))) {
        error = true;
      } else {
        tbl_def->m_key_descr_arr[keyno].reset(kd);
      }
    }

    if (error) {
      delete tbl_def;
      tbl_def = nullptr;
    }
  }
  return tbl_def;
}

Xdb_tbl_def* Xdb_ddl_manager::restore_table_from_dd(
    THD* thd, const std::string& table_name)
{
  DBUG_ASSERT(nullptr != thd);

  std::string db_name, tbl_name, part_name;
  if (xdb_split_normalized_tablename(table_name, &db_name, &tbl_name, &part_name)) {
    XHANDLER_LOG(ERROR, "XEngine: failed to parse table name",
                 "full_table_name", table_name);
    return nullptr;
  }

  char schema_name[FN_REFLEN+1], dd_tbl_name[FN_REFLEN+1];
  filename_to_tablename(db_name.c_str(), schema_name, sizeof(schema_name));
  filename_to_tablename(tbl_name.c_str(), dd_tbl_name, sizeof(dd_tbl_name));
  MDL_ticket *tbl_ticket = nullptr;
  const dd::Table *dd_table = nullptr;
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  Xdb_tbl_def* tbl = nullptr;
  if (!Xdb_dd_helper::acquire_xengine_table(thd, 0, schema_name,
          dd_tbl_name, dd_table, tbl_ticket) && nullptr != dd_table) {
    tbl = restore_table_from_dd(dd_table, table_name);
    if (tbl_ticket) dd::release_mdl(thd, tbl_ticket);
  }

  return tbl;
}

/*
  Xdb_binlog_manager class implementation
*/

bool Xdb_binlog_manager::init(Xdb_dict_manager *const dict_arg) {
  DBUG_ASSERT(dict_arg != nullptr);
  m_dict = dict_arg;

  xdb_netbuf_store_index(m_key_buf, Xdb_key_def::BINLOG_INFO_INDEX_NUMBER);
  m_key_slice = xengine::common::Slice(reinterpret_cast<char *>(m_key_buf),
                               Xdb_key_def::INDEX_NUMBER_SIZE);
  return false;
}

void Xdb_binlog_manager::cleanup() {}

/**
  Set binlog name, pos and optionally gtid into WriteBatch.
  This function should be called as part of transaction commit,
  since binlog info is set only at transaction commit.
  Actual write into XEngine is not done here, so checking if
  write succeeded or not is not possible here.
  @param binlog_name   Binlog name
  @param binlog_pos    Binlog pos
  @param binlog_gtid   Binlog max GTID
  @param batch         WriteBatch
*/
void Xdb_binlog_manager::update(const char *const binlog_name,
                                const my_off_t binlog_pos,
                                const char *const binlog_max_gtid,
                                xengine::db::WriteBatchBase *const batch) {
  if (binlog_name && binlog_pos) {
    // max binlog length (512) + binlog pos (4) + binlog gtid (57) < 1024
    const size_t XDB_MAX_BINLOG_INFO_LEN = 1024;
    uchar value_buf[XDB_MAX_BINLOG_INFO_LEN];
    m_dict->put_key(
        batch, m_key_slice,
        pack_value(value_buf, binlog_name, binlog_pos, binlog_max_gtid));
  }
}

/**
  Read binlog committed entry stored in XEngine, then unpack
  @param[OUT] binlog_name  Binlog name
  @param[OUT] binlog_pos   Binlog pos
  @param[OUT] binlog_gtid  Binlog GTID
  @return
    true is binlog info was found (valid behavior)
    false otherwise
*/
bool Xdb_binlog_manager::read(char *const binlog_name,
                              my_off_t *const binlog_pos,
                              char *const binlog_gtid) const {
  bool ret = false;
  if (binlog_name) {
    std::string value;
    xengine::common::Status status = m_dict->get_value(m_key_slice, &value);
    if (status.ok()) {
      if (!unpack_value((const uchar *)value.c_str(), binlog_name, binlog_pos,
                        binlog_gtid))
        ret = true;
    }
  }
  return ret;
}

/**
  Pack binlog_name, binlog_pos, binlog_gtid into preallocated
  buffer, then converting and returning a XEngine Slice
  @param buf           Preallocated buffer to set binlog info.
  @param binlog_name   Binlog name
  @param binlog_pos    Binlog pos
  @param binlog_gtid   Binlog GTID
  @return              xengine::common::Slice converted from buf and its length
*/
xengine::common::Slice
Xdb_binlog_manager::pack_value(uchar *const buf, const char *const binlog_name,
                               const my_off_t &binlog_pos,
                               const char *const binlog_gtid) const {
  uint pack_len = 0;

  // store version
  xdb_netbuf_store_uint16(buf, Xdb_key_def::BINLOG_INFO_INDEX_NUMBER_VERSION);
  pack_len += Xdb_key_def::VERSION_SIZE;

  // store binlog file name length
  DBUG_ASSERT(strlen(binlog_name) <= FN_REFLEN);
  const uint16_t binlog_name_len = strlen(binlog_name);
  xdb_netbuf_store_uint16(buf + pack_len, binlog_name_len);
  pack_len += sizeof(uint16);

  // store binlog file name
  memcpy(buf + pack_len, binlog_name, binlog_name_len);
  pack_len += binlog_name_len;

  // store binlog pos
  xdb_netbuf_store_uint32(buf + pack_len, binlog_pos);
  pack_len += sizeof(uint32);

  // store binlog gtid length.
  // If gtid was not set, store 0 instead
  const uint16_t binlog_gtid_len = binlog_gtid ? strlen(binlog_gtid) : 0;
  xdb_netbuf_store_uint16(buf + pack_len, binlog_gtid_len);
  pack_len += sizeof(uint16);

  if (binlog_gtid_len > 0) {
    // store binlog gtid
    memcpy(buf + pack_len, binlog_gtid, binlog_gtid_len);
    pack_len += binlog_gtid_len;
  }

  return xengine::common::Slice((char *)buf, pack_len);
}

/**
  Unpack value then split into binlog_name, binlog_pos (and binlog_gtid)
  @param[IN]  value        Binlog state info fetched from XEngine
  @param[OUT] binlog_name  Binlog name
  @param[OUT] binlog_pos   Binlog pos
  @param[OUT] binlog_gtid  Binlog GTID
  @return     true on error
*/
bool Xdb_binlog_manager::unpack_value(const uchar *const value,
                                      char *const binlog_name,
                                      my_off_t *const binlog_pos,
                                      char *const binlog_gtid) const {
  uint pack_len = 0;

  DBUG_ASSERT(binlog_pos != nullptr);

  // read version
  const uint16_t version = xdb_netbuf_to_uint16(value);
  pack_len += Xdb_key_def::VERSION_SIZE;
  if (version != Xdb_key_def::BINLOG_INFO_INDEX_NUMBER_VERSION)
    return true;

  // read binlog file name length
  const uint16_t binlog_name_len = xdb_netbuf_to_uint16(value + pack_len);
  pack_len += sizeof(uint16);
  if (binlog_name_len) {
    // read and set binlog name
    memcpy(binlog_name, value + pack_len, binlog_name_len);
    binlog_name[binlog_name_len] = '\0';
    pack_len += binlog_name_len;

    // read and set binlog pos
    *binlog_pos = xdb_netbuf_to_uint32(value + pack_len);
    pack_len += sizeof(uint32);

    // read gtid length
    const uint16_t binlog_gtid_len = xdb_netbuf_to_uint16(value + pack_len);
    pack_len += sizeof(uint16);
    if (binlog_gtid && binlog_gtid_len > 0) {
      // read and set gtid
      memcpy(binlog_gtid, value + pack_len, binlog_gtid_len);
      binlog_gtid[binlog_gtid_len] = '\0';
      pack_len += binlog_gtid_len;
    }
  }
  return false;
}

/**
  Inserts a row into mysql.slave_gtid_info table. Doing this inside
  storage engine is more efficient than inserting/updating through MySQL.

  @param[IN] id Primary key of the table.
  @param[IN] db Database name. This is column 2 of the table.
  @param[IN] gtid Gtid in human readable form. This is column 3 of the table.
  @param[IN] write_batch Handle to storage engine writer.
*/
void Xdb_binlog_manager::update_slave_gtid_info(
    const uint &id, const char *const db, const char *const gtid,
    xengine::db::WriteBatchBase *const write_batch) {
  if (id && db && gtid) {
     std::shared_ptr<Xdb_tbl_def> slave_gtid_info;
    // Make sure that if the slave_gtid_info table exists we have a
    // pointer to it via m_slave_gtid_info_tbl.
    if (!m_slave_gtid_info_tbl.load()) {
      bool from_dict = false;
      slave_gtid_info = xdb_get_ddl_manager()->find("mysql.slave_gtid_info", &from_dict);
      if (slave_gtid_info && from_dict)
        xdb_get_ddl_manager()->put(slave_gtid_info);
      m_slave_gtid_info_tbl.store(slave_gtid_info.get());
    }
    if (!m_slave_gtid_info_tbl.load()) {
      // slave_gtid_info table is not present. Simply return.
      return;
    }
    DBUG_ASSERT(m_slave_gtid_info_tbl.load()->m_key_count == 1);

    const std::shared_ptr<const Xdb_key_def> &kd =
        m_slave_gtid_info_tbl.load()->m_key_descr_arr[0];
    String value;

    // Build key
    uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE + 4] = {0};
    uchar *buf = key_buf;
    xdb_netbuf_store_index(buf, kd->get_index_number());
    buf += Xdb_key_def::INDEX_NUMBER_SIZE;
    xdb_netbuf_store_uint32(buf, id);
    buf += 4;
    const xengine::common::Slice key_slice =
        xengine::common::Slice((const char *)key_buf, buf - key_buf);

    // Build value
    uchar value_buf[128] = {0};
    DBUG_ASSERT(gtid);
    const uint db_len = strlen(db);
    const uint gtid_len = strlen(gtid);
    buf = value_buf;
    // 1 byte used for flags. Empty here.
    buf++;

    // Write column 1.
    DBUG_ASSERT(strlen(db) <= 64);
    xdb_netbuf_store_byte(buf, db_len);
    buf++;
    memcpy(buf, db, db_len);
    buf += db_len;

    // Write column 2.
    DBUG_ASSERT(gtid_len <= 56);
    xdb_netbuf_store_byte(buf, gtid_len);
    buf++;
    memcpy(buf, gtid, gtid_len);
    buf += gtid_len;
    const xengine::common::Slice value_slice =
        xengine::common::Slice((const char *)value_buf, buf - value_buf);

    write_batch->Put(kd->get_cf(), key_slice, value_slice);
  }
}

bool Xdb_dict_manager::init(xengine::db::DB *const xdb_dict,
                            const xengine::common::ColumnFamilyOptions &cf_options) {
  mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
  m_db = xdb_dict;
  bool is_automatic;
  bool create_table_space = false;
  int64_t sys_table_space_id = DEFAULT_SYSTEM_TABLE_SPACE_ID;
  m_system_cfh = cf_manager.get_or_create_cf(
      m_db, nullptr, -1, DEFAULT_SYSTEM_SUBTABLE_ID, DEFAULT_SYSTEM_SUBTABLE_NAME,
      "", nullptr, &is_automatic, cf_options,
      create_table_space, sys_table_space_id);
  xdb_netbuf_store_index(m_key_buf_max_index_id, Xdb_key_def::MAX_INDEX_ID);
  m_key_slice_max_index_id =
      xengine::common::Slice(reinterpret_cast<char *>(m_key_buf_max_index_id),
                     Xdb_key_def::INDEX_NUMBER_SIZE);
  xdb_netbuf_store_index(m_key_buf_max_table_id, Xdb_key_def::MAX_TABLE_ID);
  m_key_slice_max_table_id =
      xengine::common::Slice(reinterpret_cast<char *>(m_key_buf_max_table_id),
                     Xdb_key_def::INDEX_NUMBER_SIZE);
  //resume_drop_indexes();
  //rollback_ongoing_index_creation();

  return (m_system_cfh == nullptr);
}

std::unique_ptr<xengine::db::WriteBatch> Xdb_dict_manager::begin() const {
  return std::unique_ptr<xengine::db::WriteBatch>(new xengine::db::WriteBatch);
}

void Xdb_dict_manager::put_key(xengine::db::WriteBatchBase *const batch,
                               const xengine::common::Slice &key,
                               const xengine::common::Slice &value) const {
  batch->Put(m_system_cfh, key, value);
}

xengine::common::Status Xdb_dict_manager::get_value(const xengine::common::Slice &key,
                                            std::string *const value) const {
  xengine::common::ReadOptions options;
  options.total_order_seek = true;
  return m_db->Get(options, m_system_cfh, key, value);
}

void Xdb_dict_manager::delete_key(xengine::db::WriteBatchBase *batch,
                                  const xengine::common::Slice &key) const {
  batch->Delete(m_system_cfh, key);
}

xengine::db::Iterator *Xdb_dict_manager::new_iterator() const {
  /* Reading data dictionary should always skip bloom filter */
  xengine::common::ReadOptions read_options;
  read_options.total_order_seek = true;
  return m_db->NewIterator(read_options, m_system_cfh);
}

int Xdb_dict_manager::commit(xengine::db::WriteBatch *const batch,
                             const bool &sync) const {
  if (!batch)
    return HA_EXIT_FAILURE;
  int res = 0;
  xengine::common::WriteOptions options;
  options.sync = sync;
  xengine::common::Status s = m_db->Write(options, batch);
  res = !s.ok(); // we return true when something failed
  if (res) {
    xdb_handle_io_error(s, XDB_IO_ERROR_DICT_COMMIT);
  }
  batch->Clear();
  return res;
}

void Xdb_dict_manager::dump_index_id(uchar *const netbuf,
                                     Xdb_key_def::DATA_DICT_TYPE dict_type,
                                     const GL_INDEX_ID &gl_index_id) {
  xdb_netbuf_store_uint32(netbuf, dict_type);
  xdb_netbuf_store_uint32(netbuf + Xdb_key_def::INDEX_NUMBER_SIZE,
                          gl_index_id.cf_id);
  xdb_netbuf_store_uint32(netbuf + 2 * Xdb_key_def::INDEX_NUMBER_SIZE,
                          gl_index_id.index_id);
}

void Xdb_dict_manager::delete_with_prefix(
    xengine::db::WriteBatch *const batch, Xdb_key_def::DATA_DICT_TYPE dict_type,
    const GL_INDEX_ID &gl_index_id) const {
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE * 3] = {0};
  dump_index_id(key_buf, dict_type, gl_index_id);
  xengine::common::Slice key = xengine::common::Slice((char *)key_buf, sizeof(key_buf));

  delete_key(batch, key);
}

#if 0
void Xdb_dict_manager::add_or_update_index_cf_mapping(
    xengine::db::WriteBatch *batch, const uchar m_index_type,
    const uint16_t kv_version, const uint32_t index_id,
    const uint32_t cf_id) const {
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE * 3] = {0};
  uchar value_buf[256] = {0};
  GL_INDEX_ID gl_index_id = {cf_id, index_id};
  dump_index_id(key_buf, Xdb_key_def::INDEX_INFO, gl_index_id);
  const xengine::common::Slice key = xengine::common::Slice((char *)key_buf, sizeof(key_buf));

  uchar *ptr = value_buf;
  xdb_netbuf_store_uint16(ptr, Xdb_key_def::INDEX_INFO_VERSION_LATEST);
  ptr += 2;
  xdb_netbuf_store_byte(ptr, m_index_type);
  ptr += 1;
  xdb_netbuf_store_uint16(ptr, kv_version);
  ptr += 2;

  const xengine::common::Slice value =
      xengine::common::Slice((char *)value_buf, ptr - value_buf);
  batch->Put(m_system_cfh, key, value);
}

void Xdb_dict_manager::add_cf_flags(xengine::db::WriteBatch *const batch,
                                    const uint32_t &cf_id,
                                    const uint32_t &cf_flags) const {
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE * 2] = {0};
  uchar value_buf[Xdb_key_def::VERSION_SIZE + Xdb_key_def::INDEX_NUMBER_SIZE] =
      {0};
  xdb_netbuf_store_uint32(key_buf, Xdb_key_def::CF_DEFINITION);
  xdb_netbuf_store_uint32(key_buf + Xdb_key_def::INDEX_NUMBER_SIZE, cf_id);
  const xengine::common::Slice key = xengine::common::Slice((char *)key_buf, sizeof(key_buf));

  xdb_netbuf_store_uint16(value_buf, Xdb_key_def::CF_DEFINITION_VERSION);
  xdb_netbuf_store_uint32(value_buf + Xdb_key_def::VERSION_SIZE, cf_flags);
  const xengine::common::Slice value =
      xengine::common::Slice((char *)value_buf, sizeof(value_buf));
  batch->Put(m_system_cfh, key, value);
}
#endif

void Xdb_dict_manager::drop_cf_flags(xengine::db::WriteBatch *const batch,
                                     uint32_t cf_id) const
{
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE * 2] = {0};
  xdb_netbuf_store_uint32(key_buf, Xdb_key_def::CF_DEFINITION);
  xdb_netbuf_store_uint32(key_buf + Xdb_key_def::INDEX_NUMBER_SIZE, cf_id);

  delete_key(batch, xengine::common::Slice((char *)key_buf, sizeof(key_buf)));
}

void Xdb_dict_manager::delete_index_info(xengine::db::WriteBatch *batch,
                                         const GL_INDEX_ID &gl_index_id) const {
  delete_with_prefix(batch, Xdb_key_def::INDEX_INFO, gl_index_id);
  delete_with_prefix(batch, Xdb_key_def::INDEX_STATISTICS, gl_index_id);
  drop_cf_flags(batch, gl_index_id.index_id);
}

bool Xdb_dict_manager::get_index_info(const GL_INDEX_ID &gl_index_id,
                                      uint16_t *m_index_dict_version,
                                      uchar *m_index_type,
                                      uint16_t *kv_version) const {
  bool found = false;
  bool error = false;
  std::string value;
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE * 3] = {0};
  dump_index_id(key_buf, Xdb_key_def::INDEX_INFO, gl_index_id);
  const xengine::common::Slice &key = xengine::common::Slice((char *)key_buf, sizeof(key_buf));

  const xengine::common::Status &status = get_value(key, &value);
  if (status.ok()) {
    const uchar *const val = (const uchar *)value.c_str();
    const uchar *ptr = val;
    *m_index_dict_version = xdb_netbuf_to_uint16(val);
    *kv_version = 0;
    *m_index_type = 0;
    ptr += 2;
    switch (*m_index_dict_version) {

    case Xdb_key_def::INDEX_INFO_VERSION_VERIFY_KV_FORMAT:
    case Xdb_key_def::INDEX_INFO_VERSION_GLOBAL_ID:
      *m_index_type = xdb_netbuf_to_byte(ptr);
      ptr += 1;
      *kv_version = xdb_netbuf_to_uint16(ptr);
      found = true;
      break;

    default:
      error = true;
      break;
    }

    switch (*m_index_type) {
    case Xdb_key_def::INDEX_TYPE_PRIMARY:
    case Xdb_key_def::INDEX_TYPE_HIDDEN_PRIMARY: {
      error = *kv_version > Xdb_key_def::PRIMARY_FORMAT_VERSION_LATEST;
      break;
    }
    case Xdb_key_def::INDEX_TYPE_SECONDARY:
      error = *kv_version > Xdb_key_def::SECONDARY_FORMAT_VERSION_LATEST;
      break;
    default:
      error = true;
      break;
    }
  }

  if (error) {
    // NO_LINT_DEBUG
    sql_print_error("XEngine: Found invalid key version number (%u, %u, %u) "
                    "from data dictionary. This should never happen "
                    "and it may be a bug.",
                    *m_index_dict_version, *m_index_type, *kv_version);
    abort_with_stack_traces();
  }

  return found;
}

bool Xdb_dict_manager::is_valid_index_version(uint16_t index_dict_version)
{
  bool error;
  switch (index_dict_version) {
    case Xdb_key_def::INDEX_INFO_VERSION_VERIFY_KV_FORMAT:
    case Xdb_key_def::INDEX_INFO_VERSION_GLOBAL_ID: {
      error = false;
      break;
    }
    default: {
      error = true;
      break;
    }
  }

  return !error;
}

bool Xdb_dict_manager::is_valid_kv_version(uchar index_type, uint16_t kv_version)
{
  bool error = true;
  switch (index_type) {
    case Xdb_key_def::INDEX_TYPE_PRIMARY:
    case Xdb_key_def::INDEX_TYPE_HIDDEN_PRIMARY: {
      error = kv_version > Xdb_key_def::PRIMARY_FORMAT_VERSION_LATEST;
      break;
    }
    case Xdb_key_def::INDEX_TYPE_SECONDARY: {
      error = kv_version > Xdb_key_def::SECONDARY_FORMAT_VERSION_LATEST;
      break;
    }
    default: {
      error = true;
      break;
    }
  }
  return !error;
}

bool Xdb_dict_manager::get_cf_flags(const uint32_t &cf_id,
                                    uint32_t *const cf_flags) const {
  bool found = false;
  std::string value;
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE * 2] = {0};
  xdb_netbuf_store_uint32(key_buf, Xdb_key_def::CF_DEFINITION);
  xdb_netbuf_store_uint32(key_buf + Xdb_key_def::INDEX_NUMBER_SIZE, cf_id);
  const xengine::common::Slice key = xengine::common::Slice((char *)key_buf, sizeof(key_buf));

  const xengine::common::Status status = get_value(key, &value);
  if (status.ok()) {
    const uchar *val = (const uchar *)value.c_str();
    uint16_t version = xdb_netbuf_to_uint16(val);
    if (version == Xdb_key_def::CF_DEFINITION_VERSION) {
      *cf_flags = xdb_netbuf_to_uint32(val + Xdb_key_def::VERSION_SIZE);
      found = true;
    }
  }
  return found;
}

/*
  Returning index ids that were marked as deleted (via DROP TABLE) but
  still not removed by drop_index_thread yet, or indexes that are marked as
  ongoing creation.
 */
void Xdb_dict_manager::get_ongoing_index_operation(
    std::unordered_set<GL_INDEX_ID> *gl_index_ids,
    Xdb_key_def::DATA_DICT_TYPE dd_type) const {
  DBUG_ASSERT(dd_type == Xdb_key_def::DDL_DROP_INDEX_ONGOING ||
              dd_type == Xdb_key_def::DDL_CREATE_INDEX_ONGOING);

  uchar index_buf[Xdb_key_def::INDEX_NUMBER_SIZE];
  xdb_netbuf_store_uint32(index_buf, dd_type);
  const xengine::common::Slice index_slice(reinterpret_cast<char *>(index_buf),
                                   Xdb_key_def::INDEX_NUMBER_SIZE);

  xengine::db::Iterator *it = new_iterator();
  for (it->Seek(index_slice); it->Valid(); it->Next()) {
    xengine::common::Slice key = it->key();
    const uchar *const ptr = (const uchar *)key.data();

    /*
      Ongoing drop/create index operations require key to be of the form:
      dd_type + cf_id + index_id (== INDEX_NUMBER_SIZE * 3)

      This may need to be changed in the future if we want to process a new
      ddl_type with different format.
    */
    if (key.size() != Xdb_key_def::INDEX_NUMBER_SIZE * 3 ||
        xdb_netbuf_to_uint32(ptr) != dd_type) {
      break;
    }

    // We don't check version right now since currently we always store only
    // Xdb_key_def::DDL_DROP_INDEX_ONGOING_VERSION = 1 as a value.
    // If increasing version number, we need to add version check logic here.
    GL_INDEX_ID gl_index_id;
    gl_index_id.cf_id =
        xdb_netbuf_to_uint32(ptr + Xdb_key_def::INDEX_NUMBER_SIZE);
    gl_index_id.index_id =
        xdb_netbuf_to_uint32(ptr + 2 * Xdb_key_def::INDEX_NUMBER_SIZE);
    gl_index_ids->insert(gl_index_id);
  }
  MOD_DELETE_OBJECT(Iterator, it);
}

#if 0
/*
  Returning true if index_id is create/delete ongoing (undergoing creation or
  marked as deleted via DROP TABLE but drop_index_thread has not wiped yet)
  or not.
 */
bool Xdb_dict_manager::is_index_operation_ongoing(
    const GL_INDEX_ID &gl_index_id, Xdb_key_def::DATA_DICT_TYPE dd_type) const {
  DBUG_ASSERT(dd_type == Xdb_key_def::DDL_DROP_INDEX_ONGOING ||
              dd_type == Xdb_key_def::DDL_CREATE_INDEX_ONGOING);

  bool found = false;
  std::string value;
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE * 3] = {0};
  dump_index_id(key_buf, dd_type, gl_index_id);
  const xengine::common::Slice key = xengine::common::Slice((char *)key_buf, sizeof(key_buf));

  const xengine::common::Status status = get_value(key, &value);
  if (status.ok()) {
    found = true;
  }
  return found;
}

/*
  Adding index_id to data dictionary so that the index id is removed
  by drop_index_thread, or to track online index creation.
 */
void Xdb_dict_manager::start_ongoing_index_operation(
    xengine::db::WriteBatch *const batch, const GL_INDEX_ID &gl_index_id,
    Xdb_key_def::DATA_DICT_TYPE dd_type) const {
  DBUG_ASSERT(dd_type == Xdb_key_def::DDL_DROP_INDEX_ONGOING ||
              dd_type == Xdb_key_def::DDL_CREATE_INDEX_ONGOING);

  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE * 3] = {0};
  uchar value_buf[Xdb_key_def::VERSION_SIZE] = {0};
  dump_index_id(key_buf, dd_type, gl_index_id);

  // version as needed
  if (dd_type == Xdb_key_def::DDL_DROP_INDEX_ONGOING) {
    xdb_netbuf_store_uint16(value_buf,
                            Xdb_key_def::DDL_DROP_INDEX_ONGOING_VERSION);
  } else {
    xdb_netbuf_store_uint16(value_buf,
                            Xdb_key_def::DDL_CREATE_INDEX_ONGOING_VERSION);
  }

  const xengine::common::Slice key = xengine::common::Slice((char *)key_buf, sizeof(key_buf));
  const xengine::common::Slice value =
      xengine::common::Slice((char *)value_buf, sizeof(value_buf));
  batch->Put(m_system_cfh, key, value);
}

/*
  Removing index_id from data dictionary to confirm drop_index_thread
  completed dropping entire key/values of the index_id
 */
void Xdb_dict_manager::end_ongoing_index_operation(
    xengine::db::WriteBatch *const batch, const GL_INDEX_ID &gl_index_id,
    Xdb_key_def::DATA_DICT_TYPE dd_type) const {
  DBUG_ASSERT(dd_type == Xdb_key_def::DDL_DROP_INDEX_ONGOING ||
              dd_type == Xdb_key_def::DDL_CREATE_INDEX_ONGOING);

  delete_with_prefix(batch, dd_type, gl_index_id);
}

/*
  Returning true if there is no target index ids to be removed
  by drop_index_thread
 */
bool Xdb_dict_manager::is_drop_index_empty() const {
  std::unordered_set<GL_INDEX_ID> gl_index_ids;
  get_ongoing_drop_indexes(&gl_index_ids);
  return gl_index_ids.empty();
}

/*
  This function is supposed to be called by DROP TABLE. Logging messages
  that dropping indexes started, and adding data dictionary so that
  all associated indexes to be removed
 */
void Xdb_dict_manager::add_drop_table(
    std::shared_ptr<Xdb_key_def> *const key_descr, const uint32 &n_keys,
    xengine::db::WriteBatch *const batch) const {
  std::unordered_set<GL_INDEX_ID> dropped_index_ids;
  for (uint32 i = 0; i < n_keys; i++) {
    dropped_index_ids.insert(key_descr[i]->get_gl_index_id());
  }

  add_drop_index(dropped_index_ids, batch);
}

/*
  Called during inplace index drop operations. Logging messages
  that dropping indexes started, and adding data dictionary so that
  all associated indexes to be removed
 */
void Xdb_dict_manager::add_drop_index(
    const std::unordered_set<GL_INDEX_ID> &gl_index_ids,
    xengine::db::WriteBatch *const batch) const {
  for (const auto &gl_index_id : gl_index_ids) {
    log_start_drop_index(gl_index_id, "Begin");
    start_drop_index(batch, gl_index_id);
  }
}

/*
  Called during inplace index creation operations. Logging messages
  that adding indexes started, and updates data dictionary with all associated
  indexes to be added.
 */
void Xdb_dict_manager::add_create_index(
    const std::unordered_set<GL_INDEX_ID> &gl_index_ids,
    xengine::db::WriteBatch *const batch) const {
  for (const auto &gl_index_id : gl_index_ids) {
    // NO_LINT_DEBUG
    sql_print_information("XEngine: Begin index creation (%u,%u)",
                          gl_index_id.cf_id, gl_index_id.index_id);
    start_create_index(batch, gl_index_id);
  }
}

/*
  This function is supposed to be called by drop_index_thread, when it
  finished dropping any index, or at the completion of online index creation.
 */
void Xdb_dict_manager::finish_indexes_operation(
    const std::unordered_set<GL_INDEX_ID> &gl_index_ids,
    Xdb_key_def::DATA_DICT_TYPE dd_type) const {
  DBUG_ASSERT(dd_type == Xdb_key_def::DDL_DROP_INDEX_ONGOING ||
              dd_type == Xdb_key_def::DDL_CREATE_INDEX_ONGOING);

  const std::unique_ptr<xengine::db::WriteBatch> wb = begin();
  xengine::db::WriteBatch *const batch = wb.get();

  std::unordered_set<GL_INDEX_ID> incomplete_create_indexes;
  get_ongoing_create_indexes(&incomplete_create_indexes);

  for (const auto &gl_index_id : gl_index_ids) {
    if (is_index_operation_ongoing(gl_index_id, dd_type)) {
      // NO_LINT_DEBUG
      sql_print_information("XEngine: Finished %s (%u,%u)",
                            dd_type == Xdb_key_def::DDL_DROP_INDEX_ONGOING
                                ? "filtering dropped index"
                                : "index creation",
                            gl_index_id.cf_id, gl_index_id.index_id);

      end_ongoing_index_operation(batch, gl_index_id, dd_type);

      /*
        Remove the corresponding incomplete create indexes from data
        dictionary as well
      */
      if (dd_type == Xdb_key_def::DDL_DROP_INDEX_ONGOING) {
        if (incomplete_create_indexes.count(gl_index_id)) {
          end_ongoing_index_operation(batch, gl_index_id,
                                      Xdb_key_def::DDL_CREATE_INDEX_ONGOING);
        }
      }
    }

    if (dd_type == Xdb_key_def::DDL_DROP_INDEX_ONGOING) {
      delete_index_info(batch, gl_index_id);
    }
  }
  commit(batch);
}

/*
  This function is supposed to be called when initializing
  Xdb_dict_manager (at startup). If there is any index ids that are
  drop ongoing, printing out messages for diagnostics purposes.
 */
void Xdb_dict_manager::resume_drop_indexes() const {
  std::unordered_set<GL_INDEX_ID> gl_index_ids;
  get_ongoing_drop_indexes(&gl_index_ids);

  uint max_index_id_in_dict = 0;
  get_max_index_id(&max_index_id_in_dict);

  for (const auto &gl_index_id : gl_index_ids) {
    log_start_drop_index(gl_index_id, "Resume");
    if (max_index_id_in_dict < gl_index_id.index_id) {
      sql_print_error("XEngine: Found max index id %u from data dictionary "
                      "but also found dropped index id (%u,%u) from drop_index "
                      "dictionary. This should never happen and is possibly a "
                      "bug.",
                      max_index_id_in_dict, gl_index_id.cf_id,
                      gl_index_id.index_id);
      abort_with_stack_traces();
    }
  }
}

void Xdb_dict_manager::rollback_ongoing_index_creation() const {
  const std::unique_ptr<xengine::db::WriteBatch> wb = begin();
  xengine::db::WriteBatch *const batch = wb.get();

  std::unordered_set<GL_INDEX_ID> gl_index_ids;
  get_ongoing_create_indexes(&gl_index_ids);

  for (const auto &gl_index_id : gl_index_ids) {
    // NO_LINT_DEBUG
    sql_print_information("XEngine: Removing incomplete create index (%u,%u)",
                          gl_index_id.cf_id, gl_index_id.index_id);

    start_drop_index(batch, gl_index_id);
  }

  commit(batch);
}

// rollback the index create failed
void Xdb_dict_manager::rollback_index_creation(
     const std::unordered_set<std::shared_ptr<Xdb_key_def>> &indexes) const {
  const std::unique_ptr<xengine::db::WriteBatch> wb = begin();
  xengine::db::WriteBatch *const batch = wb.get();

  for (const auto &index : indexes) {
    sql_print_information("XEngine: Removing incomplete create index (%u,%u)",
                          index->get_gl_index_id().cf_id, index->get_gl_index_id().index_id);

    start_drop_index(batch, index->get_gl_index_id());
  }

  commit(batch);
}

void Xdb_dict_manager::log_start_drop_table(
    const std::shared_ptr<Xdb_key_def> *const key_descr, const uint32 &n_keys,
    const char *const log_action) const {
  for (uint32 i = 0; i < n_keys; i++) {
    log_start_drop_index(key_descr[i]->get_gl_index_id(), log_action);
  }
}

void Xdb_dict_manager::log_start_drop_index(GL_INDEX_ID gl_index_id,
                                            const char *log_action) const {
  uint16 m_index_dict_version = 0;
  uchar m_index_type = 0;
  uint16 kv_version = 0;

  if (!get_index_info(gl_index_id, &m_index_dict_version, &m_index_type,
                      &kv_version)) {
    /*
      If we don't find the index info, it could be that it's because it was a
      partially created index that isn't in the data dictionary yet that needs
      to be rolled back.
    */
    std::unordered_set<GL_INDEX_ID> incomplete_create_indexes;
    get_ongoing_create_indexes(&incomplete_create_indexes);

    if (!incomplete_create_indexes.count(gl_index_id)) {
      /* If it's not a partially created index, something is very wrong. */
      sql_print_error("XEngine: Failed to get sub table info "
                      "from index id (%u,%u). XEngine data dictionary may "
                      "get corrupted.",
                      gl_index_id.cf_id, gl_index_id.index_id);
      abort_with_stack_traces();
    }
  }
  sql_print_information("XEngine: %s filtering dropped index (%u,%u)",
                        log_action, gl_index_id.cf_id, gl_index_id.index_id);
}
#endif

bool Xdb_dict_manager::get_max_index_id(uint32_t *const index_id) const {
  bool found = false;
  std::string value;

  const xengine::common::Status status = get_value(m_key_slice_max_index_id, &value);
  if (status.ok()) {
    const uchar *const val = (const uchar *)value.c_str();
    const uint16_t &version = xdb_netbuf_to_uint16(val);
    if (version == Xdb_key_def::MAX_INDEX_ID_VERSION) {
      *index_id = xdb_netbuf_to_uint32(val + Xdb_key_def::VERSION_SIZE);
      found = true;
    }
  }
  return found;
}

bool Xdb_dict_manager::update_max_index_id(xengine::db::WriteBatch *const batch,
                                           const uint32_t &index_id) const {
  DBUG_ASSERT(batch != nullptr);

  uint32_t old_index_id = -1;
  if (get_max_index_id(&old_index_id)) {
    if (old_index_id > index_id) {
      sql_print_error("XEngine: Found max index id %u from data dictionary "
                      "but trying to update to older value %u. This should "
                      "never happen and possibly a bug.",
                      old_index_id, index_id);
      return true;
    }
  }

  uchar value_buf[Xdb_key_def::VERSION_SIZE + Xdb_key_def::INDEX_NUMBER_SIZE] =
      {0};
  xdb_netbuf_store_uint16(value_buf, Xdb_key_def::MAX_INDEX_ID_VERSION);
  xdb_netbuf_store_uint32(value_buf + Xdb_key_def::VERSION_SIZE, index_id);
  const xengine::common::Slice value =
      xengine::common::Slice((char *)value_buf, sizeof(value_buf));
  batch->Put(m_system_cfh, m_key_slice_max_index_id, value);
  return false;
}

bool Xdb_dict_manager::get_system_cf_version(uint16_t* system_cf_version) const
{
  DBUG_ASSERT(system_cf_version != nullptr);
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE] = {0};
  xdb_netbuf_store_index(key_buf, Xdb_key_def::SYSTEM_CF_VERSION_INDEX);
  xengine::common::Slice key_slice(reinterpret_cast<char *>(key_buf),
                                   Xdb_key_def::INDEX_NUMBER_SIZE);
  bool found = false;
  std::string value;
  auto status = get_value(key_slice, &value);
  if (status.ok()) {
    found = true;
    *system_cf_version = xdb_netbuf_to_uint16((const uchar *)value.c_str());
  }
  return found;
}

bool Xdb_dict_manager::update_system_cf_version(
  xengine::db::WriteBatch *const batch, uint16_t system_cf_version) const
{
  DBUG_ASSERT(batch != nullptr);
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE] = {0};
  xdb_netbuf_store_index(key_buf, Xdb_key_def::SYSTEM_CF_VERSION_INDEX);
  xengine::common::Slice key_slice(reinterpret_cast<char *>(key_buf),
                                   Xdb_key_def::INDEX_NUMBER_SIZE);

  uint16_t version_in_dict = 0;
  std::string value;
  auto status = get_value(key_slice, &value);
  if (status.ok()) {
    version_in_dict = xdb_netbuf_to_uint16((const uchar *)value.c_str());
    if (0 != version_in_dict && version_in_dict > system_cf_version) {
      XHANDLER_LOG(ERROR, "XEngine: set older version is disallowed.",
                   K(version_in_dict), K(system_cf_version));
      return true;
    }
  }

  if (version_in_dict != system_cf_version) {
    uchar value_buf[Xdb_key_def::VERSION_SIZE] = {0};
    xdb_netbuf_store_uint16(value_buf, system_cf_version);
    xengine::common::Slice value_slice((char *) value_buf, Xdb_key_def::VERSION_SIZE);
    batch->Put(m_system_cfh, key_slice, value_slice);
  }

  return false;
}

bool Xdb_dict_manager::get_max_table_id(uint64_t *table_id) const
{
  DBUG_ASSERT(table_id != nullptr);
  bool found = false;
  std::string value;
  auto status = get_value(m_key_slice_max_table_id, &value);
  if (status.ok()) {
    auto val = (const uchar *)value.c_str();
    const uint16_t &version = xdb_netbuf_to_uint16(val);
    if (version == Xdb_key_def::MAX_TABLE_ID_VERSION) {
      *table_id = xdb_netbuf_to_uint64(val + Xdb_key_def::VERSION_SIZE);
      found = true;
    }
  }
  return found;
}

bool Xdb_dict_manager::update_max_table_id(xengine::db::WriteBatch *const batch,
                                           uint64_t table_id) const {
  DBUG_ASSERT(batch != nullptr);
  uint64_t max_table_id_in_dict = dd::INVALID_OBJECT_ID;
  if (get_max_table_id(&max_table_id_in_dict)) {
    if (dd::INVALID_OBJECT_ID != max_table_id_in_dict &&
        max_table_id_in_dict > table_id) {
      XHANDLER_LOG(ERROR, "XEngine: found max table id from data dictionary but"
                          " trying to update to older value. This should never"
                          "happen and possibly a bug.",
                   K(max_table_id_in_dict), K(table_id));
      return true;
    }
  }

  if (max_table_id_in_dict != table_id) {
    uchar value_buf[Xdb_key_def::VERSION_SIZE + Xdb_key_def::TABLE_ID_SIZE] = {0};
    xdb_netbuf_store_uint16(value_buf, Xdb_key_def::MAX_TABLE_ID_VERSION);
    xdb_netbuf_store_uint64(value_buf + Xdb_key_def::VERSION_SIZE, table_id);
    auto value = xengine::common::Slice((char *) value_buf, sizeof(value_buf));
    batch->Put(m_system_cfh, m_key_slice_max_table_id, value);
  }
  return false;
}

void Xdb_dict_manager::add_stats(
    xengine::db::WriteBatch *const batch,
    const std::vector<Xdb_index_stats> &stats) const {
  DBUG_ASSERT(batch != nullptr);

  for (const auto &it : stats) {
    uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE * 3] = {0};
    dump_index_id(key_buf, Xdb_key_def::INDEX_STATISTICS, it.m_gl_index_id);

    // IndexStats::materialize takes complete care of serialization including
    // storing the version
    const auto value =
        Xdb_index_stats::materialize(std::vector<Xdb_index_stats>{it}, 1.);

    batch->Put(m_system_cfh, xengine::common::Slice((char *)key_buf, sizeof(key_buf)),
               value);
  }
}

Xdb_index_stats Xdb_dict_manager::get_stats(GL_INDEX_ID gl_index_id) const {
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE * 3] = {0};
  dump_index_id(key_buf, Xdb_key_def::INDEX_STATISTICS, gl_index_id);

  std::string value;
  const xengine::common::Status status = get_value(
      xengine::common::Slice(reinterpret_cast<char *>(key_buf), sizeof(key_buf)),
      &value);
  if (status.ok()) {
    std::vector<Xdb_index_stats> v;
    // unmaterialize checks if the version matches
    if (Xdb_index_stats::unmaterialize(value, &v) == 0 && v.size() == 1) {
      return v[0];
    }
  }

  return Xdb_index_stats();
}

uint Xdb_seq_generator::get_and_update_next_number(
    Xdb_dict_manager *const dict) {
  DBUG_ASSERT(dict != nullptr);

  uint res;
  XDB_MUTEX_LOCK_CHECK(m_mutex);

  res = m_next_number++;

  const std::unique_ptr<xengine::db::WriteBatch> wb = dict->begin();
  xengine::db::WriteBatch *const batch = wb.get();

  DBUG_ASSERT(batch != nullptr);
  dict->update_max_index_id(batch, res);
  dict->commit(batch);

  XDB_MUTEX_UNLOCK_CHECK(m_mutex);

  return res;
}

} // namespace myx
