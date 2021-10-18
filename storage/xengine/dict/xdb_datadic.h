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
#pragma once

/* C++ standard header files */
#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

/* C standard header files */
#include <arpa/inet.h>

/* XEngine handler header files */
#include "./ha_xengine.h"
#include "./properties_collector.h"
#include "./xdb_buff.h"
#include "./xdb_utils.h"

extern CHARSET_INFO my_charset_gbk_chinese_ci;
extern CHARSET_INFO my_charset_gbk_bin;
extern CHARSET_INFO my_charset_utf8mb4_bin;
extern CHARSET_INFO my_charset_utf16_bin;
extern CHARSET_INFO my_charset_utf16le_bin;
extern CHARSET_INFO my_charset_utf32_bin;

namespace myx {

class Xdb_dict_manager;
class Xdb_key_def;
class Xdb_field_packing;
class Xdb_cf_manager;
class Xdb_ddl_manager;
class Xdb_ddl_log_manager;

/*
  @brief
  Field packing context.
  The idea is to ensure that a call to xdb_index_field_pack_t function
  is followed by a call to xdb_make_unpack_info_t.

  @detail
  For some datatypes, unpack_info is produced as a side effect of
  xdb_index_field_pack_t function call.
  For other datatypes, packing is just calling make_sort_key(), while
  xdb_make_unpack_info_t is a custom function.
  In order to accommodate both cases, we require both calls to be made and
  unpack_info is passed as context data between the two.
*/
class Xdb_pack_field_context {
public:
  Xdb_pack_field_context(const Xdb_pack_field_context &) = delete;
  Xdb_pack_field_context &operator=(const Xdb_pack_field_context &) = delete;

  explicit Xdb_pack_field_context(Xdb_string_writer *const writer_arg)
      : writer(writer_arg) {}

  // NULL means we're not producing unpack_info.
  Xdb_string_writer *writer;
};

struct Xdb_collation_codec;

struct Xdb_inplace_ddl_dict_info;

/*
  C-style "virtual table" allowing different handling of packing logic based
  on the field type. See Xdb_field_packing::setup() implementation.
  */
using xdb_make_unpack_info_t = void (*)(const Xdb_collation_codec *codec,
                                        Field * field,
                                        Xdb_pack_field_context *const pack_ctx);
using xdb_index_field_unpack_t = int (*)(Xdb_field_packing *fpi, Field *field,
                                         uchar *field_ptr,
                                         Xdb_string_reader *reader,
                                         Xdb_string_reader *unpack_reader);
using xdb_index_field_skip_t = int (*)(const Xdb_field_packing *fpi,
                                       const Field *field,
                                       Xdb_string_reader *reader);
using xdb_index_field_pack_t = void (*)(Xdb_field_packing *fpi, Field *field,
                                        uchar *buf, uchar **dst,
                                        Xdb_pack_field_context *pack_ctx);

const uint XDB_INVALID_KEY_LEN = uint(-1);

/* How much one checksum occupies when stored in the record */
const size_t XDB_CHECKSUM_SIZE = sizeof(uint32_t);

/*
  How much the checksum data occupies in record, in total.
  It is storing two checksums plus 1 tag-byte.
*/
const size_t XDB_CHECKSUM_CHUNK_SIZE = 2 * XDB_CHECKSUM_SIZE + 1;

/*
  Checksum data starts from CHECKSUM_DATA_TAG which is followed by two CRC32
  checksums.
*/
const char XDB_CHECKSUM_DATA_TAG = 0x01;

/*
  Unpack data is variable length. It is a 1 tag-byte plus a
  two byte length field. The length field includes the header as well.
*/
const char XDB_UNPACK_DATA_TAG = 0x02;
const size_t XDB_UNPACK_DATA_LEN_SIZE = sizeof(uint16_t);
const size_t XDB_UNPACK_HEADER_SIZE =
    sizeof(XDB_UNPACK_DATA_TAG) + XDB_UNPACK_DATA_LEN_SIZE;

// Possible return values for xdb_index_field_unpack_t functions.
enum {
  UNPACK_SUCCESS = 0,
  UNPACK_FAILURE = 1,
  UNPACK_INFO_MISSING= 2
};

/*
  An object of this class represents information about an index in an SQL
  table. It provides services to encode and decode index tuples.

  Note: a table (as in, on-disk table) has a single Xdb_key_def object which
  is shared across multiple TABLE* objects and may be used simultaneously from
  different threads.

  There are several data encodings:

  === SQL LAYER ===
  SQL layer uses two encodings:

  - "Table->record format". This is the format that is used for the data in
     the record buffers, table->record[i]

  - KeyTupleFormat (see opt_range.cc) - this is used in parameters to index
    lookup functions, like handler::index_read_map().

  === Inside XENGINE ===
  Primary Key is stored as a mapping:

    index_tuple -> StoredRecord

  StoredRecord is in Table->record format, except for blobs, which are stored
  in-place. See ha_xengine::convert_record_to_storage_format for details.

  Secondary indexes are stored as one of two variants:

    index_tuple -> unpack_info
    index_tuple -> empty_string

  index_tuple here is the form of key that can be compared with memcmp(), aka
  "mem-comparable form".

  unpack_info is extra data that allows to restore the original value from its
  mem-comparable form. It is present only if the index supports index-only
  reads.
*/

class Xdb_key_def {
public:
  /* Convert a key from KeyTupleFormat to mem-comparable form */
  uint pack_index_tuple(TABLE *tbl, uchar *pack_buffer,
                        uchar *packed_tuple, const uchar *key_tuple,
                        key_part_map &keypart_map) const;

  int pack_field(Field *const field, Xdb_field_packing *pack_info,
                 uchar *&tuple, uchar *const packed_tuple,
                 uchar *const pack_buffer, Xdb_string_writer *const unpack_info,
                 uint *const n_null_fields) const;

  /* Convert a key from Table->record format to mem-comparable form */
  uint pack_record(const TABLE *const tbl, uchar *const pack_buffer,
                   const uchar *const record, uchar *const packed_tuple,
                   Xdb_string_writer *const unpack_info,
                   const bool &should_store_row_debug_checksums,
                   const longlong &hidden_pk_id = 0, uint n_key_parts = 0,
                   uint *const n_null_fields = nullptr,
                   const TABLE *const altered_table = nullptr) const;

  int pack_new_record(
      const TABLE *const old_tbl, uchar *const pack_buffer,
      const uchar *const record, uchar *const packed_tuple,
      Xdb_string_writer *const unpack_info,
      const bool &should_store_row_debug_checksums,
      const longlong &hidden_pk_id, uint n_key_parts, uint *const n_null_fields,
      const TABLE *const altered_table,
      const std::shared_ptr<Xdb_inplace_ddl_dict_info> dict_info,
      uint &size) const;

  /* Pack the hidden primary key into mem-comparable form. */
  uint pack_hidden_pk(const longlong &hidden_pk_id,
                      uchar *const packed_tuple) const;
  int unpack_field(Xdb_field_packing *const fpi,
                   Field *const             field,
                   Xdb_string_reader*       reader,
                   const uchar *const       default_value,
                   Xdb_string_reader*       unp_reader) const;
  int unpack_record(TABLE *const table, uchar *const buf,
                    const xengine::common::Slice *const packed_key,
                    const xengine::common::Slice *const unpack_info,
                    const bool &verify_row_debug_checksums) const;

  int unpack_record_pk(
      TABLE *const table, uchar *const buf,
      const xengine::common::Slice *const packed_key,
      const xengine::common::Slice *const packed_value,
      const std::shared_ptr<Xdb_inplace_ddl_dict_info> &dict_info) const;

  bool table_has_unpack_info(TABLE *const table) const;

  int unpack_record_1(TABLE *const table, uchar *const buf,
                    const xengine::common::Slice *const packed_key,
                    const xengine::common::Slice *const unpack_info,
                    const bool &verify_row_debug_checksums) const;


  static bool unpack_info_has_checksum(const xengine::common::Slice &unpack_info);
  int compare_keys(const xengine::common::Slice *key1, const xengine::common::Slice *key2,
                   std::size_t *const column_index) const;

  size_t key_length(const TABLE *const table, const xengine::common::Slice &key) const;

  /* Get the key that is the "infimum" for this index */
  inline void get_infimum_key(uchar *const key, uint *const size) const {
    xdb_netbuf_store_index(key, m_index_number);
    *size = INDEX_NUMBER_SIZE;
  }

  /* Get the key that is a "supremum" for this index */
  inline void get_supremum_key(uchar *const key, uint *const size) const {
    xdb_netbuf_store_index(key, m_index_number + 1);
    *size = INDEX_NUMBER_SIZE;
  }

  /* Make a key that is right after the given key. */
  static int successor(uchar *const packed_tuple, const uint &len);

  /*
    This can be used to compare prefixes.
    if  X is a prefix of Y, then we consider that X = Y.
  */
  // b describes the lookup key, which can be a prefix of a.
  int cmp_full_keys(const xengine::common::Slice &a, const xengine::common::Slice &b) const {
    DBUG_ASSERT(covers_key(a));
//    DBUG_ASSERT(covers_key(b));

    return memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
  }

  /* Check if given mem-comparable key belongs to this index */
  bool covers_key(const xengine::common::Slice &slice) const {
    if (slice.size() < INDEX_NUMBER_SIZE)
      return false;

    if (memcmp(slice.data(), m_index_number_storage_form, INDEX_NUMBER_SIZE))
      return false;

    return true;
  }

  /*
    Return true if the passed mem-comparable key
    - is from this index, and
    - it matches the passed key prefix (the prefix is also in mem-comparable
      form)
  */
  bool value_matches_prefix(const xengine::common::Slice &value,
                            const xengine::common::Slice &prefix) const {
    return covers_key(value) && !cmp_full_keys(value, prefix);
  }

  uint32 get_keyno() const { return m_keyno; }

  uint32 get_index_number() const { return m_index_number; }

  GL_INDEX_ID get_gl_index_id() const {
    const GL_INDEX_ID gl_index_id = {m_cf_handle->GetID(), m_index_number};
    return gl_index_id;
  }

  int read_memcmp_key_part(const TABLE *table_arg, Xdb_string_reader *reader,
                           const uint part_num) const;

  /* Must only be called for secondary keys: */
  uint get_primary_key_tuple(const TABLE *const tbl,
                             const Xdb_key_def &pk_descr,
                             const xengine::common::Slice *const key,
                             uchar *const pk_buffer) const;

  uint get_memcmp_sk_parts(const TABLE *table, const xengine::common::Slice &key,
                           uchar *sk_buffer, uint *n_null_fields) const;

  uint get_memcmp_sk_size(const TABLE *table, const xengine::common::Slice &key,
                          uint *n_null_fields) const;

  /* Return max length of mem-comparable form */
  uint max_storage_fmt_length() const { return m_maxlength; }

  uint get_key_parts() const { return m_key_parts; }

  bool get_support_icp_flag() const { return m_is_support_icp; }
  /*
    Get a field object for key part #part_no

    @detail
      SQL layer thinks unique secondary indexes and indexes in partitioned
      tables are not "Extended" with Primary Key columns.

      Internally, we always extend all indexes with PK columns. This function
      uses our definition of how the index is Extended.
  */
  inline Field *get_table_field_for_part_no(TABLE *table, uint part_no) const;

  const std::string &get_name() const { return m_name; }

  const xengine::common::SliceTransform *get_extractor() const {
    return m_prefix_extractor.get();
  }

  Xdb_key_def &operator=(const Xdb_key_def &) = delete;
  Xdb_key_def(const Xdb_key_def &k);
  Xdb_key_def(uint indexnr_arg, uint keyno_arg,
              xengine::db::ColumnFamilyHandle *cf_handle_arg,
              uint16_t index_dict_version_arg, uchar index_type_arg,
              uint16_t kv_format_version_arg, bool is_reverse_cf_arg,
              bool is_auto_cf_arg, const std::string &name,
              Xdb_index_stats stats = Xdb_index_stats());
  ~Xdb_key_def();

  enum {
    INDEX_NUMBER_SIZE = 4,
    VERSION_SIZE = 2,
    CF_NUMBER_SIZE = 4,
    CF_FLAG_SIZE = 4,
    PACKED_SIZE = 4, // one int
    TABLE_ID_SIZE = 8,
  };

  // bit flags for combining bools when writing to disk
  enum {
    REVERSE_CF_FLAG = 1,
    AUTO_CF_FLAG = 2,
  };

  // Data dictionary types
  enum DATA_DICT_TYPE {
    DDL_ENTRY_INDEX_START_NUMBER = 1,
    INDEX_INFO = 2,
    CF_DEFINITION = 3,
    BINLOG_INFO_INDEX_NUMBER = 4,
    DDL_DROP_INDEX_ONGOING = 5,
    INDEX_STATISTICS = 6,
    MAX_INDEX_ID = 7,
    DDL_CREATE_INDEX_ONGOING = 8,
    DDL_OPERATION_LOG = 9,  //DDL-LOG
    SYSTEM_CF_VERSION_INDEX = 10,
    MAX_TABLE_ID = 11,
    END_DICT_INDEX_ID = 255
  };

  enum SYSTEM_CF_VERSION {
    VERSION_0 = 0, // old system version which doesn't have system cf version
    VERSION_1 = 1, // DDL_ENTRY_INDEX_START_NUMBER records moved to global dd
    VERSION_2 = 2, // DDL_DROP_INDEX_ONGOING/DDL_CREATE_INDEX_ONGOING removed
  };

  // Data dictionary schema version. Introduce newer versions
  // if changing schema layout
  enum {
    DDL_ENTRY_INDEX_VERSION = 1,
    CF_DEFINITION_VERSION = 1,
    BINLOG_INFO_INDEX_NUMBER_VERSION = 1,
    DDL_DROP_INDEX_ONGOING_VERSION = 1,
    MAX_INDEX_ID_VERSION = 1,
    DDL_CREATE_INDEX_ONGOING_VERSION = 1,
    DDL_OPERATION_LOG_VERSION = 1,
    MAX_TABLE_ID_VERSION = 1,
    // Version for index stats is stored in IndexStats struct
  };

  // Index info version.  Introduce newer versions when changing the
  // INDEX_INFO layout. Update INDEX_INFO_VERSION_LATEST to point to the
  // latest version number.
  enum {
    INDEX_INFO_VERSION_INITIAL = 1, // Obsolete
    INDEX_INFO_VERSION_KV_FORMAT,
    INDEX_INFO_VERSION_GLOBAL_ID,
    // There is no change to data format in this version, but this version
    // verifies KV format version, whereas previous versions do not. A version
    // bump is needed to prevent older binaries from skipping the KV version
    // check inadvertently.
    INDEX_INFO_VERSION_VERIFY_KV_FORMAT,
    // This normally point to the latest (currently it does).
    INDEX_INFO_VERSION_LATEST = INDEX_INFO_VERSION_VERIFY_KV_FORMAT,
  };

  // XEngine index types
  enum INDEX_TYPE {
    INDEX_TYPE_PRIMARY = 1,
    INDEX_TYPE_SECONDARY = 2,
    INDEX_TYPE_HIDDEN_PRIMARY = 3,
  };

  // Key/Value format version for each index type
  enum {
    PRIMARY_FORMAT_VERSION_INITIAL = 10,
    // This change includes:
    //  - For columns that can be unpacked with unpack_info, PK
    //    stores the unpack_info.
    //  - DECIMAL datatype is no longer stored in the row (because
    //    it can be decoded from its mem-comparable form)
    //  - VARCHAR-columns use endspace-padding.
    PRIMARY_FORMAT_VERSION_UPDATE1 = 11,
    PRIMARY_FORMAT_VERSION_LATEST = PRIMARY_FORMAT_VERSION_UPDATE1,

    SECONDARY_FORMAT_VERSION_INITIAL = 10,
    // This change the SK format to include unpack_info.
    SECONDARY_FORMAT_VERSION_UPDATE1 = 11,
    SECONDARY_FORMAT_VERSION_LATEST = SECONDARY_FORMAT_VERSION_UPDATE1,
  };

  void setup(const TABLE *const table, const Xdb_tbl_def *const tbl_def);

  bool is_primary_key() const {
    return m_index_type == INDEX_TYPE_PRIMARY || m_index_type == INDEX_TYPE_HIDDEN_PRIMARY;
  }

  bool is_secondary_key() const {
    return m_index_type == INDEX_TYPE_SECONDARY;
  }

  bool is_hidden_primary_key() const {
    return m_index_type == INDEX_TYPE_HIDDEN_PRIMARY;
  }

  xengine::db::ColumnFamilyHandle *get_cf() const { return m_cf_handle; }
  void set_cf(xengine::db::ColumnFamilyHandle *cf) { m_cf_handle = cf; }

  /* Check if keypart #kp can be unpacked from index tuple */
  inline bool can_unpack(const uint &kp) const;
  /* Check if keypart #kp needs unpack info */
  inline bool has_unpack_info(const uint &kp) const;

  /* Check if given table has a primary key */
  static bool table_has_hidden_pk(const TABLE *const table);

  void report_checksum_mismatch(const bool &is_key, const char *const data,
                                const size_t data_size) const;

  /* Check if index is at least pk_min if it is a PK,
    or at least sk_min if SK.*/
  bool index_format_min_check(const int &pk_min, const int &sk_min) const;

  // set metadata for user-defined index
  bool write_dd_index(dd::Index *dd_index, uint64_t table_id) const;
  // set extra metadata for user-defined index and hidden primary key
  bool write_dd_index_ext(dd::Properties& prop) const;

  static bool verify_dd_index(const dd::Index *dd_index, uint64_t table_id);
  static bool verify_dd_index_ext(const dd::Properties& prop);
private:
#ifndef DBUG_OFF
  inline bool is_storage_available(const int &offset, const int &needed) const {
    const int storage_length = static_cast<int>(max_storage_fmt_length());
    return (storage_length - offset) >= needed;
  }
#endif // DBUG_OFF

  /* Global number of this index (used as prefix in StorageFormat) */
  const uint32 m_index_number;

  uchar m_index_number_storage_form[INDEX_NUMBER_SIZE];

  xengine::db::ColumnFamilyHandle *m_cf_handle;

public:
  uint16_t m_index_dict_version;
  uchar m_index_type;
  /* KV format version for the index id */
  uint16_t m_kv_format_version;
  /* If true, the column family stores data in the reverse order */
  bool m_is_reverse_cf;

  bool m_is_auto_cf;
  std::string m_name;
  mutable Xdb_index_stats m_stats;

  /* if true, DDL or DML can skip unique check */
  bool m_can_skip_unique_check = false;

private:
  friend class Xdb_tbl_def; // for m_index_number above

  /* Number of key parts in the primary key*/
  uint m_pk_key_parts;

  /*
     pk_part_no[X]=Y means that keypart #X of this key is key part #Y of the
     primary key.  Y==-1 means this column is not present in the primary key.
  */
  uint *m_pk_part_no;

  /* Array of index-part descriptors. */
  Xdb_field_packing *m_pack_info;

  uint m_keyno; /* number of this index in the table */

  /*
    Number of key parts in the index (including "index extension"). This is how
    many elements are in the m_pack_info array.
  */
  uint m_key_parts;

  /* Prefix extractor for the column family of the key definiton */
  std::shared_ptr<const xengine::common::SliceTransform> m_prefix_extractor;

  /* Maximum length of the mem-comparable form. */
  uint m_maxlength;

  /* if support index-push-down then set true */
  bool m_is_support_icp;

  /* mutex to protect setup */
  mysql_mutex_t m_mutex;
};

const uchar XDB_MAX_XFRM_MULTIPLY = 8;

struct Xdb_collation_codec {
  const my_core::CHARSET_INFO *m_cs;
  // The first element unpacks VARCHAR(n), the second one - CHAR(n).
  std::array<xdb_make_unpack_info_t, 2> m_make_unpack_info_func;
  std::array<xdb_index_field_unpack_t, 2> m_unpack_func;
  virtual ~Xdb_collation_codec() = default;
};

template <typename T, uint L>
struct Xdb_collation_codec_derived : public Xdb_collation_codec
{
  using character_type = T;
  static const uint level = L;
  static const uint size = 1 << (8 * sizeof(T));

  std::array<T, size> m_enc_idx;
  std::array<uchar, size> m_enc_size;

  std::array<uchar, size> m_dec_size;
  // Generally, m_dec_idx[idx][src] = original character. However, because the
  // array can grow quite large for utf8, we have m_dec_idx_ext which is more
  // space efficient.
  //
  // Essentially, if idx < level, then use m_dec_idx[idx][src], otherwise, use
  // m_dec_idx[src][idx - level]. If level is 0 then, m_dec_idx_ext is unused.
  std::vector<std::array<T, size>> m_dec_idx;
  std::array<std::vector<T>, size> m_dec_idx_ext;

  // m_dec_skip[{idx, src}] = number of bytes to skip given (idx, src)
  std::map<std::pair<T, T>, uchar> m_dec_skip;
};


// "Simple" collations (those specified in strings/ctype-simple.c) are simple
// because their strnxfrm function maps one byte to one byte. However, the
// mapping is not injective, so the inverse function will take in an extra
// index parameter containing information to disambiguate what the original
// character was.
//
// The m_enc* members are for encoding. Generally, we want encoding to be:
//      src -> (dst, idx)
//
// Since strnxfrm already gives us dst, we just need m_enc_idx[src] to give us
// idx.
//
// For the inverse, we have:
//      (dst, idx) -> src
//
// We have m_dec_idx[idx][dst] = src to get our original character back.
//

using Xdb_collation_codec_simple = Xdb_collation_codec_derived<uchar, 0>;
using Xdb_collation_codec_utf8 = Xdb_collation_codec_derived<uint16, 2>;
using Xdb_collation_codec_gbk = Xdb_collation_codec_derived<uint16, 2>;
using Xdb_collation_codec_utf8mb4 = Xdb_collation_codec_derived<uint16, 2>;

extern mysql_mutex_t xdb_collation_data_mutex;
extern mysql_mutex_t xdb_mem_cmp_space_mutex;
extern std::array<const Xdb_collation_codec *, MY_ALL_CHARSETS_SIZE>
    xdb_collation_data;

class Xdb_field_packing {
public:
  Xdb_field_packing(const Xdb_field_packing &) = delete;
  Xdb_field_packing &operator=(const Xdb_field_packing &) = delete;
  Xdb_field_packing() = default;

  /* Length of mem-comparable image of the field, in bytes */
  int m_max_image_len;

  /* Length of strnxfrm length of the field, in bytes */
  int m_max_strnxfrm_len;

  /* number of characters be part of key */
  uint m_weights;

  /* prefix_flag*/
  bool m_prefix_index_flag;

  /* Length of image in the unpack data */
  int m_unpack_data_len;
  int m_unpack_data_offset;

  bool m_maybe_null; /* TRUE <=> NULL-byte is stored */

  /*
    Valid only for CHAR/VARCHAR fields.
  */
  const CHARSET_INFO *m_charset;

  // (Valid when Variable Length Space Padded Encoding is used):
  uint m_segment_size; // size of segment used

  // number of bytes used to store number of trimmed (or added)
  // spaces in the upack_info
  bool m_unpack_info_uses_two_bytes;

  const std::vector<uchar> *space_xfrm;
  size_t space_xfrm_len;
  size_t space_mb_len;

  const Xdb_collation_codec *m_charset_codec;

  /*
    @return TRUE: this field makes use of unpack_info.
  */
  bool uses_unpack_info() const { return (m_make_unpack_info_func != nullptr); }

  /* TRUE means unpack_info stores the original field value */
  bool m_unpack_info_stores_value;

  xdb_index_field_pack_t m_pack_func;
  xdb_make_unpack_info_t m_make_unpack_info_func;

  /*
    This function takes
    - mem-comparable form
    - unpack_info data
    and restores the original value.
  */
  xdb_index_field_unpack_t m_unpack_func;

  /*
    This function skips over mem-comparable form.
  */
  xdb_index_field_skip_t m_skip_func;

private:
  /*
    Location of the field in the table (key number and key part number).

    Note that this describes not the field, but rather a position of field in
    the index. Consider an example:

      col1 VARCHAR (100),
      INDEX idx1 (col1)),
      INDEX idx2 (col1(10)),

    Here, idx2 has a special Field object that is set to describe a 10-char
    prefix of col1.

    We must also store the keynr. It is needed for implicit "extended keys".
    Every key in XEngine needs to include PK columns.  Generally, SQL layer
    includes PK columns as part of its "Extended Keys" feature, but sometimes
    it does not (known examples are unique secondary indexes and partitioned
    tables).
    In that case, XEngine's index descriptor has invisible suffix of PK
    columns (and the point is that these columns are parts of PK, not parts
    of the current index).
  */
  uint m_keynr;
  uint m_key_part;

public:
  bool setup(const Xdb_key_def *const key_descr, const Field *const field,
             const uint &keynr_arg, const uint &key_part_arg,
             const uint16 &key_length);

  Field *get_field_in_table(const TABLE *const tbl) const;

  uint get_field_index_in_table(const TABLE *const altered_table) const;

  void fill_hidden_pk_val(uchar **dst, const longlong &hidden_pk_id) const;
};

/*
  Descriptor telling how to decode/encode a field to on-disk record storage
  format. Not all information is in the structure yet, but eventually we
  want to have as much as possible there to avoid virtual calls.

  For encoding/decoding of index tuples, see Xdb_key_def.
  */
class Xdb_field_encoder {
public:
  Xdb_field_encoder(const Xdb_field_encoder &) = delete;
  Xdb_field_encoder &operator=(const Xdb_field_encoder &) = delete;
  /*
    STORE_NONE is set when a column can be decoded solely from their
    mem-comparable form.
    STORE_SOME is set when a column can be decoded from their mem-comparable
    form plus unpack_info.
    STORE_ALL is set when a column cannot be decoded, so its original value
    must be stored in the PK records.
    */
  enum STORAGE_TYPE {
    STORE_NONE,
    STORE_SOME,
    STORE_ALL,
  };
  STORAGE_TYPE m_storage_type;

  uint m_null_offset;
  uint16 m_field_index;

  uchar m_null_mask; // 0 means the field cannot be null

  my_core::enum_field_types m_field_type;

  uint m_pack_length_in_rec;

  bool maybe_null() const { return m_null_mask != 0; }

  bool uses_variable_len_encoding() const {
    return (m_field_type == MYSQL_TYPE_JSON ||
            m_field_type == MYSQL_TYPE_BLOB ||
            m_field_type == MYSQL_TYPE_VARCHAR);
  }
};

inline Field *Xdb_key_def::get_table_field_for_part_no(TABLE *table,
                                                       uint part_no) const {
  DBUG_ASSERT(part_no < get_key_parts());
  return m_pack_info[part_no].get_field_in_table(table);
}

inline bool Xdb_key_def::can_unpack(const uint &kp) const {
  DBUG_ASSERT(kp < m_key_parts);
  return (m_pack_info[kp].m_unpack_func != nullptr);
}

inline bool Xdb_key_def::has_unpack_info(const uint &kp) const {
  DBUG_ASSERT(kp < m_key_parts);
  return m_pack_info[kp].uses_unpack_info();
}

/*
  A table definition. This is an entry in the mapping

    dbname.tablename -> {index_nr, index_nr, ... }

  There is only one Xdb_tbl_def object for a given table.
  That's why we keep auto_increment value here, too.
*/

class Xdb_tbl_def {
private:
  void check_if_is_mysql_system_table();

  /* Stores 'dbname.tablename' */
  std::string m_dbname_tablename;

  /* Store the db name, table name, and partition name */
  std::string m_dbname;
  std::string m_tablename;
  std::string m_partition;

  uint64_t m_table_id = dd::INVALID_OBJECT_ID;

  void set_name(const std::string &name);

public:
  Xdb_tbl_def(const Xdb_tbl_def &) = delete;
  Xdb_tbl_def &operator=(const Xdb_tbl_def &) = delete;

  explicit Xdb_tbl_def(const std::string &name)
      : space_id(0), m_key_descr_arr(nullptr), m_hidden_pk_val(1), m_auto_incr_val(1), m_inplace_new_tdef(nullptr), m_dict_info(nullptr) {
    set_name(name);
  }

  Xdb_tbl_def(const char *const name, const size_t &len)
      : space_id(0), m_key_descr_arr(nullptr), m_hidden_pk_val(1), m_auto_incr_val(1), m_inplace_new_tdef(nullptr), m_dict_info(nullptr) {
    set_name(std::string(name, len));
  }

  explicit Xdb_tbl_def(const xengine::common::Slice &slice, const size_t &pos = 0)
      : space_id(0), m_key_descr_arr(nullptr), m_hidden_pk_val(1), m_auto_incr_val(1), m_inplace_new_tdef(nullptr), m_dict_info(nullptr) {
    set_name(std::string(slice.data() + pos, slice.size() - pos));
  }

  ~Xdb_tbl_def();
  
  int64_t space_id;
  /* Number of indexes */
  uint m_key_count;

  /* Array of index descriptors */
  std::shared_ptr<Xdb_key_def> *m_key_descr_arr;

  std::atomic<longlong> m_hidden_pk_val;
  std::atomic<ulonglong> m_auto_incr_val;

  /* Is this a system table */
  bool m_is_mysql_system_table;

  // save the being added keys, it's set in old table instead of altered table
  // as the old table needs them for update
  std::unordered_map<const Xdb_key_def *, Added_key_info> m_added_key;

  // We use raw pointer as the key of m_add_key, so ref the container
  // shared_ptr using this.
  std::list<std::shared_ptr<const Xdb_key_def>> m_added_key_ref;

  // we use m_inplace_new_tdef as part of old_tdef, then we can update both
  // new subtables and old subtables.
  Xdb_tbl_def* m_inplace_new_tdef;

  // for online-copy-ddl, every new key(pk,sk) has Added_key_info,
  // we can check whether there is duplicate-key error during online phase.
  std::unordered_map<const Xdb_key_def*, Added_key_info> m_inplace_new_keys;

  // We use raw pointer as the key of m_add_key, so ref the container
  // shared_ptr using this.
  std::list<std::shared_ptr<const Xdb_key_def>> m_inplace_new_keys_ref;

  // used to build new_record during online-copy-ddl
  std::shared_ptr<Xdb_inplace_ddl_dict_info> m_dict_info;

#if 0
  bool put_dict(Xdb_dict_manager *const dict, xengine::db::WriteBatch *const batch,
                uchar *const key, const size_t &keylen);
  bool put_dict(Xdb_dict_manager *const dict, xengine::db::WriteBatch *const batch);
#endif

  const std::string &full_tablename() const { return m_dbname_tablename; }
  const std::string &base_dbname() const { return m_dbname; }
  const std::string &base_tablename() const { return m_tablename; }
  const std::string &base_partition() const { return m_partition; }

  int create_added_key(std::shared_ptr<Xdb_key_def> added_key,
                       Added_key_info info);
  int clear_added_key();

  int create_new_keys_info(std::shared_ptr<Xdb_key_def> added_key, Added_key_info info);

  int clear_new_keys_info();

  int clear_keys_for_ddl();

  uint64_t get_table_id() const { return m_table_id; }
  void set_table_id(uint64_t id) { m_table_id = id; }
  bool init_table_id(Xdb_ddl_manager& ddl_manager);

  bool write_dd_table(dd::Table* dd_table) const;
  static bool verify_dd_table(const dd::Table* dd_table, uint32_t &hidden_pk);
};

/*
  A thread-safe sequential number generator. Its performance is not a concern
  hence it is ok to protect it by a mutex.
*/

class Xdb_seq_generator {
  uint m_next_number = 0;

  mysql_mutex_t m_mutex;

public:
  Xdb_seq_generator(const Xdb_seq_generator &) = delete;
  Xdb_seq_generator &operator=(const Xdb_seq_generator &) = delete;
  Xdb_seq_generator() = default;

  void init(const uint &initial_number) {
    mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
    m_next_number = initial_number;
  }

  uint get_and_update_next_number(Xdb_dict_manager *const dict);

  void cleanup() { mysql_mutex_destroy(&m_mutex); }
};

interface Xdb_tables_scanner {
  virtual int add_table(Xdb_tbl_def * tdef) = 0;
};

/*
  This contains a mapping of

     dbname.table_name -> array{Xdb_key_def}.

  objects are shared among all threads.
*/

class Xdb_ddl_manager {
  Xdb_dict_manager *m_dict = nullptr;
  //my_core::HASH m_ddl_hash; // Contains Xdb_tbl_def elements
  std::unordered_map<std::string, std::shared_ptr<Xdb_tbl_def>> m_ddl_hash;
  // Maps index id to <table_name, index number>
  std::map<GL_INDEX_ID, std::pair<std::string, uint>> m_index_num_to_keydef;

  // Maps index id to key definitons not yet committed to data dictionary.
  // This is mainly used to store key definitions during ALTER TABLE.
  std::map<GL_INDEX_ID, std::shared_ptr<Xdb_key_def>>
    m_index_num_to_uncommitted_keydef;
  mysql_rwlock_t m_rwlock;

  Xdb_seq_generator m_sequence;
  // A queue of table stats to write into data dictionary
  // It is produced by event listener (ie compaction and flush threads)
  // and consumed by the xengine background thread
  std::map<GL_INDEX_ID, Xdb_index_stats> m_stats2store;

  const std::shared_ptr<Xdb_key_def> &find(GL_INDEX_ID gl_index_id);

  // used to generate table id
  mysql_mutex_t m_next_table_id_mutex;
  uint64_t m_next_table_id;
public:
  Xdb_ddl_manager(const Xdb_ddl_manager &) = delete;
  Xdb_ddl_manager &operator=(const Xdb_ddl_manager &) = delete;
  Xdb_ddl_manager() {}

  /* Load the data dictionary from on-disk storage */
  bool init(THD *const thd, Xdb_dict_manager *const dict_arg);

  void cleanup();

  // find is designed to find Xdb_tbl_def from table cache or load Xdb_tbl_def
  // from dictionary.
  //
  //@params
  // table_name[in] key used to search in hash_map
  // from_dict[out] tell caller whether the retured object is loaded from
  //                dictionary. If true, the returned object was loaded from
  //                dictioanry. The caller can put it into cache if needed.
  // lock[in] if true(by default), a read lock on table cache will be acquired.
  //
  //@return value
  // Xdb_tbl_def
  std::shared_ptr<Xdb_tbl_def> find(const std::string &table_name,
                                    bool* from_dict, bool lock = true);
  std::shared_ptr<Xdb_tbl_def> find(const char *table_name, int64_t name_len,
                                    bool* from_dict, bool lock = true);
  std::shared_ptr<Xdb_tbl_def> find(const dd::Table* dd_table,
                                    const std::string& table_name,
                                    bool* from_dict, bool lock = true);
  std::shared_ptr<Xdb_tbl_def> find(THD* thd, const std::string& table_name,
                                    bool* from_dict, bool lock = true);

  // find Xdb_tbl_def from table cache
  //
  //@params
  // table_name[in] key used to search in hash_map
  // lock[in] if true(by default), a read lock on table cache will be acquired.
  //
  //@return value
  // Xdb_tbl_def
  std::shared_ptr<Xdb_tbl_def> find_tbl_from_cache(
      const std::string &table_name, bool lock = true);

  // Xdb_tbl_def* find_tbl_from_dict(const std::string &table_name);
  int get_index_dict(const std::string &table_name,
                     const GL_INDEX_ID &gl_index_id,
                     uint max_index_id_in_dict, uint16 &index_dict_version,
                     uchar &index_type, uint16 &kv_version, uint &flags);

  std::shared_ptr<const Xdb_key_def> safe_find(GL_INDEX_ID gl_index_id);
  void set_stats(const std::unordered_map<GL_INDEX_ID, Xdb_index_stats> &stats);
  void adjust_stats(Xdb_index_stats stats);
  void adjust_stats2(Xdb_index_stats stats, const bool increment);
  void persist_stats(const bool &sync = false);

  /* Put the data into in-memory table (only) */
  void put(const std::shared_ptr<Xdb_tbl_def>& tbl_def, bool lock = true);
  /* Modify the mapping and write it to on-disk storage */
  int put_and_write(const std::shared_ptr<Xdb_tbl_def>& tbl,
                    xengine::db::WriteBatch *const batch,
                    Xdb_ddl_log_manager *const ddl_log_manager,
                    ulong thread_id, bool write_ddl_log);

#if 0
  void remove(const std::string &dbname_tablename,
              xengine::db::WriteBatch *const batch, bool lock = true);
  void remove_dict(const std::string &dbname_tablename,
                   xengine::db::WriteBatch *const batch);
  bool rename(Xdb_tbl_def *const tbl, const std::string &to,
              xengine::db::WriteBatch *const batch);
#endif

  void remove_cache(const std::string &dbname_tablename, bool lock = true);
  bool rename_cache(const std::string &from, const std::string &to);
  bool rename_cache(Xdb_tbl_def* tbl, const std::string &to);

  uint get_and_update_next_number(Xdb_dict_manager *const dict) {
    return m_sequence.get_and_update_next_number(dict);
  }

  bool get_table_id(uint64_t &table_id);

  /* Walk the data dictionary */
  int scan_for_tables(Xdb_tables_scanner *tables_scanner);

  void erase_index_num(const GL_INDEX_ID &gl_index_id);
  void add_uncommitted_keydefs(
      const std::unordered_set<std::shared_ptr<Xdb_key_def>> &indexes);
  void remove_uncommitted_keydefs(
      const std::unordered_set<std::shared_ptr<Xdb_key_def>> &indexes);

  // check whether background thread cna purge given subtable id
  bool can_purge_subtable(THD* thd, const GL_INDEX_ID& gl_index_id);
private:
  bool upgrade_system_cf_version1(THD* thd);
  bool upgrade_system_cf_version2();
  bool load_existing_tables(uint);
  bool upgrade_existing_tables(THD *const thd);
  bool populate_existing_tables(THD *const thd);
  bool update_max_table_id(uint64_t table_id);
  bool update_system_cf_version(uint16_t system_cf_version);

  Xdb_key_def* restore_index_from_dd(const dd::Properties& prop,
      const std::string &table_name, const std::string &index_name,
      uint32_t subtable_id, uint keyno, int index_type);
  Xdb_tbl_def* restore_table_from_dd(const dd::Table* dd_table,
                                     const std::string& table_name);
  Xdb_tbl_def* restore_table_from_dd(THD* thd, const std::string& table_name);
};

/*
  Writing binlog information into XENGINE at commit(),
  and retrieving binlog information at crash recovery.
  commit() and recovery are always executed by at most single client
  at the same time, so concurrency control is not needed.

  Binlog info is stored in XENGINE as the following.
   key: BINLOG_INFO_INDEX_NUMBER
   value: packed single row:
     binlog_name_length (2 byte form)
     binlog_name
     binlog_position (4 byte form)
     binlog_gtid_length (2 byte form)
     binlog_gtid
*/
class Xdb_binlog_manager {
public:
  Xdb_binlog_manager(const Xdb_binlog_manager &) = delete;
  Xdb_binlog_manager &operator=(const Xdb_binlog_manager &) = delete;
  Xdb_binlog_manager() = default;

  bool init(Xdb_dict_manager *const dict);
  void cleanup();
  void update(const char *const binlog_name, const my_off_t binlog_pos,
              const char *const binlog_max_gtid,
              xengine::db::WriteBatchBase *const batch);
  bool read(char *const binlog_name, my_off_t *const binlog_pos,
            char *const binlog_gtid) const;
  void update_slave_gtid_info(const uint &id, const char *const db,
                              const char *const gtid,
                              xengine::db::WriteBatchBase *const write_batch);

private:
  Xdb_dict_manager *m_dict = nullptr;
  uchar m_key_buf[Xdb_key_def::INDEX_NUMBER_SIZE] = {0};
  xengine::common::Slice m_key_slice;

  xengine::common::Slice pack_value(uchar *const buf, const char *const binlog_name,
                            const my_off_t &binlog_pos,
                            const char *const binlog_gtid) const;
  bool unpack_value(const uchar *const value, char *const binlog_name,
                    my_off_t *const binlog_pos, char *const binlog_gtid) const;

  std::atomic<Xdb_tbl_def *> m_slave_gtid_info_tbl;
};

/*
   Xdb_dict_manager manages how MySQL on XENGINE stores its
  internal data dictionary.
  XEngine stores data dictionary on dedicated system column family
  named __system__. The system column family is used by Xengine
  internally only, and not used by applications.

   Currently XEngine has the following data dictionary data models.

  1. Table Name => internal index id mappings
  key: Xdb_key_def::DDL_ENTRY_INDEX_START_NUMBER(0x1) + dbname.tablename
  value: version, {cf_id, index_id}*n_indexes_of_the_table
  version is 2 bytes. cf_id and index_id are 4 bytes.

  2. internal cf_id, index id => index information
  key: Xdb_key_def::INDEX_INFO(0x2) + cf_id + index_id
  value: version, index_type, kv_format_version
  index_type is 1 byte, version and kv_format_version are 2 bytes.

  3. CF id => CF flags
  key: Xdb_key_def::CF_DEFINITION(0x3) + cf_id
  value: version, {is_reverse_cf, is_auto_cf}
  cf_flags is 4 bytes in total.

  4. Binlog entry (updated at commit)
  key: Xdb_key_def::BINLOG_INFO_INDEX_NUMBER (0x4)
  value: version, {binlog_name,binlog_pos,binlog_gtid}

  5. Ongoing drop index entry
  key: Xdb_key_def::DDL_DROP_INDEX_ONGOING(0x5) + cf_id + index_id
  value: version

  6. index stats
  key: Xdb_key_def::INDEX_STATISTICS(0x6) + cf_id + index_id
  value: version, {materialized PropertiesCollector::IndexStats}

  7. maximum index id
  key: Xdb_key_def::MAX_INDEX_ID(0x7)
  value: index_id
  index_id is 4 bytes

  8. Ongoing create index entry
  key: Xdb_key_def::DDL_CREATE_INDEX_ONGOING(0x8) + cf_id + index_id
  value: version

  Data dictionary operations are atomic inside XENGINE. For example,
  when creating a table with two indexes, it is necessary to call Put
  three times. They have to be atomic. Xdb_dict_manager has a wrapper function
  begin() and commit() to make it easier to do atomic operations.

*/
class Xdb_dict_manager {
private:
  mysql_mutex_t m_mutex;
  xengine::db::DB *m_db = nullptr;
  xengine::db::ColumnFamilyHandle *m_system_cfh = nullptr;
  /* Utility to put INDEX_INFO and CF_DEFINITION */

  uchar m_key_buf_max_index_id[Xdb_key_def::INDEX_NUMBER_SIZE] = {0};
  xengine::common::Slice m_key_slice_max_index_id;

  uchar m_key_buf_max_table_id[Xdb_key_def::INDEX_NUMBER_SIZE] = {0};
  xengine::common::Slice m_key_slice_max_table_id;

  static void dump_index_id(uchar *const netbuf,
                            Xdb_key_def::DATA_DICT_TYPE dict_type,
                            const GL_INDEX_ID &gl_index_id);
#if 0
  /* Functions for fast DROP TABLE/INDEX */
  void resume_drop_indexes() const;
  void log_start_drop_table(const std::shared_ptr<Xdb_key_def> *const key_descr,
                            const uint32 &n_keys,
                            const char *const log_action) const;
  void log_start_drop_index(GL_INDEX_ID gl_index_id,
                            const char *log_action) const;
#endif

public:
  Xdb_dict_manager(const Xdb_dict_manager &) = delete;
  Xdb_dict_manager &operator=(const Xdb_dict_manager &) = delete;
  Xdb_dict_manager() = default;

  bool init(xengine::db::DB *const xdb_dict, const xengine::common::ColumnFamilyOptions &cf_options);

  inline void cleanup() { mysql_mutex_destroy(&m_mutex); }

  inline void lock() { XDB_MUTEX_LOCK_CHECK(m_mutex); }

  inline void unlock() { XDB_MUTEX_UNLOCK_CHECK(m_mutex); }

  /* Raw XENGINE operations */
  std::unique_ptr<xengine::db::WriteBatch> begin() const;
  int commit(xengine::db::WriteBatch *const batch, const bool &sync = true) const;
  xengine::common::Status get_value(const xengine::common::Slice &key,
                            std::string *const value) const;
  void put_key(xengine::db::WriteBatchBase *const batch, const xengine::common::Slice &key,
               const xengine::common::Slice &value) const;
  void delete_key(xengine::db::WriteBatchBase *batch,
                  const xengine::common::Slice &key) const;
  xengine::db::Iterator *new_iterator() const;

#if 0
  /* Internal Index id => CF */
  void add_or_update_index_cf_mapping(xengine::db::WriteBatch *batch,
                                      const uchar index_type,
                                      const uint16_t kv_version,
                                      const uint index_id,
                                      const uint cf_id) const;
#endif
  void delete_index_info(xengine::db::WriteBatch *batch,
                         const GL_INDEX_ID &index_id) const;
  // Kept for upgrading from old version
  bool get_index_info(const GL_INDEX_ID &gl_index_id,
                      uint16_t *index_dict_version, uchar *index_type,
                      uint16_t *kv_version) const;

  static bool is_valid_index_version(uint16_t index_dict_version);
  static bool is_valid_kv_version(uchar index_type, uint16_t kv_version);

#if 0
  /* CF id => CF flags */
  void add_cf_flags(xengine::db::WriteBatch *const batch, const uint &cf_id,
                    const uint &cf_flags) const;
#endif
  void drop_cf_flags(xengine::db::WriteBatch *const batch, uint32_t cf_id) const;
  // Kept for upgrading from old version
  bool get_cf_flags(const uint &cf_id, uint *const cf_flags) const;

  void delete_with_prefix(xengine::db::WriteBatch *const batch,
                          Xdb_key_def::DATA_DICT_TYPE dict_type,
                          const GL_INDEX_ID &gl_index_id) const;

  /* Functions for fast CREATE/DROP TABLE/INDEX */
  // Kept for upgrading from old version
  void
  get_ongoing_index_operation(std::unordered_set<GL_INDEX_ID> *gl_index_ids,
                              Xdb_key_def::DATA_DICT_TYPE dd_type) const;
#if 0
  bool is_index_operation_ongoing(const GL_INDEX_ID &gl_index_id,
                                  Xdb_key_def::DATA_DICT_TYPE dd_type) const;
  void start_ongoing_index_operation(xengine::db::WriteBatch *batch,
                                     const GL_INDEX_ID &gl_index_id,
                                     Xdb_key_def::DATA_DICT_TYPE dd_type) const;
  void end_ongoing_index_operation(xengine::db::WriteBatch *const batch,
                                   const GL_INDEX_ID &gl_index_id,
                                   Xdb_key_def::DATA_DICT_TYPE dd_type) const;
  bool is_drop_index_empty() const;
  void add_drop_table(std::shared_ptr<Xdb_key_def> *const key_descr,
                      const uint32 &n_keys,
                      xengine::db::WriteBatch *const batch) const;
  void add_drop_index(const std::unordered_set<GL_INDEX_ID> &gl_index_ids,
                      xengine::db::WriteBatch *const batch) const;
  void add_create_index(const std::unordered_set<GL_INDEX_ID> &gl_index_ids,
                        xengine::db::WriteBatch *const batch) const;
  void
  finish_indexes_operation(const std::unordered_set<GL_INDEX_ID> &gl_index_ids,
                           Xdb_key_def::DATA_DICT_TYPE dd_type) const;
  void rollback_ongoing_index_creation() const;

  void rollback_index_creation(
       const std::unordered_set<std::shared_ptr<Xdb_key_def>> &indexes) const;

  inline void get_ongoing_drop_indexes(
      std::unordered_set<GL_INDEX_ID> *gl_index_ids) const {
    get_ongoing_index_operation(gl_index_ids,
                                Xdb_key_def::DDL_DROP_INDEX_ONGOING);
  }
  inline void get_ongoing_create_indexes(
      std::unordered_set<GL_INDEX_ID> *gl_index_ids) const {
    get_ongoing_index_operation(gl_index_ids,
                                Xdb_key_def::DDL_CREATE_INDEX_ONGOING);
  }
  inline void start_drop_index(xengine::db::WriteBatch *wb,
                               const GL_INDEX_ID &gl_index_id) const {
    start_ongoing_index_operation(wb, gl_index_id,
                                  Xdb_key_def::DDL_DROP_INDEX_ONGOING);
  }
  inline void start_create_index(xengine::db::WriteBatch *wb,
                                 const GL_INDEX_ID &gl_index_id) const {
    start_ongoing_index_operation(wb, gl_index_id,
                                  Xdb_key_def::DDL_CREATE_INDEX_ONGOING);
  }
  inline void finish_drop_indexes(
      const std::unordered_set<GL_INDEX_ID> &gl_index_ids) const {
    finish_indexes_operation(gl_index_ids, Xdb_key_def::DDL_DROP_INDEX_ONGOING);
  }
  inline void finish_create_indexes(
      const std::unordered_set<GL_INDEX_ID> &gl_index_ids) const {
    finish_indexes_operation(gl_index_ids,
                             Xdb_key_def::DDL_CREATE_INDEX_ONGOING);
  }
  inline bool is_drop_index_ongoing(const GL_INDEX_ID &gl_index_id) const {
    return is_index_operation_ongoing(gl_index_id,
                                      Xdb_key_def::DDL_DROP_INDEX_ONGOING);
  }
  inline bool is_create_index_ongoing(const GL_INDEX_ID &gl_index_id) const {
    return is_index_operation_ongoing(gl_index_id,
                                      Xdb_key_def::DDL_CREATE_INDEX_ONGOING);
  }
#endif

  bool get_max_index_id(uint32_t *const index_id) const;
  bool update_max_index_id(xengine::db::WriteBatch *const batch,
                           const uint32_t &index_id) const;
  bool get_system_cf_version(uint16_t* system_cf_version) const;
  bool update_system_cf_version(xengine::db::WriteBatch *const batch,
                                uint16_t system_cf_version) const;
  bool get_max_table_id(uint64_t *table_id) const;
  bool update_max_table_id(xengine::db::WriteBatch *const batch,
                           uint64_t table_id) const;
  void add_stats(xengine::db::WriteBatch *const batch,
                 const std::vector<Xdb_index_stats> &stats) const;
  Xdb_index_stats get_stats(GL_INDEX_ID gl_index_id) const;
};

} // namespace myx
