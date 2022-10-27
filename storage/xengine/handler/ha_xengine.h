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

#ifdef USE_PRAGMA_INTERFACE
#pragma interface /* gcc class implementation */
#endif

/* C++ standard header files */
#include <set>
#include <string>
#include <unordered_set>
#include <vector>
#include <future>

/* MySQL header files */
#include "ib_ut0counter.h"
#include "my_icp.h"
#include "sql_string.h"
#include "field.h"
#include "handler.h"   /* handler */
#include "sql_bitmap.h"
#include "sql_string.h"

#include "mysql/psi/mysql_thread.h"
#include "mysql/psi/mysql_rwlock.h"
#include "mysql/components/services/psi_rwlock_bits.h"

/* XENGINE header files */
#include "xengine/cache.h"
#include "xengine/perf_context.h"
#include "xengine/statistics.h"
#include "xengine/utilities/options_util.h"
#include "xengine/utilities/transaction_db.h"
#include "xengine/utilities/write_batch_with_index.h"
#include "storage/storage_manager.h"
#include "xengine/iterator.h"
#include "xengine/parallel_read.h"

/* XEngine handler header files */
#include "./dd_operations.h"
#include "./xdb_comparator.h"
#include "./xdb_index_merge.h"
#include "./xdb_sst_info.h"
#include "./xdb_utils.h"
#include "my_io_perf.h"
#include <regex>

/**
  @note XEngine Coding Conventions:
  XEngine code follows the baseline MySQL coding conventions, available at
  http://dev.mysql.com/doc/internals/en/coding-guidelines.html, with several
  refinements (@see /storage/xengine/README file).
*/

namespace myx {

// keys on creating
struct Added_key_info {
  Added_key_info(TABLE *table)
    : altered_table(table), step(TABLE_CREATE), status(HA_EXIT_SUCCESS), dup_key_saved(false) {}
  Added_key_info(const Added_key_info &r)
    : altered_table(r.altered_table), step(r.step), status(r.status.load()) {}
  TABLE *altered_table;

  enum ADD_KEY_STEP{
    /** table create point */
    TABLE_CREATE,

    /** start build index point */
    BUILDING_BASE_INDEX,

    /** build index base finished, and start to do unique check */
    CHECK_UNIQUE_CONSTRAINT,

    /** build index finished */
    FINISHED
  } step;

  std::atomic<uint> status;

  // Mark the dup_key dup_val has been saved.
  std::atomic<bool> dup_key_saved;
};

/** Flags indicating if current operation can be done instantly */
enum class Instant_Type : uint16_t {
  /** Impossible to alter instantly */
  INSTANT_IMPOSSIBLE,

  /** Can be instant without any change */
  INSTANT_NO_CHANGE,

  /** ADD COLUMN which can be done instantly, including
  adding stored column only (or along with adding virtual columns) */
  INSTANT_ADD_COLUMN
};

extern bool xengine_enable_print_ddl_log;
typedef struct System_status_var SSV; 
/*
 * class for exporting transaction information for
 * information_schema.xengine_trx
 */
struct Xdb_trx_info {
  std::string name;
  ulonglong trx_id;
  ulonglong write_count;
  ulonglong lock_count;
  int timeout_sec;
  std::string state;
  std::string waiting_key;
  ulonglong waiting_cf_id;
  int is_replication;
  int skip_trx_api;
  int read_only;
  int deadlock_detect;
  int num_ongoing_bulk_load;
  ulong thread_id;
  std::string query_str;
};

std::vector<Xdb_trx_info> xdb_get_all_trx_info();

/*
  const for pack/unpack record
*/
const size_t XENGINE_RECORD_HEADER_LENGTH = 1;
const size_t XENGINE_RECORD_FIELD_NUMBER_LENGTH = 2;
const size_t XENGINE_RECORD_NULLABLE_BYTES = 2;
const char INSTANT_DDL_FLAG = 0x80;

/*
  This is
  - the name of the default Column Family (the CF which stores indexes which
    didn't explicitly specify which CF they are in)
  - the name used to set the default column family parameter for per-cf
    arguments.
*/
const char *const DEFAULT_CF_NAME = "default";

/*
  This is the name of the Column Family used for storing the data dictionary.
*/
const char *const DEFAULT_SYSTEM_SUBTABLE_NAME = "__system__";

/*
  Now, we only use cf_id communicate with subtable in xengine, for system_cf, cf_id is 1,
  for other user subtable, cf_id is same with index_id, which is start from 256.
*/
const int DEFAULT_SYSTEM_SUBTABLE_ID = 1;

/*
  This is the default system table space id, use for system subtable or other subtable which table space is shared
*/
const int DEFAULT_SYSTEM_TABLE_SPACE_ID = 0;

/*
  This is the name of the hidden primary key for tables with no pk.
*/
const char *const HIDDEN_PK_NAME = "HIDDEN_PK_ID";

/*
  Column family name which means "put this index into its own column family".
  See Xdb_cf_manager::get_per_index_cf_name().
*/
const char *const PER_INDEX_CF_NAME = "$per_index_cf";

/*
  Name for the background thread.
*/
const char *const BG_THREAD_NAME = "xengine-bg";

/*
  Name for the drop index thread.
*/
const char *const INDEX_THREAD_NAME = "xengine-index";

/*
  Default, minimal valid, and maximum valid sampling rate values when collecting
  statistics about table.
*/
#define XDB_DEFAULT_TBL_STATS_SAMPLE_PCT 10
#define XDB_TBL_STATS_SAMPLE_PCT_MIN 1
#define XDB_TBL_STATS_SAMPLE_PCT_MAX 100

/*
  Default and maximum values for xengine-compaction-sequential-deletes and
  xengine-compaction-sequential-deletes-window to add basic boundary checking.
*/
#define DEFAULT_COMPACTION_SEQUENTIAL_DELETES 0
#define MAX_COMPACTION_SEQUENTIAL_DELETES 2000000

#define DEFAULT_COMPACTION_SEQUENTIAL_DELETES_WINDOW 0
#define MAX_COMPACTION_SEQUENTIAL_DELETES_WINDOW 2000000

/*
  Default and maximum values for various compaction and flushing related
  options. Numbers are based on the hardware we currently use and our internal
  benchmarks which indicate that parallelization helps with the speed of
  compactions.

  Ideally of course we'll use heuristic technique to determine the number of
  CPU-s and derive the values from there. This however has its own set of
  problems and we'll choose simplicity for now.
*/
#define MAX_BACKGROUND_COMPACTIONS 64
#define MAX_BACKGROUND_FLUSHES 64
#define MAX_BACKGROUND_DUMPS 64

#define DEFAULT_SUBCOMPACTIONS 1
#define MAX_SUBCOMPACTIONS 64

/*
  Defines the field sizes for serializing XID object to a string representation.
  string byte format: [field_size: field_value, ...]
  [
    8: XID.formatID,
    1: XID.gtrid_length,
    1: XID.bqual_length,
    XID.gtrid_length + XID.bqual_length: XID.data
  ]
*/
#define XDB_FORMATID_SZ 8
#define XDB_GTRID_SZ 1
#define XDB_BQUAL_SZ 1
#define XDB_XIDHDR_LEN (XDB_FORMATID_SZ + XDB_GTRID_SZ + XDB_BQUAL_SZ)

/*
  To fix an unhandled exception we specify the upper bound as LONGLONGMAX
  instead of ULONGLONGMAX because the latter is -1 and causes an exception when
  cast to jlong (signed) of JNI

  The reason behind the cast issue is the lack of unsigned int support in Java.
*/
#define MAX_RATE_LIMITER_BYTES_PER_SEC static_cast<uint64_t>(LLONG_MAX)

/*
  Hidden PK column (for tables with no primary key) is a longlong (aka 8 bytes).
  static_assert() in code will validate this assumption.
*/
#define XENGINE_SIZEOF_HIDDEN_PK_COLUMN sizeof(longlong)

//combine buffer size is sort_buffer_size*16
#define XENGINE_MERGE_COMBINE_READ_SIZE_RATIO (16)

/*
  XEngine specific error codes. NB! Please make sure that you will update
  HA_ERR_XENGINE_LAST when adding new ones.
*/
#define HA_ERR_XENGINE_UNIQUE_NOT_SUPPORTED (HA_ERR_LAST + 1)
#define HA_ERR_XENGINE_PK_REQUIRED (HA_ERR_LAST + 2)
#define HA_ERR_XENGINE_TOO_MANY_LOCKS (HA_ERR_LAST + 3)
#define HA_ERR_XENGINE_TABLE_DATA_DIRECTORY_NOT_SUPPORTED (HA_ERR_LAST + 4)
#define HA_ERR_XENGINE_TABLE_INDEX_DIRECTORY_NOT_SUPPORTED (HA_ERR_LAST + 5)
#define HA_ERR_XENGINE_OUT_OF_SORTMEMORY (HA_ERR_LAST + 6)
#define HA_ERR_XENGINE_LAST HA_ERR_XENGINE_OUT_OF_SORTMEMORY


#if MAX_INDEXES <= 64
typedef Bitmap<64>  key_map;          /* Used for finding keys */
#elif MAX_INDEXES > 255
#error "MAX_INDEXES values greater than 255 is not supported."
#else
typedef Bitmap<((MAX_INDEXES+7)/8*8)> key_map; /* Used for finding keys */
#endif

//key_map key_map_full[0]; 

inline bool looks_like_per_index_cf_typo(const char *const name) {
  return (name && name[0] == '$' && strcmp(name, PER_INDEX_CF_NAME));
}

/**
  @brief
  Xdb_table_handler is a reference-counted structure storing information for
  each open table. All the objects are stored in a global hash map.

  //TODO: join this with Xdb_tbl_def ?
*/
struct Xdb_table_handler {
  char *m_table_name;
  uint m_table_name_length;
  int m_ref_count;

  my_core::THR_LOCK m_thr_lock; ///< MySQL latch needed by m_db_lock
};

class Xdb_key_def;
class Xdb_tbl_def;
class Xdb_transaction;
class Xdb_transaction_impl;
class Xdb_writebatch_impl;
class Xdb_field_encoder;
struct Minitables;
struct Xdb_inplace_ddl_dict_info;
struct ParallelDDLScanCtx;

const char *const xengine_hton_name = "XENGINE";

typedef struct _gl_index_id_s {
  uint32_t cf_id;
  uint32_t index_id;
  bool operator==(const struct _gl_index_id_s &other) const {
    return cf_id == other.cf_id && index_id == other.index_id;
  }
  bool operator!=(const struct _gl_index_id_s &other) const {
    return cf_id != other.cf_id || index_id != other.index_id;
  }
  bool operator<(const struct _gl_index_id_s &other) const {
    return cf_id < other.cf_id ||
           (cf_id == other.cf_id && index_id < other.index_id);
  }
  bool operator<=(const struct _gl_index_id_s &other) const {
    return cf_id < other.cf_id ||
           (cf_id == other.cf_id && index_id <= other.index_id);
  }
  bool operator>(const struct _gl_index_id_s &other) const {
    return cf_id > other.cf_id ||
           (cf_id == other.cf_id && index_id > other.index_id);
  }
  bool operator>=(const struct _gl_index_id_s &other) const {
    return cf_id > other.cf_id ||
           (cf_id == other.cf_id && index_id >= other.index_id);
  }
} GL_INDEX_ID;

enum operation_type {
  ROWS_DELETED = 0,
  ROWS_INSERTED,
  ROWS_READ,
  ROWS_UPDATED,
#if 0 // DEL-SYSSTAT
  ROWS_DELETED_BLIND,
#endif
  ROWS_MAX
};

#if defined(HAVE_SCHED_GETCPU)
#define XDB_INDEXER get_sched_indexer_t
#else
#define XDB_INDEXER thread_id_indexer_t
#endif

/* Global statistics struct used inside XEngine */
struct st_global_stats {
  ib_counter_t<ulonglong, 64, XDB_INDEXER> rows[ROWS_MAX];

  // system_rows_ stats are only for system
  // tables. They are not counted in rows_* stats.
  ib_counter_t<ulonglong, 64, XDB_INDEXER> system_rows[ROWS_MAX];
};

/* Struct used for exporting status to MySQL */
struct st_export_stats {
  ulonglong rows_deleted;
  ulonglong rows_inserted;
  ulonglong rows_read;
  ulonglong rows_updated;
#if 0 // DEL-SYSSTAT
  ulonglong rows_deleted_blind;
#endif

  ulonglong system_rows_deleted;
  ulonglong system_rows_inserted;
  ulonglong system_rows_read;
  ulonglong system_rows_updated;
};

} // namespace myx

#include "./xdb_buff.h"

/* Provide hash function for GL_INDEX_ID so we can include it in sets */
namespace std {
template <> struct hash<myx::GL_INDEX_ID> {
  std::size_t operator()(const myx::GL_INDEX_ID &gl_index_id) const {
    const uint64_t val =
        ((uint64_t)gl_index_id.cf_id << 32 | (uint64_t)gl_index_id.index_id);
    return std::hash<uint64_t>()(val);
  }
};
} // namespace std

namespace myx {

struct InstantDDLInfo {
  InstantDDLInfo() {
    clearup();
  }
  void clearup() {
    m_instant_cols = std::numeric_limits<uint32_t>::max();
    m_null_bytes = std::numeric_limits<uint32_t>::max();
    instantly_added_default_value_null.clear();
    instantly_added_default_values.clear();
  }
  bool have_instantly_added_columns() {
    return !(m_instant_cols == std::numeric_limits<uint32_t>::max() || 
             m_instant_cols == 0);
  }
  uint32_t m_instant_cols = std::numeric_limits<uint32_t>::max();
  uint32_t m_null_bytes = std::numeric_limits<uint32_t>::max();
  std::vector<bool> instantly_added_default_value_null;
  std::vector<std::string> instantly_added_default_values;
};

/* Describes instructions on how to decode the field */
class READ_FIELD
{
public:
  /* Points to Xdb_field_encoder describing the field */
  Xdb_field_encoder *m_field_enc;
  /* if true, decode the field, otherwise skip it */
  bool m_decode;
  /* Skip this many bytes before reading (or skipping) this field */
  int m_skip;
};

// simplify code with alias
using IteratorUptr = std::unique_ptr<xengine::db::Iterator, xengine::memory::ptr_destruct_delete<xengine::db::Iterator>>;

/**
  @brief
  Class definition for XENGINE storage engine plugin handler
*/

class ha_xengine : public my_core::handler {

  friend struct ParallelScanCtx;

  my_core::THR_LOCK_DATA m_db_lock; ///< MySQL database lock

  Xdb_table_handler *m_table_handler; ///< Open table handler

  /* Iterator used for range scans and for full table/index scans */
  xengine::db::Iterator *m_scan_it;

  bool m_cmp_end_key;

  uint m_max_packed_sk_len;
  /* length of packed pk tuple */
  uint m_pack_key_len;
  /* Whether m_scan_it was created with skip_bloom=true */
  bool m_scan_it_skips_bloom;

  const xengine::db::Snapshot *m_scan_it_snapshot;

  std::shared_ptr<Xdb_tbl_def> m_tbl_def;

  /* instant ddl information used for decoding */
  InstantDDLInfo m_instant_ddl_info;

  /* number of fields not stored in a X-Engine value */
  uint32_t m_fields_no_needed_to_decode;

  /* Primary Key encoder from KeyTupleFormat to StorageFormat */
  std::shared_ptr<Xdb_key_def> m_pk_descr;

  /* Array of index descriptors */
  std::shared_ptr<Xdb_key_def> *m_key_descr_arr;

  bool check_keyread_allowed(uint inx, uint part, bool all_parts) const;

  /*
    Number of key parts in PK. This is the same as
      table->key_info[table->s->primary_key].keyparts
  */
  uint m_pk_key_parts;

  /*
    TRUE <=> Primary Key columns can be decoded from the index
  */
  mutable bool m_pk_can_be_decoded;

  /*
   TRUE <=> Some fields in the PK may require unpack_info.
  */
  bool m_maybe_unpack_info;

  uchar *m_pk_tuple;        /* Buffer for storing PK in KeyTupleFormat */
  uchar *m_pk_packed_tuple; /* Buffer for storing PK in StorageFormat */
  // ^^ todo: change it to 'char*'? TODO: ^ can we join this with last_rowkey?

  /* Buffer for storing old-pk for new_tbl in StorageFormat during online-ddl */
  uchar *m_pk_packed_tuple_old;
  /*
    Temporary buffers for storing the key part of the Key/Value pair
    for secondary indexes.
  */
  uchar *m_sk_packed_tuple;

  /*
    Temporary buffers for storing end key part of the Key/Value pair.
    This is used for range scan only.
  */
  uchar *m_end_key_packed_tuple;

  Xdb_string_writer m_sk_tails;
  Xdb_string_writer m_pk_unpack_info;

  /*
    ha_rockdb->index_read_map(.. HA_READ_KEY_EXACT or similar) will save here
    mem-comparable form of the index lookup tuple.
  */
  uchar *m_sk_match_prefix;
  uint m_sk_match_length;

  /* Buffer space for the above */
  uchar *m_sk_match_prefix_buf;

  /* Second buffers, used by UPDATE. */
  uchar *m_sk_packed_tuple_old;
  Xdb_string_writer m_sk_tails_old;

  /* Buffers used for duplicate checking during unique_index_creation */
  uchar *m_dup_sk_packed_tuple;
  uchar *m_dup_sk_packed_tuple_old;

  /*
    Temporary space for packing VARCHARs (we provide it to
    pack_record()/pack_index_tuple() calls).
  */
  uchar *m_pack_buffer;

  /* new table Primary Key encoder from KeyTupleFormat to StorageFormat */
  std::shared_ptr<Xdb_key_def> m_new_pk_descr;

  /* Buffers for new_record during online-copy-ddl */
  uchar *m_new_record;

  /* rowkey of the last record we've read, in StorageFormat. */
  String m_last_rowkey;

  /* Buffer used by convert_record_to_storage_format() */
  String m_storage_record;

  /* Buffer used by convert_new_record_to_storage_format() */
  String m_new_storage_record;
  bool m_new_maybe_unpack_info;

  /*
    Last retrieved record, in table->record[0] data format.

    This is used only when we get the record with xengine's Get() call (The
    other option is when we get a xengine::common::Slice from an iterator)
  */
  std::string m_retrieved_record;

  /* Type of locking to apply to rows */
  enum { XDB_LOCK_NONE, XDB_LOCK_READ, XDB_LOCK_WRITE } m_lock_rows;

  /* TRUE means we're doing an index-only read. FALSE means otherwise. */
  bool m_keyread_only;

  bool m_skip_scan_it_next_call;

  /* TRUE means we are accessing the first row after a snapshot was created */
  bool m_rnd_scan_is_new_snapshot;

  /* TRUE means the replication slave will use Read Free Replication */
  bool m_use_read_free_rpl;

  /*
    TRUE means we should skip unique key checks for this table if the
    replication lag gets too large
   */
  bool m_skip_unique_check;

  /**
    @brief
    This is a bitmap of indexes (i.e. a set) whose keys (in future, values) may
    be changed by this statement. Indexes that are not in the bitmap do not need
    to be updated.
    @note Valid inside UPDATE statements, IIF(m_update_scope_is_valid == true).
  */
  key_map m_update_scope;
  bool m_update_scope_is_valid;

  /* SST information used for bulk loading the primary key */
  std::shared_ptr<Xdb_sst_info> m_sst_info;
  std::shared_ptr<xengine::db::MiniTables> mtables_;
  std::unique_ptr<xengine::storage::ChangeInfo> m_change_info;
  Xdb_transaction *m_bulk_load_tx;
  /* Mutex to protect finalizing bulk load */
  mysql_mutex_t m_bulk_load_mutex;

  /*
    MySQL index number for duplicate key error
  */
  int m_dupp_errkey;

  int create_key_defs(const TABLE *const table_arg,
                      Xdb_tbl_def *tbl_def_arg,
                      const char *old_table_name = nullptr,
                      const TABLE *const old_table_arg = nullptr,
                      const Xdb_tbl_def *const old_tbl_def_arg = nullptr,
                      const bool need_rebuild = false) const
      MY_ATTRIBUTE(( __warn_unused_result__));
  int secondary_index_read(const int keyno, uchar *const buf)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  void setup_iterator_for_rnd_scan();
  void setup_scan_iterator(const Xdb_key_def &kd, xengine::common::Slice *const slice)
      MY_ATTRIBUTE((__nonnull__)) {
    setup_scan_iterator(kd, slice, false, false, 0);
  }
  bool is_ascending(const Xdb_key_def &keydef,
                    enum ha_rkey_function find_flag) const
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  void setup_scan_iterator(const Xdb_key_def &kd, xengine::common::Slice *slice,
                           const bool use_all_keys, const bool is_ascending,
                           const uint eq_cond_len,
                           const xengine::common::Slice* end_key = nullptr)
                           MY_ATTRIBUTE((__nonnull__(1)));
  void release_scan_iterator(void);

  xengine::common::Status
  get_for_update(Xdb_transaction *const tx,
                 xengine::db::ColumnFamilyHandle *const column_family,
                 const xengine::common::Slice &key, std::string *const value, bool lock_unique = false) const;

  xengine::common::Status lock_unique_key(Xdb_transaction *const tx,
                                          xengine::db::ColumnFamilyHandle *const column_family,
                                          const xengine::common::Slice &key,
                                          const bool skip_bloom_filter,
                                          const bool fill_cache) const;

  int get_row_by_rowid(uchar *const buf, const char *const rowid,
                       const uint rowid_size, std::string& retrieved_record,
                       TABLE* tbl, String& key, const bool skip_lookup = false)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  int get_row_by_rowid(uchar *const buf, const uchar *const rowid,
                       const uint rowid_size, const bool skip_lookup = false)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__)) {
    return get_row_by_rowid(buf, reinterpret_cast<const char *>(rowid),
                            rowid_size, skip_lookup);
  }
  int get_row_by_rowid(uchar *const buf, const char *const rowid,
                       const uint rowid_size, const bool skip_lookup = false)
  MY_ATTRIBUTE((__nonnull__, __warn_unused_result__)) {
    return get_row_by_rowid(buf, rowid, rowid_size, m_retrieved_record, table,
                            m_last_rowkey, skip_lookup);
  }

  int get_latest_row_by_rowid(uchar *const buf, const char *const rowid,
                              const uint rowid_size)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  void update_auto_incr_val();
  void load_auto_incr_value();
  longlong update_hidden_pk_val();
  int load_hidden_pk_value() MY_ATTRIBUTE((__warn_unused_result__));
//  int read_hidden_pk_id_from_rowkey(longlong *const hidden_pk_id, String* str_key = nullptr)
//      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  int read_hidden_pk_id_from_rowkey(longlong *const hidden_pk_id,
                                    const String *key)
  MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  bool can_use_single_delete(const uint &index) const
      MY_ATTRIBUTE((__warn_unused_result__));
  bool is_blind_delete_enabled();
  bool skip_unique_check() const MY_ATTRIBUTE((__warn_unused_result__));
  //void set_force_skip_unique_check(bool skip);
  bool commit_in_the_middle() MY_ATTRIBUTE((__warn_unused_result__));
  bool do_bulk_commit(Xdb_transaction *const tx)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  bool has_hidden_pk(const TABLE *const table) const
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  void update_row_stats(const operation_type &type);

  void set_last_rowkey(const uchar *const old_data);

  /*
    Array of table->s->fields elements telling how to store fields in the
    record.
  */
  Xdb_field_encoder *m_encoder_arr;

  /*
    This tells which table fields should be decoded (or skipped) when
    decoding table row from (pk, encoded_row) pair. (Secondary keys are
    just always decoded in full currently)
  */
  std::vector<READ_FIELD> m_decoders_vect;

  /* Setup field_decoders based on type of scan and table->read_set */
  int setup_read_decoders();
  int setup_read_decoders(const TABLE *table, Xdb_field_encoder *encoder_arr, std::vector<READ_FIELD> &decoders_vect, bool force_decode_all_fields = false);

  /*
    Number of bytes in on-disk (storage) record format that are used for
    storing SQL NULL flags.
  */
  uint m_null_bytes_in_rec;

  void get_storage_type(Xdb_field_encoder *const encoder, std::shared_ptr<Xdb_key_def> pk_descr, const uint &kp, bool &maybe_unpack_info);
  int setup_field_converters();
  int setup_field_converters(const TABLE *table, std::shared_ptr<Xdb_key_def> pk_descr, Xdb_field_encoder* &encoder_arr, uint &fields_no_needed_to_decode, uint &null_bytes_in_rec, bool &maybe_unpack_info);
  int alloc_key_buffers(const TABLE *const table_arg,
                        const Xdb_tbl_def *const tbl_def_arg,
                        bool alloc_alter_buffers = false)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  void free_key_buffers();

  // the buffer size should be at least 2*Xdb_key_def::INDEX_NUMBER_SIZE
  xengine::db::Range get_range(const int &i, uchar buf[]) const;

  /*
    A counter of how many row checksums were checked for this table. Note that
    this does not include checksums for secondary index entries.
  */
  my_core::ha_rows m_row_checksums_checked;

  /*
    Update stats
  */
  void update_stats(void);

  void get_instant_ddl_info_if_needed(const dd::Table *table_def);

public:
  /*
    Controls whether writes include checksums. This is updated from the session
    variable
    at the start of each query.
  */
  bool m_store_row_debug_checksums;

  /* Same as above but for verifying checksums when reading */
  bool m_verify_row_debug_checksums;
  int m_checksums_pct;

  ha_xengine(my_core::handlerton *const hton,
             my_core::TABLE_SHARE *const table_arg);
  ~ha_xengine() {
    int err MY_ATTRIBUTE((__unused__));
    err = finalize_bulk_load();
    DBUG_ASSERT(err == 0);
    mysql_mutex_destroy(&m_bulk_load_mutex);
  }

  //enum row_type get_row_type() const  {
  //  return ROW_TYPE_DYNAMIC;
  //}

  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(xengine_hton_name);
  }

  ///* The following is only used by SHOW KEYS: */
  //const char *index_type(uint inx)  {
  //  DBUG_ENTER_FUNC();

  //  DBUG_RETURN("LSMTREE");
  //}

  /** @brief
    The file extensions.
   */
  //const char **bas_ext() const ;

  /*
    See if this is the same base table - this should only be true for different
    partitions of the same table.
  */
  bool same_table(const ha_xengine &other) const;

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  handler::Table_flags table_flags() const override {
    DBUG_ENTER_FUNC();

    /*
      HA_BINLOG_STMT_CAPABLE
        We are saying that this engine is just statement capable to have
        an engine that can only handle statement-based logging. This is
        used in testing.
      HA_REC_NOT_IN_SEQ
        If we don't set it, filesort crashes, because it assumes rowids are
        1..8 byte numbers
    */
    DBUG_RETURN(HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |
                HA_CAN_INDEX_BLOBS |
                (m_pk_can_be_decoded ? HA_PRIMARY_KEY_IN_READ_INDEX : 0) |
                HA_PRIMARY_KEY_REQUIRED_FOR_POSITION | HA_NULL_IN_KEY |
                HA_PARTIAL_COLUMN_READ | HA_ATTACHABLE_TRX_COMPATIBLE);
  }

  bool init_with_fields() ;

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

    @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint inx, uint part, bool all_parts) const override;
 
  bool rpl_can_handle_stm_event() const noexcept; 

  //const key_map *keys_to_use_for_scanning()  {
  //  DBUG_ENTER_FUNC();

  //  DBUG_RETURN(&key_map_full);
  //}

  bool primary_key_is_clustered() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(true);
  }

  bool should_store_row_debug_checksums() const {
    return m_store_row_debug_checksums && (rand() % 100 < m_checksums_pct);
  }

  int rename_table(const char *const from, const char *const to,
                   const dd::Table *from_table_def,
                   dd::Table *to_table_def) override
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int convert_blob_from_storage_format(my_core::Field_blob *const blob,
                                       Xdb_string_reader *const   reader,
                                       bool                       decode)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int convert_varchar_from_storage_format(
                                my_core::Field_varstring *const field_var,
                                Xdb_string_reader *const        reader,
                                bool                            decode)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int convert_field_from_storage_format(my_core::Field *const    field,
                                        Xdb_string_reader *const reader,
                                        bool                     decode,
                                        uint                     len)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int convert_record_from_storage_format(const xengine::common::Slice *key,
                                         const xengine::common::Slice *value,
                                         uchar *const buf, TABLE* t,
                                         uint32_t null_bytes_in_rec,
                                         bool maybe_unpack_info,
                                         const Xdb_key_def *pk_descr,
                                         std::vector<READ_FIELD>& decoders_vect,
                                         InstantDDLInfo* instant_ddl_info,
                                         bool verify_row_debug_checksums,
                                         my_core::ha_rows& row_checksums_checked)
      MY_ATTRIBUTE((/*__nonnull__, */__warn_unused_result__));

  int convert_record_from_storage_format(const xengine::common::Slice *key,
                                         std::string& retrieved_record,
                                         uchar *const buf, TABLE* tbl)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int convert_record_from_storage_format(const xengine::common::Slice *key,
                                         const xengine::common::Slice *value,
                                         uchar *const buf, TABLE* tbl)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__)) {
    return convert_record_from_storage_format(
        key, value, buf, tbl, m_null_bytes_in_rec,
        m_maybe_unpack_info, m_pk_descr.get(), m_decoders_vect, &m_instant_ddl_info,
        m_verify_row_debug_checksums, m_row_checksums_checked);
  }

  int append_blob_to_storage_format(String &storage_record,
                                    my_core::Field_blob *const blob);

  int append_varchar_to_storage_format(String &storage_record,
                                       my_core::Field_varstring *const field_var);

  int convert_record_to_storage_format(const xengine::common::Slice &pk_packed_slice,
                                        Xdb_string_writer *const pk_unpack_info,
                                        xengine::common::Slice *const packed_rec)
      MY_ATTRIBUTE((__nonnull__));

  int convert_new_record_from_old_record(
      const TABLE *table, const TABLE *altered_table,
      const std::shared_ptr<Xdb_inplace_ddl_dict_info>& dict_info,
      const xengine::common::Slice &pk_packed_slice,
      Xdb_string_writer *const pk_unpack_info,
      xengine::common::Slice *const packed_rec,
      String& new_storage_record);

  static const char *get_key_name(const uint index,
                                  const TABLE *const table_arg,
                                  const Xdb_tbl_def *const tbl_def_arg)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  static const char *get_key_comment(const uint index,
                                     const TABLE *const table_arg,
                                     const Xdb_tbl_def *const tbl_def_arg)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  static bool is_hidden_pk(const uint index, const TABLE *const table_arg,
                           const Xdb_tbl_def *const tbl_def_arg)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  static uint pk_index(const TABLE *const table_arg,
                       const Xdb_tbl_def *const tbl_def_arg)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  static bool is_pk(const uint index, const TABLE *table_arg,
                    const Xdb_tbl_def *tbl_def_arg)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_record_length() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(HA_MAX_REC_LENGTH);
  }

  uint max_supported_keys() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(MAX_INDEXES);
  }

  uint max_supported_key_parts() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(MAX_REF_PARTS);
  }

  uint max_supported_key_part_length(
    HA_CREATE_INFO *create_info MY_ATTRIBUTE((unused))) const override{
    DBUG_ENTER_FUNC();

    // Use 3072 from ha_innobase::max_supported_key_part_length
    DBUG_RETURN(MAX_KEY_LENGTH);
  }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(16 * 1024); /* just to return something*/
  }

  /**
    TODO: return actual upper bound of number of records in the table.
    (e.g. save number of records seen on full table scan and/or use file size
    as upper bound)
  */
  ha_rows estimate_rows_upper_bound() override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(HA_POS_ERROR);
  }

  /* At the moment, we're ok with default handler::index_init() implementation.
   */
  int index_read_map(uchar *const buf, const uchar *const key,
                     key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int index_read_map_impl(uchar *const buf, const uchar *const key,
                          key_part_map keypart_map,
                          enum ha_rkey_function find_flag,
                          const key_range *end_key)
      MY_ATTRIBUTE((__warn_unused_result__));

  int index_read_last_map(uchar *const buf, const uchar *const key,
                          key_part_map keypart_map) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int read_range_first(const key_range *const start_key,
                       const key_range *const end_key, bool eq_range,
                       bool sorted) override
      MY_ATTRIBUTE((__warn_unused_result__));
  virtual int read_range_next() override;

  virtual double scan_time() override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(
        static_cast<double>((stats.records + stats.deleted) / 20.0 + 10));
  }

  virtual double read_time(uint, uint, ha_rows rows) override;

  handler *clone(const char *name, MEM_ROOT *mem_root) override;

  int open(const char *const name, int mode, uint test_if_locked,
           const dd::Table *table_def) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int close(void) override MY_ATTRIBUTE((__warn_unused_result__));

  int write_row(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int update_row(const uchar *const old_data, uchar *const new_data) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int delete_row(const uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));

  xengine::common::Status delete_or_singledelete(
      uint index, Xdb_transaction *const tx,
      xengine::db::ColumnFamilyHandle *const cf,
      const xengine::common::Slice &key) MY_ATTRIBUTE((__warn_unused_result__));

  xengine::common::Status delete_or_singledelete_new_table(
      const TABLE *altered_table, Xdb_tbl_def *new_tbl_def,
      uint index, Xdb_transaction *const tx,
      xengine::db::ColumnFamilyHandle *const cf,
      const xengine::common::Slice &key) MY_ATTRIBUTE((__warn_unused_result__));

  int index_next(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int index_next_with_direction(uchar *const buf, bool move_forward)
      MY_ATTRIBUTE((__warn_unused_result__));
  int index_prev(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int index_first(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int index_last(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));

  class Item *idx_cond_push(uint keyno, class Item *const idx_cond) override;
  /*
    Default implementation from cancel_pushed_idx_cond() suits us
  */
private:
  struct key_def_cf_info {
    xengine::db::ColumnFamilyHandle *cf_handle;
    bool is_reverse_cf;
    bool is_auto_cf;
  };

  // used for both update and delete operations
  struct update_row_info {
    Xdb_transaction *tx;
    const uchar *new_data;
    const uchar *old_data;
    xengine::common::Slice new_pk_slice;
    xengine::common::Slice old_pk_slice;

    // "unpack_info" data for the new PK value
    Xdb_string_writer *new_pk_unpack_info;

    longlong hidden_pk_id;
    bool skip_unique_check;
  };

  //  Used to check for duplicate entries during fast unique index creation.
  struct unique_key_buf_info {
    bool key_buf_switch = false;
    xengine::common::Slice memcmp_key;
    xengine::common::Slice memcmp_key_old;
    uchar *dup_key_buf;
    uchar *dup_key_buf_old;

    unique_key_buf_info(uchar *key_buf, uchar *key_buf_old)
        : dup_key_buf(key_buf), dup_key_buf_old(key_buf_old) {}
    /*
      This method is meant to be called back to back during inplace creation
      of unique indexes.  It will switch between two buffers, which
      will each store the memcmp form of secondary keys, which are then
      converted to slices in memcmp_key or memcmp_key_old.

      Switching buffers on each iteration allows us to retain the
      memcmp_key_old value for duplicate comparison.
    */
    inline uchar *swap_and_get_key_buf() {
      key_buf_switch = !key_buf_switch;
      return key_buf_switch ? dup_key_buf : dup_key_buf_old;
    }
  };

  // used during prepare_inplace_alter_table to determine key stat in new table
  struct prepare_inplace_key_stat {
    std::string key_name;
    /* for key in new table, stat can be ADDED, COPIED, RENAMED or REDEFINED
     * for key in old table, stat can be COPIED, RENAMED or DROPPED
     */
    enum {
      ADDED=0, COPIED, RENAMED, REDEFINED, DROPPED
    } key_stat;

    // if new key is copied or renamed from key in old table
    std::string mapped_key_name; // name of old key
    uint mapped_key_index; // index of old key in Xdb_tbl_def::m_key_descr_arr

    // for debug message
    std::string to_string() const {
      if (!key_name.empty()) {
        std::ostringstream oss;
        switch (key_stat) {
          case ADDED:
            oss << key_name << " is newly ADDED.";
            break;
          case COPIED:
            oss << key_name << " is COPIED from " << mapped_key_name << " in old table.";
            break;
          case RENAMED:
            oss << key_name << " is RENAMED from " << mapped_key_name << " in old table.";
            break;
          case REDEFINED:
            oss << key_name << " is REDEFINED.";
            break;
          case DROPPED:
            oss << key_name << " is DROPPED.";
            break;
        }
        return oss.str();
      }
      return "";
    }
  };
  std::map<std::string, prepare_inplace_key_stat> new_key_stats;

  int  create_cfs(const TABLE *const table_arg, Xdb_tbl_def *tbl_def_arg,
                 std::array<struct key_def_cf_info, MAX_INDEXES + 1> *const cfs,
                 std::array<uint32_t, MAX_INDEXES + 1> *const index_ids,
                 const char *old_table_name = nullptr,
                 const TABLE *const old_table_arg = nullptr,
                 const Xdb_tbl_def *const old_tbl_def_arg = nullptr,
                 const bool need_rebuild = false)
      const MY_ATTRIBUTE(( __warn_unused_result__));

  int create_key_def(const TABLE *const table_arg, const uint &i,
                     const Xdb_tbl_def *const tbl_def_arg,
                     std::shared_ptr<Xdb_key_def> *const new_key_def,
                     const struct key_def_cf_info &cf_info,
                     const uint32_t index_id) const
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int create_inplace_key_defs(
      const TABLE *const table_arg, Xdb_tbl_def *vtbl_def_arg,
      const TABLE *const old_table_arg,
      const Xdb_tbl_def *const old_tbl_def_arg,
      const std::array<key_def_cf_info, MAX_INDEXES + 1> &cfs,
      const std::array<uint32_t, MAX_INDEXES + 1> &index_ids) const
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  std::unordered_map<std::string, uint>
  get_old_key_positions(const TABLE *table_arg, const Xdb_tbl_def *tbl_def_arg,
                        const TABLE *old_table_arg,
                        const Xdb_tbl_def *old_tbl_def_arg) const
      MY_ATTRIBUTE((__nonnull__));

  int compare_key_parts(const KEY *const old_key,
                        const KEY *const new_key) const;
  MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int index_first_intern(uchar *buf)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  int index_last_intern(uchar *buf)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  enum icp_result check_index_cond() const;
  int find_icp_matching_index_rec(const bool &move_forward, uchar *const buf)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int calc_updated_indexes();
  int update_write_row(const uchar *const old_data, const uchar *const new_data,
                       const bool skip_unique_check)
      MY_ATTRIBUTE((__warn_unused_result__));
  int get_pk_for_update(struct update_row_info *const row_info);

  int get_new_pk_for_update(struct update_row_info *const row_info, const TABLE *altered_table);

  int get_new_pk_for_delete(struct update_row_info *const row_info,
                                      const TABLE *altered_table);

  int delete_indexes(const uchar *const old_record);

  int delete_row_new_table(struct update_row_info &row_info);

  int check_and_lock_unique_pk(const uint &key_id,
                               const struct update_row_info &row_info,
                               bool *const found, bool *const pk_changed)
      MY_ATTRIBUTE((__warn_unused_result__));

  void check_new_index_unique_inner(const Xdb_key_def &kd, Added_key_info &ki,
                                    const struct update_row_info &row_info,
                                    const xengine::common::Slice &key,
                                    bool is_rebuild,
                                    bool *const found) const;
  int lock_and_check_new_index_pk(const Xdb_key_def &kd, Added_key_info &ki,
                                  const struct update_row_info &row_info,
                                  bool *const pk_chnaged,
                                  bool *const found) const
      MY_ATTRIBUTE((__warn_unused_result__));
  int lock_and_check_new_index_sk(const Xdb_key_def &kd, Added_key_info &ki,
                                  const struct update_row_info &row_info,
                                  bool *const found,
                                  bool is_rebuild = false) const
      MY_ATTRIBUTE((__warn_unused_result__));

  int check_and_lock_sk(const uint &key_id,
                        const struct update_row_info &row_info,
                        bool *const found) const
      MY_ATTRIBUTE((__warn_unused_result__));
  int check_uniqueness_and_lock(const struct update_row_info &row_info,
                                bool *const pk_changed)
      MY_ATTRIBUTE((__warn_unused_result__));
  int check_uniqueness_and_lock_rebuild(const struct update_row_info &row_info,
                                        bool *const pk_changed)
      MY_ATTRIBUTE((__warn_unused_result__));
  bool over_bulk_load_threshold(int *err)
      MY_ATTRIBUTE((__warn_unused_result__));
  bool check_duplicate_in_base(const TABLE *table_arg, const Xdb_key_def &index,
                               const xengine::common::Slice& key,
                               unique_key_buf_info *key_buf_info)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int fill_new_duplicate_record(const Xdb_key_def *index,
                                TABLE *new_table_arg,
                                const xengine::common::Slice &dup_key,
                                const xengine::common::Slice &dup_val,
                                bool is_rebuild)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int fill_old_table_record(TABLE *new_table, TABLE *old_table,
                            const xengine::common::Slice &dup_key,
                            const Xdb_key_def *dup_kd)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int bulk_load_key(Xdb_transaction *const tx, const Xdb_key_def &kd,
                    const xengine::common::Slice &key,
                    const xengine::common::Slice &value, int level)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int update_pk(const Xdb_key_def &kd, const struct update_row_info &row_info,
                const bool &pk_changed) MY_ATTRIBUTE((__warn_unused_result__));

  int update_sk(const TABLE *const table_arg, const Xdb_key_def &kd,
                const struct update_row_info &row_info,
                const TABLE *const altered_table = nullptr)
      MY_ATTRIBUTE((__warn_unused_result__));
  int update_indexes(const struct update_row_info &row_info,
                     const bool &pk_changed)
      MY_ATTRIBUTE((__warn_unused_result__));

  int update_pk_for_new_table(
      const Xdb_key_def &kd, Added_key_info &ki,
      const struct update_row_info &row_info,
      const bool pk_changed, const TABLE *altered_table)
      MY_ATTRIBUTE((__warn_unused_result__));

  int update_sk_for_new_table(const Xdb_key_def &kd,
                              Added_key_info &ki, 
                              const struct update_row_info &row_info,
                              const TABLE *const altered_table)
      MY_ATTRIBUTE((__warn_unused_result__));

  int update_new_table(struct update_row_info &row_info, bool pk_changed)
      MY_ATTRIBUTE((__warn_unused_result__));

  int read_key_exact(const Xdb_key_def &kd, xengine::db::Iterator *const iter,
                     const bool &using_full_key,
                     const xengine::common::Slice &key_slice) const
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  int read_before_key(const Xdb_key_def &kd, const bool &using_full_key,
                      const xengine::common::Slice &key_slice)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  int read_after_key(const Xdb_key_def &kd, const bool &using_full_key,
                     const xengine::common::Slice &key_slice)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int position_to_correct_key(
      const Xdb_key_def &kd, enum ha_rkey_function &find_flag,
      bool &full_key_match, const uchar *key,
      key_part_map &keypart_map, xengine::common::Slice &key_slice,
      bool *move_forward) MY_ATTRIBUTE((__warn_unused_result__));

  int read_row_from_primary_key(uchar *const buf)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  int read_row_from_secondary_key(uchar *const buf, const Xdb_key_def &kd,
                                  bool move_forward)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int calc_eq_cond_len(const Xdb_key_def &kd,
                       enum ha_rkey_function &find_flag,
                       xengine::common::Slice &slice,
                       int &bytes_changed_by_succ,
                       const key_range *end_key,
                       uint *end_key_packed_size)
      MY_ATTRIBUTE((__warn_unused_result__));

  void read_thd_vars(THD *const thd) MY_ATTRIBUTE((__nonnull__));
  const char *thd_xengine_tmpdir()
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  bool contains_foreign_key(THD *const thd)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  // avoid  write sst of creating second index when do shrink
  // extent space crash
  int inplace_populate_indexes(
      TABLE *const table_arg,
      const std::unordered_set<std::shared_ptr<Xdb_key_def>> &indexes,
      const std::shared_ptr<Xdb_key_def>& primary_key,
      bool is_rebuild)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int inplace_populate_index(
      TABLE *const new_table_arg,
      const std::shared_ptr<Xdb_key_def>& index,
      bool is_rebuild)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  Instant_Type check_if_support_instant_ddl(
               const Alter_inplace_info *ha_alter_info);

public:
  int index_init(uint idx, bool sorted) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int index_end() override MY_ATTRIBUTE((__warn_unused_result__));

  void unlock_row() override;

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan) override MY_ATTRIBUTE((__warn_unused_result__));
  int rnd_end() override MY_ATTRIBUTE((__warn_unused_result__));

  int rnd_next(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int rnd_next_with_direction(uchar *const buf, bool move_forward)
      MY_ATTRIBUTE((__warn_unused_result__));

  int rnd_pos(uchar *const buf, uchar *const pos) override
      MY_ATTRIBUTE((__warn_unused_result__));
  void position(const uchar *const record) override;
  int info(uint) override;

  /* This function will always return success, therefore no annotation related
   * to checking the return value. Can't change the signature because it's
   * required by the interface. */
  int extra(enum ha_extra_function operation) override;

  int start_stmt(THD *const thd, thr_lock_type lock_type) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int external_lock(THD *const thd, int lock_type) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int truncate(dd::Table *table_def) override  MY_ATTRIBUTE((__warn_unused_result__));

  int reset() override {
    DBUG_ENTER_FUNC();

    /* Free blob data */
    m_retrieved_record.clear();

    DBUG_RETURN(HA_EXIT_SUCCESS);
  }

  int records(ha_rows *num_rows) override;

  int records_from_index(ha_rows *num_rows, uint index) override;

  /* used for scan xengine by single thread */
  int records_from_index_single(ha_rows *num_rows, uint index);

  int scan_parallel(Xdb_key_def *kd, Xdb_transaction *const tx,
                    xengine::common::ParallelReader::F &&f);

  int check_parallel(THD *const thd, HA_CHECK_OPT *const check_opt)
  MY_ATTRIBUTE((__warn_unused_result__));

  int check(THD *const thd, HA_CHECK_OPT *const check_opt) override
      MY_ATTRIBUTE((__warn_unused_result__));
  void remove_rows(Xdb_tbl_def *const tbl);
  ha_rows records_in_range(uint inx, key_range *const min_key,
                           key_range *const max_key) override
      MY_ATTRIBUTE((__warn_unused_result__));

  /** mark ddl transaction modified xengine */
  void mark_ddl_trx_read_write();

  int delete_table(const char *const from, const dd::Table *table_def) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int create(const char *const name, TABLE *const form,
             HA_CREATE_INFO *const create_info, dd::Table *table_def) override
      MY_ATTRIBUTE((__warn_unused_result__));
  bool check_if_incompatible_data(HA_CREATE_INFO *const info,
                                  uint table_changes) override
      MY_ATTRIBUTE((__warn_unused_result__));

  THR_LOCK_DATA **store_lock(THD *const thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override
      MY_ATTRIBUTE((__warn_unused_result__));

  //bool register_query_cache_table(THD * thd, char * table_key,
  //                                   size_t key_length,
  //                                   qc_engine_callback * engine_callback,
  //                                   ulonglong * engine_data)  {
  //  DBUG_ENTER_FUNC();

  //  /* Currently, we don't support query cache */
  //  DBUG_RETURN(FALSE);
  //}

  bool get_error_message(const int error, String *const buf) override
      MY_ATTRIBUTE((__nonnull__));

  void get_auto_increment(ulonglong offset, ulonglong increment,
                          ulonglong nb_desired_values,
                          ulonglong *const first_value,
                          ulonglong *const nb_reserved_values) override;
  void update_create_info(HA_CREATE_INFO *const create_info) override;
  int optimize(THD *const thd, HA_CHECK_OPT *const check_opt) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int analyze(THD *const thd, HA_CHECK_OPT *const check_opt) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int calculate_stats(const TABLE *const table_arg, THD *const thd,
                      HA_CHECK_OPT *const check_opt)
      MY_ATTRIBUTE((__warn_unused_result__));

  enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *altered_table,
      my_core::Alter_inplace_info *const ha_alter_info) override;


  int prepare_inplace_alter_table_dict(
    Alter_inplace_info *ha_alter_info, const TABLE *altered_table,
    const TABLE *old_table, Xdb_tbl_def *old_tbl_def,
    const char *table_name);

  int prepare_inplace_alter_table_rebuild(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info,
      Xdb_tbl_def *const new_tdef,
      std::shared_ptr<Xdb_key_def> *const old_key_descr, uint old_n_keys,
      std::shared_ptr<Xdb_key_def> *const new_key_descr, uint new_n_keys);

  int prepare_inplace_alter_table_skip_unique_check(
      TABLE *const altered_table, Xdb_tbl_def *const new_tbl_def) const;

  int prepare_inplace_alter_table_collect_key_stats(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info);

  int inplace_populate_new_table(TABLE *altered_table, Xdb_tbl_def *new_tdef);

  size_t get_parallel_read_threads();
  void print_dup_err(int res, std::atomic<int>& dup_ctx_id,
                     const std::shared_ptr<Xdb_key_def>& index,
                     bool is_rebuild, TABLE *const new_table_arg,
                     std::vector<xengine::common::Slice>& dup_key,
                     std::vector<xengine::common::Slice>& dup_val,
                     Added_key_info& added_key_info, KEY* key_info);
  void print_common_err(int res);
  int build_index(xengine::common::Slice& iter_key,
                  xengine::common::Slice& iter_value,
                  ParallelDDLScanCtx & ddl_ctx, xengine::common::Slice& key,
                  xengine::common::Slice& val, TABLE* tbl, bool hidden_pk_exists,
                  const std::shared_ptr<Xdb_key_def>& index, bool is_rebuild,
                  TABLE *const new_table_arg);

  int build_base_global_merge(
      std::vector<std::shared_ptr<ParallelDDLScanCtx>>& ddl_ctx_set,
      const std::shared_ptr<Xdb_key_def>& index, bool is_rebuild,
      TABLE *const new_table_arg, size_t max_threads, bool need_unique_check,
      Added_key_info& added_key_info, size_t part_id,
      xengine::common::Slice& dup_key, xengine::common::Slice& dup_val,
      std::atomic<int>& dup_ctx_id);

  int inplace_build_base_phase_parallel(TABLE *const thread_id,
                               const std::shared_ptr<Xdb_key_def>& index,
                               bool is_unique_index,
                               Added_key_info& added_key_info,
                               bool is_rebuild);

  int inplace_build_base_phase(TABLE *const new_table_arg,
                               const std::shared_ptr<Xdb_key_def>& index,
                               bool is_unique_index,
                               Added_key_info& added_key_info,
                               bool is_rebuild);

  using UserKeySequence = std::pair<xengine::common::SequenceNumber, xengine::db::Iterator::RecordStatus>;
  static bool check_user_key_sequence(std::vector<UserKeySequence> &user_key_sequences);

  int inplace_check_unique_phase(TABLE *new_table_arg,
                                 const std::shared_ptr<Xdb_key_def>& index,
                                 Added_key_info& added_key_info,
                                 bool is_rebuild);

  int inplace_update_added_key_info_status_dup_key(
      TABLE *const new_table_arg, const std::shared_ptr<Xdb_key_def>& index,
      Added_key_info& added_key_info, const xengine::common::Slice& dup_key,
      const xengine::common::Slice& dup_val, bool is_rebuild, bool use_key=true);

  int inplace_update_added_key_info_step(
      Added_key_info& added_key_info, Added_key_info::ADD_KEY_STEP new_step) const;

  // During build base phase, when every this number of keys merged,
  // duplicate status of DML will checked.
  const int BUILD_BASE_CHECK_ERROR_FREQUENCY = 100;
  static int inplace_check_dml_error(TABLE *const table_arg, KEY *key,
                                     const Added_key_info& added_key_info,
                                     bool print_err = true);

  int prepare_inplace_alter_table_norebuild(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info,
      Xdb_tbl_def *const new_tdef,
      std::shared_ptr<Xdb_key_def> *const old_key_descr, uint old_n_keys,
      std::shared_ptr<Xdb_key_def> *const new_key_descr, uint new_n_keys);

  bool prepare_inplace_alter_table(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info,
      const dd::Table *old_table_def, dd::Table *new_table_def) override;

  bool inplace_alter_table(TABLE *const altered_table,
                           my_core::Alter_inplace_info *const ha_alter_info,
                           const dd::Table *old_table_def,
                           dd::Table *new_table_def) override;

  void dd_commit_instant_table(const TABLE *old_table,
                               const TABLE *altered_table,
                               const dd::Table *old_dd_tab,
                               dd::Table *new_dd_tab);

  void dd_commit_inplace_no_change(const dd::Table *old_dd_tab,
                                   dd::Table *new_dd_tab);

  void dd_commit_inplace_instant(Alter_inplace_info *ha_alter_info,
                                 const TABLE *old_table,
                                 const TABLE *altered_table,
                                 const dd::Table *old_dd_tab,
                                 dd::Table *new_dd_tab);

  bool commit_inplace_alter_table(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info, bool commit,
      const dd::Table *old_dd_tab, dd::Table *new_dd_tab);

  bool commit_inplace_alter_table_norebuild(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info, bool commit,
      const dd::Table *old_dd_tab, dd::Table *new_dd_tab);

  bool commit_inplace_alter_table_rebuild(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info, bool commit,
      const dd::Table *old_dd_tab, dd::Table *new_dd_tab);

  int commit_inplace_alter_table_common(
      my_core::TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alters_info, bool commit,
      const std::unordered_set<std::shared_ptr<Xdb_key_def>>
          &new_added_indexes);

  int commit_inplace_alter_get_autoinc(Alter_inplace_info *ha_alter_info,
                                       const TABLE *altered_table,
                                       const TABLE *old_table);
  int rollback_added_index(
      const std::unordered_set<std::shared_ptr<Xdb_key_def>>& added_indexes);

  int finalize_bulk_load() MY_ATTRIBUTE((__warn_unused_result__));

  void set_use_read_free_rpl(const char *const whitelist);
  void set_skip_unique_check_tables(const char *const whitelist);

public:
  virtual void rpl_before_delete_rows() ;
  virtual void rpl_after_delete_rows() ;
  virtual void rpl_before_update_rows() ;
  virtual void rpl_after_update_rows() ;
  virtual bool use_read_free_rpl();

private:
  bool is_using_full_key(key_part_map keypart_map, uint actual_key_parts);
  bool is_using_full_unique_key(uint active_index,
                                key_part_map keypart_map,
                                enum ha_rkey_function find_flag);
  bool is_using_prohibited_gap_locks(TABLE *table,
                                     bool using_full_unique_key);

private:
  /* Flags tracking if we are inside different replication operation */
  bool m_in_rpl_delete_rows;
  bool m_in_rpl_update_rows;

  bool m_force_skip_unique_check;
};
struct ParallelScanCtx {
  ParallelScanCtx(ha_xengine* h);
  virtual ~ParallelScanCtx();
  virtual int init();
  virtual void destroy();

  uchar *pk_packed_tuple;
  uchar *sk_packed_tuple;
  uchar *pack_buffer;
  Xdb_string_writer sk_tails;
  /* temporary TABLE for each thread */
  TABLE thd_table;
  ha_xengine* ha;
  const char *const table_name;
  const char *const db_name;
  ha_rows checksums;
  ha_rows rows;
  /* used to check whether unique attribute is ok */
  String last_key;
  String first_key;

  String primary_key;
  String secondary_key;
  std::string record;

  Xdb_string_writer pk_unpack_info;
  String new_storage_record;
};

struct ParallelDDLScanCtx : public ParallelScanCtx {
  ParallelDDLScanCtx(
      ha_xengine* h, const std::shared_ptr<Xdb_index_merge>& merge,
      size_t max_partition_num) : ParallelScanCtx(h), xdb_merge(merge),
                                  bg_merge(merge, max_partition_num) {}
  virtual ~ParallelDDLScanCtx() override {}
  virtual int init() override;

  std::shared_ptr<Xdb_index_merge> xdb_merge;
  Xdb_index_merge::Bg_merge bg_merge;
};

class ParallelDDLMergeCtx {
 public:
  ParallelDDLMergeCtx(
      std::vector<std::shared_ptr<ParallelDDLScanCtx>>& ddl_ctx_set,
      std::function<int(size_t)> merge_func)
      : m_ddl_ctx_set(ddl_ctx_set), m_merge_func(merge_func) {}
  ~ParallelDDLMergeCtx();
  void start(size_t max_threads, bool inject_err);
  int finish();

 private:
  std::vector<std::future<int>> m_merges;
  std::vector<std::shared_ptr<ParallelDDLScanCtx>>& m_ddl_ctx_set;
  std::function<int(size_t)> m_merge_func;
};

class XEngineAsyncCommitCallback : public xengine::common::AsyncCallback {
public:
  XEngineAsyncCommitCallback()
                     : AsyncCallback(false){
    reset();
  }

  ~XEngineAsyncCommitCallback() {}

  xengine::common::Status call_back() override {
    assert(nullptr != this->thd_);
    //not support async commit now in 8.0
    abort();
    //async_commit_direct(this->thd_);
    return xengine::common::Status::OK();
  }

  void reset() {
    this->thd_ = nullptr;
  }

  void reset_thd(THD* thd) {
    assert(nullptr != thd);
    this->thd_ = thd;
  }

  bool destroy_on_complete() override{ return false; }
private:
  THD*                             thd_;
};


/*
  This is a helper class that is passed to XEngine to get notifications when
  a snapshot gets created.
*/

class Xdb_snapshot_notifier : public xengine::util::TransactionNotifier {
  Xdb_transaction *m_owning_tx;

  void SnapshotCreated(const xengine::db::Snapshot *snapshot) override;

public:
  Xdb_snapshot_notifier(const Xdb_snapshot_notifier &) = delete;
  Xdb_snapshot_notifier &operator=(const Xdb_snapshot_notifier &) = delete;

  explicit Xdb_snapshot_notifier(Xdb_transaction *const owning_tx)
      : m_owning_tx(owning_tx) {}

  // If the owning Xdb_transaction gets destructed we need to not reference
  // it anymore.
  void detach() { m_owning_tx = nullptr; }
};

/*
  Very short (functor-like) interface to be passed to
  Xdb_transaction::walk_tx_list()
*/

interface Xdb_tx_list_walker {
  virtual ~Xdb_tx_list_walker() {}
  virtual void process_tran(const Xdb_transaction *const) = 0;
};


/* This is the base class for transactions when interacting with xengine.
*/
class Xdb_transaction {
protected:
  ulonglong m_write_count = 0;
  ulonglong m_lock_count = 0;

  bool m_is_delayed_snapshot = false;
  bool m_is_two_phase = true;

  THD *m_thd = nullptr;

  xengine::common::ReadOptions m_read_opts;

  static std::multiset<Xdb_transaction *> s_tx_list;
  static mysql_mutex_t s_tx_list_mutex;

  bool m_tx_read_only = false;

  int m_timeout_sec; /* Cached value of @@xengine_lock_wait_timeout */

  /* Maximum number of locks the transaction can have */
  ulonglong m_max_row_locks;

  bool m_is_tx_failed = false;
  bool m_rollback_only = false;

  std::shared_ptr<Xdb_snapshot_notifier> m_notifier;

  /*
    Tracks the number of tables in use through external_lock.
    This should not be reset during start_tx().
  */
  int64_t m_n_mysql_tables_in_use = 0;

  bool m_prepared = false;

  bool m_backup_running = false;

  bool m_writebatch_iter_valid_flag = true;

  // This should be used only when updating binlog information.
  virtual xengine::db::WriteBatchBase *get_write_batch() = 0;
  virtual bool commit_no_binlog() = 0;
  virtual bool commit_no_binlog2() = 0;
  virtual xengine::db::Iterator *
  get_iterator(const xengine::common::ReadOptions &options,
               xengine::db::ColumnFamilyHandle *column_family) = 0;

public:
  XEngineAsyncCommitCallback  async_call_back;
  const char *m_mysql_log_file_name;
  my_off_t m_mysql_log_offset;
  const char *m_mysql_gtid;
  const char *m_mysql_max_gtid;
  String m_detailed_error;
  int64_t m_snapshot_timestamp = 0;
  bool m_ddl_transaction;

  /*
    for distinction between xdb_transaction_impl and xdb_writebatch_impl
    when using walk tx list
  */
  virtual bool is_writebatch_trx() const = 0;

  static void init_mutex();

  static void term_mutex();

  static void walk_tx_list(Xdb_tx_list_walker *walker);

  int set_status_error(THD *const thd, const xengine::common::Status &s,
                       const Xdb_key_def &kd, Xdb_tbl_def *const tbl_def);

  THD *get_thd() const { return m_thd; }

  void set_params(int timeout_sec_arg, int max_row_locks_arg) {
    m_timeout_sec = timeout_sec_arg;
    m_max_row_locks = max_row_locks_arg;
    set_lock_timeout(timeout_sec_arg);
  }
  void add_table_in_use() { ++m_n_mysql_tables_in_use; }
  void dec_table_in_use() {
    DBUG_ASSERT(m_n_mysql_tables_in_use > 0);
    --m_n_mysql_tables_in_use;
  }
  int64_t get_table_in_use() { return m_n_mysql_tables_in_use; }

  virtual void set_lock_timeout(int timeout_sec_arg) = 0;

  ulonglong get_write_count() const { return m_write_count; }

  int get_timeout_sec() const { return m_timeout_sec; }

  ulonglong get_lock_count() const { return m_lock_count; }

  /* if commit-in-middle happened, all data in writebatch has been committed,
   * and WBWI(WriteBatchWithIterator is invalid), we should not access it
   * anymore */
  void invalid_writebatch_iterator() { m_writebatch_iter_valid_flag = false; }

  /* if transaction commit/rollback from sever layer, valid writebatch iterator
   * agagin. */
  void reset_writebatch_iterator() { m_writebatch_iter_valid_flag = true; }

  bool is_writebatch_valid() { return m_writebatch_iter_valid_flag; }

  virtual void set_sync(bool sync) = 0;

  virtual void release_lock(xengine::db::ColumnFamilyHandle *const column_family,
                            const std::string &rowkey) = 0;

  virtual bool prepare(const xengine::util::TransactionName &name) = 0;

  bool commit_or_rollback();

  bool commit();

  bool commit2() {
    return commit_no_binlog2();
  }

  virtual void rollback() = 0;

  void snapshot_created(const xengine::db::Snapshot *const snapshot);

  virtual void acquire_snapshot(bool acquire_now) = 0;
  virtual void release_snapshot() = 0;

  bool has_snapshot() const { return m_read_opts.snapshot != nullptr; }

  virtual bool has_prepared() { return m_prepared; };
  virtual void detach_from_xengine() = 0;
  void set_backup_running(const bool running) { m_backup_running = running; }
  bool get_backup_running() const { return m_backup_running; }
private:
  // The tables we are currently loading.  In a partitioned table this can
  // have more than one entry
  std::vector<ha_xengine *> m_curr_bulk_load;

public:
  int finish_bulk_load();

  void start_bulk_load(ha_xengine *const bulk_load);

  void end_bulk_load(ha_xengine *const bulk_load);

  int num_ongoing_bulk_load() const { return m_curr_bulk_load.size(); }

  /*
    Flush the data accumulated so far. This assumes we're doing a bulk insert.

    @detail
      This should work like transaction commit, except that we don't
      synchronize with the binlog (there is no API that would allow to have
      binlog flush the changes accumulated so far and return its current
      position)

    @todo
      Add test coverage for what happens when somebody attempts to do bulk
      inserts while inside a multi-statement transaction.
  */
  bool flush_batch();

  virtual xengine::common::Status put(xengine::db::ColumnFamilyHandle *const column_family,
                              const xengine::common::Slice &key,
                              const xengine::common::Slice &value) = 0;
  virtual xengine::common::Status
  delete_key(xengine::db::ColumnFamilyHandle *const column_family,
             const xengine::common::Slice &key) = 0;
  virtual xengine::common::Status
  single_delete(xengine::db::ColumnFamilyHandle *const column_family,
                const xengine::common::Slice &key) = 0;

  virtual bool has_modifications() const = 0;

  virtual xengine::db::WriteBatchBase *get_indexed_write_batch() = 0;
  /*
    Return a WriteBatch that one can write to. The writes will skip any
    transaction locking. The writes will NOT be visible to the transaction.
  */
  xengine::db::WriteBatchBase *get_blind_write_batch() {
    return get_indexed_write_batch()->GetWriteBatch();
  }

  virtual xengine::common::Status get(xengine::db::ColumnFamilyHandle *const column_family,
                              const xengine::common::Slice &key,
                              std::string *value) const = 0;

  virtual xengine::common::Status get_latest(
      xengine::db::ColumnFamilyHandle *const column_family,
      const xengine::common::Slice &key, std::string *value) const = 0;

  virtual xengine::common::Status get_for_update(
      xengine::db::ColumnFamilyHandle *const column_family,
      const xengine::common::Slice &key, std::string *const value,
      bool exclusive, bool only_lock = false) = 0;

  virtual xengine::common::Status lock_unique_key(xengine::db::ColumnFamilyHandle *const column_family,
                                                  const xengine::common::Slice &key,
                                                  const bool skip_bloom_filter,
                                                  const bool fill_cache,
                                                  const bool exclusive) = 0;

  xengine::db::Iterator *
  get_iterator(xengine::db::ColumnFamilyHandle *const column_family,
               bool skip_bloom_filter, bool fill_cache,
               bool read_current = false, bool create_snapshot = true,
               bool exclude_l2 = false, bool unique_check = false);

  virtual bool is_tx_started() const = 0;
  virtual void start_tx() = 0;
  virtual void start_stmt() = 0;
  virtual void rollback_stmt() = 0;

  void set_tx_failed(bool failed_arg) { m_is_tx_failed = failed_arg; }

  bool can_prepare() const;

  int rollback_to_savepoint(void *const savepoint);

  /*
    This is used by transactions started with "START TRANSACTION WITH "
    "CONSISTENT [XENGINE] SNAPSHOT". When tx_read_only is turned on,
    snapshot has to be created via DB::GetSnapshot(), not via Transaction
    API.
  */
  bool is_tx_read_only() const { return m_tx_read_only; }

  bool is_two_phase() const { return m_is_two_phase; }

  void set_tx_read_only(bool val) { m_tx_read_only = val; }

  xengine::common::ReadOptions& get_read_opts() { return m_read_opts; } 

  explicit Xdb_transaction(THD *const thd);

  virtual ~Xdb_transaction();
};

Xdb_transaction *get_or_create_tx(THD *const thd);
void xengine_register_tx(handlerton *const hton, THD *const thd,
                         Xdb_transaction *const tx);
bool can_use_bloom_filter(THD *thd, const Xdb_key_def &kd,
                          const xengine::common::Slice &eq_cond,
                          const bool use_all_keys, bool is_ascending);
}  // namespace myx
