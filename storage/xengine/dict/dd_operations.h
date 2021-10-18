/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "sql/table.h"
#include "field_types.h"
#include "dd/cache/dictionary_client.h"
#include "dd/dd.h"
#include "dd/dd_schema.h"
#include "dd/dd_table.h"
#include "dd/dictionary.h"
#include "dd/properties.h"
#include "dd/types/column.h"
#include "dd/types/foreign_key.h"
#include "dd/types/foreign_key_element.h"
#include "dd/types/index.h"
#include "dd/types/index_element.h"
#include "dd/types/partition.h"
#include "dd/types/partition_index.h"
#include "dd/types/table.h"
#include "dd/types/tablespace.h"
#include "dd/types/tablespace_file.h"
#include "dd_table_share.h"

#pragma once

namespace myx {

typedef unsigned char byte;

enum dd_table_reserved {
  /* reserved id space for dd table, refer to DICT_MAX_DD_TABLES in dict0dd.h */
  DD_TABLE_ID_END = 1024,
  /* invalid subtable id */
  DD_SUBTABLE_ID_INVALID = 0x7fffffff,
};

enum dd_table_keys {
  /** Columns before first instant ADD COLUMN */
  DD_TABLE_INSTANT_COLS,
  /** Number of null bytes before first instant ADD COLUMN*/
  DD_TABLE_NULL_BYTES,
  /** subtable id of hidden primary key added by X-Engine */
  DD_TABLE_HIDDEN_PK_ID,
  /** Sentinel */
  DD_TABLE__LAST
};

/** X-Engine private keys for dd::Index */
enum dd_index_keys {
  DD_INDEX_TABLE_ID,
  DD_INDEX_SUBTABLE_ID,
  DD_INDEX_TYPE,   // primary key or secondary key
  DD_INDEX_VERSION_ID,
  DD_INDEX_KV_VERSION,
  DD_INDEX_FLAGS,
  DD_INDEX__LAST,
};

/** X-Engine private keys for dd::Column */
enum dd_column_keys {
  /** Default value when it was added instantly */
  DD_INSTANT_COLUMN_DEFAULT,
  /** Default value is null or not */
  DD_INSTANT_COLUMN_DEFAULT_NULL,
  /** Sentinel */
  DD_COLUMN__LAST
};

/** X-Engine private key strings for dd::Table. @see dd_table_keys */
extern const char *const dd_table_key_strings[DD_TABLE__LAST];
/** X-Engine private key strings for dd::Index. @see dd_index_keys */
extern const char *const dd_index_key_strings[DD_INDEX__LAST];
/** X-Engine private key strings for dd::Column, @see dd_column_keys */
extern const char *const dd_column_key_strings[DD_COLUMN__LAST];

/** Copy the engine-private parts of a table or partition definition
when the change does not affect X-Engine. This mainly copies the common
private data between dd::Table and dd::Partition
@tparam		Table		dd::Table or dd::Partition
@param[in,out]	new_table	Copy of old table or partition definition
@param[in]	old_table	Old table or partition definition */
void dd_copy_private(dd::Table &new_table, const dd::Table &old_table);

/** Look up a column in a table using the system_charset_info collation.
@param[in]	dd_table	data dictionary table
@param[in]	name		column name
@return the column
@retval nullptr if not found */
inline const dd::Column *dd_find_column(const dd::Table *dd_table,
                                        const char *name) {
  for (const dd::Column *c : dd_table->columns()) {
    if (!my_strcasecmp(system_charset_info, c->name().c_str(), name)) {
      return (c);
    }
  }
  return (nullptr);
}

/** Determine if a dd::Table has any instant column
@param[in]	table	dd::Table
@return	true	If it's a table with instant columns
@retval	false	Not a table with instant columns */
inline bool dd_table_has_instant_cols(const dd::Table &table) {
  bool instant = table.se_private_data().exists(
      dd_table_key_strings[DD_TABLE_INSTANT_COLS]);

  //ut_ad(!instant || dd_instant_columns_exist(table));

  return (instant);
}

/** Copy the engine-private parts of column definitions of a table.
@param[in,out]	new_table	Copy of old table
@param[in]	old_table	Old table */
void dd_copy_table_columns(dd::Table &new_table, const dd::Table &old_table);

/** Add column default values for new instantly added columns
@param[in]	old_table	MySQL table as it is before the ALTER operation
@param[in]	altered_table	MySQL table that is being altered
@param[in,out]	new_dd_table	New dd::Table
@param[in]	new_table	New X-Engine table object */
void dd_add_instant_columns(const TABLE *old_table, const TABLE *altered_table,
                            dd::Table *new_dd_table);

class DD_instant_col_val_coder {
 public:
  /** Constructor */
  DD_instant_col_val_coder() : m_result(nullptr) {}

  /** Destructor */
  ~DD_instant_col_val_coder() { cleanup(); }

  /** Encode the specified stream in format of bytes into chars
  @param[in]	stream	stream to encode in bytes
  @param[in]	in_len	length of the stream
  @param[out]	out_len	length of the encoded stream
  @return	the encoded stream, which would be destroyed if the class
  itself is destroyed */
  const char *encode(const byte *stream, size_t in_len, size_t *out_len);

  /** Decode the specified stream, which is encoded by encode()
  @param[in]	stream	stream to decode in chars
  @param[in]	in_len	length of the stream
  @param[out]	out_len	length of the decoded stream
  @return	the decoded stream, which would be destroyed if the class
  itself is destroyed */
  const byte *decode(const char *stream, size_t in_len, size_t *out_len);

 private:
  /** Clean-up last result */
  void cleanup();

 private:
  /** The encoded or decoded stream */
  byte *m_result;
};

} //namespace myx
