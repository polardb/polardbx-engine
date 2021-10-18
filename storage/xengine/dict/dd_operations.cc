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

#include "dd_operations.h"
#include "field.h"

namespace myx {

const char *const dd_table_key_strings[DD_TABLE__LAST] = {
    "instant_col", "null_bytes", "hidden_pk_id",
};

const char *const dd_index_key_strings[DD_INDEX__LAST] = {
    "table_id", "subtable_id",
    "index_type", "index_version_id", "index_kv_version", "index_flags",
};

const char *const dd_column_key_strings[DD_COLUMN__LAST] = {
    "default", "default_null",
};

/** Copy the engine-private parts of a table or partition definition
when the change does not affect X-Engine. This mainly copies the common
private data between dd::Table and dd::Partition
@tparam		Table		dd::Table or dd::Partition
@param[in,out]	new_table	Copy of old table or partition definition
@param[in]	old_table	Old table or partition definition */
void dd_copy_private(dd::Table &new_table, const dd::Table &old_table) {
  uint64 autoinc = 0;
  uint64 version = 0;
  bool reset = false;
  dd::Properties &se_private_data = new_table.se_private_data();

  /* AUTOINC metadata could be set at the beginning for
  non-partitioned tables. So already set metadata should be kept */
  /*if (se_private_data.exists(dd_table_key_strings[DD_TABLE_AUTOINC])) {
    se_private_data.get(dd_table_key_strings[DD_TABLE_AUTOINC], &autoinc);
    se_private_data.get(dd_table_key_strings[DD_TABLE_VERSION], &version);
    reset = true;
  }*/

  new_table.se_private_data().clear();

  new_table.set_se_private_id(old_table.se_private_id());
  new_table.set_se_private_data(old_table.se_private_data());

  /*if (reset) {
    se_private_data.set(dd_table_key_strings[DD_TABLE_VERSION], version);
    se_private_data.set(dd_table_key_strings[DD_TABLE_AUTOINC], autoinc);
  }*/

  assert(new_table.indexes()->size() == old_table.indexes().size());

  /* Note that server could provide old and new dd::Table with
  different index order in this case, so always do a double loop */
  for (const auto old_index : old_table.indexes()) {
    auto idx = new_table.indexes()->begin();
    for (; (*idx)->name() != old_index->name(); ++idx)
      ;
    assert(idx != new_table.indexes()->end());

    auto new_index = *idx;
    assert(new_index != nullptr);
    assert(new_index->se_private_data().empty());
    assert(new_index->name() == old_index->name());

    new_index->set_se_private_data(old_index->se_private_data());
    new_index->set_tablespace_id(old_index->tablespace_id());
    new_index->options().clear();
    new_index->set_options(old_index->options());
  }

  //new_table.table().set_row_format(old_table.table().row_format());
  new_table.options().clear();
  new_table.set_options(old_table.options());
}

/** Copy the engine-private parts of column definitions of a table.
@param[in,out]	new_table	Copy of old table
@param[in]	old_table	Old table */
void dd_copy_table_columns(dd::Table &new_table, const dd::Table &old_table) {
  /* Columns in new table maybe more than old tables, when this is
  called for adding instant columns. Also adding and dropping
  virtual columns instantly is another case. */
  for (const auto old_col : old_table.columns()) {
    dd::Column *new_col = const_cast<dd::Column *>(
        dd_find_column(&new_table, old_col->name().c_str()));

    if (new_col == nullptr) {
      //may be column is dropped in the new table
      //assert(old_col->is_virtual());
      continue;
    }

    if (!old_col->se_private_data().empty()) {
      if (!new_col->se_private_data().empty())
        new_col->se_private_data().clear();
      new_col->set_se_private_data(old_col->se_private_data());
    }
  }
}

/** Add column default values for new instantly added columns
@param[in]	old_table	MySQL table as it is before the ALTER operation
@param[in]	altered_table	MySQL table that is being altered
@param[in,out]	new_dd_table	New dd::Table
@param[in]	new_table	New X-Engine table object */
void dd_add_instant_columns(const TABLE *old_table, const TABLE *altered_table,
                            dd::Table *new_dd_table) {
  assert(altered_table->s->fields > old_table->s->fields);

#ifdef UNIV_DEBUG
  for (uint32_t i = 0; i < old_table->s->fields; ++i) {
    assert(strcmp(old_table->field[i]->field_name,
                 altered_table->field[i]->field_name) == 0);
  }
#endif /* UNIV_DEBUG */

  uint16_t num_instant_cols = 0;

  for (uint32_t i = old_table->s->fields; i < altered_table->s->fields; ++i) {
    Field *field = altered_table->field[i];

    /* The MySQL type code has to fit in 8 bits
    in the metadata stored in the X-Engine change buffer. */
    assert(field->charset() == nullptr || field->charset()->number > 0);

    dd::Column *column = const_cast<dd::Column *>(
        dd_find_column(new_dd_table, field->field_name));
    assert(column != nullptr);
    dd::Properties &se_private = column->se_private_data();

    assert(++num_instant_cols);

    // TODO
    //se_private.set(dd_index_key_strings[DD_TABLE_ID], new_table->id);

    if (field->is_real_null()) {
      se_private.set(dd_column_key_strings[DD_INSTANT_COLUMN_DEFAULT_NULL],
                     true);
      continue;
    }

    unsigned col_len = field->pack_length();

    uint field_offset = field->ptr - altered_table->record[0];
    size_t length = 0;
    DD_instant_col_val_coder coder;
    const char *value = coder.encode(field->ptr,col_len, &length);

    dd::String_type default_value;
    default_value.assign(dd::String_type(value, length));
    se_private.set(dd_column_key_strings[DD_INSTANT_COLUMN_DEFAULT],
                   default_value);
  }

  assert(num_instant_cols > 0);
}

const char *DD_instant_col_val_coder::encode(const byte *stream, size_t in_len,
                                             size_t *out_len) {
  cleanup();

  m_result = new byte[in_len * 2];
  char *result = reinterpret_cast<char *>(m_result);

  for (size_t i = 0; i < in_len; ++i) {
    uint8_t v1 = ((stream[i] & 0xF0) >> 4);
    uint8_t v2 = (stream[i] & 0x0F);

    result[i * 2] = (v1 < 10 ? '0' + v1 : 'a' + v1 - 10);
    result[i * 2 + 1] = (v2 < 10 ? '0' + v2 : 'a' + v2 - 10);
  }

  *out_len = in_len * 2;

  return (result);
}

const byte *DD_instant_col_val_coder::decode(const char *stream, size_t in_len,
                                             size_t *out_len) {
  assert(in_len % 2 == 0);

  cleanup();

  m_result = new byte[in_len / 2];

  for (size_t i = 0; i < in_len / 2; ++i) {
    char c1 = stream[i * 2];
    char c2 = stream[i * 2 + 1];

    assert(isdigit(c1) || (c1 >= 'a' && c1 <= 'f'));
    assert(isdigit(c2) || (c2 >= 'a' && c2 <= 'f'));

    m_result[i] = ((isdigit(c1) ? c1 - '0' : c1 - 'a' + 10) << 4) +
                  ((isdigit(c2) ? c2 - '0' : c2 - 'a' + 10));
  }

  *out_len = in_len / 2;

  return (m_result);
}

void DD_instant_col_val_coder::cleanup() {
  if (m_result != nullptr) {
    delete[] m_result;
    m_result = nullptr;
  }
}

} //namespace myx
