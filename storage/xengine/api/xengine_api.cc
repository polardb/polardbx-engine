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

#include "xengine_api.h"

#include <string>
#include <set>
#include <unordered_map>
#include "table.h"
#include "./xdb_cf_manager.h"
#include "./xdb_datadic.h"
#include "./ha_xengine.h"

// Export utilities functions from myx
namespace myx {
extern int xdb_normalize_tablename(const std::string &tablename,
																	 std::string *const strbuf);
extern Xdb_dict_manager dict_manager;
extern Xdb_cf_manager cf_manager;
extern Xdb_ddl_manager ddl_manager;
extern xengine::util::TransactionDB *xdb;
}  // namespace myx

// Static function for internal use.
namespace {
static int xengine_strcasecmp(const char *a, const char *b) {
	if (!a) {
		if (!b) {
			return(0);
		} else {
			return(-1);
		}
	} else if (!b) {
		return(1);
	}
	return my_strcasecmp(system_charset_info, a, b);
}

static xengine_col_type_t get_col_type(Field *field) {
	static std::unordered_map<int64_t, xengine_col_type_t>
			mysql_engine_map = {
		{MYSQL_TYPE_DECIMAL, XENGINE_DECIMAL},
		{MYSQL_TYPE_NEWDECIMAL, XENGINE_NEWDECIMAL},
		{MYSQL_TYPE_TINY, XENGINE_INT},
		{MYSQL_TYPE_SHORT, XENGINE_INT},
		{MYSQL_TYPE_LONG, XENGINE_INT},
		{MYSQL_TYPE_FLOAT, XENGINE_FLOAT},
		{MYSQL_TYPE_DOUBLE, XENGINE_DOUBLE},
		{MYSQL_TYPE_LONGLONG, XENGINE_INT},
		{MYSQL_TYPE_INT24, XENGINE_INT},
		{MYSQL_TYPE_VARCHAR, XENGINE_VARCHAR_ANYCHARSET},
		{MYSQL_TYPE_TINY_BLOB, XENGINE_BLOB},
		{MYSQL_TYPE_MEDIUM_BLOB, XENGINE_BLOB},
		{MYSQL_TYPE_LONG_BLOB, XENGINE_BLOB},
		{MYSQL_TYPE_BLOB, XENGINE_BLOB},
		{MYSQL_TYPE_VAR_STRING, XENGINE_VARCHAR_ANYCHARSET},
		{MYSQL_TYPE_DATETIME, XENGINE_DATETIME},
		{MYSQL_TYPE_DATETIME2, XENGINE_DATETIME2},
	};
	xengine_col_type_t result = XENGINE_VARCHAR;
	enum_field_types field_type = field->real_type();
	auto it = mysql_engine_map.find(field_type);
	if (it != mysql_engine_map.end()) {
		result = it->second;
	}
	if (MYSQL_TYPE_STRING == field_type) {
		if (field->flags & BINARY_FLAG) {
			result = XENGINE_BINARY;
		} else {
			result = XENGINE_CHAR;
		}
	}
	return result;
}

static uint32_t get_pk_index(const TABLE *const table,
														 const myx::Xdb_tbl_def *const table_def) {
  return table->s->primary_key == MAX_INDEXES ? table_def->m_key_count - 1 :
																	table->s->primary_key;
}

static xengine_err_t get_unpack_info(
			const char *db_value,
			const char **output,
			uint32_t *data_len) {
	*output = db_value;
	*data_len = myx::xdb_netbuf_to_uint16(
			reinterpret_cast<const uchar *>(db_value + 1));
	return DB_SUCCESS;
}

//unused function comment by beilou
#if 0 
static xengine_err_t skip_unpack_info(
			bool has_unpack_info,
			const char *db_value,
			int64_t *pos) {
  if (has_unpack_info) {
		*pos += myx::xdb_netbuf_to_uint16(
				reinterpret_cast<const uchar *>(db_value + 1));
	}
	return DB_SUCCESS;
}

static xengine_err_t get_varchar_col(
			const Field_varstring *field,
			char *db_value,
			const char **output) {
	if (nullptr == db_value) {
    return DB_INVALID_NULL;
	}
  /* field->length_bytes is 1 or 2 */
	*output = db_value + field->length_bytes;
	return DB_SUCCESS;
}
#endif

static xengine_err_t copy_varchar_col(
			const Field_varstring *field,
			const char *db_value,
			char *output) {
  if (nullptr == db_value || nullptr == output) {
    return DB_INVALID_NULL;
	}
	uint32_t data_len;
  /* field->length_bytes is 1 or 2 */
  if (field->length_bytes == 1) {
    data_len = (uchar)db_value[0];
  } else {
    DBUG_ASSERT(field->length_bytes == 2);
    data_len = uint2korr(db_value);
  }
	memcpy(output, db_value, field->length_bytes);

  if (data_len > field->field_length) {
    /* The data on disk is longer than table DDL allows? */
    return DB_DATA_MISMATCH;
  }

	memcpy(output + field->length_bytes, db_value + field->length_bytes,
				 data_len);
  return DB_SUCCESS;
}

static xengine_err_t skip_varchar_col(
			const Field_varstring *field,
			const char *db_value,
			int64_t *pos) {
  if (nullptr == db_value) {
    return DB_INVALID_NULL;
	}
  /* field->length_bytes is 1 or 2 */
	*pos += field->length_bytes;
  if (field->length_bytes == 1) {
    *pos += (uchar)db_value[0];
  } else {
    DBUG_ASSERT(field->length_bytes == 2);
    *pos += uint2korr(db_value);
  }
	return DB_SUCCESS;
}

static xengine_err_t get_varchar_col_length(const Field_varstring *field,
																						const char *db_value,
																						uint64_t *res) {
  if (nullptr == db_value) {
		return DB_INVALID_NULL;
	}
	if (field->length_bytes == 1) {
		*res = (uchar)db_value[0];
	} else {
		*res = uint2korr(db_value);
	}
	return DB_SUCCESS;
}

static int64_t get_length_bytes(const Field_varstring *field) {
	return field->length_bytes;
}

#define PORTABLE_SIZEOF_CHAR_PTR 8

//unused function, comment by beilou
#if 0
static xengine_err_t get_blob_col(const Field_blob *field,
																	char *db_value,
																	bool db_low_byte_first,
																	const char **output) {
  if (nullptr == db_value) {
    return DB_INVALID_NULL;
	}
	*output = db_value + field->pack_length_no_ptr();
	return DB_SUCCESS;
}
#endif

static xengine_err_t copy_blob_col(const Field_blob *field,
																	 const char *db_value,
																	 bool db_low_byte_first,
																	 char *output) {
  if (nullptr == db_value || nullptr == output) {
    return DB_INVALID_NULL;
	}
  uint32_t length_bytes = field->pack_length_no_ptr();
	memcpy(output, db_value, length_bytes);
  memset(output + length_bytes, 0, PORTABLE_SIZEOF_CHAR_PTR);
  const char *blob_ptr = db_value + length_bytes;
  memcpy(output + length_bytes, &blob_ptr, sizeof(char**));
  return DB_SUCCESS;
}

static xengine_err_t skip_blob_col(const Field_blob *field,
																	 const char *db_value,
																	 bool db_low_byte_first,
																	 int64_t *pos) {
	if (nullptr == db_value) {
    return DB_INVALID_NULL;
	}
  uint32_t length_bytes = field->pack_length_no_ptr();
  uint64_t data_len = 0;
	*pos += length_bytes;
  memcpy(&data_len, db_value, length_bytes);
  *pos += data_len;
	return DB_SUCCESS;
}

static xengine_err_t get_blob_col_length(const Field_blob *field,
																				 const char *db_value,
																				 bool db_low_byte_first,
																				 uint64_t *res) {
	if (nullptr == db_value || nullptr == field) {
		return DB_INVALID_NULL;
	}
  memcpy(res, db_value, field->pack_length_no_ptr());
	return DB_SUCCESS;
}

static int64_t get_length_bytes(const Field_blob *field) {
	return field->pack_length_no_ptr();
}

//unused function  comment by beilou
#if 0
static xengine_err_t get_normal_col(const Field *field,
																		const char *db_value,
																		const char **output) {
  if (nullptr == db_value || nullptr == output) {
    return DB_INVALID_NULL;
	}
	*output = db_value;
	return DB_SUCCESS;
}
#endif 

static xengine_err_t copy_normal_col(const Field *field,
																		 const char *db_value,
																		 char *output) {
  if (nullptr == db_value || nullptr == output) {
    return DB_INVALID_NULL;
	}
  memcpy(output, db_value, field->pack_length_in_rec());
	return DB_SUCCESS;
}

static xengine_err_t skip_normal_col(const Field *field,
			 	  													 const char *db_value,
																		 int64_t *pos) {
  if (nullptr == db_value) {
    return DB_INVALID_NULL;
	}
	*pos += field->pack_length_in_rec();
	return DB_SUCCESS;
}

static xengine_err_t get_normal_col_length(const Field *field,
																					 const char *db_value,
																					 uint64_t *res) {
  if (nullptr == db_value || nullptr == res) {
    return DB_INVALID_NULL;
	}
	*res = field->pack_length_in_rec();
	return DB_SUCCESS;
}
//unused function comment by beilou
#if 0
static int64_t get_length_bytes(const Field *field) {
	return 0;
}
#endif

static xengine_err_t xengine_tuple_copy_col(int64_t src_idx,
																						xengine_tuple_t *src,
																						int64_t idx,
																						xengine_tuple_t *tpl) {
	uint64_t len = tpl->length_array_[idx] = src->length_array_[src_idx];
	tpl->offset_array_[idx] = src->offset_array_[src_idx];
	tpl->data_array_[idx] = src->data_array_[src_idx] - src->data_ + tpl->data_;
	memcpy(tpl->data_array_[idx], src->data_array_[src_idx], len);
    return DB_SUCCESS;
}

static xengine_err_t xengine_convert_value(std::set<uint32_t> &field_offset_set,
																					 const char *row_value,
																					 xengine_table_def_t table_def,
																					 myx::Xdb_key_def *key_def,
																					 xengine_tuple_t *result) {
	const char *unpack_info = nullptr;
	uint32_t unpack_info_len = 0;
	auto table = static_cast<TABLE *>(table_def->table_);
	int64_t pos = table_def->null_bytes_;
	const char *null_bytes = row_value;
	if (key_def->table_has_unpack_info(table)) {
		get_unpack_info(row_value + pos, &unpack_info, &unpack_info_len);
		pos += unpack_info_len;
	}

	// Parse and copy column one by one.
	Field **field_array = reinterpret_cast<Field **>(table_def->field_array_);
	for (int64_t k = 0; k < table_def->col_array_len_; ++k) {
		// For unpacked key, don't call the skip...col function bscause this
		// field is not in the value.
		if (field_offset_set.find(static_cast<uint32_t>(k)) !=
				field_offset_set.end()) {
			continue;
		}

		const Field *field = field_array[k];
		int64_t field_offset = field->ptr - table->record[0];
		char *tpl_cursor = result->data_ + field_offset;
		enum_field_types field_type = field->real_type();
		result->offset_array_[k] = field_offset;

		// null columns, fill default value.
		int64_t null_byte_offset = table_def->null_byte_offset_array_[k];
		unsigned char null_mask = table_def->null_mask_array_[k];
		if (null_mask & (*(null_bytes + null_byte_offset))) {
			const char *default_value = reinterpret_cast<const char *>(
						table->s->default_values + field_offset);
			memcpy(tpl_cursor, default_value, field->pack_length());
			result->length_array_[k] = XENGINE_SQL_NULL;
			result->data_array_[k] = nullptr;
			continue;
		}

	  // normal fill.
		if (MYSQL_TYPE_BLOB == field_type) {
			const Field_blob *blob_field = static_cast<const Field_blob *>(field);
			copy_blob_col(blob_field, row_value + pos, true, tpl_cursor);
			skip_blob_col(blob_field, row_value + pos, true, &pos);
			get_blob_col_length(blob_field, tpl_cursor, true,
													result->length_array_ + k);
      result->data_array_[k] = *(reinterpret_cast<char**>(
            tpl_cursor + get_length_bytes(blob_field)));
		} else if (MYSQL_TYPE_VARCHAR == field_type) {
			const Field_varstring *varchar_field =
					static_cast<const Field_varstring *>(field);
			copy_varchar_col(varchar_field, row_value + pos, tpl_cursor);
			skip_varchar_col(varchar_field, row_value + pos, &pos);
			get_varchar_col_length(varchar_field, tpl_cursor,
														 result->length_array_ + k);
			result->data_array_[k] = tpl_cursor + get_length_bytes(varchar_field);
		} else {
			copy_normal_col(field, row_value + pos, tpl_cursor);
			skip_normal_col(field, row_value + pos, &pos);
			get_normal_col_length(field, tpl_cursor, result->length_array_ + k);
      result->data_array_[k] = tpl_cursor;
		}
	}
	return DB_SUCCESS;
}

// Malloc data for the member of xengine_tuple_t. Need a tuple instance created.
// This function will not set the content in offset_array_, length_array_ and 
// data_array_.
// This function will only allocate once for all memoery requirements.
// The buffer can be divided into 4 continuous parts:
// [data_: col_buffer_size]
// [offset_array_: sizeof(int64_t) * col_array_len]
// [length_array_: sizeof(uint64_t) * col_array_len]
// [data_array_: sizeof(char*) * col_array_len]
static xengine_err_t xengine_create_tuple(int64_t col_array_len,
																			 		int64_t col_buffer_size,
																			 		xengine_tuple_t *tuple) {
  // tuple will be sent to C code in X-KV and then free, so use malloc.
	tuple->size_ = col_buffer_size;
	int64_t offset_data = 0;
	int64_t offset_offset =
			offset_data + sizeof(tuple->data_[0]) * col_buffer_size;
	int64_t offset_length =
			offset_offset + sizeof(tuple->offset_array_[0]) * col_array_len;
	int64_t offset_data_array =
			offset_length + sizeof(tuple->length_array_[0]) * col_array_len;
	int64_t offset_null_array =
			offset_data_array + sizeof(tuple->data_array_[0]) * col_array_len;
	int64_t offset_mask_array =
			offset_null_array + sizeof(tuple->null_offset_array_[0]) * col_array_len;
	int64_t total_mem_size =
			offset_mask_array + sizeof(tuple->null_mask_array_[0]) * col_array_len;


	void *mem = malloc(total_mem_size);
	if (NULL == mem) {
		return DB_OUT_OF_MEMORY;
	}
	memset(mem, 0x00, sizeof(char) * total_mem_size);

	tuple->data_ = (char *) mem + offset_data;
	tuple->offset_array_ = (int64_t *) ((char *) mem + offset_offset);
	tuple->length_array_ = (uint64_t *) ((char *) mem + offset_length);
	tuple->data_array_ = (char**) ((char *) mem + offset_data_array);
	tuple->null_offset_array_ = (int64_t *) ((char *) mem + offset_null_array);
	tuple->null_mask_array_ = (uchar *) ((char *) mem + offset_mask_array);
	tuple->key_parts_ = 0;
  tuple->buffer_ = nullptr;
	return DB_SUCCESS;
}

static void xengine_free_tuple(xengine_tuple_t *tuple) {
	if (NULL != tuple) {
		free(tuple->data_);
    delete static_cast<std::string*>(tuple->buffer_);
	}
	free(tuple);
}

static void xengine_free_iter(xengine_iter_t *iter) {
	if (NULL != iter) {
		free(iter->key_tuple_.data_);
    delete static_cast<std::string*>(iter->key_tuple_.buffer_);
		free(iter->value_tuple_.data_);
    delete static_cast<std::string*>(iter->value_tuple_.buffer_);
		free(iter->upper_bound_);
		free(iter->lower_bound_);
		delete static_cast<xengine::db::Iterator *>(iter->iter_);
	}
	free(iter);
}


// We construct the key tuple in the following rule:
// 1. Get the total buffer size including all columns in this table.
// 2. Create a buffer with the buffer size.
// 3. For each part of the key, find its column offset in the buffer.
// 4. Index these offset using increamental index from 0 in
// data_array_ and offset_array_
// The key tuple is different with value tuple. In a value tuple, the
// data_array_[i] and offset_array_[i] allway point to the i th column
// in the buffer. But in a key tuple, the data_array_[i] and offset_array_[i]
// will point to the real column of ths i th part of the key
static xengine_err_t xengine_bless_key_tuple(xengine_table_def_t table_def,
																						 xengine_key_def_t key_def,
																						 xengine_tuple_t *tuple,
																						 int64_t key_parts) {
  xengine_err_t result = DB_SUCCESS;
	auto table = static_cast<TABLE *>(table_def->table_);
  auto db_key_def = static_cast<myx::Xdb_key_def *>(key_def);

	std::unique_ptr<Field*[]> field_array;
	field_array.reset(new Field*[key_parts]);
	for (int64_t i = 0; i < key_parts; ++i) {
		Field *field = db_key_def->get_table_field_for_part_no(table, i);
		field_array[i] = field;
	}
	int64_t col_num = table_def->col_array_len_;
	int64_t buffer_size = table_def->col_buffer_size_;
	result = xengine_create_tuple(col_num, buffer_size, tuple);
	if (DB_SUCCESS != result) {
		return result;
	}
	int64_t first_offset = field_array[0]->ptr - table->record[0];
	memset(tuple->data_, 0x00, sizeof(tuple->data_[0]) * first_offset);
	for (int64_t i = 0; i < key_parts; ++i) {
		Field *field = field_array[i];
		int64_t offset = field->ptr - table->record[0];
		tuple->offset_array_[i] = offset;
		int64_t length_bytes = 0;
		enum_field_types field_type = field->real_type();
		if (MYSQL_TYPE_BLOB == field_type) {
			length_bytes = get_length_bytes(static_cast<Field_blob *>(field));
		} else if (MYSQL_TYPE_VARCHAR == field_type) {
			length_bytes = get_length_bytes(static_cast<Field_varstring *>(field));
		}
		tuple->data_array_[i] = tuple->data_ + offset + length_bytes;
		if (MYSQL_TYPE_STRING == field_type) {
			memset(tuple->data_array_[i], 0x20, field->pack_length_in_rec());
		}
		// Here, field->m_null_ptr and field->null_bit is nullptr and so useless.
		uint16_t index = field->field_index;
		tuple->null_offset_array_[i] = table_def->null_byte_offset_array_[index];
		tuple->null_mask_array_[i] = table_def->null_mask_array_[index];
	}
	return result;
}

static xengine_err_t xengine_create_iter(xengine_table_def_t table_def,
																				 xengine_key_def_t key_def,
																				 xengine_iter_t **iter) {
  auto db_key_def = static_cast<myx::Xdb_key_def *>(key_def);
	auto db_table_def = static_cast<myx::Xdb_tbl_def *>(table_def->db_obj_);
	auto table = static_cast<TABLE *>(table_def->table_);
	int64_t value_col_num = table_def->col_array_len_;
	int64_t value_buffer_size = table_def->col_buffer_size_;
	uint32_t pk_parts =
			static_cast<myx::Xdb_key_def *>(table_def->pk_def_)->get_key_parts();
	xengine_iter_t *new_iter = (xengine_iter_t *) malloc(sizeof(xengine_iter_t));
	if (NULL == new_iter) {
		goto end;
	}
	new_iter->key_tuple_.data_ = NULL;
	new_iter->value_tuple_.data_ = NULL;

	if (myx::Xdb_key_def::INDEX_TYPE_PRIMARY == db_key_def->m_index_type) {
		new_iter->seek_key_parts_ = pk_parts;
  } else if (!db_key_def->table_has_hidden_pk(table)) {
		new_iter->seek_key_parts_ = db_key_def->get_key_parts() - pk_parts;
	} else {
		new_iter->seek_key_parts_ = db_key_def->get_key_parts() - 1 /* hidden pk */;
	}

	if (DB_SUCCESS != xengine_bless_key_tuple(table_def, key_def,
																						 &new_iter->key_tuple_,
																						 new_iter->seek_key_parts_)) {
		goto free_iter;
	}
	if (DB_SUCCESS != xengine_create_tuple(value_col_num, value_buffer_size,
																				 &new_iter->value_tuple_)) {
		goto free_key_tuple;
	}

	new_iter->upper_bound_ = (char *) malloc(
			sizeof(char) * db_key_def->max_storage_fmt_length());
	new_iter->upper_bound_size_ = 0;
	if (NULL == new_iter->upper_bound_) {
		goto free_value_tuple;
	}
	new_iter->lower_bound_ = (char *) malloc(
			sizeof(char) * db_key_def->max_storage_fmt_length());
	new_iter->lower_bound_size_ = 0;
	if (NULL == new_iter->lower_bound_) {
		goto free_upper_bound;
	}
	*iter = new_iter;
	return DB_SUCCESS;
free_upper_bound:
	free(new_iter->upper_bound_);
free_value_tuple:
	xengine_free_tuple(&new_iter->value_tuple_);
free_key_tuple:
	xengine_free_tuple(&new_iter->key_tuple_);
free_iter:
	xengine_free_iter(new_iter);
end:
	return DB_OUT_OF_MEMORY;
}

};  // namespace

using namespace xengine;
using namespace common;

xengine_err_t xengine_trx_start(xengine_trx_t *trx,
																xengine_trx_level_t trx_level,
																xengine_bool_t read_write,
																xengine_bool_t auto_commit,
																void *thd)
{
	return DB_ERROR;
}

xengine_trx_t *xengine_trx_begin(xengine_trx_level_t trx_level,
																xengine_bool_t read_write,
																xengine_bool_t auto_commit) {
	xengine_trx_t *new_trx = (xengine_trx_t *) malloc(sizeof(xengine_trx_t));
	new_trx->db_snapshot_ = const_cast<void*>(static_cast<const void *>(
				myx::xdb->GetSnapshot()));
	return new_trx;
}

uint32_t xengine_trx_read_only(xengine_trx_t *trx) {
  //TODO comment by beilou 5.7->8.0
  //i don't know why there is a empty function, so just abort it
  abort();
  return 0;
}

xengine_err_t xengine_trx_release(xengine_trx_t *trx) {
	if (trx != nullptr) {
		auto snapshot = static_cast<xengine::db::Snapshot *>(trx->db_snapshot_);
		myx::xdb->ReleaseSnapshot(snapshot);
		free(trx);
	}
	return DB_SUCCESS;
}

xengine_err_t xengine_trx_commit(xengine_trx_t *xengine_trx) {
	return DB_ERROR;
}

xengine_err_t xengine_trx_rollback(xengine_trx_t *xengine_trx) {
	return DB_ERROR;
}

xengine_err_t xengine_open_table_using_id(xengine_id_u64_t table_id,
																					xengine_trx_t *xengine_trx,
																					xengine_request_t *xengine_crsr) {
  return DB_ERROR;
}

xengine_err_t xengine_open_index_using_name(xengine_request_t req,
    																				const char *index_name,
    																				xengine_iter_t **iter,
																						int *idx_type,
																						xengine_id_u64_t *idx_id) {
  xengine_err_t result = DB_SUCCESS;
	auto db_table_def = static_cast<myx::Xdb_tbl_def*>(req->table_def_->db_obj_);
	auto table = static_cast<TABLE *>(req->table_def_->table_);

	myx::Xdb_key_def *db_key_def = nullptr;
	for (size_t i = 0; i != db_table_def->m_key_count; ++i) {
		db_key_def = db_table_def->m_key_descr_arr[i].get();
		if (xengine_strcasecmp(db_key_def->get_name().data(), index_name) == 0) {
			*idx_id = db_key_def->get_index_number();
			*idx_type = db_key_def->m_index_type ==
					myx::Xdb_key_def::INDEX_TYPE_PRIMARY ? XENGINE_CLUSTERED :
							table->key_info[i].flags & HA_NOSAME ? XENGINE_UNIQUE:
																										 XENGINE_SECONDARY;
			break;
		}
	}
	if (nullptr == db_key_def) {
		return DB_NOT_FOUND;
	}
	xengine_iter_t *new_iter;
	xengine_create_iter(req->table_def_, db_key_def, &new_iter);
	// target index is found, create xengine iterator.
	new_iter->key_def_ = static_cast<void *>(db_key_def);
	myx::GL_INDEX_ID gl_index_id = db_key_def->get_gl_index_id();
	new_iter->cf_id_ = gl_index_id.cf_id;
	new_iter->index_number_ = db_key_def->get_index_number();
	xengine::common::ReadOptions ro;
  ro.total_order_seek = true;
	xengine::db::ColumnFamilyHandle *cfh = db_key_def->get_cf();
	new_iter->iter_ = static_cast<void *>(myx::xdb->NewIterator(ro, cfh));
	new_iter->is_primary_ =
			db_key_def->m_index_type == myx::Xdb_key_def::INDEX_TYPE_PRIMARY;
	*iter = new_iter;
  return result;
}

xengine_err_t xengine_open_table(const char *db_name,
																 const char *table_name,
																 void *thd,
																 void *mysql_table,
																 int64_t lock_type,
																 xengine_request_t *req) {
	int64_t db_name_len = strlen(db_name);
	int64_t table_name_len = strlen(table_name);
	int64_t name_len = db_name_len + table_name_len + 1;
	char *name_buffer = new char[name_len + 1];
	std::unique_ptr<char[]> normalized_name {name_buffer};
	memcpy(name_buffer, db_name, db_name_len);
	name_buffer[db_name_len] = '.';
	memcpy(name_buffer + db_name_len + 1, table_name, table_name_len);

	bool from_dict = false;
	auto tbl_def = myx::ddl_manager.find(name_buffer, name_len, &from_dict, false);
	myx::Xdb_tbl_def *xdb_table_def = tbl_def.get();
	if (nullptr == xdb_table_def) {
		return DB_TABLE_NOT_FOUND;
	}
	myx::Xdb_key_def *xdb_key_def = xdb_table_def->m_key_descr_arr->get();
	if (nullptr == xdb_key_def) {
		return DB_NOT_FOUND;
	}

	// malloc and init xegnine_request
	*req = (xengine_request_t) malloc(sizeof(xengine_request));
	xengine_table_def_t table_def = 
				(xengine_table_def_t) malloc(sizeof(*((*req)->table_def_)));
	table_def->db_name_ = db_name;
	table_def->table_name_ = table_name;
	table_def->db_name_len_ = strlen(db_name);
	table_def->table_name_len_ = strlen(table_name);
	table_def->db_obj_ = static_cast<void *>(xdb_table_def);

	TABLE *table = static_cast<TABLE *>(mysql_table);
	int64_t number_fields = table->s->fields;
	table_def->col_array_len_ = number_fields;
	table_def->col_array_len_ = number_fields;
	table_def->col_name_array_ = (const char **) malloc(
				sizeof(char *) * number_fields);
	table_def->col_length_array_ = (int64_t *) malloc(
				sizeof(int64_t) * number_fields);
	table_def->field_array_ = (void **) malloc(
				sizeof(void *) * number_fields);
	table_def->null_byte_offset_array_ = (int64_t *) malloc(
				sizeof(int64_t) * number_fields);
	table_def->null_mask_array_ = (unsigned char *) malloc(
				sizeof(unsigned char) * number_fields);

	unsigned char current_null_mask = 0x1;
	int64_t current_null_byte = 0;
	table_def->col_buffer_size_ = 1;
	for (uint32_t i = 0; i < number_fields; ++i) {
		Field *const field = table->field[i];
		table_def->col_buffer_size_ += field->pack_length();
		table_def->col_name_array_[i] = field->field_name;
		table_def->col_length_array_[i] = field->pack_length();
		table_def->field_array_[i] = static_cast<void *>(field);
		if (field->real_maybe_null()) {
			table_def->null_mask_array_[i] = current_null_mask;
			table_def->null_byte_offset_array_[i] = current_null_byte;
			if (0x80 == current_null_mask) {
				current_null_mask = 0x1;
				current_null_byte += 1;
			} else  {
				current_null_mask <<= 1;
			}
		} else {
			table_def->null_mask_array_[i] = 0;
			table_def->null_byte_offset_array_[i] = 0;
		}
	}
	table_def->null_bytes_ = current_null_byte + (~current_null_mask & 1);
	table_def->table_ = table;
	uint32_t pk_index = get_pk_index(table, xdb_table_def);
	myx::Xdb_key_def *key_def = xdb_table_def->m_key_descr_arr[pk_index].get();
	table_def->pk_def_ = key_def;
	table_def->pk_parts_ = key_def->get_key_parts();
	(*req)->lock_type_ = lock_type;
	(*req)->table_def_ = table_def;
	(*req)->read_trx_ = nullptr;
	(*req)->thd_ = thd;
	return DB_SUCCESS;
}

void xengine_delete_tuple(xengine_tuple_t *tuple) {
	xengine_free_tuple(tuple);
}

xengine_err_t xengine_close(xengine_request_t req) {
	return DB_ERROR;
}

xengine_err_t xengine_close_table(xengine_request_t req) {
	if (NULL != req) {
		free(req->table_def_->col_name_array_);
		free(req->table_def_->col_length_array_);
		free(req->table_def_->field_array_);
		free(req->table_def_->null_byte_offset_array_);
		free(req->table_def_->null_mask_array_);
		free(req->table_def_);
		free(req);
	}
	return DB_SUCCESS;
}


xengine_err_t xengine_new_trx(xengine_request_t req,
    													xengine_trx_t *xengine_trx) {
	return DB_ERROR;
}

xengine_err_t xengine_commit_trx(xengine_request_t req,
    														 xengine_trx_t *xengine_trx) {
  return DB_ERROR;
}

xengine_err_t xengine_insert_row(xengine_request_t req,
    														 const xengine_tpl_t xengine_tpl) {
  return DB_ERROR;
}

xengine_err_t xengine_update_row(xengine_request_t xengine_crsr,
																const xengine_tpl_t xengine_old_tpl,
																const xengine_tpl_t xengine_new_tpl) {
  return DB_ERROR;
}


xengine_err_t xengine_delete_row(xengine_request_t xengine_crsr) {
	return DB_ERROR;
}

// Read using PK.
xengine_err_t xengine_pk_search(xengine_request_t req,
																xengine_tuple_t *key_tuple,
																xengine_tuple_t *value_tuple) {
  xengine_table_def_t table_def = req->table_def_;
  auto pk_def = static_cast<myx::Xdb_key_def *>(table_def->pk_def_);
	auto table = static_cast<TABLE *>(table_def->table_);
	uint32_t pack_buffer_size = pk_def->max_storage_fmt_length();
	std::unique_ptr<uchar[]> pack_buf {new uchar[pack_buffer_size]};
	std::unique_ptr<char[]> pack_res {new char[pack_buffer_size]};
	char *packed_key_data = pack_res.get();
	int64_t key_parts = pk_def->get_key_parts();
	uint32_t packed_key_size = pk_def->pack_record(table, pack_buf.get(),
					reinterpret_cast<uchar *>(key_tuple->data_),
					reinterpret_cast<uchar *>(packed_key_data),
					nullptr, false, 0, key_parts, nullptr);
	assert(packed_key_size <= pack_buffer_size);
  Slice key(packed_key_data, packed_key_size);
	ReadOptions ro;
  std::string *row_value = value_tuple->buffer_ != nullptr ?
                           static_cast<std::string*>(value_tuple->buffer_) :
                           new std::string();
	xengine_trx_t *trx = req->read_trx_;
	if (trx != nullptr) {
		ro.snapshot = static_cast<xengine::db::Snapshot *>(trx->db_snapshot_);
	}

	Status s = myx::xdb->Get(ro, pk_def->get_cf(), key, row_value);
	if (!s.ok()) {
		return DB_GET_ERROR;
	}

	std::set<uint32_t> field_id_set;
	if (!pk_def->table_has_hidden_pk(table)) {
		for (int64_t k = 0; k != pk_def->get_key_parts(); ++k) {
			Field *field = pk_def->get_table_field_for_part_no(table, k);
			uint16_t field_index = field->field_index;
			xengine_tuple_copy_col(k, key_tuple, field_index, value_tuple);
			field_id_set.insert(field_index);
		}
	}

  value_tuple->buffer_ = static_cast<void*>(row_value);
	// Parse and copy column one by one.
  return xengine_convert_value(field_id_set, row_value->data(), table_def,
															 pk_def, value_tuple);
}

// Read from iter.
// When seach using SK, automatically switch to PK.
xengine_err_t xengine_read_row(xengine_request_t req,
															 xengine_iter_t *iter,
															 xengine_tpl_t cmp_tpl,
															 int mode,
															 void **row_buf,
															 xengine_ulint_t *row_len,
															 xengine_ulint_t *used_len) {
	int64_t pos = 0;
	auto db_iter = static_cast<xengine::db::Iterator *>(iter->iter_);
	xengine_table_def_t table_def = req->table_def_;
	if (nullptr == db_iter) {
		assert(false);
		return DB_INVALID_NULL;
	}
	xengine_tuple_t *tpl = &(iter->value_tuple_);
	xengine::common::Slice value = db_iter->value();
	xengine::common::Slice key = db_iter->key();

	const char *db_value = value.data();
	const char *null_bytes = db_value;
	// Secondary index store null info just in field data position.
	int64_t null_bytes_size = iter->is_primary_ ? table_def->null_bytes_ : 0;
	const char *unpack_info = nullptr;
	uint32_t unpack_info_len = 0;
	TABLE *table = static_cast<TABLE *>(table_def->table_);
	myx::Xdb_key_def *key_def = static_cast<myx::Xdb_key_def *>(iter->key_def_);

	pos += null_bytes_size;
	if (key_def->table_has_unpack_info(table)) {
		get_unpack_info(db_value + pos, &unpack_info, &unpack_info_len);
		pos += unpack_info_len;
		xengine::common::Slice unpack_slice(unpack_info, unpack_info_len);
		if (key_def->unpack_record(table,
									reinterpret_cast<unsigned char *>(tpl->data_),
	 								&key, unpack_info ? &unpack_slice : nullptr,
									false /* verify_checksum */)) {
			return DB_UNPACK_ERROR;
		}
  } else if (key_def->unpack_record_1(table,
																reinterpret_cast<unsigned char *>(tpl->data_),
																&key, nullptr, false /* verify_checksum */)) {
    return DB_UNPACK_ERROR;
  }

	// Read from secondary key to get PK and then call db->Get using PK.
  std::string *row_value = tpl->buffer_ != nullptr ?
                           static_cast<std::string*>(tpl->buffer_) :
                           new std::string();
	if (!(iter->is_primary_)) {
		myx::Xdb_key_def *pk_def =
				static_cast<myx::Xdb_key_def *> (table_def->pk_def_);
		uint32_t pk_len = key_def->get_primary_key_tuple(table, *pk_def, &key,
								reinterpret_cast<unsigned char *>(iter->key_tuple_.data_));
    xengine::common::Slice row_key(iter->key_tuple_.data_, pk_len);
		xengine::common::ReadOptions ro;
		xengine_trx_t *trx = req->read_trx_;
		// Must access the referenced PK row in the same snapshot.
		if (nullptr == trx) {
			return DB_SNAPSHOT_ERROR;
		}
		ro.snapshot = static_cast<xengine::db::Snapshot *>(trx->db_snapshot_);
		xengine::common::Status status =
				myx::xdb->Get(ro, pk_def->get_cf(), row_key, row_value);
		if (!status.ok()) {
			return DB_GET_ERROR;
		}
		// For hidden PK, value contains all fields.
		// So fill field_d_set with only PK.
		key_def = pk_def;
		db_value = row_value->data();
	}

	std::set<uint32_t> field_id_set;
	if (!key_def->table_has_hidden_pk(table)) {
		for (int64_t k = 0; k != key_def->get_key_parts(); ++k) {
			Field *field = key_def->get_table_field_for_part_no(table, k);
			char *tpl_cursor = field->ptr - table->record[0] + tpl->data_;
			uint16_t field_index = field->field_index;
			field_id_set.insert(field_index);
			enum_field_types field_type = field->real_type();
			if (MYSQL_TYPE_BLOB == field_type) {
				auto blob_field = static_cast<const Field_blob *>(field);
				get_blob_col_length(blob_field, tpl_cursor, true,
														tpl->length_array_ + field_index);
				tpl->data_array_[field_index] =
						tpl_cursor + get_length_bytes(blob_field);
			} else if (MYSQL_TYPE_VARCHAR == field_type) {
				auto varchar_field = static_cast<const Field_varstring *>(field);
				get_varchar_col_length(varchar_field, tpl_cursor,
																tpl->length_array_ + field_index);
				tpl->data_array_[field_index] =
						tpl_cursor + get_length_bytes(varchar_field);
			} else {
				get_normal_col_length(field, tpl_cursor,
															tpl->length_array_ + field_index);
				tpl->data_array_[field_index] = tpl_cursor;
			}
		}
	}

	// Parse and copy column one by one.
  tpl->buffer_ = static_cast<void*>(row_value);
	return xengine_convert_value(field_id_set, db_value, table_def, key_def, tpl);
}

xengine_err_t xengine_iter_valid(xengine_iter_t *iter) {
	auto db_key_def = static_cast<myx::Xdb_key_def *>(iter->key_def_);
	auto db_iter = static_cast<xengine::db::Iterator *>(iter->iter_);
	xengine_err_t result = DB_SUCCESS;
	if (!db_iter->Valid() || !db_key_def->covers_key(db_iter->key())) {
		result = DB_END_OF_INDEX;
	} else {
		xengine::common::Slice current_key = db_iter->key();
		// If iter->upper_bound_size_ is 0, memcmp return 0.
		uint64_t cmp_size = std::min(current_key.size(), iter->upper_bound_size_);
		if (memcmp(current_key.data(), iter->upper_bound_, cmp_size) > 0) {
			result = DB_END_OF_INDEX;
		}
	}
	return result;
}

xengine_err_t xengine_iter_first(xengine_iter_t *iter) {
	xengine_err_t result = DB_SUCCESS;
	assert(iter != nullptr);
	auto db_iter = static_cast<xengine::db::Iterator *>(iter->iter_);
	if (db_iter != nullptr && iter->key_def_ != nullptr) {
		uchar first_key[myx::Xdb_key_def::INDEX_NUMBER_SIZE];
		uint first_key_size = 0;
		auto db_key_def = static_cast<myx::Xdb_key_def *>(iter->key_def_);
		db_key_def->get_infimum_key(first_key, &first_key_size);
		uchar* key_ptr = first_key;
		db_iter->Seek(xengine::common::Slice(
					reinterpret_cast<const char *>(key_ptr), first_key_size));
        result = xengine_iter_valid(iter);
	} else {
		assert(false);
		result = DB_INVALID_NULL;
	}
	return result;
}

xengine_err_t xengine_iter_next(xengine_iter_t *iter) {
	xengine_err_t result = DB_SUCCESS;
	auto db_key_def = static_cast<myx::Xdb_key_def *>(iter->key_def_);
	auto db_iter = static_cast<xengine::db::Iterator *>(iter->iter_);
	if (db_iter != nullptr) {
		db_iter->Next();
        result = xengine_iter_valid(iter);
	} else {
		assert(false);
		result = DB_INVALID_NULL;
	}
	return result;
}

xengine_err_t xengine_iter_seek(xengine_request_t req,
    														xengine_tpl_t tpl,
																xengine_iter_t *iter,
    														xengine_srch_mode_t xengine_srch_mode,
    														xengine_ulint_t direction) {
  xengine_err_t result = DB_SUCCESS;
	auto db_key_def = static_cast<myx::Xdb_key_def *>(iter->key_def_);
	auto table = static_cast<TABLE *>(req->table_def_->table_);
	uint32_t pack_buffer_size = db_key_def->max_storage_fmt_length();
	std::unique_ptr<uchar[]> pack_buf {new uchar[pack_buffer_size]};
	std::unique_ptr<char[]> pack_res {new char[pack_buffer_size]};
	char *packed_key_data = pack_res.get();
	int64_t key_parts = tpl->key_parts_;
	if (key_parts == 0) {
		key_parts = iter->seek_key_parts_;
	}
	assert(key_parts <= iter->seek_key_parts_);
	uint32_t packed_key_size = db_key_def->pack_record(table, pack_buf.get(),
					reinterpret_cast<uchar *>(tpl->data_),
					reinterpret_cast<uchar *>(packed_key_data),
					nullptr, false, 0, key_parts, nullptr);
	assert(packed_key_size <= pack_buffer_size);
	memcpy(iter->upper_bound_, packed_key_data, packed_key_size);
	iter->upper_bound_size_ = packed_key_size;
	auto db_iter = static_cast<xengine::db::Iterator *>(iter->iter_);
	if (db_iter != nullptr) {
		xengine::common::Slice key(packed_key_data, packed_key_size);
		db_iter->Seek(key);
	} else {
		assert(false);
		result = DB_INVALID_NULL;
	}
	if (DB_SUCCESS == result) {
		result = xengine_iter_valid(iter);
	}
	return result;
}

void xengine_set_match_mode(xengine_request_t req,
    												xengine_match_mode_t match_mode) {
  if (req != nullptr) {
  	req->match_mode_ = match_mode;
  }
}

xengine_err_t xengine_col_set_value(xengine_tuple_t *tpl,
    																xengine_ulint_t col,
    																const void *src,
    																xengine_ulint_t len,
    																xengine_bool_t need_cpy) {
  // TODO @zhencheng: process need_cpy
	if (len != XENGINE_SQL_NULL) {
		assert(src != nullptr);
		memcpy(tpl->data_array_[col], src, len);
		// Not equal means this field has length parts.
		int64_t field_offset = tpl->offset_array_[col];
		int64_t len_len = tpl->data_array_[col] - tpl->data_ - field_offset;
		// Assume little endian.
		memcpy(tpl->data_ + field_offset, &len, len_len);
	} else {
		tpl->data_[tpl->null_offset_array_[col]] |= tpl->null_mask_array_[col];
	}
	// For null fields, only set the len to XENGINE_SQL_NULL
	tpl->length_array_[col] = len;
  return DB_SUCCESS;
}

xengine_ulint_t xengine_col_get_len(xengine_tpl_t xengine_tpl,
    																xengine_ulint_t i) {
  
  abort(); //TODO add by beilou 5.7->8.0
  return 0;
}

xengine_ulint_t xengine_col_copy_value(xengine_tpl_t xengine_tpl,
    																	 xengine_ulint_t i,
    																	 void *dst,
    																	 xengine_ulint_t len) {
  abort(); //TODO add by beilou 5.7->8.0
  return 0;
}

xengine_err_t xengine_tuple_read_i8(xengine_tpl_t tpl,
    																xengine_ulint_t i,
    																xengine_i8_t *ival) {
	*ival = *(reinterpret_cast<xengine_i8_t *>(tpl->data_array_[i]));
  return DB_ERROR;
}

xengine_err_t xengine_tuple_read_u8(xengine_tpl_t tpl,
    																xengine_ulint_t i,
    																xengine_u8_t *ival) {
  *ival = *(reinterpret_cast<xengine_u8_t *>(tpl->data_array_[i]));
  return DB_SUCCESS;
}

xengine_err_t xengine_tuple_read_i16(xengine_tpl_t tpl,
    																 xengine_ulint_t i,
    																 xengine_i16_t *ival) {
  *ival = *(reinterpret_cast<xengine_i16_t *>(tpl->data_array_[i]));
  return DB_SUCCESS;
}

xengine_err_t xengine_tuple_read_u16(xengine_tpl_t tpl,
    																 xengine_ulint_t i,
    																 xengine_u16_t *ival) {
  *ival = *(reinterpret_cast<xengine_u16_t *>(tpl->data_array_[i]));
  return DB_SUCCESS;
}

xengine_err_t xengine_tuple_read_i24(xengine_tpl_t tpl,
    																 xengine_ulint_t i,
    																 xengine_i32_t *ival) {
  *ival = *(reinterpret_cast<xengine_i32_t *>(tpl->data_array_[i]));
  return DB_SUCCESS;
}

xengine_err_t xengine_tuple_read_u24(xengine_tpl_t tpl,
    																 xengine_ulint_t i,
    																 xengine_u32_t *ival) {
  *ival = *(reinterpret_cast<xengine_i32_t *>(tpl->data_array_[i]));
  return DB_SUCCESS;
}

xengine_err_t xengine_tuple_read_i32(xengine_tpl_t tpl,
    																 xengine_ulint_t i,
    																 xengine_i32_t *ival) {
  *ival = *(reinterpret_cast<xengine_i32_t *>(tpl->data_array_[i]));
  return DB_SUCCESS;
}

xengine_err_t xengine_tuple_read_u32(xengine_tpl_t tpl,
    																 xengine_ulint_t i,
    																 xengine_u32_t *ival) {
  *ival = *(reinterpret_cast<xengine_i32_t *>(tpl->data_array_[i]));
  return DB_SUCCESS;
}

xengine_err_t xengine_tuple_read_i64(xengine_tpl_t tpl,
    																 xengine_ulint_t i,
    																 xengine_i64_t *ival) {
  *ival = *(reinterpret_cast<xengine_i64_t *>(tpl->data_array_[i]));
  return DB_SUCCESS;
}

xengine_err_t xengine_tuple_read_u64(xengine_tpl_t tpl,
    																 xengine_ulint_t i,
    																 xengine_u64_t *ival) {
  *ival = *(reinterpret_cast<xengine_u64_t *>(tpl->data_array_[i]));
  return DB_SUCCESS;
}

const void *xengine_col_get_value(xengine_tuple_t *tpl,
																	xengine_ulint_t i) {
	return static_cast<void*>(tpl->data_array_[i]);
}

xengine_ulint_t xengine_col_get_meta(xengine_request_t req,
																		 xengine_tuple_t *tpl,
    																 xengine_ulint_t i,
    																 xengine_col_meta_t *col_meta) {
  Field *field = reinterpret_cast<Field**>(req->table_def_->field_array_)[i];
  col_meta->type = get_col_type(field);
	uint16_t type = col_meta->client_type = col_meta->prtype = field->real_type();
	col_meta->type_len = field->pack_length();
	col_meta->attr = XENGINE_COL_NONE;
	if (field->flags & UNSIGNED_FLAG) {
		col_meta->attr |= XENGINE_COL_UNSIGNED;
	}
  if (nullptr == tpl) {
		return 0;
	}
	const char *null_bytes = tpl->data_;
	int64_t null_byte_offset = req->table_def_->null_byte_offset_array_[i];
	unsigned char null_mask = req->table_def_->null_mask_array_[i];
	if (null_mask & (*(null_bytes + null_byte_offset))) {
		return XENGINE_SQL_NULL;
	}
	return static_cast<xengine_ulint_t>(tpl->length_array_[i]);
}

void xengine_tuple_clear(xengine_iter_t **iter,
												 xengine_tuple_t **tpl) {
  xengine_free_iter(*iter);
	xengine_free_tuple(*tpl);
}

xengine_err_t xengine_tuple_get_cluster_key(xengine_request_t xengine_crsr,
		xengine_tpl_t *xengine_dst_tpl,
		const xengine_tpl_t xengine_src_tpl) {
  return DB_UNSUPPORTED;
}

xengine_err_t xengine_key_tuple_create(xengine_request_t req,
																			 xengine_key_def_t key_def,
																			 int64_t key_part,
																			 xengine_tuple_t **tuple) {
  xengine_err_t result = DB_SUCCESS;
	xengine_tuple_t *new_tuple =
			(xengine_tuple_t *) malloc(sizeof(xengine_tuple_t));
	result = xengine_bless_key_tuple(req->table_def_, key_def,
																	 new_tuple, key_part);
	if (DB_SUCCESS != result) {
		return result;
	}
	*tuple = new_tuple;
  return DB_SUCCESS;
}

xengine_err_t xengine_value_tuple_create(xengine_table_def_t table_def,
																				 xengine_tuple_t **tuple) {
  auto new_tuple = (xengine_tuple_t *) malloc(sizeof(xengine_tuple_t));
	int64_t col_num = table_def->col_array_len_;
	int64_t buffer_size = table_def->col_buffer_size_;
  xengine_create_tuple(col_num, buffer_size, new_tuple);
	*tuple = new_tuple;
	return DB_SUCCESS;
}

xengine_err_t xengine_sec_iter_create(xengine_request_t req,
																			const char *index_name,
																			xengine_iter_t **iter) {
	if (NULL == iter || NULL == req->table_def_) {
		assert(false);
		return DB_INVALID_NULL;
	}

	xengine_iter_t *new_iter;
	auto db_table_def = static_cast<myx::Xdb_tbl_def *>(req->table_def_->db_obj_);
	myx::Xdb_key_def *db_key_def = nullptr;

	for (size_t i = 0; i != db_table_def->m_key_count; ++i) {
		db_key_def = db_table_def->m_key_descr_arr[i].get();
		if (0 == xengine_strcasecmp(db_key_def->get_name().data(), index_name)) {
			break;
		}
	}
	if (nullptr == db_key_def) {
		return DB_NOT_FOUND;
	}

	xengine_create_iter(req->table_def_, db_key_def, &new_iter);
	new_iter->key_def_ = static_cast<void *>(db_key_def);
	myx::GL_INDEX_ID gl_index_id = db_key_def->get_gl_index_id();
	new_iter->cf_id_ = gl_index_id.cf_id;
	new_iter->index_number_ = gl_index_id.index_id;
	xengine::common::ReadOptions ro;
  ro.total_order_seek = true;
	xengine_trx_t *trx = req->read_trx_;
	if (trx != nullptr) {
		ro.snapshot = static_cast<xengine::db::Snapshot *>(trx->db_snapshot_);
	}
	xengine::db::ColumnFamilyHandle *cfh = db_key_def->get_cf();
	new_iter->iter_ = static_cast<void *>(myx::xdb->NewIterator(ro, cfh));
	new_iter->is_primary_ = false;
	*iter = new_iter;
	return DB_SUCCESS;
}

xengine_err_t xengine_clust_iter_create(xengine_request_t req, 																													xengine_iter_t **iter) {
	if (NULL == iter || NULL == req->table_def_) {
		assert(false);
		return DB_INVALID_NULL;
	}

	xengine_iter_t *new_iter;
	auto db_table_def = static_cast<myx::Xdb_tbl_def *>(req->table_def_->db_obj_);
	auto db_key_def = static_cast<myx::Xdb_key_def *>(req->table_def_->pk_def_);

	xengine_create_iter(req->table_def_, db_key_def, &new_iter);
	new_iter->key_def_ = static_cast<void *>(db_key_def);
	myx::GL_INDEX_ID gl_index_id = db_key_def->get_gl_index_id();
	new_iter->cf_id_ = gl_index_id.cf_id;
	new_iter->index_number_ = gl_index_id.index_id;
	xengine::common::ReadOptions ro;
  ro.total_order_seek = true;
	xengine_trx_t *trx = req->read_trx_;
	if (trx != nullptr) {
		ro.snapshot = static_cast<xengine::db::Snapshot *>(trx->db_snapshot_);
	}
	xengine::db::ColumnFamilyHandle *cfh = db_key_def->get_cf();
	new_iter->iter_ = static_cast<void *>(myx::xdb->NewIterator(ro, cfh));
	new_iter->is_primary_ = true;
	*iter = new_iter;
	return DB_SUCCESS;
}

// The primary key is one part of the key tuple. Don't include primary key
// part for the range scan condition in X-KV.
xengine_ulint_t xengine_iter_get_n_user_cols(const xengine_iter_t *iter) {
	return iter->seek_key_parts_;
}

xengine_ulint_t xengine_iter_get_n_cols(const xengine_iter_t *iter) {
	auto db_key_def = static_cast<myx::Xdb_key_def *>(iter->key_def_);
	return db_key_def->get_key_parts();
}

void xengine_tuple_set_n_fields_cmp(const xengine_tpl_t tpl,
    																uint32_t n_fields_cmp) {
  tpl->key_parts_ = static_cast<int64_t>(n_fields_cmp);
}

void xengine_delete_iter(xengine_iter_t *iter) {
  xengine_free_iter(iter);
}

xengine_err_t xengine_clust_search_tuple_create(xengine_request_t req,
																								xengine_iter_t *iter) {
	return DB_UNSUPPORTED;
}

xengine_err_t xengine_truncate(xengine_request_t *xengine_crsr,
    													 xengine_id_u64_t *table_id) {
  return DB_ERROR;
}

xengine_err_t xengine_table_get_id(const char *table_name,
    															 xengine_id_u64_t *table_id) {
  return DB_ERROR;
}

xengine_bool_t xengine_is_positioned(const xengine_request_t xengine_crsr) {
  abort();//TODO add by beilou
  return false;
}

xengine_bool_t xengine_schema_lock_is_exclusive(
			const xengine_trx_t *xengine_trx) {
  abort();//TODO add by beilou
  return false;
}

xengine_err_t xengine_lock(xengine_request_t xengine_crsr,
    											 xengine_lck_mode_t xengine_lck_mode) {
  return DB_ERROR;
}

xengine_err_t xengine_table_lock(xengine_trx_t *xengine_trx,
    														 xengine_id_u64_t table_id,
    														 xengine_lck_mode_t xengine_lck_mode) {
  return DB_ERROR;
}

xengine_err_t xengine_set_lock_mode(
    xengine_request_t xengine_crsr,
    xengine_lck_mode_t xengine_lck_mode) {
  abort(); //TODO add by beilou
  return DB_SUCCESS; 
}

void xengine_set_cluster_access(xengine_request_t xengine_crsr) {

}

void xengine_stmt_begin(xengine_request_t xengine_crsr) {

}

xengine_err_t xengine_tuple_write_double(xengine_tpl_t xengine_tpl,
    																		 int col_no,
    																		 double val) {
  return DB_ERROR;
}

xengine_err_t xengine_tuple_read_double(xengine_tpl_t tpl,
    																		xengine_ulint_t col_no,
    																		double *dval) {
  *dval = *(reinterpret_cast<double *>(tpl->data_array_[col_no]));
  return DB_SUCCESS;
}

xengine_err_t xengine_tuple_write_float(xengine_tpl_t xengine_tpl,
    																		int col_no,
    																		float val) {
  return DB_ERROR;
}

xengine_err_t xengine_tuple_read_float(xengine_tpl_t tpl,
    																	 xengine_ulint_t col_no,
    																	 float *fval) {
  *fval = *(reinterpret_cast<float *>(tpl->data_array_[col_no]));
  return DB_SUCCESS;
}

const char *xengine_col_get_name(xengine_request_t req,
    														 xengine_ulint_t i) {
  return req->table_def_->col_name_array_[i];
}

const char *xengine_get_idx_field_name(xengine_request_t req,
																			 xengine_iter_t *iter,
    																	 xengine_ulint_t i) {
  auto db_key_def = static_cast<myx::Xdb_key_def*>(iter->key_def_);
	Field *field = db_key_def->get_table_field_for_part_no(
				static_cast<TABLE *>(req->table_def_->table_), i);
	return field->field_name;
}

xengine_err_t xengine_table_truncate(const char *table_name,
    																 xengine_id_u64_t *table_id) {
  return DB_ERROR;
}

int xengine_cfg_get_cfg() {
	return 0;
}

xengine_err_t xengine_set_memcached_sync(xengine_request_t xengine_crsr,
																				 xengine_bool_t flag) {
  return DB_ERROR;
}

xengine_trx_level_t xengine_cfg_trx_level() {
 abort();//TODO add by beilou
 return XENGINE_TRX_READ_COMMITTED;
}

xengine_ulint_t xengine_cfg_bk_commit_interval() {
	return 0;
}

xengine_u64_t xengine_trx_get_start_time(xengine_trx_t *xengine_trx) {
	return 0;
}

const char *xengine_ut_strerr(xengine_err_t num) {
	switch (num) {
	case DB_SUCCESS:
		return "Success";
	case DB_SUCCESS_LOCKED_REC:
		return "Success, record lock created";
	case DB_ERROR:
		return "Generic error";
	case DB_READ_ONLY:
		return "Read only transaction";
	case DB_INTERRUPTED:
		return "Operation interrupted";
	case DB_OUT_OF_MEMORY:
		return "Cannot allocate memory";
	case DB_OUT_OF_FILE_SPACE:
		return "Out of disk space";
	case DB_LOCK_WAIT:
		return "Lock wait";
	case DB_DEADLOCK:
		return "Deadlock";
	case DB_ROLLBACK:
		return "Rollback";
	case DB_DUPLICATE_KEY:
		return "Duplicate key";
	case DB_QUE_THR_SUSPENDED:
		return "The queue thread has been suspended";
	case DB_MISSING_HISTORY:
		return "Required history data has been deleted";
	case DB_CLUSTER_NOT_FOUND:
		return "Cluster not found";
	case DB_TABLE_NOT_FOUND:
		return "Table not found";
	case DB_MUST_GET_MORE_FILE_SPACE:
		return "More file space needed";
	case DB_TABLE_IS_BEING_USED:
		return "Table is being used";
	case DB_TOO_BIG_RECORD:
		return "Record too big";
	case DB_TOO_BIG_INDEX_COL:
		return "Index columns size too big";
	case DB_LOCK_WAIT_TIMEOUT:
		return "Lock wait timeout";
	case DB_NO_REFERENCED_ROW:
		return "Referenced key value not found";
	case DB_ROW_IS_REFERENCED:
		return "Row is referenced";
	case DB_CANNOT_ADD_CONSTRAINT:
		return "Cannot add constraint";
	case DB_CORRUPTION:
		return "Data structure corruption";
	case DB_CANNOT_DROP_CONSTRAINT:
		return "Cannot drop constraint";
	case DB_NO_SAVEPOINT:
		return "No such savepoint";
	case DB_TABLESPACE_EXISTS:
		return "Tablespace already exists";
	case DB_TABLESPACE_DELETED:
		return "Tablespace deleted or being deleted";
	case DB_TABLESPACE_TRUNCATED:
		return "Tablespace was truncated";
	case DB_TABLESPACE_NOT_FOUND:
		return "Tablespace not found";
	case DB_LOCK_TABLE_FULL:
		return "Lock structs have exhausted the buffer pool";
	case DB_FOREIGN_DUPLICATE_KEY:
		return "Foreign key activated with duplicate keys";
	case DB_FOREIGN_EXCEED_MAX_CASCADE:
		return "Foreign key cascade delete/update exceeds max depth";
	case DB_TOO_MANY_CONCURRENT_TRXS:
		return "Too many concurrent transactions";
	case DB_UNSUPPORTED:
		return "Unsupported";
	case DB_INVALID_NULL:
		return "NULL value encountered in NOT NULL column";
	case DB_STATS_DO_NOT_EXIST:
		return "Persistent statistics do not exist";
	case DB_FAIL:
		return "Failed, retry may succeed";
	case DB_OVERFLOW:
		return "Overflow";
	case DB_UNDERFLOW:
		return "Underflow";
	case DB_STRONG_FAIL:
		return "Failed, retry will not succeed";
	case DB_ZIP_OVERFLOW:
		return "Zip overflow";
	case DB_RECORD_NOT_FOUND:
		return "Record not found";
	case DB_CHILD_NO_INDEX:
		return "No index on referencing keys in referencing table";
	case DB_PARENT_NO_INDEX:
		return "No index on referenced keys in referenced table";
	case DB_FTS_INVALID_DOCID:
		return "FTS Doc ID cannot be zero";
	case DB_INDEX_CORRUPT:
		return "Index corrupted";
	case DB_UNDO_RECORD_TOO_BIG:
		return "Undo record too big";
	case DB_END_OF_INDEX:
		return "End of index";
	case DB_IO_ERROR:
		return "I/O error";
	case DB_TABLE_IN_FK_CHECK:
		return "Table is being used in foreign key check";
	case DB_DATA_MISMATCH:
		return "data mismatch";
	case DB_SCHEMA_NOT_LOCKED:
		return "schema not locked";
    case DB_SNAPSHOT_ERROR:
        return "snapshot error";
    case DB_UNPACK_ERROR:
        return "unpack error";
    case DB_GET_ERROR:
        return "get error";
	case DB_NOT_FOUND:
		return "not found";
	case DB_ONLINE_LOG_TOO_BIG:
		return "Log size exceeded during online index creation";
	case DB_IDENTIFIER_TOO_LONG:
		return "Identifier name is too long";
	case DB_FTS_EXCEED_RESULT_CACHE_LIMIT:
		return "FTS query exceeds result cache limit";
	case DB_TEMP_FILE_WRITE_FAIL:
		return "Temp file write failure";
	case DB_CANT_CREATE_GEOMETRY_OBJECT:
		return "Can't create specificed geometry data object";
	case DB_CANNOT_OPEN_FILE:
		return "Cannot open a file";
	case DB_TABLE_CORRUPT:
		return "Table is corrupted";
	case DB_FTS_TOO_MANY_WORDS_IN_PHRASE:
		return "Too many words in a FTS phrase or proximity search";
	case DB_IO_DECOMPRESS_FAIL:
		return "Page decompress failed after reading from disk";
	case DB_IO_NO_PUNCH_HOLE:
		return "No punch hole support";
	case DB_IO_NO_PUNCH_HOLE_FS:
		return "Punch hole not supported by the file system";
	case DB_IO_NO_PUNCH_HOLE_TABLESPACE:
		return "Punch hole not supported by the tablespace";
	case DB_IO_NO_ENCRYPT_TABLESPACE:
		return "Page encryption not supported by the tablespace";
	case DB_IO_DECRYPT_FAIL:
		return "Page decryption failed after reading from disk";
	case DB_IO_PARTIAL_FAILED:
		return "Partial IO failed";
	case DB_FORCED_ABORT:
		return "Transaction aborted by another higher priority"
					 "transaction";
	case DB_WRONG_FILE_NAME:
		return "Invalid Filename";
	case DB_NO_FK_ON_S_BASE_COL:
		return "Cannot add foreign key on the base column"
					 "of stored column";
	case DB_COMPUTE_VALUE_FAILED:
		return "Compute generated column failed";
	case DB_X_PROTOCOL_INVISIBLE_CONFIG:
		return "X PROTOCOL: invisible config";
	case DB_X_PROTOCOL_WRONG_VERSION:
		return "X PROTOCOL: wrong version";
	case DB_X_PROTOCOL_EINVAL:
		return "X PROTOCL: invalid argument";
	case DB_META_INVAL:
		return "meta information in containers table is invalid";
	case DB_RES_OVERFLOW:
		return "result is overflow";
		/* do not add default: in order to produce a warning if new code
	is added to the enum but not added here */
	}
	/* NOT REACHED */
	return "Unknown error";
}

xengine_bool_t xengine_is_virtual_table(xengine_request_t crsr) {
	return false;
}

void xengine_set_tuple_key(xengine_tpl_t xengine_tpl) {

}
