/* Copyright (c) 2025, 2025, Alibaba and/or its affiliates. All rights reserved.

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

#include <cmath>  // std::isinf, std::isnan

#include "my_byteorder.h"   // get_float
#include "sql/sql_const.h"  // MAX_FLOAT_STR_LENGTH
#include "sql_string.h"     // String

#include "sql/item_func.h"
#include "sql/item_strfunc.h"
#include "vidx/vidx_field.h"
#include "vidx/vidx_text_parser.h"

namespace vidx {
static double calc_distance_euclidean(float *v1, float *v2, size_t v_len) {
  double d = 0;
  for (size_t i = 0; i < v_len; i++, v1++, v2++) {
    double dist = get_float(v1) - get_float(v2);
    d += dist * dist;
  }
  return sqrt(d);
}

static double calc_distance_cosine(float *v1, float *v2, size_t v_len) {
  double dotp = 0, abs1 = 0, abs2 = 0;
  for (size_t i = 0; i < v_len; i++, v1++, v2++) {
    float f1 = get_float(v1), f2 = get_float(v2);
    abs1 += f1 * f1;
    abs2 += f2 * f2;
    dotp += f1 * f2;
  }
  /*
    Zero-vector COSINE semantics must be explicit and, more importantly,
    identical to the indexed HNSW path.

    Cross-implementation check:
      - Qdrant / Weaviate / hnswlib normalize cosine vectors before using
        dot product.  A zero vector is kept as all-zero, so its cosine
        similarity with any vector is 0 and the distance is 1.
      - OpenSearch k-NN script scoring treats a zero-norm operand as the
        minimum cosine score 0, again equivalent to distance 1.
      - pgvector is the main different design: scalar cosine_distance can
        return NaN, but HNSW/IVFFlat cosine indexes explicitly skip zero-norm
        vectors.  That would be a larger semantic change for this engine,
        because our hlindex/HNSW path already indexes zero vectors.

    Our HNSW path uses FVector::create(COSINE): a zero vector keeps all-zero
    dimensions and gets abs2=0.5, so distance_to() returns 1 against every
    vector, including another zero vector.  Return 1 here as well to avoid
    scalar/filesort producing NaN while the vector-index path returns a
    deterministic distance.
  */
  if (abs1 == 0 || abs2 == 0) return 1.0;

  return 1 - dotp / sqrt(abs1 * abs2);
}

static double calc_distance_inner_product(float *v1, float *v2, size_t v_len) {
  double dotp = 0;
  for (size_t i = 0; i < v_len; i++, v1++, v2++) {
    double f1 = get_float(v1), f2 = get_float(v2);
    dotp += f1 * f2;
  }
  /* Negate so that ORDER BY ASC yields max-inner-product first. */
  return -dotp;
}

static distance_kind mhnsw_uses_distance(KEY *keyinfo) {
  return static_cast<distance_kind>(keyinfo->vector_distance);
}

bool from_vector_to_string(String *input, const uint32 precision,
                           CHARSET_INFO *cs, String *output) {
  assert(input != nullptr && input->ptr() != nullptr);
  assert(output != nullptr);

  const uint32 input_dims = get_dimensions_low(input->length(), precision);

  if (input_dims == UINT_MAX32) {
    return true;
  }

  output->length(0);
  output->set_charset(cs);
  if (output->reserve(input_dims * (MAX_FLOAT_STR_LENGTH + 1) + 2)) {
    return true; /* OOM */
  }

  if (input_dims == 0) {
    return false;
  }

  float val;
  size_t len;
  char buf[MAX_FLOAT_STR_LENGTH + 1];
  auto ptr = (const uchar *)input->ptr();

  output->append('[');

  for (size_t i = 0; i < input_dims; i++) {
    if (i != 0) {
      output->append(',');
    }

    val = float4get(ptr);
    if (std::isinf(val))
      if (val < 0)
        output->append(STRING_WITH_LEN("-Inf"));
      else
        output->append(STRING_WITH_LEN("Inf"));
    else if (std::isnan(val))
      output->append(STRING_WITH_LEN("NaN"));
    else {
      len = my_gcvt(val, MY_GCVT_ARG_FLOAT, MAX_FLOAT_STR_LENGTH, buf, 0);
      output->append(buf, len);
    }

    ptr += precision;
  }

  output->append(']');

  return false;
}

bool Item_func_vec_distance::resolve_type(THD *thd) {
  switch (kind) {
    case EUCLIDEAN: {
      calc_distance_func = calc_distance_euclidean;
      break;
    }
    case COSINE: {
      calc_distance_func = calc_distance_cosine;
      break;
    }
    case INNER_PRODUCT: {
      calc_distance_func = calc_distance_inner_product;
      break;
    }
    case AUTO: {
      for (uint fno = 0; fno < 2; fno++) {
        if (args[fno]->type() == Item::FIELD_ITEM) {
          Field *f = ((Item_field *)args[fno])->field;
          KEY *key_info = f->table->s->key_info;
          for (uint i = f->table->s->keys; i < f->table->s->total_keys; i++) {
            if (i >= MAX_INDEXES) break;
            assert(key_info[i].flags & HA_VECTOR);
            assert(key_info[i].user_defined_key_parts == 1);
            if (f->key_start.is_set(i)) {
              kind = mhnsw_uses_distance(key_info + i);
              return resolve_type(thd);
            }
          }
        }
      }
      /*
        No VECTOR INDEX found to infer the distance type. Do not raise an
        error here: defer the decision to val_real() so that NULL arguments
        propagate as NULL (SQL convention). A non-NULL argument without a
        resolvable distance type is reported in val_real() with a hint to
        use the explicit VEC_DISTANCE_* functions.
      */
      calc_distance_func = nullptr;
      break;
    }
    default:
      my_error(ER_VEC_DISTANCE_TYPE, MYF(0));
      return true;
  }

  return Item_real_func::resolve_type(thd);
}

int Item_func_vec_distance::get_key() {
  if (check_args()) {
    Field *f = field_arg->field;

    /*
      Validate the query vector at optimizer time.  Required here (not only
      in mhnsw_read_first) because the optimizer can pick a non-vector plan
      (e.g. FORCE INDEX(PRIMARY) → filesort), in which case mhnsw_read_first
      is never called and an invalid query vector would silently produce
      NULL distances rather than raising ER_WRONG_ARGUMENTS.  With
      result_cached in Item_func_vec_fromtext, the val_str() call below is
      a cache hit on subsequent invocations, so the cost is negligible.
    */
    String tmp;
    String *r = const_arg->val_str(&tmp);

    if (!r || r->length() != f->field_length || r->length() % sizeof(float)) {
      my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
      return -1;
    }

    KEY *keyinfo = f->table->s->key_info;
    for (uint i = f->table->s->keys; i < f->table->s->total_keys; i++) {
      if (i >= MAX_INDEXES) break;
      assert(keyinfo[i].flags & HA_VECTOR);
      assert(keyinfo[i].user_defined_key_parts == 1);

      if (f->key_start.is_set(i) && kind == mhnsw_uses_distance(keyinfo + i)) {
        return i;
      }
    }
  }

  return -1;
}

double Item_func_vec_distance::val_real() {
  String tmp1, tmp2;
  String *r1 = args[0]->val_str(&tmp1);
  String *r2 = args[1]->val_str(&tmp2);

  /* If the dimensions of two vectors are not equal, result should be NULL. */
  if (!r1 || !r2 || r1->length() != r2->length() ||
      r1->length() % sizeof(float)) {
    null_value = true;
    return 0;
  }

  null_value = false;

  /*
    AUTO distance type could not be resolved at resolve_type() time (no
    VECTOR INDEX available) and both arguments are non-NULL. Report a clear
    error pointing to the explicit distance functions.
  */
  if (calc_distance_func == nullptr) {
    my_error(ER_VEC_DISTANCE_TYPE, MYF(0));
    null_value = true;
    return 0;
  }

  float *v1 = (float *)r1->ptr();
  float *v2 = (float *)r2->ptr();

  return calc_distance_func(v1, v2, (r1->length()) / sizeof(float));
}

bool Item_func_vec_distance::check_args() {
  assert((field_arg == nullptr) == (const_arg == nullptr));

  if (field_arg != nullptr) {
    return true;
  }

  /* MDEV-35922 Server crashes in mhnsw_read_first upon using vector key with
   * views */
  if (args[0]->real_item()->type() == Item::FIELD_ITEM &&
      args[1]->const_for_execution()) {
    field_arg = (Item_field *)(args[0]->real_item());
    const_arg = args[1];
    return true;
  }

  if (args[1]->real_item()->type() == Item::FIELD_ITEM &&
      args[0]->const_for_execution()) {
    field_arg = (Item_field *)(args[1]->real_item());
    const_arg = args[0];
    return true;
  }

  return false;
}

bool Item_func_vec_fromtext::resolve_type(THD *thd) {
  if (Item_str_func::resolve_type(thd)) {
    return true;
  }
  if (args[0]->result_type() != STRING_RESULT) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    return true;
  }
  if (reject_geometry_args(arg_count, args, this)) return true;
  set_data_type_vector(
      static_cast<ulonglong>(Field_vector::dimension_bytes(MAX_DIMENSIONS)));
  return false;
}

String *Item_func_vec_fromtext::val_str(String *str) {
  assert(fixed);
  /* Fast path: return cached result for constant string arguments. */
  if (result_cached) {
    null_value = false;
    return &buffer;
  }

  null_value = false;
  String *res = args[0]->val_str(str);
  if (res == nullptr || res->ptr() == nullptr) {
    return error_str();
  }

  uint32 output_dims = MAX_DIMENSIONS;
  if (buffer.mem_realloc(Field_vector::dimension_bytes(output_dims)))
    return error_str();

  bool err = from_string_to_vector(res->charset(), res->ptr(), res->length(),
                                  buffer.ptr(), &output_dims);
  if (err) {
    if (output_dims == MAX_DIMENSIONS) {
      String truncated(*res);
      truncated.replace(32, 5, "... \0", 5);
      my_error(ER_DATA_OUT_OF_RANGE, MYF(0), truncated.ptr(), func_name());
    } else {
      my_error(ER_TO_VECTOR_CONVERSION, MYF(0), res->length(), res->ptr());
    }
    return error_str();
  }

  buffer.length(Field_vector::dimension_bytes(output_dims));
  if (args[0]->const_for_execution()) result_cached = true;
  return &buffer;
}

bool Item_typecast_vector::resolve_type(THD *thd) {
  if (Item_str_func::resolve_type(thd)) {
    return true;
  }
  if (args[0]->result_type() != STRING_RESULT) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    return true;
  }
  if (reject_geometry_args(arg_count, args, this)) return true;
  uint32 max_dims =
      (m_cast_dimensions >= 0) ? static_cast<uint32>(m_cast_dimensions)
                               : MAX_DIMENSIONS;
  set_data_type_vector(
      static_cast<ulonglong>(Field_vector::dimension_bytes(max_dims)));
  return false;
}

String *Item_typecast_vector::val_str(String *str) {
  assert(fixed);
  null_value = false;
  String *res = args[0]->val_str(str);
  if (res == nullptr || res->ptr() == nullptr) {
    null_value = true;
    return nullptr;
  }

  uint32 output_dims = MAX_DIMENSIONS;
  if (buffer.mem_realloc(Field_vector::dimension_bytes(output_dims)))
    return error_str();

  bool err = from_string_to_vector(res->charset(), res->ptr(), res->length(),
                                   buffer.ptr(), &output_dims);
  if (err) {
    if (output_dims == MAX_DIMENSIONS) {
      String truncated(*res);
      truncated.replace(32, 5, "... \0", 5);
      my_error(ER_DATA_OUT_OF_RANGE, MYF(0), truncated.ptr(), func_name());
    } else {
      my_error(ER_TO_VECTOR_CONVERSION, MYF(0), res->length(), res->ptr());
    }
    return error_str();
  }

  if (m_cast_dimensions >= 0 &&
      output_dims != static_cast<uint32>(m_cast_dimensions)) {
    my_error(ER_VECTOR_DIMENSION_MISMATCH, MYF(0),
             static_cast<uint>(m_cast_dimensions), output_dims);
    return error_str();
  }

  buffer.length(Field_vector::dimension_bytes(output_dims));
  return &buffer;
}

void Item_typecast_vector::print(const THD *thd, String *str,
                                 enum_query_type query_type) const {
  str->append(STRING_WITH_LEN("cast("));
  args[0]->print(thd, str, query_type);
  str->append(STRING_WITH_LEN(" as vector"));
  if (m_cast_dimensions >= 0) str->append_parenthesized(m_cast_dimensions);
  str->append(')');
}

bool Item_func_vec_totext::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_VARCHAR)) {
    return true;
  }
  bool valid_type = (args[0]->data_type() == MYSQL_TYPE_VARCHAR) ||
                    (args[0]->result_type() == STRING_RESULT &&
                     args[0]->collation.collation == &my_charset_bin);
  if (!valid_type) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    return true;
  }
  set_data_type_string(Item_func_vec_totext::max_output_bytes);
  return false;
}

String *Item_func_vec_totext::val_str(String *str) {
  assert(fixed);
  String *res = args[0]->val_str(str);
  null_value = false;
  if (res == nullptr || res->ptr() == nullptr) {
    return error_str();
  }

  if (from_vector_to_string(res, VECTOR_PRECISION, &my_charset_numeric,
                            &buffer)) {
    my_error(ER_VECTOR_BINARY_FORMAT_INVALID, MYF(0));
    return error_str();
  }

  return &buffer;
}

longlong Item_func_vector_dim::val_int() {
  assert(fixed);
  String *res = args[0]->val_str(&value);
  null_value = false;
  if (res == nullptr || res->ptr() == nullptr) {
    return error_int(); /* purecov: inspected */
  }
  uint32 dimensions = get_dimensions_low(res->length(), VECTOR_PRECISION);
  if (dimensions == UINT_MAX32) {
    my_error(ER_TO_VECTOR_CONVERSION, MYF(0), res->length(), res->ptr());
    return error_int(); /* purecov: inspected */
  }
  return (longlong)dimensions;
}
}  // namespace vidx
