#include <memory>
#include <string>
#include <vector>

#include "../global_defines.h"

#include "bloomfilter.h"
#include "expr.h"
#include "log.h"
#include "meta.h"
#include "murmurhash3.h"

#ifndef MYSQL8
static inline std::string to_string(const String &str) {
  return std::string(str.ptr(), str.length());
}
#endif

namespace rpc_executor {

constexpr int64_t BITS_PER_BYTE = 8;
constexpr char FIRST_BIT_MASK = 0x80;
constexpr int64_t INT_SIZE = sizeof(int);
constexpr int64_t LONG_SIZE = sizeof(long long);
constexpr int64_t FLOAT_SIZE = sizeof(float);
constexpr int64_t DOUBLE_SIZE = sizeof(double);

int BloomFilterItem::init(std::vector<ExprItem *> &param_exprs) {
  int ret = HA_EXEC_SUCCESS;
  if (param_exprs.size() != 5) {
    ret = HA_ERR_UNSUPPORTED;
    log_exec_error(
        "BloomFilterItem needs  4 params, "
        "actually get %lu",
        param_exprs.size());
  } else {
    target_item_ = param_exprs[0];
    total_bits_ = param_exprs[1]->val_int();
    number_hash_ = param_exprs[2]->val_int();
    String str_buffer;
    strategy_.assign(to_string(*(param_exprs[3]->val_str(&str_buffer))));
    String *bitmap_str = param_exprs[4]->val_str(&str_buffer);
    bitmap_size_ = bitmap_str->length();
    bitmap_ = bitmap_str->ptr();
    if (bitmap_size_ < (total_bits_ + BITS_PER_BYTE - 1) / BITS_PER_BYTE) {
      ret = HA_ERR_UNSUPPORTED;
      log_exec_error(
          "BloomFilterItem param wrong, bitmap too small, "
          "total_bits: %ld, bitmap_size: %ld",
          total_bits_, bitmap_size_);
    }
  }
  return ret;
}

int BloomFilterItem::may_contain(bool &contain) {
  int ret = HA_EXEC_SUCCESS;

  // Allow false positive.
  contain = true;
  if (target_item_->is_null()) {
    return ret;
  }

  uchar hash_bytes[16];
#ifdef MYSQL8
  switch (target_item_->data_type()) {
#else
  switch (target_item_->field_type()) {
#endif
    case MYSQL_TYPE_NULL:
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_JSON: {
      String str_buffer;
      String *key_str = target_item_->val_str(&str_buffer);
      if (!key_str) {
        ret = HA_ERR_INTERNAL_ERROR;
        log_exec_error(
            "BloomFilterItem hash for string, string is null, "
            "item->is_null(): %d, item->null_value: %d",
            target_item_->is_null(), target_item_->null_value);
      } else {
        MurmurHash3_x64_128(key_str->ptr(), key_str->length(), 0, hash_bytes);
      }
      break;
    }

    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG: {
      int64_t data = target_item_->val_int();
      if (target_item_->null_value) {
        ret = HA_ERR_INTERNAL_ERROR;
        log_exec_error(
            "target_item_->val_* return null value, "
            "target_item_->is_null(): %d",
            target_item_->is_null());
      } else {
        MurmurHash3_x64_128(&data, INT_SIZE, 0, hash_bytes);
      }
      break;
    }

    case MYSQL_TYPE_LONGLONG: {
      int64_t data = target_item_->val_int();
      if (target_item_->null_value) {
        ret = HA_ERR_INTERNAL_ERROR;
        log_exec_error(
            "target_item_->val_* return null value, "
            "target_item_->is_null(): %d",
            target_item_->is_null());
      } else {
        MurmurHash3_x64_128(&data, LONG_SIZE, 0, hash_bytes);
      }
      break;
    }

    case MYSQL_TYPE_FLOAT: {
      float data = static_cast<float>(target_item_->val_real());
      if (target_item_->null_value) {
        ret = HA_ERR_INTERNAL_ERROR;
        log_exec_error(
            "target_item_->val_* return null value, "
            "target_item_->is_null(): %d",
            target_item_->is_null());
      } else {
        MurmurHash3_x64_128(&data, FLOAT_SIZE, 0, hash_bytes);
      }
      break;
    }

    case MYSQL_TYPE_DOUBLE: {
      double data = target_item_->val_real();
      if (target_item_->null_value) {
        ret = HA_ERR_INTERNAL_ERROR;
        log_exec_error(
            "target_item_->val_* return null value, "
            "target_item_->is_null(): %d",
            target_item_->is_null());
      } else {
        MurmurHash3_x64_128(&data, DOUBLE_SIZE, 0, hash_bytes);
      }
      break;
    }

    default:
      ret = HA_ERR_UNSUPPORTED;
      // contain remains true
      log_exec_error(
          "BloomFilterItem hash on target_item_ failed, "
          "data_type: %d does not support",
#ifdef MYSQL8
          target_item_->data_type());
#else
          target_item_->field_type());
#endif
  }

  if (!ret) {
    int64_t hash1 = *(reinterpret_cast<int64_t *>(hash_bytes));
    int64_t hash2 = *(reinterpret_cast<int64_t *>(hash_bytes + 8));
    log_debug(
        "BloomFilterItem murmurhash3 result: "
        "hash1: %ld, %02x %02x %02x %02x %02x %02x %02x %02x "
        "hash2: %ld, %02x %02x %02x %02x %02x %02x %02x %02x",
        hash1, hash_bytes[0], hash_bytes[1], hash_bytes[2], hash_bytes[3],
        hash_bytes[4], hash_bytes[5], hash_bytes[6], hash_bytes[7], hash2,
        hash_bytes[8], hash_bytes[9], hash_bytes[10], hash_bytes[11],
        hash_bytes[12], hash_bytes[13], hash_bytes[14], hash_bytes[15]);

    int64_t combined_hash = hash1;
    int64_t max_int64 = std::numeric_limits<int64_t>::max();
    for (int64_t i = 0; i < number_hash_; ++i) {
      int64_t bit_index = (combined_hash & max_int64) % total_bits_;
      int64_t byte_index = bit_index / BITS_PER_BYTE;
      char byte_mask = 1 << (bit_index % BITS_PER_BYTE);
      if (!(bitmap_[byte_index] & byte_mask)) {
        contain = false;
        break;
      }
      combined_hash += hash2;
    }
  }
  return ret;
}

}  // namespace rpc_executor