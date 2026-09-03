#include "encdb_encode_impl.h"
#include "auto_encode.h"
#include "encdb_common_exception.h"
#include "mask.h"
#include "mysql.hh"
#include "unique_ptr_patch.h"

#include <string>
#include "bytes.h"
#include "encdb.h"
#include "encdb_encode_types.h"
#include "masking_util.h"

namespace encdb {

static std::shared_ptr<encdb::AutoEncode> backup_auto_encoder = nullptr;

EncdbEncode::EncdbEncode(const std::string &name, const std::string &params)
    : Encode(name),
      enc_alg_(EncodeBasicAlgo::__INVALIDALGO__),
      mask_alg_(EncodeBasicAlgo::__INVALIDALGO__),
      alg_params_(params) {
  size_t connector_pos;
  std::string encrypt_alg = name;
  std::string mask_alg = name;
  if ((connector_pos = name.find(ALGO_CONNECTOR)) != std::string::npos) {
    // composite algorithm
    encrypt_alg = name.substr(0, connector_pos);
    mask_alg = name.substr(connector_pos + strlen(ALGO_CONNECTOR));
    this->enc_alg_ = from_string(encrypt_alg);
    this->mask_alg_ = from_string(mask_alg);
  } else {
    // just 1 basic algorithm, encrypt or mask
    EncodeBasicAlgo algo = from_string(name);
    EncodeAlgoClass algo_class = get_encode_class(algo);
    if (algo_class == EncodeAlgoClass::ENCRYPT) {
      this->enc_alg_ = algo;
    } else if (algo_class == EncodeAlgoClass::MASK) {
      this->mask_alg_ = algo;
    } else {
      // error will be thrown later
    }
  }

  // try init backup encoder if needed
  if (backup_auto_encoder == nullptr) {
    backup_auto_encoder =
        std::make_shared<encdb::AutoEncode>("backup-auto", "{}");
  }
}
EncdbEncode::~EncdbEncode() {}
bool EncdbEncode::is_variadic_encoding() { return true; }
bool EncdbEncode::need_encrypting() {
  return this->enc_alg_ != EncodeBasicAlgo::__INVALIDALGO__;
}
uint32_t EncdbEncode::get_encode_size(uint32_t len,
                                      const ENCODE_PARA &para) const {
  try {
    if (this->enc_alg_ == EncodeBasicAlgo::__INVALIDALGO__ &&
        this->mask_alg_ == EncodeBasicAlgo::__INVALIDALGO__) {
      return len +
             10 * para.field_num;  // extra room for result length bytes
                                   // A mysql length-encoded string can take
                                   // 8 bytes maximum as a length info
    } else if (this->enc_alg_ != EncodeBasicAlgo::__INVALIDALGO__ &&
               this->mask_alg_ == EncodeBasicAlgo::__INVALIDALGO__) {
      return EncdbCipher::expectedCipherSize(len, para.field_num) +
             10 * para.field_num;
    } else if (this->enc_alg_ == EncodeBasicAlgo::__INVALIDALGO__ &&
               this->mask_alg_ != EncodeBasicAlgo::__INVALIDALGO__) {
      return AbstractMask::getMaskInstance(to_string(this->mask_alg_),
                                           this->alg_params_)
                 ->maxResultSize(len) +
             10 * para.field_num;
    } else {
      return EncdbCipher::expectedCipherSize(
                 AbstractMask::getMaskInstance(to_string(this->mask_alg_),
                                               this->alg_params_)
                     ->maxResultSize(len),
                 para.field_num) +
             10 * para.field_num;
    }
  } catch (const EncdbException &e) {
    REPORT_ENCDB_ERROR(e);
    return len;
  }
}
encode_ret EncdbEncode::variadic_encode(ENCODE_VALUE &value,
                                        const ENCODE_PARA &para,
                                        uint8_t *result, uint32_t *result_len) {
  CQRResultsetValue v = encode_value_convert(value);
  encode_ret ret;
  if ((this->mask_alg_ != EncodeBasicAlgo::__INVALIDALGO__ &&
       (!v.is_string() && !v.is_null())) ||
      (this->mask_alg_ == EncodeBasicAlgo::__INVALIDALGO__ &&
       this->enc_alg_ == EncodeBasicAlgo::__INVALIDALGO__)) {
    // Possible situations:
    // 1. User specified a mask algo correctly, but on a data type inapplcable.
    // 2. User tried to specify mask&enc algos, but mis config.
    // In both cases, we use auto encode for data protection.
    ENCDB_LOG(
        LL_WARNING,
        "auto encode is used, because mask algo %d is applied on incorrect "
        "data type %d or encode algo is not configured properly with %s, %s",
        this->mask_alg_, v.get_type(), this->m_name.c_str(),
        this->alg_params_.c_str());
    this->mask_alg_ = EncodeBasicAlgo::__INVALIDALGO__;
    // in-place encode
    ret = backup_auto_encoder->encode(value, para);
    if (ret != ENCODE_SUCC) {
      return ret;
    }
  }
  // NULL handling
  if (v.is_null()) {
    if (v.is_binary_result()) {
      *result_len = 0;
    } else {
      *result = (uint8_t)0xfb;
      *result_len = 1;
    }
    return ENCODE_SUCC;
  } else {
    bytes_t data = v.to_bytes();
    if (encode_bytes(data, para, result, result_len, v.get_type())) {
      return ENCODE_SUCC;
    }
  }
  return ENCODE_FAIL;
}
bool EncdbEncode::encode_bytes(const bytes_t &b, const ENCODE_PARA &para,
                               uint8_t *result, uint32_t *result_len,
                               enum_field_types type) const {
  *result_len = 0;
  size_t data_len = b.size();
  const uint8_t *start = b.data();
  int status = 0;
  size_t len = 0;
  uint32_t encoded_length = this->get_encode_size(data_len, para);
  auto temp_buffer = std::make_unique<uint8_t[]>(encoded_length);
  std::string tmp_str((const char *)start, data_len);

  do {
    if (this->mask_alg_ == EncodeBasicAlgo::__INVALIDALGO__ &&
        this->enc_alg_ == EncodeBasicAlgo::__INVALIDALGO__) {
      // Although error occured, we have protected data with auto encoder.
      // Just print error message, and return success.
      ENCDB_LOG(LL_ERROR, "no valid algo speicified with name %s",
                this->m_name.c_str());
      break;
    }
    // 1. Mask
    if (this->mask_alg_ != EncodeBasicAlgo::__INVALIDALGO__) {
      try {
        auto masker = AbstractMask::getMaskInstance(to_string(this->mask_alg_),
                                                    this->alg_params_);
        tmp_str = masker->maskString(tmp_str);
      } catch (const EncdbException &e) {
        REPORT_ENCDB_ERROR(e);
        break;
      }
    }
    // 2. Encrypt
    if (this->enc_alg_ != EncodeBasicAlgo::__INVALIDALGO__) {
      status =
          EncdbCipher::encrypt(para.user, temp_buffer.get(), &encoded_length,
                               (const uint8_t *)tmp_str.data(), tmp_str.size(),
                               (int32_t)type, (uint8_t)this->enc_alg_);
      if (status != 0) {
        break;
      }
    } else {
      memcpy(temp_buffer.get(), tmp_str.data(), tmp_str.size());
      encoded_length = tmp_str.size();
    }

    len = mxs_set_leint_bytes(result, encoded_length);
    result += len;
    *result_len += len;
    memcpy(result, temp_buffer.get(), encoded_length);
    result += encoded_length;
    *result_len += encoded_length;
    return true;
  } while (0);

  // Use input data
  len = mxs_set_leint_bytes(result, data_len);
  result += len;
  *result_len += len;
  memcpy(result, start, data_len);
  result += data_len;
  *result_len += data_len;
  return true;
}

}  // namespace encdb
