/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: symmetric cryptographic
 */
#include "encdb_symmetric.h"
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_map>
#include "encdb_crypto_exception.h"
#include "encdb_crypto_provider.h"
#include "encdb_error.h"
#include "encdb_hash.h"
#include "encdb_symmetric_stream.h"

namespace encdb {

uint32_t Symmetric::get_key_size(Symmetric::Alg alg) {
  uint32_t key_size = 0;
  switch (alg) {
    case Symmetric::Alg::AES_128_GCM:
    case Symmetric::Alg::AES_128_CTR:
    case Symmetric::Alg::AES_128_ECB:
    case Symmetric::Alg::AES_128_CBC:
    case Symmetric::Alg::SM4_128_ECB:
    case Symmetric::Alg::SM4_128_CBC:
    case Symmetric::Alg::SM4_128_CTR:
    case Symmetric::Alg::SM4_128_GCM:
      key_size = 16;
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
  }

  return key_size;
}
uint32_t Symmetric::get_block_size(Symmetric::Alg alg) {
  uint32_t block_size = 0;
  switch (alg) {
    case Symmetric::Alg::AES_128_GCM:
    case Symmetric::Alg::AES_128_CTR:
    case Symmetric::Alg::AES_128_ECB:
    case Symmetric::Alg::AES_128_CBC:
    case Symmetric::Alg::SM4_128_ECB:
    case Symmetric::Alg::SM4_128_CBC:
    case Symmetric::Alg::SM4_128_CTR:
    case Symmetric::Alg::SM4_128_GCM:
      block_size = 16;
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
  }
  return block_size;
}

template <Symmetric::Params blockSize>
static uint32_t paddingSize(uint32_t length) noexcept {
  constexpr uint32_t size = static_cast<uint32_t>(blockSize);
  return size - length % size + length;
}
/**
 * @brief Calculate the exactly required cipher size to encrypt data of length
 * \c length using \c alg
 * @param [in]alg           algorithm used to form the cipher
 * @param [in]length        plaintext length
 * @return                  exactly required cipher size
 */
uint32_t Symmetric::size(uint32_t length, Symmetric::Alg alg) {
  uint32_t szCipher = 0;
  switch (alg) {
    // without padding
    case Symmetric::Alg::AES_128_GCM:
    case Symmetric::Alg::SM4_128_GCM:
      szCipher += sizeof(GCMCipher) + length;
      break;
    case Symmetric::Alg::AES_128_CTR:
    case Symmetric::Alg::SM4_128_CTR:
      szCipher += sizeof(CTRCipher) + length;
      break;
    // with padding
    case Symmetric::Alg::AES_128_ECB:
      szCipher += sizeof(ECBCipher) +
                  paddingSize<Symmetric::Params::AES_BLOCK_SIZE>(length);
      break;
    case Symmetric::Alg::AES_128_CBC:
      szCipher += sizeof(CBCCipher) +
                  paddingSize<Symmetric::Params::AES_BLOCK_SIZE>(length);
      break;
    case Symmetric::Alg::SM4_128_ECB:
      szCipher += sizeof(ECBCipher) +
                  paddingSize<Symmetric::Params::SM4_BLOCK_SIZE>(length);
      break;
    case Symmetric::Alg::SM4_128_CBC:
      szCipher += sizeof(CBCCipher) +
                  paddingSize<Symmetric::Params::SM4_BLOCK_SIZE>(length);
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
  }
  return szCipher;
}
/**
 * @brief Calculate the max required cipher size to encrypt data of length \c
 * length
 * @param [in]length        plaintext length
 * @return                  max required cipher size
 */
uint32_t Symmetric::size(uint32_t length) noexcept {
  uint32_t szMaxCipher = 0;
  szMaxCipher += sizeof(CipherBlob);
  // add padding size
  szMaxCipher += paddingSize<Symmetric::Params::MAX_BLOCK_SIZE>(length);
  return szMaxCipher;
}

const std::string to_string(Symmetric::Alg alg) noexcept {
  std::string str;
  switch (alg) {
    case Symmetric::Alg::AES_128_GCM:
      str = "AES_128_GCM";
      break;
    case Symmetric::Alg::AES_128_ECB:
      str = "AES_128_ECB";
      break;
    case Symmetric::Alg::AES_128_CTR:
      str = "AES_128_CTR";
      break;
    case Symmetric::Alg::AES_128_CBC:
      str = "AES_128_CBC";
      break;
    case Symmetric::Alg::SM4_128_GCM:
      str = "SM4_128_GCM";
      break;
    case Symmetric::Alg::SM4_128_CBC:
      str = "SM4_128_CBC";
      break;
    case Symmetric::Alg::SM4_128_ECB:
      str = "SM4_128_ECB";
      break;
    case Symmetric::Alg::SM4_128_CTR:
      str = "SM4_128_CTR";
      break;
    default:
      str = "ALG_NOT_SUPPORT";
      break;
  }
  return str;
}

/**
 * @brief encrypt \c in data with given \c key and \c iv using \c alg algorithm
 * @param [out]out          encrypted result, in the format specified by
 * algorithm
 * @param [in]alg           algorithm used to do encrypt
 * @param [in]key           key used to do encrypt
 * @param [in]iv            iv used to do encrypt, IV will be truncated if IV is
 * too long
 * @param [in]in            data to be encrypted
 */
void Symmetric::encrypt(bytes_t &out, Alg alg, const bytes_t &key,
                        const bytes_t &iv, const bytes_t &in,
                        const Flags &flags) {
  bytes_t tag, cc, aad;
  uint32_t ivlen = 0;

  uint32_t expected_size = Symmetric::size(in.size(), alg);
  if (out.size() < expected_size) {
    throw ResultOutOfMemoryException("Not enough memory for result cipher",
                                     expected_size);
  }

  bool enable_stream_mode = false;
  out.resize(expected_size);
  CipherBlob *cipher = reinterpret_cast<CipherBlob *>(out.data());
  switch (alg) {
    case Alg::AES_128_GCM:
    case Alg::SM4_128_GCM:
      cipher->gcm.iv.fill(0);
      tag.set(cipher->gcm.tag.data(), cipher->gcm.tag.size(), true);
      cc.set(cipher->gcm.data, out.size() - sizeof(GCMCipher), true);
      ivlen = sizeof(cipher->gcm.iv);
      break;
    case Alg::AES_128_ECB:
    case Alg::SM4_128_ECB:
      cc.set(cipher->ecb.data, out.size() - sizeof(ECBCipher), true);
      ivlen = 0;
      break;
    case Alg::AES_128_CTR:
    case Alg::SM4_128_CTR:
      cipher->ctr.iv.fill(0);
      cc.set(cipher->ctr.data, out.size() - sizeof(CTRCipher), true);
      ivlen = sizeof(cipher->ctr.iv);
      // for CTR mode, check whether use STREAM_MODE to boost encryption
      // FIXME: for DET mode, we do NOT support stream mode
      enable_stream_mode = flags & Flags::STREAM_MODE & (~Flags::DET);
      break;
    case Alg::AES_128_CBC:
    case Alg::SM4_128_CBC:
      cipher->cbc.iv.fill(0);
      cc.set(cipher->cbc.data, out.size() - sizeof(CBCCipher), true);
      ivlen = sizeof(cipher->cbc.iv);
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
      break;
  }
  memset(out.data(), 0, ivlen);
  ivlen = std::min(ivlen, iv.size());
  memcpy(out.data(), iv.data(), ivlen);
  bytes_t tmp_iv(out.data(), ivlen);

  // for large plaintext, there is no need to use stream mode even enabled.
  if (enable_stream_mode && in.size() < Symmetric::MIN_BATCH_SIZE) {
    CryptorHandle cryptor = StreamCipherCache::getCryptor(alg, key, flags);
    cryptor->encrypt(cc, in, tmp_iv);
  } else {
    encdb_symmetric_encrypt(cc, alg, key, tmp_iv, in, tag, aad, flags);
  }
}
bytes_t Symmetric::encrypt(Alg alg, const bytes_t &key, const bytes_t &iv,
                           const bytes_t &in, const Flags &flags) {
  bytes_t out(Symmetric::size(in.size(), alg));
  Symmetric::encrypt(out, alg, key, iv, in, flags);
  return out;
}

bytes_t generateDetIv(Symmetric::Alg alg, const bytes_t &key,
                      const bytes_t &in) {
  // hmac to get DET IV
  Hash::Alg hash_alg;
  switch (alg) {
    case Symmetric::Alg::SM4_128_ECB:
    case Symmetric::Alg::SM4_128_CBC:
    case Symmetric::Alg::SM4_128_CTR:
    case Symmetric::Alg::SM4_128_GCM:
      hash_alg = Hash::Alg::SM3;
      break;
    default:
      hash_alg = Hash::Alg::SHA256;
  }
  return Hash::hmac(hash_alg, key, in);
}
/**
 * @brief encrypt \c in data with given \c key and IV deriving from \c in (i.e.,
 * DET mode) using \c alg algorithm
 * @param [out]out          encrypted result, in the format specified by
 * algorithm
 * @param [in]alg           algorithm used to do encrypt
 * @param [in]key           key used to do encrypt
 * @param [in]in            data to be encrypted
 */
void Symmetric::encrypt(bytes_t &out, Alg alg, const bytes_t &key,
                        const bytes_t &in, const Flags &flags) {
  bytes_t iv = generateDetIv(alg, key, in);
  Symmetric::encrypt(out, alg, key, iv, in,
                     static_cast<Flags>(flags | Flags::DET));
}
bytes_t Symmetric::encrypt(Alg alg, const bytes_t &key, const bytes_t &in,
                           const Flags &flags) {
  bytes_t out(Symmetric::size(in.size(), alg));
  Symmetric::encrypt(out, alg, key, in, flags);
  return out;
}

/**
 * @brief decrypt \c in data with given \c key using \c alg algorithm
 * @param [out]out          decrypted result, i.e., plaintext
 * @param [in]alg           algorithm used to do decrypt
 * @param [in]key           key used to do decrypt
 * @param [in]iv            iv used to do decrypt
 * @param [in]in            cipher data to be decrypted, in the format specified
 * by algorithm
 */
void Symmetric::decrypt(bytes_t &out, Alg alg, const bytes_t &key,
                        const bytes_t &in, const Flags &flags) {
  bytes_t tag, iv, cc;

  const CipherBlob *cipher = reinterpret_cast<const CipherBlob *>(in.data());
  switch (alg) {
    case Alg::AES_128_GCM:
    case Alg::SM4_128_GCM:
      iv.set(cipher->gcm.iv.data(), cipher->gcm.iv.size(), true);
      tag.set(cipher->gcm.tag.data(), cipher->gcm.tag.size(), true);
      cc.set(cipher->gcm.data, in.size() - sizeof(GCMCipher), true);
      break;
    case Alg::AES_128_ECB:
    case Alg::SM4_128_ECB:
      cc.set(cipher->ecb.data, in.size() - sizeof(ECBCipher), true);
      break;
    case Alg::AES_128_CTR:
    case Alg::SM4_128_CTR:
      iv.set(cipher->ctr.iv.data(), cipher->ctr.iv.size(), true);
      cc.set(cipher->ctr.data, in.size() - sizeof(CTRCipher), true);
      break;
    case Alg::AES_128_CBC:
    case Alg::SM4_128_CBC:
      iv.set(cipher->cbc.iv.data(), cipher->cbc.iv.size(), true);
      cc.set(cipher->cbc.data, in.size() - sizeof(CBCCipher), true);
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
      break;
  }

  // FIXME: except GCM/CTR, other modes may contain padding bytes
  uint32_t expected_size = cc.size();
  if (out.size() < expected_size) {
    throw ResultOutOfMemoryException("Not enough memory for result plaintext",
                                     expected_size);
  }

  try {
    // throw ResultOutOfMemoryException if not enough memory for out
    uint32_t exact_size =
        encdb_symmetric_decrypt(out, alg, key, iv, cc, tag, bytes_t(), flags);
    // resize to real size
    out.resize(exact_size);
  } catch (std::bad_alloc &e) {
    throw OutOfMemoryException("throw on decrypt");
  }
}
bytes_t Symmetric::decrypt(Alg alg, const bytes_t &key, const bytes_t &in,
                           const Flags &flags) {
  bytes_t out(Symmetric::dataSize(alg, in));
  Symmetric::decrypt(out, alg, key, in, flags);
  return out;
}

uint32_t Symmetric::ivSize(Alg alg) {
  switch (alg) {
    case Alg::AES_128_CBC:
    case Alg::SM4_128_CBC:
      return static_cast<uint32_t>(Params::CBC_IV_SIZE);
    case Alg::AES_128_CTR:
    case Alg::SM4_128_CTR:
      return static_cast<uint32_t>(Params::CTR_IV_SIZE);
    case Alg::AES_128_ECB:
    case Alg::SM4_128_ECB:
      return static_cast<uint32_t>(Params::ECB_IV_SIZE);
    case Alg::AES_128_GCM:
    case Alg::SM4_128_GCM:
      return static_cast<uint32_t>(Params::GCM_IV_SIZE);
    default:
      throw NoSuchAlgorithmException(to_string(alg));
      // return is added to supppress compiler warning
      return 0;
  }
}

uint32_t Symmetric::keySize(Alg alg) {
  switch (alg) {
    case Alg::AES_128_CBC:
    case Alg::AES_128_CTR:
    case Alg::AES_128_ECB:
    case Alg::AES_128_GCM:
      return static_cast<uint32_t>(Params::AES_128_KEY_SIZE);
    case Alg::SM4_128_CBC:
    case Alg::SM4_128_CTR:
    case Alg::SM4_128_ECB:
    case Alg::SM4_128_GCM:
      return static_cast<uint32_t>(Params::SM4_128_KEY_SIZE);
    default:
      throw NoSuchAlgorithmException(to_string(alg));
      // return is added to supppress compiler warning
      return 0;
  }
}

/**
 * @brief Calculate the closest plaintext size of encrypt data \c cipherBlob
 * @param [in]cipherBlob    encrypted cipher blob
 * @return                  closest plaintext size
 */
uint32_t Symmetric::dataSize(Alg alg, const bytes_t &cipherBlob) {
  uint32_t headerSize = 0;
  switch (alg) {
    case Alg::AES_128_GCM:
    case Alg::SM4_128_GCM:
      headerSize = sizeof(GCMCipher);
      break;
    case Alg::AES_128_ECB:
    case Alg::SM4_128_ECB:
      headerSize = sizeof(ECBCipher);
      break;
    case Alg::AES_128_CTR:
    case Alg::SM4_128_CTR:
      headerSize = sizeof(CTRCipher);
      break;
    case Alg::SM4_128_CBC:
    case Alg::AES_128_CBC:
      headerSize = sizeof(CBCCipher);
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
  }

  if (cipherBlob.size() < headerSize) {
    throw BadCipherException("cipher blob is shorter than blob header");
  }

  return cipherBlob.size() - headerSize;
}

Symmetric::Alg to_symmetric_alg(const std::string &str) {
  if (str == "AES_128_GCM") {
    return Symmetric::Alg::AES_128_GCM;
  } else if (str == "AES_128_ECB") {
    return Symmetric::Alg::AES_128_ECB;
  } else if (str == "AES_128_CTR") {
    return Symmetric::Alg::AES_128_CTR;
  } else if (str == "AES_128_CBC") {
    return Symmetric::Alg::AES_128_CBC;
  } else if (str == "SM4_128_CBC") {
    return Symmetric::Alg::SM4_128_CBC;
  } else if (str == "SM4_128_ECB") {
    return Symmetric::Alg::SM4_128_ECB;
  } else if (str == "SM4_128_CTR") {
    return Symmetric::Alg::SM4_128_CTR;
  } else if (str == "SM4_128_GCM") {
    return Symmetric::Alg::SM4_128_GCM;
  } else {
    throw NoSuchAlgorithmException(str);
    // return is added to supppress compiler warning
    return Symmetric::Alg::__COUNT__;
  }
}

}  // namespace encdb
