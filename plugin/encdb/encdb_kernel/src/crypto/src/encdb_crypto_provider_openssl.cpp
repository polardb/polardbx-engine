/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: crypto APIs based on OpenSSL
 */
#include "encdb_crypto_provider.h"
// symmetric
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/opensslv.h>
// rand
#include <openssl/rand.h>
// hash, kdf, hmac
#include <openssl/rsa.h>
#if (OPENSSL_VERSION_NUMBER >= 0x10100000L)
#include <openssl/kdf.h>
#endif
#include <openssl/hmac.h>
// asymmetric
#ifdef __x86_64__
#include <cpuid.h>
#endif
#include <openssl/pem.h>
#include <memory>
#include <unordered_map>
#include "bytes.h"
#include "concurrent_limited_cache.hpp"
#include "encdb_crypto_exception.h"
#include "unique_ptr_patch.h"

namespace encdb {

const bytes_t SM2_DEFAULT_ID(
    reinterpret_cast<const uint8_t *>("1234567812345678"), 16);

#ifndef OPENSSL_IS_SUCCESS
#define OPENSSL_IS_SUCCESS(x) ((x) > 0)
#endif

#ifdef __x86_64__
uint8_t support_rdrand() {
  static uint8_t init = 0;
  static uint8_t ret = 0;
  if (!init) {
    unsigned int eax, ebx, ecx, edx;
    __cpuid(1, eax, ebx, ecx, edx);
    if (ecx & bit_RDRND) {
      ret = 1;
    } else {
      ret = 0;
    }
    init = 1;
  }
  return ret;
}

void rdrand64_step(uint64_t *rand) {
  uint8_t ok;
  do {
    asm volatile("rdrand %0; setc %1" : "=r"(*rand), "=qm"(ok));
  } while (!ok);
}
void rdrand_get_bytes(uint8_t *bytes, size_t nbytes) {
  size_t dwords = nbytes / sizeof(uint64_t);
  for (size_t i = 0; i < dwords; i++) {
    rdrand64_step(reinterpret_cast<uint64_t *>(bytes + i * sizeof(uint64_t)));
  }
  if (nbytes % sizeof(uint64_t)) {
    uint64_t left;
    rdrand64_step(&left);
    memcpy(bytes + dwords * sizeof(uint64_t), &left, nbytes % sizeof(uint64_t));
  }
}
#endif

void openssl_get_bytes(uint8_t *bytes, size_t nbytes) {
  // RAND_xxx is slow, use aes-ctr encryption to boost performance
  thread_local uint8_t seed_bytes[16];
  thread_local uint64_t count = 0;

  if (count == 0) {
    // use random bytes from RAND_xxx as secret seed
    RAND_bytes(seed_bytes, sizeof(seed_bytes));
  }

  // refer to output buffer
  bytes_t rnd(bytes, nbytes);
  bytes_t key(seed_bytes, sizeof(seed_bytes));
  bytes_t iv(seed_bytes, sizeof(seed_bytes));
  bytes_t tag;
  encdb_symmetric_encrypt(rnd, Symmetric::Alg::AES_128_CTR, key, iv, rnd, tag,
                          bytes_t(), Symmetric::Flags::DEFAULT);

  // feed back cipher output to update seed, one byte is enough
  seed_bytes[count % sizeof(seed_bytes)] ^= bytes[count % nbytes];
  count += ((nbytes - 1) / (uint32_t)(Symmetric::Params::AES_BLOCK_SIZE) + 1);
}

void encdb_rand_bytes(bytes_t &rnd) {
#ifdef __x86_64__
  if (support_rdrand()) {
    rdrand_get_bytes(rnd.data(), rnd.size());
  } else {
    openssl_get_bytes(rnd.data(), rnd.size());
  }
#else
  // ARM64 uses __aarch64__, so use openssl rand
  openssl_get_bytes(rnd.data(), rnd.size());
#endif  // __x86_64__
}

constexpr int Tries = 100;

/*
 * @brief wrap EVP_CIPHER_CTX
 */
class EvpCipherCtx : public EncdbBaseObj {
 protected:
  EVP_CIPHER_CTX *handle_;

 public:
  EvpCipherCtx(const EvpCipherCtx &) = delete;
  void operator=(const EvpCipherCtx &) = delete;

  EvpCipherCtx() {
    handle_ = EVP_CIPHER_CTX_new();
    if (!handle_) {
      throw OutOfMemoryException("EVP_CIPHER_CTX_new fail");
    }
  }
  EvpCipherCtx(const EVP_CIPHER_CTX *in) : EvpCipherCtx() {
    if (!OPENSSL_IS_SUCCESS(EVP_CIPHER_CTX_copy(handle_, in))) {
      throw CryptoException(ERROR_CRYPTO_SYMMETRIC, "EVP_CIPHER_CTX_copy fail");
    }
  }
  ~EvpCipherCtx() { EVP_CIPHER_CTX_free(handle_); }
  EVP_CIPHER_CTX *getCtx() const noexcept { return handle_; }
};
using CtxHandle = std::shared_ptr<EvpCipherCtx>;

CtxHandle SymmetricCryptInit(const EVP_CIPHER *evpCipher, const uint8_t *key,
                             uint32_t keyLen, const CryptoOpType &cop_type) {
  int ret;
  CtxHandle ctx_handle = std::shared_ptr<EvpCipherCtx>(new EvpCipherCtx());
  EVP_CIPHER_CTX *ctx = ctx_handle->getCtx();
  int enc = (cop_type == CryptoOpType::ENCRYPT) ? 1 : 0;

  // Set cipher type and mode
  int cnt = 0;
  do {
    ret = EVP_CipherInit_ex(ctx, evpCipher, NULL, NULL, NULL, enc);
  } while (!OPENSSL_IS_SUCCESS(ret) && cnt++ < Tries);
  if (!OPENSSL_IS_SUCCESS(ret)) {
    throw CryptoException(ERROR_CRYPTO_SYMMETRIC,
                          "EVP_CipherInit_ex cipher fail");
  }

  // Initialise key
  if ((int)keyLen < EVP_CIPHER_key_length(evpCipher)) {
    throw InvalidAlgorithmParameterException(
        "Invalid key length " + std::to_string(keyLen) + ", expected " +
        std::to_string(EVP_CIPHER_key_length(evpCipher)));
  }
  ret = EVP_CipherInit_ex(ctx, NULL, NULL, key, NULL, enc);
  if (!OPENSSL_IS_SUCCESS(ret)) {
    throw InvalidKeyException("EVP_CipherInit_ex key fail");
  }

  return ctx_handle;
}

uint32_t SymmetricCryptPending(CtxHandle &ctx_handle, const uint8_t *iv,
                               uint32_t ivlen, const uint8_t *in,
                               uint32_t inlen, const uint8_t *aad,
                               uint32_t aadlen, uint8_t *out,
                               uint32_t out_cap_len, bool no_padding) {
  int ret;
  EVP_CIPHER_CTX *ctx = ctx_handle->getCtx();
  int enc = EVP_CIPHER_CTX_encrypting(ctx);

  // Initialise IV
  if (ivlen) {
    uint32_t expected_iv_len = EVP_CIPHER_CTX_iv_length(ctx);
    if (ivlen < expected_iv_len) {
      throw InvalidAlgorithmParameterException(
          "Invalid iv length " + std::to_string(ivlen) + ", expected " +
          std::to_string(expected_iv_len));
    }
    ret = EVP_CipherInit_ex(ctx, NULL, NULL, NULL, iv, enc);
    if (!OPENSSL_IS_SUCCESS(ret)) {
      throw InvalidAlgorithmParameterException("EVP_CipherInit_ex iv fail");
    }
  } else {
    // for EVP_CIPH_ECB_MODE ==EVP_CIPHER_CTX_mode(ctx), reset context state
    // only
    ret = EVP_CipherInit_ex(ctx, NULL, NULL, NULL, NULL, enc);
    if (!OPENSSL_IS_SUCCESS(ret)) {
      throw InvalidAlgorithmParameterException(
          "EVP_CipherInit_ex reset context state fail");
    }
  }

  if (no_padding) {
    int block_size = EVP_CIPHER_CTX_block_size(ctx);
    if (inlen % block_size != 0) {
      throw InvalidAlgorithmParameterException(
          "Set no padding while input length is not multipule times of block "
          "size");
    }
    ret = EVP_CIPHER_CTX_set_padding(ctx, 0);
    if (!OPENSSL_IS_SUCCESS(ret)) {
      throw InvalidAlgorithmParameterException(
          "EVP_CIPHER_CTX_set_padding disable padding fail");
    }
  }

  // Zero or more calls to specify any AAD
  if (aad && aadlen) {
    int outLen;
    ret = EVP_CipherUpdate(ctx, NULL, &outLen, aad, aadlen);
    if (!OPENSSL_IS_SUCCESS(ret)) {
      throw InvalidAlgorithmParameterException("EVP_CipherUpdate aad fail");
    }
  }

  /*
   * Encrypt plaintext
   * NOTE: for GCM, nullptr input will lead to EVP_CipherFinal_ex fail, fix it
   * by referring to non-null value
   */
  if (in && inlen) {
    uint8_t tmp[EVP_MAX_BLOCK_LENGTH];
    int outLen = 0;
    if (inlen <= EVP_MAX_BLOCK_LENGTH) {
      ret = EVP_CipherUpdate(ctx, tmp, &outLen, in, inlen);
      if (outLen <= (int)out_cap_len) {
        memcpy(out, tmp, outLen);
      }
    } else {
      ret = EVP_CipherUpdate(ctx, out, &outLen, in, inlen);
    }
    if (!OPENSSL_IS_SUCCESS(ret)) {
      throw CryptoException(ERROR_CRYPTO_SYMMETRIC,
                            "EVP_CipherUpdate data fail");
    }

    if (outLen > (int)out_cap_len) {
      throw ResultOutOfMemoryException(
          "Error: in pending stage - not enough memory for result cipher",
          outLen);
    }

    return outLen;
  } else if (in == nullptr && inlen) {
    throw InvalidAlgorithmParameterException(
        "input is nullptr but of non-zero length");
  } else {
    // do nothing
    return 0;
  }
}
uint32_t SymmetricCryptFinal(CtxHandle &ctx_handle, uint8_t *tag,
                             uint32_t tagLen, uint8_t *out,
                             uint32_t out_cap_len) {
  int ret;
  EVP_CIPHER_CTX *ctx = ctx_handle->getCtx();
  int enc = EVP_CIPHER_CTX_encrypting(ctx);

  // Set expected tag value.
  if (0 == enc && tag && tagLen) {
#if (OPENSSL_VERSION_NUMBER >= 0x10100000L)
    int evp_ctrl_type = EVP_CTRL_AEAD_SET_TAG;
#else
    int evp_ctrl_type = EVP_CTRL_GCM_SET_TAG;
#endif
    ret = EVP_CIPHER_CTX_ctrl(
        ctx, evp_ctrl_type, tagLen,
        reinterpret_cast<void *>(const_cast<uint8_t *>(tag)));
    if (!OPENSSL_IS_SUCCESS(ret)) {
      throw InvalidAlgorithmParameterException(
          "EVP_CIPHER_CTX_ctrl set tag fail, typically tag is wrong or cipher "
          "was corrupted");
    }
  }

  // Finalise: note get no output for no padding
  uint8_t tmp[EVP_MAX_BLOCK_LENGTH];
  int outLen;
  ret = EVP_CipherFinal_ex(ctx, tmp, &outLen);
  if (!OPENSSL_IS_SUCCESS(ret)) {
    throw BadPaddingException("EVP_CipherFinal_ex fail");
  }
  if (outLen > (int)out_cap_len) {
    throw ResultOutOfMemoryException(
        "Error: in final stage - not enough memory for result cipher", outLen);
  }
  memcpy(out, tmp, outLen);

  // restore default padding state, i.e., enabled
  ret = EVP_CIPHER_CTX_set_padding(ctx, 1);
  if (!OPENSSL_IS_SUCCESS(ret)) {
    throw InvalidAlgorithmParameterException(
        "EVP_CIPHER_CTX_set_padding enable padding fail");
  }

  // Get tag
  if (1 == enc && tag && tagLen) {
#if (OPENSSL_VERSION_NUMBER >= 0x10100000L)
    int evp_ctrl_type = EVP_CTRL_AEAD_GET_TAG;
#else
    int evp_ctrl_type = EVP_CTRL_GCM_GET_TAG;
#endif
    ret = EVP_CIPHER_CTX_ctrl(ctx, evp_ctrl_type, tagLen, tag);
    if (!OPENSSL_IS_SUCCESS(ret)) {
      throw InvalidAlgorithmParameterException(
          "EVP_CIPHER_CTX_ctrl get tag fail");
    }
  }

  return outLen;
}

class CryptoCache : public EncdbBaseObj {
 private:
  static uint64_t getFastHashId(const EVP_CIPHER *evp_cipher,
                                const bytes_t &key) {
    return std::hash<bytes_t>{}(key) ^ reinterpret_cast<uint64_t>(evp_cipher);
  }

 public:
  CryptoCache(const CryptoCache &) = delete;
  CryptoCache() = delete;
  void operator=(const CryptoCache &) = delete;

  static CtxHandle getHandle(const EVP_CIPHER *evp_cipher, const bytes_t &key,
                             const CryptoOpType &cop_type) {
    thread_local std::pair<CtxHandle, CtxHandle> handle_p;
    thread_local bytes_t cached_key;
    thread_local const EVP_CIPHER *cahced_evp_cipher = nullptr;

    if (cached_key != key || cahced_evp_cipher != evp_cipher) {
      cached_key.set(key.data(), key.size(), false);
      cahced_evp_cipher = evp_cipher;
      // init encryptor
      CtxHandle encryptor = SymmetricCryptInit(
          evp_cipher, key.data(), key.size(), CryptoOpType::ENCRYPT);
      // init decryptor
      CtxHandle decryptor = SymmetricCryptInit(
          evp_cipher, key.data(), key.size(), CryptoOpType::DECRYPT);

      handle_p.first = encryptor;
      handle_p.second = decryptor;
    }
    auto handle =
        CryptoOpType::ENCRYPT == cop_type ? handle_p.first : handle_p.second;
    return handle;
  }
};

const EVP_CIPHER *to_evp_cipher(const Symmetric::Alg &alg) {
  switch (alg) {
    case Symmetric::Alg::AES_128_GCM:
      return EVP_aes_128_gcm();
#if (OPENSSL_VERSION_NUMBER >= 0x30100000L)
    case Symmetric::Alg::SM4_128_GCM:
      return EVP_sm4_gcm();
#endif  // OPENSSL_VERSION_NUMBER >= 0x30100000L
    case Symmetric::Alg::AES_128_ECB:
      return EVP_aes_128_ecb();
    case Symmetric::Alg::SM4_128_ECB:
      return EVP_sm4_ecb();
    case Symmetric::Alg::AES_128_CTR:
      return EVP_aes_128_ctr();
    case Symmetric::Alg::SM4_128_CTR:
      return EVP_sm4_ctr();
    case Symmetric::Alg::SM4_128_CBC:
      return EVP_sm4_cbc();
    case Symmetric::Alg::AES_128_CBC:
      return EVP_aes_128_cbc();
    default:
      throw NoSuchAlgorithmException(to_string(alg));
  }
}

/**
 * @return real size of the \c out data
 */
uint32_t SymmetricCrypt(const EVP_CIPHER *evp_cipher, const uint8_t *key,
                        uint32_t keylen, const uint8_t *iv, uint32_t ivlen,
                        const uint8_t *in, uint32_t inlen, uint8_t *tag,
                        uint32_t taglen, const uint8_t *aad, uint32_t aadlen,
                        uint8_t *out, uint32_t out_cap_len,
                        const CryptoOpType &cop_type, bool no_padding,
                        bool use_cache, bool partial_crypt = false) {
  CtxHandle ctx_handle = nullptr;
  if (use_cache) {
    // get or set crypto ctx cache
    ctx_handle =
        CryptoCache::getHandle(evp_cipher, bytes_t(key, keylen), cop_type);
  } else {
    ctx_handle = SymmetricCryptInit(evp_cipher, key, keylen, cop_type);
  }

  uint32_t out_size =
      SymmetricCryptPending(ctx_handle, iv, ivlen, in, inlen, aad, aadlen, out,
                            out_cap_len, no_padding);
  if (!partial_crypt) {
    uint32_t final_size = SymmetricCryptFinal(
        ctx_handle, tag, taglen, out + out_size, out_cap_len - out_size);
    out_size += final_size;
  }
  return out_size;
}

// implement gcm mode for sm4
#if (OPENSSL_VERSION_NUMBER < 0x30100000L)
#include <openssl/modes.h>  // for gcm128
#include "sm4.h"
/*
 * @brief wrap GCM128_CONTEXT
 */
class Gcm128Ctx : public EncdbBaseObj {
 protected:
  GCM128_CONTEXT *handle_;

 public:
  Gcm128Ctx(const Gcm128Ctx &) = delete;
  void operator=(const Gcm128Ctx &) = delete;

  Gcm128Ctx(void *key, block128_f block) {
    handle_ = CRYPTO_gcm128_new(key, block);
    if (!handle_) {
      throw OutOfMemoryException("CRYPTO_gcm128_new fail");
    }
  }
  ~Gcm128Ctx() { CRYPTO_gcm128_release(handle_); }
  GCM128_CONTEXT *getCtx() const noexcept { return handle_; }
};
using Gcm128CtxHandle = std::shared_ptr<Gcm128Ctx>;

SM4_KEY &getSm4Key(const bytes_t &key) {
  thread_local SM4_KEY cached_ks;
  thread_local bytes_t cached_key;

  if (cached_key != key) {
    cached_key.set(key.data(), key.size(), false);
    // expand raw key to SM4_KEY
    SM4_set_key(cached_key.data(), &cached_ks);
  }
  return cached_ks;
}

uint32_t sm4Gcm(const uint8_t *key, uint32_t key_len, const uint8_t *iv,
                uint32_t iv_len, const uint8_t *in, uint32_t in_len,
                uint8_t *tag, uint32_t tag_len, const uint8_t *aad,
                uint32_t aad_len, uint8_t *out, uint32_t out_cap_len,
                const CryptoOpType &cop_type, bool use_cache) {
  if (in_len > out_cap_len) {
    throw ResultOutOfMemoryException(
        "Error: not enough memory for result cipher", in_len);
  }
  if (tag_len < SM4_BLOCK_SIZE) {
    throw InvalidAlgorithmParameterException("output tag length less than " +
                                             std::to_string(SM4_BLOCK_SIZE));
  }

  SM4_KEY key_st;
  if (use_cache) {
    key_st = getSm4Key(bytes_t(key, key_len));
  } else {
    SM4_set_key(key, &key_st);
  }
  block128_f block_func = reinterpret_cast<block128_f>(SM4_encrypt);
  /*
   * FIXME: Following code does NOT work.
   *
   * In theory, encrypt and decrypt should be identical for symmetric.
   * But it crashes for SM4_encrypt and SM4_decrypt from openssl.
   */
  // if (cop_type == CryptoOpType::ENCRYPT) {
  //     block_func = reinterpret_cast<block128_f>(SM4_encrypt);
  // } else {
  //     block_func = reinterpret_cast<block128_f>(SM4_decrypt);
  // }
  Gcm128CtxHandle ctx_handle =
      std::shared_ptr<Gcm128Ctx>(new Gcm128Ctx(&key_st, block_func));

  CRYPTO_gcm128_setiv(ctx_handle->getCtx(), iv, iv_len);
  if (aad && aad_len > 0) {
    // FIXME: according to regression tests, CRYPTO_gcm128_aad returns 0 on
    // success
    if (0 != CRYPTO_gcm128_aad(ctx_handle->getCtx(), aad, aad_len)) {
      throw InvalidAlgorithmParameterException("CRYPTO_gcm128_encrypt fail");
    }
  }

  if (cop_type == CryptoOpType::ENCRYPT) {
    // FIXME: according to regression tests, CRYPTO_gcm128_encrypt returns 0 on
    // success
    if (0 != CRYPTO_gcm128_encrypt(ctx_handle->getCtx(), in, out, in_len)) {
      throw InvalidAlgorithmParameterException("CRYPTO_gcm128_encrypt fail");
    }
    // CRYPTO_gcm128_encrypt(ctx_handle->getCtx(), in, out, in_len);
    CRYPTO_gcm128_tag(ctx_handle->getCtx(), tag, tag_len);
  } else {
    // FIXME: according to regression tests,
    // CRYPTO_gcm128_decrypt/CRYPTO_gcm128_finish return 0 on success
    if (0 != CRYPTO_gcm128_decrypt(ctx_handle->getCtx(), in, out, in_len)) {
      throw InvalidAlgorithmParameterException("CRYPTO_gcm128_decrypt fail");
    }
    if (0 != CRYPTO_gcm128_finish(ctx_handle->getCtx(), tag, tag_len)) {
      throw InvalidAlgorithmParameterException(
          "CRYPTO_gcm128_finish fail, typically tag is wrong or cipher was "
          "corrupted");
    }
  }

  return in_len;
}

#else  // OPENSSL_VERSION_NUMBER >= 0x30100000L
#error \
    "sm4-gcm will be supported in 3.1 release. Refer to https://github.com/openssl/openssl/issues/13667."

/*
 * Note: call EVP_CIPHER_fetch(NULL, "sm4-gcm", NULL) to access the cipher.
 * EVP_CIPHER_fetch() returns a pointer to a EVP_CIPHER for success and NULL for
 * failure. i.e., const EVP_CIPHER* EVP_sm4_gcm = EVP_CIPHER_fetch(NULL,
 * "sm4-gcm", NULL);
 */
#endif  // OPENSSL_VERSION_NUMBER < 0x30100000L

/**
 * @return real size of \c ciphertext text data
 */
uint32_t encdb_symmetric_encrypt(bytes_t &ciphertext, Symmetric::Alg alg,
                                 const bytes_t &key, const bytes_t &iv,
                                 const bytes_t &plaintext, bytes_t &tag,
                                 const bytes_t &aad,
                                 const Symmetric::Flags flags) {
  // FIXME: EVP engine does NOT support sm4-gcm mode until 3.1
  if (alg == Symmetric::Alg::SM4_128_GCM) {
    return sm4Gcm(key.data(), key.size(), iv.data(), iv.size(),
                  plaintext.data(), plaintext.size(), tag.data(), tag.size(),
                  aad.data(), aad.size(), ciphertext.data(), ciphertext.size(),
                  CryptoOpType::ENCRYPT,
                  flags & Symmetric::Flags::USE_CRYPTO_CACHE);
  }
  return SymmetricCrypt(
      to_evp_cipher(alg), key.data(), key.size(), iv.data(), iv.size(),
      plaintext.data(), plaintext.size(), tag.data(), tag.size(), aad.data(),
      aad.size(), ciphertext.data(), ciphertext.size(), CryptoOpType::ENCRYPT,
      flags & Symmetric::Flags::NO_PADDING,
      flags & Symmetric::Flags::USE_CRYPTO_CACHE,
      flags & Symmetric::Flags::PARTIAL_CRYPT);
}
/**
 * @return real size of \c plaintext data
 */
uint32_t encdb_symmetric_decrypt(bytes_t &plaintext, Symmetric::Alg alg,
                                 const bytes_t &key, const bytes_t &iv,
                                 const bytes_t &ciphertext, const bytes_t &tag,
                                 const bytes_t &aad,
                                 const Symmetric::Flags flags) {
  // FIXME: EVP engine does NOT support sm4-gcm mode until 3.1
  if (alg == Symmetric::Alg::SM4_128_GCM) {
    return sm4Gcm(
        key.data(), key.size(), iv.data(), iv.size(), ciphertext.data(),
        ciphertext.size(), const_cast<uint8_t *>(tag.data()), tag.size(),
        aad.data(), aad.size(), plaintext.data(), plaintext.size(),
        CryptoOpType::DECRYPT, flags & Symmetric::Flags::USE_CRYPTO_CACHE);
  }
  return SymmetricCrypt(
      to_evp_cipher(alg), key.data(), key.size(), iv.data(), iv.size(),
      ciphertext.data(), ciphertext.size(), const_cast<uint8_t *>(tag.data()),
      tag.size(), aad.data(), aad.size(), plaintext.data(), plaintext.size(),
      CryptoOpType::DECRYPT, flags & Symmetric::Flags::NO_PADDING,
      flags & Symmetric::Flags::USE_CRYPTO_CACHE,
      flags & Symmetric::Flags::PARTIAL_CRYPT);
}

/**
 * @brief import pkey pem string
 * @param [in]pemStr    pkey pem string
 * @param [in]puk       true - \c pemStr is a public key pem string; false - \c
 * pemStr is a private key pem string
 * @return EVP_PKEY handle of pkey
 */
std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> encdb_import_pkey_from_pem(
    const std::string &pemStr, bool puk) {
  std::unique_ptr<BIO, decltype(&BIO_free)> uniqCtx(
      BIO_new_mem_buf(pemStr.data(), pemStr.size()), &BIO_free);
  if (!uniqCtx) {
    throw OutOfMemoryException("BIO_new_mem_buf fail");
  }

  decltype(&PEM_read_bio_PUBKEY) pem_read_fn;
  if (puk) {
    pem_read_fn = PEM_read_bio_PUBKEY;
  } else {
    pem_read_fn = PEM_read_bio_PrivateKey;
  }

  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> pkey(
      pem_read_fn(uniqCtx.get(), NULL, NULL, NULL), &EVP_PKEY_free);
  if (!pkey) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                          "PEM_read_bio_* import pkey fail");
  }

  return pkey;
}
/**
 * @brief export pkey pem string from given pkey handle
 * @param [in]pkey      pkey handle
 * @param [in]puk       true - \c pkey is a public key handle; false - \c pkey
 * is a private key handle
 * @return pkey pem string
 */
const std::string encdb_export_pkey_to_pem(EVP_PKEY *pkey, bool puk) {
  std::unique_ptr<BIO, decltype(&BIO_free_all)> uniqCtx(BIO_new(BIO_s_mem()),
                                                        &BIO_free_all);
  if (!uniqCtx) {
    throw OutOfMemoryException("BIO_new fail");
  }

  if (puk) {
    if (!OPENSSL_IS_SUCCESS(PEM_write_bio_PUBKEY(uniqCtx.get(), pkey))) {
      throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                            "PEM_write_bio_PUBKEY fail");
    }
  } else {
    if (!OPENSSL_IS_SUCCESS(PEM_write_bio_PKCS8PrivateKey(
            uniqCtx.get(), pkey, NULL, NULL, 0, NULL, NULL))) {
      throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                            "PEM_write_bio_PKCS8PrivateKey fail");
    }
  }

  // make sure having enough room for exported key
  int len = BIO_pending(uniqCtx.get());
  std::string pemStr(len, '\0');

  if (!OPENSSL_IS_SUCCESS(
          BIO_read(uniqCtx.get(),
                   reinterpret_cast<void *>(const_cast<char *>(pemStr.data())),
                   pemStr.size()))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC, "BIO_read export pkey fail");
  }

  return pemStr;
}
/**
 * @brief RSA-PKCS1-OAEP-with-SHA256 encryption/decryption/sign
 * @param [in]pkey      public or private key handle
 * @param [in]enc       0 - encryption; 1 - decryption;
 *                      even for public key, odd for private key
 * @return real size of out
 */
uint32_t encdb_rsa_pkcs1_oaep_sha256_crypt(bytes_t &out, EVP_PKEY *pkey,
                                           const bytes_t &in,
                                           const CryptoOpType &cop_type) {
  std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> uniqCtx(
      EVP_PKEY_CTX_new(pkey, NULL), &EVP_PKEY_CTX_free);
  if (!uniqCtx) {
    throw OutOfMemoryException("EVP_PKEY_CTX_new fail");
  }

  decltype(&EVP_PKEY_encrypt_init) init_fn;
  decltype(&EVP_PKEY_encrypt) crypt_fn;
  if (CryptoOpType::ENCRYPT == cop_type) {
    init_fn = EVP_PKEY_encrypt_init;
    crypt_fn = EVP_PKEY_encrypt;
  } else if (CryptoOpType::DECRYPT == cop_type) {
    init_fn = EVP_PKEY_decrypt_init;
    crypt_fn = EVP_PKEY_decrypt;
  } else {
    throw NoSuchAlgorithmException("invalid CryptoOpType mode");
  }

  if (!OPENSSL_IS_SUCCESS(init_fn(uniqCtx.get()))) {
    throw InvalidKeyException("EVP_PKEY_encrypt/decrypt_init fail");
  }
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set_rsa_padding(
          uniqCtx.get(), RSA_PKCS1_OAEP_PADDING))) {
    throw BadPaddingException("EVP_PKEY_CTX_set_rsa_padding fail");
  }
  if (!OPENSSL_IS_SUCCESS(
          EVP_PKEY_CTX_set_rsa_oaep_md(uniqCtx.get(), EVP_sha256()))) {
    throw InvalidAlgorithmParameterException(
        "EVP_PKEY_CTX_set_rsa_oaep_md fail");
  }

  // Determine buffer length
  size_t expectedLen = 0;
  if (!OPENSSL_IS_SUCCESS(crypt_fn(uniqCtx.get(), nullptr, &expectedLen,
                                   in.data(), in.size()))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                          "EVP_PKEY_encrypt/decrypt get length fail");
  }
  if (out.size() < expectedLen) {
    throw ResultOutOfMemoryException(
        std::string(__FUNCTION__) + " - Error: not enough memory for output",
        expectedLen);
  }
  if (!OPENSSL_IS_SUCCESS(crypt_fn(uniqCtx.get(), out.data(), &expectedLen,
                                   in.data(), in.size()))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                          "EVP_PKEY_encrypt/decrypt fail");
  }
  out.resize(expectedLen);

  return out.size();
}
uint32_t encdb_rsa_pss_sha256_signature_sign(bytes_t &out, EVP_PKEY *pkey,
                                             const bytes_t &in) {
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> uniqMdCtx(
      EVP_MD_CTX_new(), &EVP_MD_CTX_free);
  if (!uniqMdCtx) {
    throw OutOfMemoryException("EVP_MD_CTX_new fail");
  }

  // it will be created inside EVP_DigestVerifyInit() and it will be freed
  // automatically when the EVP_MD_CTX is freed, so DO NOT free it directly.
  EVP_PKEY_CTX *pctx = NULL;
  if (!OPENSSL_IS_SUCCESS(EVP_DigestSignInit(uniqMdCtx.get(), &pctx,
                                             EVP_sha256(), NULL, pkey))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC, "EVP_DigestVerifyInit fail");
  }
  // !!! EVP_PKEY_CTX_set_rsa_padding and EVP_PKEY_CTX_set_rsa_pss_saltlen are
  // critial for Java compatibility !!!
  if (!OPENSSL_IS_SUCCESS(
          EVP_PKEY_CTX_set_rsa_padding(pctx, RSA_PKCS1_PSS_PADDING))) {
    throw BadPaddingException("EVP_PKEY_CTX_set_rsa_padding fail");
  }
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set_rsa_pss_saltlen(pctx, 32))) {
    throw BadPaddingException("EVP_PKEY_CTX_set_rsa_pss_saltlen fail");
  }
  // if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set_signature_md(pctx, EVP_sha256())))
  // {
  //     throw BadPaddingException("EVP_PKEY_CTX_set_signature_md fail");
  // }
  // if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set_rsa_mgf1_md(pctx, EVP_sha256())))
  // {
  //     throw BadPaddingException("EVP_PKEY_CTX_set_rsa_mgf1_md fail");
  // }

  // Determine buffer length
  size_t expectedLen = 0;
  if (!OPENSSL_IS_SUCCESS(EVP_DigestSign(uniqMdCtx.get(), NULL, &expectedLen,
                                         in.data(), in.size()))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                          "EVP_DigestSign get length fail");
  }
  if (out.size() < expectedLen) {
    throw ResultOutOfMemoryException(
        std::string(__FUNCTION__) + " - Error: not enough memory for output",
        expectedLen);
  }
  if (!OPENSSL_IS_SUCCESS(EVP_DigestSign(uniqMdCtx.get(), out.data(),
                                         &expectedLen, in.data(), in.size()))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC, "EVP_DigestSign fail");
  }
  out.resize(expectedLen);

  return out.size();
}
bool encdb_rsa_pss_sha256_signature_verify(EVP_PKEY *pkey, const bytes_t &sig,
                                           const bytes_t &in) {
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> uniqMdCtx(
      EVP_MD_CTX_new(), &EVP_MD_CTX_free);
  if (!uniqMdCtx) {
    throw OutOfMemoryException("EVP_MD_CTX_new fail");
  }

  // it will be created inside EVP_DigestVerifyInit() and it will be freed
  // automatically when the EVP_MD_CTX is freed, so DO NOT free it directly.
  EVP_PKEY_CTX *pctx = NULL;
  if (!OPENSSL_IS_SUCCESS(EVP_DigestVerifyInit(uniqMdCtx.get(), &pctx,
                                               EVP_sha256(), NULL, pkey))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC, "EVP_DigestVerifyInit fail");
  }
  // !!! EVP_PKEY_CTX_set_rsa_padding and EVP_PKEY_CTX_set_rsa_pss_saltlen are
  // critial for Java compatibility !!!
  if (!OPENSSL_IS_SUCCESS(
          EVP_PKEY_CTX_set_rsa_padding(pctx, RSA_PKCS1_PSS_PADDING))) {
    throw BadPaddingException("EVP_PKEY_CTX_set_rsa_padding fail");
  }
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set_rsa_pss_saltlen(pctx, 32))) {
    throw BadPaddingException("EVP_PKEY_CTX_set_rsa_pss_saltlen fail");
  }
  // if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set_signature_md(pctx, EVP_sha256())))
  // {
  //     throw BadPaddingException("EVP_PKEY_CTX_set_signature_md fail");
  // }
  // if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set_rsa_mgf1_md(pctx, EVP_sha256())))
  // {
  //     throw BadPaddingException("EVP_PKEY_CTX_set_rsa_mgf1_md fail");
  // }

  // refer to https://www.openssl.org/docs/man1.1.1/man3/EVP_DigestVerify.html
  // for more details
  int ret = EVP_DigestVerify(uniqMdCtx.get(), sig.data(), sig.size(), in.data(),
                             in.size());
  if (OPENSSL_IS_SUCCESS(ret)) {
    return true;
  } else if (0 == ret) {
    return false;
  } else {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                          "EVP_DigestVerify fail: an error other that "
                          "signature verification failure");
  }
}

/**
 * @brief SM2 encryption/decryption using evp, cipher result is ans1encoded.
 * https://blog.csdn.net/henter/article/details/105883478
 * @param [in]pkey      public or private key handle
 * @param [in]enc       0 - encryption; 1 - decryption;
 *                      even for public key, odd for private key
 * @return real size of out
 */
uint32_t encdb_evp_sm2_crypt(bytes_t &out, EVP_PKEY *pkey, const bytes_t &in,
                             const CryptoOpType &cop_type) {
#if (OPENSSL_VERSION_NUMBER < 0x30000000L)
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_set_alias_type(pkey, EVP_PKEY_SM2))) {
    throw CryptoException(
        ERROR_CRYPTO_ASYMMETRIC,
        "EVP_PKEY_set_alias_type set alias type to EVP_PKEY_SM2 fail");
  }
#endif  // OPENSSL_VERSION_NUMBER < 0x30100000L
  std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> uniqCtx(
      EVP_PKEY_CTX_new(pkey, NULL), &EVP_PKEY_CTX_free);
  if (!uniqCtx) {
    throw OutOfMemoryException("EVP_PKEY_CTX_new fail");
  }

  decltype(&EVP_PKEY_encrypt_init) init_fn;
  decltype(&EVP_PKEY_encrypt) crypt_fn;
  bool using_private_key = true;
  if (CryptoOpType::ENCRYPT == cop_type) {
    init_fn = EVP_PKEY_encrypt_init;
    crypt_fn = EVP_PKEY_encrypt;
    using_private_key = false;
  } else if (CryptoOpType::DECRYPT == cop_type) {
    init_fn = EVP_PKEY_decrypt_init;
    crypt_fn = EVP_PKEY_decrypt;
  } else {
    throw NoSuchAlgorithmException("invalid CryptoOpType mode");
  }

  if (!OPENSSL_IS_SUCCESS(init_fn(uniqCtx.get()))) {
    throw InvalidKeyException("EVP_PKEY_encrypt/decrypt/sign_init fail");
  }

  // Determine buffer length
  size_t expectedLen = 0;
  if (!OPENSSL_IS_SUCCESS(crypt_fn(uniqCtx.get(), nullptr, &expectedLen,
                                   in.data(), in.size()))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                          "EVP_PKEY_encrypt/decrypt/sign get length fail");
  }
  /*
   * SM2 Decryption Buffer Overflow (CVE-2021-3711)
   * https://www.openssl.org/news/secadv/20210824.txt
   * FIXME: upgrade OpenSSL to 1.1.1l+
   *
   * For SM2, coordinates are expected to be of 32 bytes, but for corner case,
   * they may be of 31 bytes. Always allocate 2 bytes (1 byte per coordinate)
   * more to avoid CVE-2021-3711 bug currently
   */
  if (using_private_key) {
    expectedLen += 2;
  }
  if (out.size() < expectedLen) {
    throw ResultOutOfMemoryException(
        std::string(__FUNCTION__) + " - Error: not enough memory for output",
        expectedLen);
  }
  if (!OPENSSL_IS_SUCCESS(crypt_fn(uniqCtx.get(), out.data(), &expectedLen,
                                   in.data(), in.size()))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                          "EVP_PKEY_encrypt/decrypt/sign fail");
  }
  out.resize(expectedLen);

  return out.size();
}

uint32_t encdb_evp_sm2_signature_sign(bytes_t &out, EVP_PKEY *pkey,
                                      const bytes_t &in) {
#if (OPENSSL_VERSION_NUMBER < 0x30000000L)
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_set_alias_type(pkey, EVP_PKEY_SM2))) {
    throw CryptoException(
        ERROR_CRYPTO_ASYMMETRIC,
        "EVP_PKEY_set_alias_type set alias type to EVP_PKEY_SM2 fail");
  }
#endif  // OPENSSL_VERSION_NUMBER < 0x30100000L
  std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> uniqPkeyCtx(
      EVP_PKEY_CTX_new(pkey, NULL), &EVP_PKEY_CTX_free);
  if (!uniqPkeyCtx) {
    throw OutOfMemoryException("EVP_PKEY_CTX_new fail");
  }
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set1_id(
          uniqPkeyCtx.get(), SM2_DEFAULT_ID.data(), SM2_DEFAULT_ID.size()))) {
    throw CryptoException(
        ERROR_CRYPTO_ASYMMETRIC,
        "EVP_PKEY_CTX_set1_id set default ID ('1234567812345678') fail");
  }

  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> uniqMdCtx(
      EVP_MD_CTX_new(), &EVP_MD_CTX_free);
  if (!uniqMdCtx) {
    throw OutOfMemoryException("EVP_MD_CTX_new fail");
  }
  EVP_MD_CTX_set_pkey_ctx(uniqMdCtx.get(), uniqPkeyCtx.get());

  if (!OPENSSL_IS_SUCCESS(
          EVP_DigestSignInit(uniqMdCtx.get(), NULL, EVP_sm3(), NULL, pkey))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC, "EVP_DigestVerifyInit fail");
  }
  // Determine buffer length
  size_t expectedLen = 0;
  if (!OPENSSL_IS_SUCCESS(EVP_DigestSign(uniqMdCtx.get(), NULL, &expectedLen,
                                         in.data(), in.size()))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                          "EVP_DigestSign get length fail");
  }
  if (out.size() < expectedLen) {
    throw ResultOutOfMemoryException(
        std::string(__FUNCTION__) + " - Error: not enough memory for output",
        expectedLen);
  }
  if (!OPENSSL_IS_SUCCESS(EVP_DigestSign(uniqMdCtx.get(), out.data(),
                                         &expectedLen, in.data(), in.size()))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC, "EVP_DigestSign fail");
  }
  out.resize(expectedLen);

  return out.size();
}
bool encdb_evp_sm2_signature_verify(EVP_PKEY *pkey, const bytes_t &sig,
                                    const bytes_t &in) {
#if (OPENSSL_VERSION_NUMBER < 0x30000000L)
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_set_alias_type(pkey, EVP_PKEY_SM2))) {
    throw CryptoException(
        ERROR_CRYPTO_ASYMMETRIC,
        "EVP_PKEY_set_alias_type set alias type to EVP_PKEY_SM2 fail");
  }
#endif  // OPENSSL_VERSION_NUMBER < 0x30100000L
  std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> uniqPkeyCtx(
      EVP_PKEY_CTX_new(pkey, NULL), &EVP_PKEY_CTX_free);
  if (!uniqPkeyCtx) {
    throw OutOfMemoryException("EVP_PKEY_CTX_new fail");
  }
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set1_id(
          uniqPkeyCtx.get(), SM2_DEFAULT_ID.data(), SM2_DEFAULT_ID.size()))) {
    throw CryptoException(
        ERROR_CRYPTO_ASYMMETRIC,
        "EVP_PKEY_CTX_set1_id set default ID ('1234567812345678') fail");
  }

  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> uniqMdCtx(
      EVP_MD_CTX_new(), &EVP_MD_CTX_free);
  if (!uniqMdCtx) {
    throw OutOfMemoryException("EVP_MD_CTX_new fail");
  }
  EVP_MD_CTX_set_pkey_ctx(uniqMdCtx.get(), uniqPkeyCtx.get());

  if (!OPENSSL_IS_SUCCESS(
          EVP_DigestVerifyInit(uniqMdCtx.get(), NULL, EVP_sm3(), NULL, pkey))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC, "EVP_DigestVerifyInit fail");
  }
  // refer to https://www.openssl.org/docs/man1.1.1/man3/EVP_DigestVerify.html
  // for more details
  int ret = EVP_DigestVerify(uniqMdCtx.get(), sig.data(), sig.size(), in.data(),
                             in.size());
  if (OPENSSL_IS_SUCCESS(ret)) {
    return true;
  } else if (0 == ret) {
    return false;
  } else {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC,
                          "EVP_DigestVerify fail: an error other that "
                          "signature verification failure");
  }
}

uint32_t encdb_asymmetric_encrypt(bytes_t &ciphertext, Asymmetric::Alg alg,
                                  const std::string &key_string,
                                  const bytes_t &plaintext) {
  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> uniqCtx =
      encdb_import_pkey_from_pem(key_string, true);
  switch (alg) {
    case Asymmetric::Alg::RSA_WITH_SHA256:
      // RSA encrypt with pem string key
      encdb_rsa_pkcs1_oaep_sha256_crypt(ciphertext, uniqCtx.get(), plaintext,
                                        CryptoOpType::ENCRYPT);
      break;
    case Asymmetric::Alg::SM2:
      // sm2 encrypt with pem string key
      encdb_evp_sm2_crypt(ciphertext, uniqCtx.get(), plaintext,
                          CryptoOpType::ENCRYPT);
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
      break;
  }

  return ciphertext.size();
}
uint32_t encdb_asymmetric_decrypt(bytes_t &plaintext, Asymmetric::Alg alg,
                                  const std::string &key_string,
                                  const bytes_t &ciphertext) {
  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> uniqCtx =
      encdb_import_pkey_from_pem(key_string, false);
  switch (alg) {
    case Asymmetric::Alg::RSA_WITH_SHA256:
      // RSA decrypt with pem string key
      encdb_rsa_pkcs1_oaep_sha256_crypt(plaintext, uniqCtx.get(), ciphertext,
                                        CryptoOpType::DECRYPT);
      break;
    case Asymmetric::Alg::SM2:
      // sm2 decrypt with pem string key
      encdb_evp_sm2_crypt(plaintext, uniqCtx.get(), ciphertext,
                          CryptoOpType::DECRYPT);
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
      break;
  }

  return plaintext.size();
}
uint32_t encdb_asymmetric_sign(bytes_t &sig, Asymmetric::Alg alg,
                               const std::string &key_string,
                               const bytes_t &in) {
  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> uniqCtx =
      encdb_import_pkey_from_pem(key_string, false);
  switch (alg) {
    case Asymmetric::Alg::RSA_WITH_SHA256:
      encdb_rsa_pss_sha256_signature_sign(sig, uniqCtx.get(), in);
      break;
    case Asymmetric::Alg::SM2:
      encdb_evp_sm2_signature_sign(sig, uniqCtx.get(), in);
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
      break;
  }

  return sig.size();
}
bool encdb_asymmetric_verify(Asymmetric::Alg alg, const std::string &key_string,
                             const bytes_t &sig, const bytes_t &in) {
  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> uniqCtx =
      encdb_import_pkey_from_pem(key_string, true);
  switch (alg) {
    case Asymmetric::Alg::RSA_WITH_SHA256:
      return encdb_rsa_pss_sha256_signature_verify(uniqCtx.get(), sig, in);
    case Asymmetric::Alg::SM2:
      return encdb_evp_sm2_signature_verify(uniqCtx.get(), sig, in);
    default:
      throw NoSuchAlgorithmException(to_string(alg));
  }
}

/**
 * @brief generate pkey pair according to given NID
 * @param [in]nid               only EVP_PKEY_RSA or EVP_PKEY_SM2 support now
 * @param [opt]keySizeInBits    specify asymmetric key size in bits, only for
 * RSA
 * @return EVP_PKEY handle of pkey pair
 */
std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>
encdb_asymmetric_generate_key_pair(int nid, uint32_t keySizeInBits) {
  if (nid != EVP_PKEY_RSA && nid != EVP_PKEY_SM2) {
    throw NoSuchAlgorithmException(
        "only EVP_PKEY_RSA or EVP_PKEY_SM2 support now");
  }

  std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> uniqCtx(
      EVP_PKEY_CTX_new_id(nid == EVP_PKEY_SM2 ? EVP_PKEY_EC : nid, NULL),
      &EVP_PKEY_CTX_free);
  if (!uniqCtx) {
    throw OutOfMemoryException("EVP_PKEY_CTX_new_id fail");
  }
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_keygen_init(uniqCtx.get()))) {
    throw InvalidAlgorithmParameterException("EVP_PKEY_keygen_init fail");
  }
  if (nid == EVP_PKEY_RSA) {
    if (!OPENSSL_IS_SUCCESS(
            EVP_PKEY_CTX_set_rsa_keygen_bits(uniqCtx.get(), keySizeInBits))) {
      throw InvalidAlgorithmParameterException(
          "EVP_PKEY_CTX_set_rsa_keygen_bits fail");
    }
  } else if (nid == EVP_PKEY_SM2) {
    if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set_ec_paramgen_curve_nid(
            uniqCtx.get(), EVP_PKEY_SM2))) {
      throw InvalidAlgorithmParameterException(
          "EVP_PKEY_CTX_set_ec_paramgen_curve_nid fail");
    }
  }

  EVP_PKEY *pkey = nullptr;
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_keygen(uniqCtx.get(), &pkey))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC, "EVP_PKEY_keygen fail");
  }

  return {pkey, &EVP_PKEY_free};
}
/**
 * @brief generate pkey pair pem string according to given algorithm
 * @param [in]alg               Asymmetric::Alg
 * @param [out]prikeyPemStr     private key pem string
 * @param [out]pubkeyPemSt      public key pem string
 * @param [opt]keySizeInBits    specify asymmetric key size in bits, only for
 * RSA
 * @return total size of public and private key pem string size
 */
uint32_t encdb_asymmetric_generate_key_pair(std::string &prikeyPemStr,
                                            std::string &pubkeyPemStr,
                                            Asymmetric::Alg alg,
                                            uint32_t keySizeInBits) {
  int nid;
  switch (alg) {
    case Asymmetric::Alg::RSA_WITH_SHA256:
      nid = EVP_PKEY_RSA;
      break;
    case Asymmetric::Alg::SM2:
      nid = EVP_PKEY_SM2;
      break;
    default:
      throw NoSuchAlgorithmException("Not support generate pem string for " +
                                     to_string(alg));
      break;
  }

  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> uniqKey =
      encdb_asymmetric_generate_key_pair(nid, keySizeInBits);

  prikeyPemStr = encdb_export_pkey_to_pem(uniqKey.get(), false);
  pubkeyPemStr = encdb_export_pkey_to_pem(uniqKey.get(), true);

  return prikeyPemStr.size() + pubkeyPemStr.size();
}

#ifdef USE_ECDH
/**
 * @brief derive shared secret from \c pkey and \c peerkey using ECDH algorithm
 * @return size of drived shared secret
 */
uint32_t encdb_ecdh(bytes_t &secret, EVP_PKEY *pkey, EVP_PKEY *peerkey) {
  std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> uniqCtx(
      EVP_PKEY_CTX_new(pkey, NULL), &EVP_PKEY_CTX_free);
  if (!uniqCtx) {
    throw OutOfMemoryException("EVP_PKEY_CTX_new fail");
  }

  // Initialise
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_derive_init(uniqCtx.get()))) {
    throw InvalidAlgorithmParameterException("EVP_PKEY_derive_init fail");
  }
  // Provide the peer public key
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_derive_set_peer(uniqCtx.get(), peerkey))) {
    throw InvalidAlgorithmParameterException("EVP_PKEY_derive_set_peer fail");
  }
  // Determine buffer length for shared secret
  size_t len;
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_derive(uniqCtx.get(), NULL, &len))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC, "EVP_PKEY_derive fail");
  }
  secret.resize(len);
  // Derive the shared secret
  if (!OPENSSL_IS_SUCCESS(
          EVP_PKEY_derive(uniqCtx.get(), secret.data(), &len))) {
    throw CryptoException(ERROR_CRYPTO_ASYMMETRIC, "EVP_PKEY_derive fail");
  }

  if (len != secret.size()) {
    throw ResultOutOfMemoryException("Error: not enough memory for secret",
                                     len);
  }

  return secret.size();
}
#endif  // USE_ECDH

#if ((OPENSSL_VERSION_NUMBER >> 16) < 0x1010)
static int HKDF(const EVP_MD *evp_md, const unsigned char *salt,
                size_t salt_len, const unsigned char *key, size_t key_len,
                const unsigned char *info, size_t info_len, unsigned char *okm,
                size_t okm_len);
static int HKDF_Extract(const EVP_MD *evp_md, const unsigned char *salt,
                        size_t salt_len, const unsigned char *ikm,
                        size_t ikm_len, unsigned char *prk, size_t prk_len);
static int HKDF_Expand(const EVP_MD *evp_md, const unsigned char *prk,
                       size_t prk_len, const unsigned char *info,
                       size_t info_len, unsigned char *okm, size_t okm_len);

/*
 * Refer to "HMAC-based Extract-and-Expand Key Derivation Function (HKDF)"
 * Section 2 (https://tools.ietf.org/html/rfc5869#section-2) and
 * "Cryptographic Extraction and Key Derivation: The HKDF Scheme"
 * Section 4.2 (https://eprint.iacr.org/2010/264.pdf).
 *
 * From the paper:
 *   The scheme HKDF is specified as:
 *     HKDF(XTS, SKM, CTXinfo, L) = K(1) | K(2) | ... | K(t)
 *
 *     where:
 *       SKM is source key material
 *       XTS is extractor salt (which may be null or constant)
 *       CTXinfo is context information (may be null)
 *       L is the number of key bits to be produced by KDF
 *       k is the output length in bits of the hash function used with HMAC
 *       t = ceil(L/k)
 *       the value K(t) is truncated to its first d = L mod k bits.
 *
 * From RFC 5869:
 *   2.2.  Step 1: Extract
 *     HKDF-Extract(salt, IKM) -> PRK
 *   2.3.  Step 2: Expand
 *     HKDF-Expand(PRK, info, L) -> OKM
 */
static int HKDF(const EVP_MD *evp_md, const unsigned char *salt,
                size_t salt_len, const unsigned char *ikm, size_t ikm_len,
                const unsigned char *info, size_t info_len, unsigned char *okm,
                size_t okm_len) {
  unsigned char prk[EVP_MAX_MD_SIZE];
  int ret, sz;
  size_t prk_len;

  sz = EVP_MD_size(evp_md);
  if (sz < 0) {
    return 0;
  }
  prk_len = static_cast<size_t>(sz);

  // Step 1: HKDF-Extract(salt, IKM) -> PRK
  if (!HKDF_Extract(evp_md, salt, salt_len, ikm, ikm_len, prk, prk_len)) {
    return 0;
  }

  // Step 2: HKDF-Expand(PRK, info, L) -> OKM
  ret = HKDF_Expand(evp_md, prk, prk_len, info, info_len, okm, okm_len);
  OPENSSL_cleanse(prk, sizeof(prk));

  return ret;
}

/*
 * Refer to "HMAC-based Extract-and-Expand Key Derivation Function (HKDF)"
 * Section 2.2 (https://tools.ietf.org/html/rfc5869#section-2.2).
 *
 * 2.2.  Step 1: Extract
 *
 *   HKDF-Extract(salt, IKM) -> PRK
 *
 *   Options:
 *      Hash     a hash function; HashLen denotes the length of the
 *               hash function output in octets
 *
 *   Inputs:
 *      salt     optional salt value (a non-secret random value);
 *               if not provided, it is set to a string of HashLen zeros.
 *      IKM      input keying material
 *
 *   Output:
 *      PRK      a pseudorandom key (of HashLen octets)
 *
 *   The output PRK is calculated as follows:
 *
 *   PRK = HMAC-Hash(salt, IKM)
 */
static int HKDF_Extract(const EVP_MD *evp_md, const unsigned char *salt,
                        size_t salt_len, const unsigned char *ikm,
                        size_t ikm_len, unsigned char *prk, size_t prk_len) {
  int sz = EVP_MD_size(evp_md);

  if (sz < 0) {
    return 0;
  }
  if (prk_len != static_cast<size_t>(sz)) {
    EVPerr(ERR_LIB_HMAC, ERR_R_CRYPTO_LIB);
    return 0;
  }
  // calc: PRK = HMAC-Hash(salt, IKM)
  return HMAC(evp_md, salt, salt_len, ikm, ikm_len, prk, NULL) != NULL;
}

/*
 * Refer to "HMAC-based Extract-and-Expand Key Derivation Function (HKDF)"
 * Section 2.3 (https://tools.ietf.org/html/rfc5869#section-2.3).
 *
 * 2.3.  Step 2: Expand
 *
 *   HKDF-Expand(PRK, info, L) -> OKM
 *
 *   Options:
 *      Hash     a hash function; HashLen denotes the length of the
 *               hash function output in octets
 *
 *   Inputs:
 *      PRK      a pseudorandom key of at least HashLen octets
 *               (usually, the output from the extract step)
 *      info     optional context and application specific information
 *               (can be a zero-length string)
 *      L        length of output keying material in octets
 *               (<= 255*HashLen)
 *
 *   Output:
 *      OKM      output keying material (of L octets)
 *
 *   The output OKM is calculated as follows:
 *
 *   N = ceil(L/HashLen)
 *   T = T(1) | T(2) | T(3) | ... | T(N)
 *   OKM = first L octets of T
 *
 *   where:
 *   T(0) = empty string (zero length)
 *   T(1) = HMAC-Hash(PRK, T(0) | info | 0x01)
 *   T(2) = HMAC-Hash(PRK, T(1) | info | 0x02)
 *   T(3) = HMAC-Hash(PRK, T(2) | info | 0x03)
 *   ...
 *
 *   (where the constant concatenated to the end of each T(n) is a
 *   single octet.)
 */
static int HKDF_Expand(const EVP_MD *evp_md, const unsigned char *prk,
                       size_t prk_len, const unsigned char *info,
                       size_t info_len, unsigned char *okm, size_t okm_len) {
  HMAC_CTX *hmac;
  int ret = 0, sz;
  unsigned int i;
  unsigned char prev[EVP_MAX_MD_SIZE];
  size_t done_len = 0, dig_len, n;

  sz = EVP_MD_size(evp_md);
  if (sz <= 0) {
    return 0;
  }
  dig_len = static_cast<size_t>(sz);

  // calc: N = ceil(L/HashLen)
  n = okm_len / dig_len;
  if (okm_len % dig_len) {
    n++;
  }

  if (n > 255 || okm == NULL) {
    return 0;
  }
#if (OPENSSL_VERSION_NUMBER >= 0x10100000L)
  if ((hmac = HMAC_CTX_new()) == NULL) {
    return 0;
  }
#else
  HMAC_CTX ctx;
  HMAC_CTX_init(&ctx);
  hmac = &ctx;
#endif

  if (!HMAC_Init_ex(hmac, prk, prk_len, evp_md, NULL)) {
    goto err;
  }

  for (i = 1; i <= n; i++) {
    size_t copy_len;
    const unsigned char ctr = i;

    // calc: T(i) = HMAC-Hash(PRK, T(i - 1) | info | i)
    if (i > 1) {
      if (!HMAC_Init_ex(hmac, NULL, 0, NULL, NULL)) {
        goto err;
      }

      if (!HMAC_Update(hmac, prev, dig_len)) {
        goto err;
      }
    }

    if (!HMAC_Update(hmac, info, info_len)) {
      goto err;
    }

    if (!HMAC_Update(hmac, &ctr, 1)) {
      goto err;
    }

    if (!HMAC_Final(hmac, prev, NULL)) {
      goto err;
    }

    copy_len = (done_len + dig_len > okm_len) ? okm_len - done_len : dig_len;

    memcpy(okm + done_len, prev, copy_len);

    done_len += copy_len;
  }
  ret = 1;

err:
  OPENSSL_cleanse(prev, sizeof(prev));
#if (OPENSSL_VERSION_NUMBER >= 0x10100000L)
  HMAC_CTX_free(hmac);
#else
  HMAC_CTX_cleanup(hmac);
#endif
  return ret;
}
#endif

uint32_t Digest(const EVP_MD *evpMd, const uint8_t *message,
                uint32_t message_len, uint8_t *digest, uint32_t digest_len) {
  if ((int)digest_len < EVP_MD_size(evpMd)) {
    throw OutOfMemoryException("Invalid digest length " +
                               std::to_string(digest_len) + ", expected " +
                               std::to_string(EVP_MD_size(evpMd)));
  }

#if (OPENSSL_VERSION_NUMBER >= 0x10100000L)
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> uniqCtx(
      EVP_MD_CTX_new(), &EVP_MD_CTX_free);
#else
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_destroy)> uniqCtx(
      EVP_MD_CTX_create(), &EVP_MD_CTX_destroy);
#endif
  if (!uniqCtx) {
    throw OutOfMemoryException("create EVP_MD_CTX fail");
  }
  EVP_MD_CTX *mdctx = uniqCtx.get();

  if (!OPENSSL_IS_SUCCESS(EVP_DigestInit_ex(mdctx, evpMd, NULL))) {
    throw CryptoException(ERROR_CRYPTO_HASH, "EVP_DigestInit_ex md fail");
  }

  if (!OPENSSL_IS_SUCCESS(EVP_DigestUpdate(mdctx, message, message_len))) {
    throw CryptoException(ERROR_CRYPTO_HASH, "EVP_DigestUpdate md fail");
  }

  if (!OPENSSL_IS_SUCCESS(EVP_DigestFinal_ex(mdctx, digest, &digest_len))) {
    throw CryptoException(ERROR_CRYPTO_HASH, "EVP_DigestFinal_ex md fail");
  }

  return digest_len;
}
uint32_t encdb_hash(bytes_t &digest, Hash::Alg alg, const bytes_t &msg) {
  const EVP_MD *md = nullptr;
  switch (alg) {
    case Hash::Alg::SHA256:
      md = EVP_sha256();
      break;
    case Hash::Alg::SM3:
      md = EVP_sm3();
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
      break;
  }

  uint32_t expected_size = EVP_MD_size(md);
  if (digest.size() < expected_size) {
    throw ResultOutOfMemoryException(
        "Error: not enough memory for output digest", expected_size);
  }
  digest.resize(expected_size);
  return Digest(md, msg.data(), msg.size(), digest.data(), digest.size());
}

uint32_t encdb_hkdf(bytes_t &out, Hash::Alg alg, const bytes_t &secret,
                    const bytes_t &salt, const bytes_t &aad) {
  const EVP_MD *md = nullptr;
  switch (alg) {
    case Hash::Alg::SHA256:
      md = EVP_sha256();
      break;
    case Hash::Alg::SM3:
      md = EVP_sm3();
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
      break;
  }

#if (OPENSSL_VERSION_NUMBER >= 0x10100000L)
  std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> uniqCtx(
      EVP_PKEY_CTX_new_id(EVP_PKEY_HKDF, NULL), &EVP_PKEY_CTX_free);
  if (!uniqCtx) {
    throw OutOfMemoryException("create EVP_PKEY_CTX_new_id fail");
  }
  EVP_PKEY_CTX *pctx = uniqCtx.get();

  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_derive_init(pctx))) {
    throw CryptoException(ERROR_CRYPTO_KDF, "EVP_PKEY_derive_init fail");
  }
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_CTX_set_hkdf_md(pctx, md))) {
    throw CryptoException(ERROR_CRYPTO_KDF, "EVP_PKEY_CTX_set_hkdf_md fail");
  }

  if (salt) {
    if (!OPENSSL_IS_SUCCESS(
            EVP_PKEY_CTX_set1_hkdf_salt(pctx, salt.data(), salt.size()))) {
      throw CryptoException(ERROR_CRYPTO_KDF,
                            "EVP_PKEY_CTX_set1_hkdf_salt fail");
    }
  }
  if (!secret) {
    throw InvalidAlgorithmParameterException(
        "secret is empty, which is required");
  }
  if (!OPENSSL_IS_SUCCESS(
          EVP_PKEY_CTX_set1_hkdf_key(pctx, secret.data(), secret.size()))) {
    throw CryptoException(ERROR_CRYPTO_KDF, "EVP_PKEY_CTX_set1_hkdf_key fail");
  }
  if (aad) {
    if (!OPENSSL_IS_SUCCESS(
            EVP_PKEY_CTX_add1_hkdf_info(pctx, aad.data(), aad.size()))) {
      throw CryptoException(ERROR_CRYPTO_KDF,
                            "EVP_PKEY_CTX_add1_hkdf_info fail");
    }
  }

  size_t len = out.size();
  if (!OPENSSL_IS_SUCCESS(EVP_PKEY_derive(pctx, out.data(), &len))) {
    throw CryptoException(ERROR_CRYPTO_KDF, "EVP_PKEY_derive fail");
  }
  if (len != out.size()) {
    throw ResultOutOfMemoryException("Error: not enough memory for output",
                                     len);
  }
#else
  if (!OPENSSL_IS_SUCCESS(HKDF(md, salt.data(), salt.size(), secret.data(),
                               secret.size(), aad.data(), aad.size(),
                               out.data(), out.size()))) {
    throw CryptoException(ERROR_CRYPTO_KDF, "HKDF key fail");
  }
#endif

  return out.size();
}

/**
 * @brief compute hmac of \c data with \c key using \c alg algorithm
 * @param [out]out          digest result
 * @param [in]alg           algorithm used to do hash
 * @param [in]key           key used
 * @param [in]data          data to be MACed
 */
uint32_t encdb_hmac(bytes_t &out, Hash::Alg alg, const bytes_t &key,
                    const bytes_t &data) {
  const EVP_MD *md = nullptr;

  switch (alg) {
    case Hash::Alg::SM3:
      md = EVP_sm3();
      break;
    case Hash::Alg::SHA256:
      md = EVP_sha256();
      break;
    default:
      throw NoSuchAlgorithmException(to_string(alg));
      break;
  }

  uint32_t expected_size = EVP_MD_size(md);
  if (out.size() < expected_size) {
    throw ResultOutOfMemoryException("Error: not enough memory for output hmac",
                                     expected_size);
  }
  out.resize(expected_size);

  uint32_t len = out.size();
  if (!HMAC(md, key.data(), key.size(), data.data(), data.size(), out.data(),
            &len)) {
    throw CryptoException(ERROR_CRYPTO_HMAC, "HMAC fail");
  }
  if (len != out.size()) {
    throw ResultOutOfMemoryException("Error: not enough memory for output hmac",
                                     len);
  }

  return out.size();
}

}  // namespace encdb
