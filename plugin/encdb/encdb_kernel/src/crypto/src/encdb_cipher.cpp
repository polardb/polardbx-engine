/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: implementation for encdb::Cipher
 */

#include "encdb_cipher.h"
#include <unordered_map>
#include "encdb_asymmetric.h"
#include "encdb_cipher_suite.h"
#include "encdb_crypto_exception.h"
#include "encdb_hash.h"
#include "encdb_rand.h"
#include "encdb_symmetric.h"

#include <iostream>
#include <mutex>
#define DUMMY_CTXID 0xff

namespace encdb {

template <uint32_t SIZE>
std::array<uint8_t, SIZE> xorArray(const uint8_t *data, uint32_t len) noexcept {
  constexpr uint32_t BLOCK_SIZE = sizeof(uint64_t);
  // SIZE MUST be divided into sizeof(uint64_t).
  // static_assert(SIZE == 1 || SIZE == 2 || SIZE == 4 || SIZE == 8, "result
  // size should be divided into BLOCK_SIZE, i.e., sizeof(uint64_t)");

  // boost performance by block-XOR
  uint64_t tmp_res_64 = 0;
  const uint64_t *trans_data = reinterpret_cast<const uint64_t *>(data);
  for (size_t i = 0; i < len / BLOCK_SIZE; i++) {
    tmp_res_64 ^= trans_data[i];
  }

  std::array<uint8_t, SIZE> result;
  result.fill(0);

  // fill block-XOR result into final result
  if (0 != tmp_res_64) {
    const uint8_t *tmp_res_bytes =
        reinterpret_cast<const uint8_t *>(&tmp_res_64);
    for (size_t i = 0; i < BLOCK_SIZE; i++) {
      result[i % SIZE] ^= tmp_res_bytes[i];
    }
  }

  // process last block: XOR remaining bytes
  for (size_t i = (len / BLOCK_SIZE) * BLOCK_SIZE; i < len; i++) {
    result[i % SIZE] ^= data[i];
  }

  return result;
}

#pragma pack(1)
/**
 * @brief Cipher format: check_code | enc_type | ctx_id | cipher bytes | cipher
 * check code
 * @param check_code    check code for whole cipher
 * @param enc_type      ref to enc_type_t
 * @param ctx_id        Cipher Context ID, compressed into \c ctx_id_len bytes
 *                      length of ctx_id bytes and enc_type share one byte.
 * @param cipher bytes  cipher bytes of given algorithm, specified in Cipher
 * Context
 * @param check code    append \c CHECK_CODE_SIZE bytes cipher check code to
 * cipher bytes
 */
struct CipherHeader {
  // length of check code in bytes
  static constexpr uint32_t __check_code_size__ =
      static_cast<uint8_t>(Params::CHECK_CODE_SIZE);

  using CheckCode = std::array<uint8_t, __check_code_size__>;
  CheckCode check_code;
  uint8_t version;
  uint8_t data_type;
  uint8_t algo;
  struct CipherNonce {
    uint8_t data[NONCE_LENGTH];
  } nonce;
  static_assert(sizeof(CipherNonce) == 8, "nonce size incorrect");
  uint8_t header_bytes[0];  // At least one byte, max to __max_cid_size__ bytes
  // cipher blob bytes are appended

  CipherFormatVersion getVersion() const {
    return static_cast<CipherFormatVersion>(version);
  }
  void setVersion(CipherFormatVersion v) {
    this->version = static_cast<uint8_t>(v);
  }

  uint8_t getAlg() const { return this->algo; }
  void setAlg(Symmetric::Alg alg) { this->algo = static_cast<uint8_t>(alg); }

  uint8_t getDataType() const noexcept { return this->data_type; }

  void setDataType(uint8_t type) { this->data_type = type; }

  uint32_t size() const noexcept { return sizeof(CipherHeader); }
  uint8_t *end() noexcept { return header_bytes; }
  const uint8_t *end() const noexcept { return header_bytes; }

  void generateCheckCode(uint32_t blobSize) noexcept {
    check_code = xorArray<__check_code_size__>(
        check_code.end(), size() - __check_code_size__ + blobSize);
  }
  bool verifyCheckCode(uint32_t blobSize) const noexcept {
    CipherHeader::CheckCode cc = xorArray<__check_code_size__>(
        check_code.end(), size() - __check_code_size__ + blobSize);

    return cc == check_code;
  }

  uint8_t *getNonce() { return this->nonce.data; }
  void setNonce(const bytes_t &data) {
    memcpy(this->nonce.data, data.data(), NONCE_LENGTH);
  }
};
#pragma pack()

uint8_t Cipher::getDataType() const noexcept {
  const CipherHeader *ch =
      reinterpret_cast<const CipherHeader *>(bytes_.data());
  return ch->getDataType();
}
void Cipher::setDataType(uint8_t type) {
  CipherHeader *ch = reinterpret_cast<CipherHeader *>(bytes_.data());
  return ch->setDataType(type);
}

void checkCipherBytes(const bytes_t &bytes) {
  if (bytes.size() < sizeof(CipherHeader)) {
    throw BadCipherException("Cipher too short with " +
                             std::to_string(bytes.size()) + " bytes");
  }
  const CipherHeader *ch = reinterpret_cast<const CipherHeader *>(bytes.data());
  if (!ch->verifyCheckCode(bytes.size() - ch->size())) {
    throw BadCipherException("Cipher failed check code test");
  }
  encdb::CipherFormatVersion v = ch->getVersion();
  if (v != CipherFormatVersion::VERSION_64) {
    throw BadCipherException("Cipher incorrect version");
  }
  Symmetric::Alg alg = Symmetric::to_alg(ch->getAlg());
  bool valid = true;
  switch (alg) {
    case Symmetric::Alg::SM4_128_CBC:
      valid = (bytes.size() - sizeof(CipherHeader) -
               (int)Symmetric::Params::CBC_IV_SIZE) %
                  (int)Symmetric::Params::SM4_BLOCK_SIZE ==
              0;
      break;
    case Symmetric::Alg::AES_128_CBC:
      valid = (bytes.size() - sizeof(CipherHeader) -
               (int)Symmetric::Params::CBC_IV_SIZE) %
                  (int)Symmetric::Params::AES_BLOCK_SIZE ==
              0;
      break;
    case Symmetric::Alg::SM4_128_GCM:
    case Symmetric::Alg::AES_128_GCM:
      valid = (bytes.size() - sizeof(CipherHeader) -
               (int)Symmetric::Params::GCM_TAG_SIZE -
               (int)Symmetric::Params::GCM_IV_SIZE) > 0;
      break;
    case Symmetric::Alg::SM4_128_ECB:
      valid = (bytes.size() - sizeof(CipherHeader) -
               (int)Symmetric::Params::ECB_IV_SIZE) %
                  (int)Symmetric::Params::SM4_BLOCK_SIZE ==
              0;
      break;
    case Symmetric::Alg::AES_128_ECB:
      valid = (bytes.size() - sizeof(CipherHeader) -
               (int)Symmetric::Params::ECB_IV_SIZE) %
                  (int)Symmetric::Params::AES_BLOCK_SIZE ==
              0;
      break;
    case Symmetric::Alg::SM4_128_CTR:
    case Symmetric::Alg::AES_128_CTR:
      valid = bytes.size() - sizeof(CipherHeader) -
                  (int)Symmetric::Params::CTR_IV_SIZE >
              0;
      break;
    default:
      valid = false;
      break;
  }
  if (!valid) {
    throw BadCipherException("Cipher not valid for alg " + to_string(alg));
  }
}

Cipher::Cipher(bytes_t &bytes, bool check) {
  if (check) {
    checkCipherBytes(bytes);
  }
  this->setBytes(std::move(bytes_t(bytes.data(), bytes.size())));
}

Cipher::Cipher(const bytes_t &bytes, bool check) {
  if (check) {
    checkCipherBytes(bytes);
  }
  this->setBytes(std::move(bytes_t(bytes.data(), bytes.size())));
}

void Cipher::setBytes(bytes_t &&bytes) noexcept { bytes_ = std::move(bytes); }
/**
 * @brief Get cipher total size or data, including cipher header
 */
uint32_t Cipher::size() const noexcept { return bytes_.size(); }
uint32_t Cipher::headerSize() const noexcept {
  const CipherHeader *ch =
      reinterpret_cast<const CipherHeader *>(bytes_.data());
  return ch->size();
}
const uint8_t *Cipher::data() const noexcept { return bytes_.data(); }
uint8_t *Cipher::data() noexcept { return bytes_.data(); }

/**
 * @brief Calculate max required size for plaintext of \c length using \c alg to
 * encrypt it if exists
 * @param [in]length        plaintext length
 * @param [opt]alg          algorithm used to encrypt plaintext
 */
uint32_t Cipher::size(uint32_t length, Symmetric::Alg alg) noexcept {
  return sizeof(CipherHeader) +
         Symmetric::size(length + CipherHeader::__check_code_size__, alg);
}

// For MySQL, now we need to append nonce in cipher data
uint32_t Cipher::size(uint32_t length) noexcept {
  return sizeof(CipherHeader) +
         Symmetric::size(length + CipherHeader::__check_code_size__);
}
// For MySQL proxy. Calculate max cipher len for n plaintexts with total length
// given. For ease of calculation, I just add max padding for each cipher.
uint32_t Cipher::size(uint32_t n, uint32_t total_size) noexcept {
  return n * (sizeof(CipherHeader)) +
         +Symmetric::size(total_size + n * CipherHeader::__check_code_size__) +
         sizeof(CipherBlob) * (n - 1) +
         static_cast<uint32_t>(Symmetric::Params::MAX_BLOCK_SIZE) * n;
}

uint32_t Cipher::encrypt(bytes_t &out, Symmetric::Alg alg, CCFlags flags,
                         const bytes_t &key, const bytes_t &in,
                         const bytes_t &nonce) {
  bytes_t encryption_in;
  uint32_t expectedBlobSize;

  CipherHeader::CheckCode checkCode =
      xorArray<CipherHeader::__check_code_size__>(in.data(), in.size());
  bytes_t inWithCheckCode(in.size() + checkCode.size());
  memcpy(inWithCheckCode.data(), in.data(), in.size());
  // append plaintext integrity check code
  memcpy(inWithCheckCode.data() + in.size(), checkCode.data(),
         checkCode.size());

  encryption_in = std::move(inWithCheckCode);
  expectedBlobSize = Symmetric::size(encryption_in.size(), alg);

  CipherHeader *ch = reinterpret_cast<CipherHeader *>(out.data());
  ch->setAlg(alg);
  ch->setVersion(CipherFormatVersion::VERSION_64);
  out.resize(ch->size() + expectedBlobSize);
  // update CipherHeader handle since resize may update out buffer
  ch = reinterpret_cast<CipherHeader *>(out.data());

  bytes_t cipherBlob(ch->end(), expectedBlobSize, true);
  // check clww first
  if (flags == CCFlags::DET) {
    Symmetric::encrypt(cipherBlob, alg, key, encryption_in);
  } else {
    bytes_t iv(static_cast<uint32_t>(Symmetric::Params::MAX_IV_SIZE));
    Rand::rand(iv);
    Symmetric::encrypt(cipherBlob, alg, key, iv, encryption_in);
  }

  ch->setNonce(nonce);
  ch->generateCheckCode(expectedBlobSize);

  return out.size();
}

/**
 * @brief encrypt \c in data of type set using \c setDataType with algorithm,
 * flags and key specified by CtxID, which should be previously set using \c
 * setCtxID
 */
uint32_t Cipher::encrypt(const CipherContext &cc, const bytes_t &in) {
  return Cipher::encrypt(bytes_, cc.symmAlg, cc.flags, cc.dek, in,
                         bytes_t(cc.nonce, NONCE_LENGTH));
}

uint32_t Cipher::encrypt(const CipherContext &cc, Symmetric::Alg alg,
                         const bytes_t &in) {
  return Cipher::encrypt(bytes_, alg, cc.flags, cc.dek, in,
                         bytes_t(cc.nonce, NONCE_LENGTH));
}

/**
 * @brief decrypt \c in data with given \c key using \c alg algorithm
 * @param [out]out          decrypted result, i.e., plaintext
 * @param [in]alg           algorithm used to do decrypt
 * @param [in]key           key used to do decrypt
 * @param [in]in            encdb cipher data to be decrypted
 * @return decrypted data size, excluding plaintext integrity check code
 */
uint32_t Cipher::decrypt(bytes_t &out, uint8_t &type, const bytes_t &key,
                         const bytes_t &in) {
  const CipherHeader *ch = reinterpret_cast<const CipherHeader *>(in.data());
  if (in.size() < sizeof(CipherHeader)) {
    throw BadCipherException("Cipher way too short");
  }
  if (ch->getVersion() != CipherFormatVersion::VERSION_64) {
    throw BadCipherException("Incorrect cipher version");
  }
  if (!ch->verifyCheckCode(in.size() - ch->size())) {
    throw BadCipherException("Incorrect cipher, invalid check code");
  }
  type = ch->getDataType();

  bytes_t refCipherBlob(ch->end(), in.size() - ch->size());

  bytes_t tmpResult =
      Symmetric::decrypt(Symmetric::to_alg(ch->getAlg()), key, refCipherBlob);

  uint32_t expectedOutSize =
      tmpResult.size() - CipherHeader::__check_code_size__;
  CipherHeader::CheckCode checkCode =
      xorArray<CipherHeader::__check_code_size__>(tmpResult.data(),
                                                  expectedOutSize);
  if (0 != std::memcmp(checkCode.data(), tmpResult.data() + expectedOutSize,
                       checkCode.size())) {
    throw BadCipherException("plaintext integrity check code NOT match");
  }

  if (out.size() < expectedOutSize) {
    throw ResultOutOfMemoryException("Not enough memory for result plaintext",
                                     expectedOutSize);
  }

  out.resize(expectedOutSize);
  memcpy(out.data(), tmpResult.data(), expectedOutSize);

  return out.size();
}
bytes_t Cipher::decrypt(uint8_t &type, const bytes_t &key, const bytes_t &in) {
  const CipherHeader *ch = reinterpret_cast<const CipherHeader *>(in.data());
  bytes_t out(in.size() - ch->size());
  Cipher::decrypt(out, type, key, in);
  return out;
}

/**
 * @brief decrypt current cipher using algorithm and key specified by CtxID,
 * which should be previously set using \c setCtxID decrypted result is set to
 * \c in data
 */
uint32_t Cipher::decrypt(const CipherContext &cc, bytes_t &out) const {
  // Re-derive DEK from MEK + the per-cipher nonce stored in the cipher header.
  // The nonce was embedded at encrypt time (ch->setNonce(cc.nonce)), so any
  // session holding the same MEK can decrypt regardless of which session
  // produced the cipher. This mirrors the JDBC client decrypt path.
  const CipherHeader *ch = reinterpret_cast<const CipherHeader *>(bytes_.data());
  bytes_t cipher_nonce(ch->nonce.data, NONCE_LENGTH);
  bytes_t derived_dek = Hash::hkdf(
      Symmetric::keySize(cc.symmAlg), cc.hashAlg, cc.mek, cipher_nonce, bytes_t());
  uint8_t type;
  return Cipher::decrypt(out, type, derived_dek, bytes_);
}
bytes_t Cipher::decrypt(const CipherContext &cc) const {
  const CipherHeader *ch = reinterpret_cast<const CipherHeader *>(bytes_.data());
  bytes_t cipher_nonce(ch->nonce.data, NONCE_LENGTH);
  bytes_t derived_dek = Hash::hkdf(
      Symmetric::keySize(cc.symmAlg), cc.hashAlg, cc.mek, cipher_nonce, bytes_t());
  uint8_t type;
  bytes_t out = Cipher::decrypt(type, derived_dek, bytes_);
  return out;
}

std::array<uint8_t, static_cast<uint32_t>(Params::CHECK_CODE_SIZE)>
Cipher::CalcCheckCode(const uint8_t *data, uint32_t len) {
  return xorArray<static_cast<uint32_t>(Params::CHECK_CODE_SIZE)>(data, len);
}

#if 0  // NOT_IN_USE
#pragma pack(1)
struct KeyCipherBlob {
  Symmetric::Alg alg;
  uint8_t data[];  // encrypted KeyPlainBlob
};

struct KeyPlainBlob {
  DekId dek_id;
  uint8_t data[];
};
#pragma pack()

bytes_t KeyCipher::encrypt(const Symmetric::Alg &alg, const DekId &dek_id,
                           const bytes_t &plaintext_key, const bytes_t &key) {
  uint32_t dek_blob_length = plaintext_key.size() + sizeof(KeyPlainBlob);
  uint32_t cipher_length = Symmetric::size(dek_blob_length, alg);
  bytes_t result(sizeof(KeyCipherBlob) + cipher_length);

  KeyCipherBlob *key_cipher_blob =
      reinterpret_cast<KeyCipherBlob *>(result.data());
  key_cipher_blob->alg = alg;

  bytes_t tmp(key_cipher_blob->data, cipher_length);
  bytes_t iv =
      Rand::rand(static_cast<uint32_t>(Symmetric::Params::MAX_IV_SIZE));

  bytes_t key_plain_blob_bytes(dek_blob_length);
  KeyPlainBlob *key_plain_blob =
      reinterpret_cast<KeyPlainBlob *>(key_plain_blob_bytes.data());
  key_plain_blob->dek_id = dek_id;
  memcpy(key_plain_blob->data, plaintext_key.data(), plaintext_key.size());

  Symmetric::encrypt(tmp, alg, key, iv, key_plain_blob_bytes);

  return result;
}
bytes_t KeyCipher::decrypt(DekId &dek_id, const bytes_t &ciphertext_key,
                           const bytes_t &key) {
  const KeyCipherBlob *key_cipher_blob =
      reinterpret_cast<const KeyCipherBlob *>(ciphertext_key.data());
  bytes_t tmp(key_cipher_blob->data,
              ciphertext_key.size() - sizeof(KeyCipherBlob));

  bytes_t key_plain_blob_bytes =
      Symmetric::decrypt(key_cipher_blob->alg, key, tmp);

  KeyPlainBlob *key_plain_blob =
      reinterpret_cast<KeyPlainBlob *>(key_plain_blob_bytes.data());
  dek_id = key_plain_blob->dek_id;
  bytes_t plaintext_key(key_plain_blob->data,
                        key_plain_blob_bytes.size() - sizeof(KeyPlainBlob),
                        false);

  return plaintext_key;
}

/**
 * @brief compute KeyCipher size for given \c key_length and \c alg
 *
 * @param key_length    plaintext key length, excluding dekid
 * @param alg           algorithm used to encrypt plaintext key
 * @return expected size
 */
uint32_t KeyCipher::size(uint32_t key_length, Symmetric::Alg alg) noexcept {
  return Symmetric::size(key_length, alg) + sizeof(KeyPlainBlob) +
         sizeof(KeyCipherBlob);
}
uint32_t KeyCipher::size(uint32_t key_length) noexcept {
  return Symmetric::size(key_length) + sizeof(KeyPlainBlob) +
         sizeof(KeyCipherBlob);
}
#endif

#pragma pack(1)
struct Envelope {
  // cipher length of ephemeral symmetric key encrypted by public key. 2^16 is
  // enough for all public key length.
  uint16_t sealedEphemeralKeyLen;
  uint8_t sealedEphemeralKey[0];  // cipher of ephemeral symmetric key encrypted
                                  // by public key

  // CipherBlob is appended, which is encrypted using algorithm specified in
  // Cipher Suite
  const uint8_t *data() const noexcept {
    return sealedEphemeralKey + sealedEphemeralKeyLen;
  }
  uint8_t *data() noexcept {
    return sealedEphemeralKey + sealedEphemeralKeyLen;
  }
};
#pragma pack()

/**
 * @brief seal \c in data into envelope with given \c pubkey using \c suite
 * algorithm, following:
 *      - encrypting \c in with ephemeral symmetric key,
 *      - and then encrypting the ephemeral key with \c pubkey
 * @param [out]out          envelope blob, in the format specified by algorithm
 * @param [in]suite         cipher suite used to do encrypt
 * @param [in]pubkey        public key used to do encrypt, either raw bytes or
 * pem string
 * @param [in]in            data to be encrypted
 */
bytes_t KeyExchange::seal(const CipherSuite &suite,
                          const std::string &pubkey_string, const bytes_t &in) {
  Asymmetric::Alg asymmAlg = suite.get_asymm();
  Symmetric::Alg symmAlg = suite.get_symm();
  uint32_t symmKeySize = Symmetric::get_key_size(symmAlg);

  bytes_t ephemeralKey = Rand::rand(symmKeySize);

  bytes_t pukEncResult;
  {
    // TODO(yx): Asymmetric::encrypt has parallel problem. Temporary fix as
    // this.
    static std::mutex asymmEncMutex;
    const std::lock_guard<std::mutex> asymmEncLock(asymmEncMutex);
    pukEncResult = Asymmetric::encrypt(asymmAlg, pubkey_string, ephemeralKey);
  }

  uint32_t cipherLen = Symmetric::size(in.size(), symmAlg);
  bytes_t out(sizeof(Envelope) + pukEncResult.size() + cipherLen);

  Envelope *envelope = reinterpret_cast<Envelope *>(out.data());
  envelope->sealedEphemeralKeyLen = pukEncResult.size();
  std::copy(pukEncResult.data(), pukEncResult.data() + pukEncResult.size(),
            envelope->sealedEphemeralKey);

  bytes_t cipher(envelope->data(), cipherLen);
  Symmetric::encrypt(cipher, symmAlg, ephemeralKey, in);

  return out;
}
/**
 * @brief open envelope \c in with given \c prikey using \c suite algorithm
 *      - decrypting the ephemeral symmetric key with \c prikey
 *      - and then decrypting the encrypted data with the ephemeral key
 * @param [out]out          decrypted result, i.e., plaintext
 * @param [in]suite         cipher suite used to do decrypt
 * @param [in]prikey        private key used to do decrypt, either raw bytes or
 * pem string
 * @param [in]in            envelope blob to be decrypted, in the format
 * specified by algorithm
 */
void KeyExchange::open(bytes_t &out, const CipherSuite &suite,
                       const std::string &prikey_string, const bytes_t &in) {
  Asymmetric::Alg asymmAlg = suite.get_asymm();
  Symmetric::Alg symmAlg = suite.get_symm();

  const Envelope *envelope = reinterpret_cast<const Envelope *>(in.data());
  bytes_t ephemeralKey = Asymmetric::decrypt(
      asymmAlg, prikey_string,
      bytes_t(envelope->sealedEphemeralKey, envelope->sealedEphemeralKeyLen));

  uint32_t symmKeySize = Symmetric::get_key_size(symmAlg);
  if (ephemeralKey.size() != symmKeySize) {
    throw InvalidAlgorithmParameterException(
        "Envelope invalid key length " + std::to_string(ephemeralKey.size()) +
        ", expected " + std::to_string(static_cast<uint32_t>(symmKeySize)));
  }

  bytes_t cipher(envelope->data(), in.size() - sizeof(Envelope) -
                                       envelope->sealedEphemeralKeyLen);
  Symmetric::decrypt(out, symmAlg, ephemeralKey, cipher);
}
bytes_t KeyExchange::open(const CipherSuite &suite,
                          const std::string &prikey_string, const bytes_t &in) {
  const Envelope *envelope = reinterpret_cast<const Envelope *>(in.data());
  bytes_t out(in.size() - sizeof(Envelope) - envelope->sealedEphemeralKeyLen);

  KeyExchange::open(out, suite, prikey_string, in);
  return out;
}

// NOT_IN_USE
#if 0
#pragma pack(1)
struct SignatureBlob {
  Asymmetric::Alg alg;  // signature algorithm
  uint8_t data[];       // signture value
};
#pragma pack()

/**
 * @brief Sign the digest of input data \c in using the given signature
 * algorithm. The hash algorithm selected: RSA - SHA256; SM2 - SM3
 *
 * @param alg asymmetric signature algorithm
 * @param prikey_string private key pem string
 * @param in input data of any length, the digest of it computing from related
 * hash algorithm will be signed
 * @return bytes_t signature blob, including signatureAlgorithm and
 * signagtureValue
 */
bytes_t Signature::sign(const Asymmetric::Alg &alg,
                        const std::string &prikey_string, const bytes_t &in) {
  bytes_t sig_value = Asymmetric::sign(alg, prikey_string, in);

  bytes_t out(sizeof(SignatureBlob) + sig_value.size());
  SignatureBlob *blob_ptr = reinterpret_cast<SignatureBlob *>(out.data());
  blob_ptr->alg = alg;
  memcpy(blob_ptr->data, sig_value.data(), sig_value.size());

  return out;
}
/**
 * @brief Verify the signature of input data
 *
 * @param pubkey_string public key pem string
 * @param sig_blob signature blob, including signatureAlgorithm and
 * signagtureValue
 * @param in input data of any length
 * @return true
 * @return false
 */
bool Signature::verify(const std::string &pubkey_string,
                       const bytes_t &sig_blob, const bytes_t &in) {
  const SignatureBlob *blob_ptr =
      reinterpret_cast<const SignatureBlob *>(sig_blob.data());
  bytes_t sig_value(blob_ptr->data, sig_blob.size() - sizeof(SignatureBlob));

  return Asymmetric::verify(blob_ptr->alg, pubkey_string, sig_value, in);
}

#pragma pack(1)
struct HmacBlob {
  Hash::Alg alg;   // signature algorithm
  uint8_t data[];  // signture value
};
#pragma pack()

/**
 * @brief sign input data \c in with \c key using given hmac algorithm \c alg
 *
 * @param alg hash algorithm used to do hmac
 * @return hmac result
 */
bytes_t Signature::sign_hmac(const Hash::Alg &alg, const bytes_t &key,
                             const bytes_t &in) {
  bytes_t sig_value = Hash::hmac(alg, key, in);

  bytes_t out(sizeof(HmacBlob) + sig_value.size());
  HmacBlob *blob_ptr = reinterpret_cast<HmacBlob *>(out.data());
  blob_ptr->alg = alg;
  memcpy(blob_ptr->data, sig_value.data(), sig_value.size());

  return out;
}
/**
 * @brief verify the hmac \c mac_blob of the input data \c in
 *
 * @param alg hash algorithm used to do hmac
 * @return true - signature match; otherwise false - mismatch
 */
bool Signature::verify_hmac(const bytes_t &key, const bytes_t &mac_blob,
                            const bytes_t &in) {
  const HmacBlob *blob_ptr =
      reinterpret_cast<const HmacBlob *>(mac_blob.data());
  return mac_blob == Signature::sign_hmac(blob_ptr->alg, key, in);
}
#endif

}  // namespace encdb
