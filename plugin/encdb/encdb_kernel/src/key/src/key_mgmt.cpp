/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: implementation for untrusted key management
 */
#include "key_mgmt.h"
#include "storage.h"
#include <cassert>
#include <climits>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>
#include "db_operation.h"
#include "encdb_cipher.h"
#include "encdb_common_exception.h"
#include "encdb_crypto_exception.h"
#include "encdb_crypto_provider.h"
#include "encdb_key_exception.h"
#include "encdb_req_types.h"
#include "encdb_symmetric.h"
#include "helper.hpp"
#include "json.hpp"
#include "logger.h"

// define variables
extern char debug;
extern char verbose;

namespace encdb {
// internal key mgmt utils
using std::string;
using std::to_string;

static std::mutex key_pair_mutex;

// Random CC for DUMMY_USERNAME (users without RESTRICTED_ACCESS).
// Initialized once on first access; read-only thereafter.
static CipherContext g_dummy_cc;
static std::once_flag g_dummy_cc_once;

// Initialize g_dummy_cc with random MEK+DEK so DUMMY_USERNAME encrypts
// with ephemeral key material that no real user can derive or match.
static void initDummyCC() {
  auto cipher_suite = CipherSuite::RSA_WITH_AES_128_CBC_SHA256;
  g_dummy_cc.username = DUMMY_USERNAME;
  g_dummy_cc.symmAlg = cipher_suite.get_symm();
  g_dummy_cc.hashAlg = cipher_suite.get_hash();
  bytes_t rand_mek(Symmetric::keySize(g_dummy_cc.symmAlg));
  bytes_t rand_nonce(g_dummy_cc.nonce, NONCE_LENGTH);
  encdb_rand_bytes(rand_mek);
  encdb_rand_bytes(rand_nonce);
  g_dummy_cc.dek = Hash::hkdf(Symmetric::keySize(g_dummy_cc.symmAlg),
                               cipher_suite.get_hash(), rand_mek, rand_nonce,
                               bytes_t());
  g_dummy_cc.mek = std::move(rand_mek);
}

static std::string private_key, public_key, public_key_hash;

/** Default encryption algorithm, only for importing new meks. */
Symmetric::Alg default_algo = Symmetric::Alg::SM4_128_GCM;

void setDefaultEncryptionAlgorithm(std::string algorithm) {
  if (algorithm == "AES_128_GCM")
    default_algo = Symmetric::Alg::AES_128_GCM;
  else if (algorithm == "AES_128_ECB")
    default_algo = Symmetric::Alg::AES_128_ECB;
  else if (algorithm == "AES_128_CTR")
    default_algo = Symmetric::Alg::AES_128_CTR;
  else if (algorithm == "AES_128_CBC")
    default_algo = Symmetric::Alg::AES_128_CBC;
  else if (algorithm == "SM4_128_CBC")
    default_algo = Symmetric::Alg::SM4_128_CBC;
  else if (algorithm == "SM4_128_ECB")
    default_algo = Symmetric::Alg::SM4_128_ECB;
  else if (algorithm == "SM4_128_CTR")
    default_algo = Symmetric::Alg::SM4_128_CTR;
  else
    default_algo = Symmetric::Alg::SM4_128_GCM;
}

inline Symmetric::Alg getDefaultEncryptionAlgorithm() { return default_algo; }

CipherSuite getDefaultCipherSuite() {
  return CipherSuite::RSA_WITH_AES_128_GCM_SHA256;
}

void generateAndSetKeyPair() {
  CipherSuite cipher_suite = getDefaultCipherSuite();
  Asymmetric::Alg alg = cipher_suite.get_asymm();
  Hash::Alg hash_alg = cipher_suite.get_hash();
  uint32_t num_bits =
      static_cast<uint32_t>(Params::DEFAULT_RSA_KEY_SIZE_IN_BITS);
  if (alg == Asymmetric::Alg::SM2) {
    num_bits = 0;
  }

  std::string private_key_pem, public_key_pem;
  Asymmetric::generateKeyPair(private_key_pem, public_key_pem, alg, num_bits);

  std::string public_key_pem_hash = Utils::base64_encode(Hash::hash(
      hash_alg,
      bytes_t(reinterpret_cast<const uint8_t *>(public_key_pem.data()),
              public_key_pem.size())));

  private_key = private_key_pem;
  public_key = public_key_pem;
  public_key_hash = public_key_pem_hash;
}

std::string getPrivateKey() {
  const std::lock_guard<std::mutex> lock(key_pair_mutex);
  if (private_key.empty()) {
    generateAndSetKeyPair();
  }
  return private_key;
}

std::string getPublicKey() {
  const std::lock_guard<std::mutex> lock(key_pair_mutex);
  if (public_key.empty()) {
    generateAndSetKeyPair();
  }
  return public_key;
}

std::string getPublicKeyHash() {
  const std::lock_guard<std::mutex> lock(key_pair_mutex);
  if (public_key_hash.empty()) {
    generateAndSetKeyPair();
  }
  return public_key_hash;
}

const CipherContext &getCC(const std::string &uname) {
  if (uname == DUMMY_USERNAME) {
    std::call_once(g_dummy_cc_once, initDummyCC);
    return g_dummy_cc;
  }
  // Per-connection CC, set during MEK_PROVISION on this connection.
  // If not found, the caller has not performed MEK_PROVISION.
  const CipherContext *cc = encdb_plugin_get_session_cc();
  if (cc && cc->username == uname) return *cc;
  throw EncdbCcMissException(uname);
}

// Since all users share same MEK in this version, we need to protect MEK
// operations with locks. For simplicity, I just control all MEK operations with
// 1 single lock.
static std::mutex mek_op_mutex;

/*
 * skipCheck avoid checking cipher_suite and public_key_hash
 * should be set to true when importing mek from another encdb node
 */
void encdb_envelope_open_mek(const std::string &envelope,
                             CipherSuite &cipher_suite, CipherContext &cc,
                             bytes_t &old_mek) {
  // extract the right envelope according to the public key hash
  bytes_t envelope_bytes;
  {
    json::JSON envelopeJson = json::JSON::Load(envelope);
    if (envelopeJson.IsNull()) {
      throw InvalidJsonDataException("Load envelope json failure");
    }
    std::string tempRightEnvelope = envelopeJson["envelope"].ToString();
    if (tempRightEnvelope.empty()) {
      throw InvalidJsonDataException(
          "Not valid envelope: missing \"envelope\" field");
    }
    envelope_bytes = Utils::base64_decode_to_bytes(tempRightEnvelope);
    if (envelopeJson["public_key_hash"].ToString() != getPublicKeyHash()) {
      throw InvalidJsonDataException(
          "public key used to seal envelope is not matched with that in encdb");
    }
    cipher_suite = getDefaultCipherSuite();
    if (envelopeJson.hasKey("algorithm")) {
      cc.symmAlg = to_symmetric_alg(envelopeJson["algorithm"].ToString());
    } else {
      cc.symmAlg = getDefaultEncryptionAlgorithm();
    }
  }

  // open envelope to get key info
  {
    bytes_t keyInfo =
        KeyExchange::open(cipher_suite, getPrivateKey(), envelope_bytes);

    json::JSON recvKeyInfoJson = json::JSON::Load(
        string(reinterpret_cast<char *>(keyInfo.data()), keyInfo.size()));
    if (recvKeyInfoJson.IsNull()) {
      throw InvalidJsonDataException("Load keyInfo json failure");
    }

    bytes_t temp_mek =
        Utils::base64_decode_to_bytes(recvKeyInfoJson["mek"].ToString());
    // TODO: other fields unused

    size_t mek_size = temp_mek.size();
    if (mek_size < 16) {
      throw InvalidAlgorithmParameterException(
          "Invalid key length " + std::to_string(mek_size) +
          ", minimum 16 bytes required");
    }

    /*
     * Get old_mek from envelope. Will be compared using old_mac later.
     */
    if (recvKeyInfoJson.hasKey("old_mek")) {
      old_mek =
          Utils::base64_decode_to_bytes(recvKeyInfoJson["old_mek"].ToString());
    }
    cc.mek = std::move(temp_mek);
  }
}

// used to import MEK (assume no MEK exists for same session id)
void import_mek(const std::string &envelope_str, const std::string &username,
                std::string &new_mac_str) {
  CipherSuite cipher_suite;
  CipherContext cc;
  cc.username = username;

  bytes_t mock_old_mek;
  encdb_envelope_open_mek(envelope_str, cipher_suite, cc, mock_old_mek);
  // generate salt and mac
  Hash::Alg hash_alg = cipher_suite.get_hash();
  bytes_t new_mac_bytes(static_cast<uint32_t>(Params::SALT_SIZE) +
                        static_cast<uint32_t>(Params::MAC_SIZE));
  uint32_t salt_length = static_cast<uint32_t>(Params::SALT_SIZE);
  bytes_t salt_bytes(new_mac_bytes.data(), salt_length);
  bytes_t mac_bytes(new_mac_bytes.data() + salt_length,
                    new_mac_bytes.size() - salt_length);
  encdb_rand_bytes(salt_bytes);
  Hash::hmac(mac_bytes, hash_alg, cc.mek, salt_bytes);
  new_mac_str = Utils::base64_encode(new_mac_bytes);

  // MEK import done. Generate nonce and DEK.
  bytes_t nonce(cc.nonce, NONCE_LENGTH);
  encdb_rand_bytes(nonce);
  bytes_t dek = Hash::hkdf(Symmetric::keySize(cipher_suite.get_symm()),
                           cipher_suite.get_hash(), cc.mek, nonce, bytes_t());
  cc.dek = std::move(dek);
  cc.hashAlg = cipher_suite.get_hash();
  encdb_plugin_set_session_cc(&cc);
}

// Check input MEK against its mac
void check_mek(const std::string &envelope_str, const std::string &username,
               const std::string &old_mac_str) {
  CipherSuite cipher_suite;
  CipherContext cc;
  cc.username = username;

  bytes_t mock_old_mek;
  encdb_envelope_open_mek(envelope_str, cipher_suite, cc, mock_old_mek);

  Hash::Alg hash_alg = cipher_suite.get_hash();
  uint32_t salt_length = static_cast<uint32_t>(Params::SALT_SIZE);
  bytes_t old_mac_bytes = Utils::base64_decode_to_bytes(old_mac_str);
  bytes_t tmp_mac_bytes =
      Hash::hmac(hash_alg, cc.mek, bytes_t(old_mac_bytes.data(), salt_length));
  if (bytes_t(old_mac_bytes.data() + salt_length,
              old_mac_bytes.size() - salt_length) != tmp_mac_bytes) {
    throw MacMismatchException(
        "verify mek mac fail: real_mac=" + old_mac_bytes.to_string() +
        ", expected_mac=" + tmp_mac_bytes.to_string());
  }
  // update dek
  bytes_t nonce(cc.nonce, NONCE_LENGTH);
  encdb_rand_bytes(nonce);
  bytes_t dek = Hash::hkdf(Symmetric::keySize(cc.symmAlg), hash_alg, cc.mek,
                           nonce, bytes_t());
  cc.dek = std::move(dek);
  cc.hashAlg = hash_alg;
  encdb_plugin_set_session_cc(&cc);
}

// Local-mode MEK rotation: client re-seals new+old MEK in an envelope and
// provides the previous salt_mac for verification.  KMS-mode rotation uses
// encdbImportMekFromKms (request_type=50) with an incremented version instead.
void rotate_local_mek(const std::string &envelope_str,
                      const std::string &username,
                      const std::string &old_mac_str, std::string &new_mac_str) {
  CipherSuite cipher_suite;
  CipherContext updated_cc;
  // Set username so the rotated CC can be matched by getCC() on this
  // connection; otherwise the next query would CC_MISS after local rotation.
  updated_cc.username = username;

  bytes_t old_mek;
  encdb_envelope_open_mek(envelope_str, cipher_suite, updated_cc, old_mek);
  if (!old_mek) {
    throw EncdbOldMekMissException("miss old_mek in envelope");
  }
  bytes_t old_salt_mac_bytes = Utils::base64_decode_to_bytes(old_mac_str);
  bytes_t new_salt_mac_bytes(old_salt_mac_bytes.size());
  Hash::Alg hash_alg = cipher_suite.get_hash();
  /*
   * provided old_mek should be matched with that current used in enclave,
   * and the mac should be matched as well.
   */
  uint32_t salt_length = static_cast<uint32_t>(Params::SALT_SIZE);
  bytes_t salt_bytes(old_salt_mac_bytes.data(), salt_length);
  bytes_t old_mac_bytes(old_salt_mac_bytes.data() + salt_length,
                        old_salt_mac_bytes.size() - salt_length);
  bytes_t tmp_mac_bytes = Hash::hmac(hash_alg, old_mek, salt_bytes);
  if (old_mac_bytes != tmp_mac_bytes) {
    throw MacMismatchException("verify old mek mac fail");
  }

  memcpy(new_salt_mac_bytes.data(), old_salt_mac_bytes.data(),
         old_salt_mac_bytes.size());
  bytes_t updated_mac_bytes(new_salt_mac_bytes.data() + salt_length,
                            new_salt_mac_bytes.size() - salt_length);
  Hash::hmac(updated_mac_bytes, hash_alg, updated_cc.mek, salt_bytes);
  new_mac_str = Utils::base64_encode(new_salt_mac_bytes);

  // regenerate dek
  bytes_t nonce(updated_cc.nonce, NONCE_LENGTH);
  encdb_rand_bytes(nonce);
  bytes_t dek =
      Hash::hkdf(Symmetric::keySize(cipher_suite.get_symm()),
                 cipher_suite.get_hash(), updated_cc.mek, nonce, bytes_t());
  updated_cc.dek = std::move(dek);
  updated_cc.hashAlg = cipher_suite.get_hash();
  encdb_plugin_set_session_cc(&updated_cc);
}

/*
 * Both encdb_sql_get_saltmac and encdb_sql_set_saltmac should always be
 * executed as hook function (with high privileges) Otherwise, only users with
 * high privileges can do MEK_PROVISION. This is meaningless, because these same
 * users can modify our database in anyway they want. Our purpose is to restrict
 * the behavior of users with low-level privileges.
 */
#if ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB
EncdbStatus encdbUpdateMek(const std::string &keyEnvelope,
                           const std::string &username) {
  std::string old_mac, new_mac;
  const std::lock_guard<std::mutex> mekOpLock(mek_op_mutex);
  hook::encdb_execute_sql_read(encdb_sql_get_saltmac(username), old_mac);
#elif ENCDB_DB_TYPE == POLARDB_MYSQL
EncdbStatus encdbUpdateMek(const std::string &keyEnvelope,
                           const std::string &username,
                           const std::string &old_mac, std::string &new_mac) {
  const std::lock_guard<std::mutex> mekOpLock(mek_op_mutex);
#endif
  if (old_mac.empty()) {
    ENCDB_LOG(LL_WARNING, "no existing mac in db. use import mek instead..");
    import_mek(keyEnvelope, username, new_mac);
  } else {
    rotate_local_mek(keyEnvelope, username, old_mac, new_mac);
  }
#if ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB
  hook::encdb_execute_sql_write(encdb_sql_set_saltmac(username, new_mac));
#endif
  return ENCDB_STATUS_OK;
}
/**
 * Non-KMS path: check existing MEK (if mac present) or import new MEK.
 */
static void check_or_import_mek(const std::string &keyEnvelope,
                                const std::string &username,
                                const std::string &verify_mac,
                                std::string &new_mac) {
  if (!verify_mac.empty()) {
    if (verbose) {
      ENCDB_LOG(LL_DEBUG, "verify mac exists, check_mek...\n");
    }
    check_mek(keyEnvelope, username, verify_mac);
  } else {
    // Non-KMS + first-time user only.
    // (KMS mode never reaches here -- kms_salt_mac is written during import.)
    if (verbose) {
      ENCDB_LOG(LL_DEBUG, "no mac found, import_mek...\n");
    }
    import_mek(keyEnvelope, username, new_mac);
  }
}

#if ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB

EncdbStatus encdbRecvMek(const std::string &keyEnvelope,
                         const std::string &username) {
  const std::lock_guard<std::mutex> mekOpLock(mek_op_mutex);

  KmsMetaRecord kms_rec;
  bool kms_mode = (encdb_plugin_kms_meta_read_latest(&kms_rec) == 0);

  if (kms_mode) {
    // KMS: verify only. Throws MacMismatchException on failure; never degrades.
    check_mek(keyEnvelope, username, kms_rec.kms_salt_mac);
    return ENCDB_STATUS_OK;
  }

  // Non-KMS: read user_salt_mac from local auth_users table
  std::string user_salt_mac;
  hook::encdb_execute_sql_read(encdb_sql_get_saltmac(username), user_salt_mac);
  std::string new_mac;
  check_or_import_mek(keyEnvelope, username, user_salt_mac, new_mac);
  if (!new_mac.empty()) {
    hook::encdb_execute_sql_write(encdb_sql_set_saltmac(username, new_mac));
  }
  return ENCDB_STATUS_OK;
}

#elif ENCDB_DB_TYPE == POLARDB_MYSQL

EncdbStatus encdbRecvMek(const std::string &keyEnvelope,
                         const std::string &username,
                         const std::string &old_mac, std::string &new_mac) {
  const std::lock_guard<std::mutex> mekOpLock(mek_op_mutex);

  KmsMetaRecord kms_rec;
  bool kms_mode = (encdb_plugin_kms_meta_read_latest(&kms_rec) == 0);

  if (kms_mode) {
    // KMS: verify only. Throws MacMismatchException on failure; never degrades.
    check_mek(keyEnvelope, username, kms_rec.kms_salt_mac);
    return ENCDB_STATUS_OK;
  }

  // Non-KMS: use CN-provided old_mac
  check_or_import_mek(keyEnvelope, username, old_mac, new_mac);
  return ENCDB_STATUS_OK;
}

#endif


// --- KMS mode support ---

// MEK_IMPORT_FROM_KMS (request_type=0x32): register or rotate MEK via KMS.
// Request format:
//   { "request_type": 50,
//     "kms_enc_mek": "<base64 KMS-encrypted MEK>",
//     "plaintext":   "<base64 plaintext MEK>",
//     "kms_region":  "<KMS region string>",
//     "kms_key_id":  "<KMS key ID string>" }
void encdbImportMekFromKms(const std::string &msgIn) {
  json::JSON req = json::JSON::Load(msgIn);

  // Per-field validation: each missing key gets a distinct error message
  // so that MTR tests can assert on individual field-level errors.
  if (!req.hasKey("kms_enc_mek"))
    throw InvalidJsonDataException("does not have kms_enc_mek");
  if (!req.hasKey("plaintext"))
    throw InvalidJsonDataException("does not have plaintext");
  if (!req.hasKey("kms_region"))
    throw InvalidJsonDataException("does not have kms_region");
  if (!req.hasKey("kms_key_id"))
    throw InvalidJsonDataException("does not have kms_key_id");

  std::string kms_enc_mek = req["kms_enc_mek"].ToString();
  std::string plaintext = req["plaintext"].ToString();
  std::string kms_region = req["kms_region"].ToString();
  std::string kms_key_id = req["kms_key_id"].ToString();

  // Required fields present but empty
  if (kms_enc_mek.empty() || plaintext.empty() || kms_region.empty() ||
      kms_key_id.empty()) {
    throw InvalidJsonDataException(
        "MEK_IMPORT_FROM_KMS: missing required fields "
        "(kms_enc_mek, plaintext, kms_region, kms_key_id)");
  }

  // Decode MEK plaintext (base64 → raw bytes)
  bytes_t mek = Utils::base64_decode_to_bytes(plaintext);

  // Generate kms_salt_mac (HMAC over MEK with random salt)
  CipherSuite cipher_suite = getDefaultCipherSuite();
  Hash::Alg hash_alg = cipher_suite.get_hash();
  uint32_t salt_length = static_cast<uint32_t>(Params::SALT_SIZE);
  bytes_t mac_bytes_buf(salt_length +
                        static_cast<uint32_t>(Params::MAC_SIZE));
  bytes_t salt_bytes(mac_bytes_buf.data(), salt_length);
  bytes_t mac_bytes(mac_bytes_buf.data() + salt_length,
                    mac_bytes_buf.size() - salt_length);
  encdb_rand_bytes(salt_bytes);
  Hash::hmac(mac_bytes, hash_alg, mek, salt_bytes);
  std::string kms_salt_mac = Utils::base64_encode(mac_bytes_buf);

  // Persist to KMS meta table
  if (encdb_plugin_kms_meta_insert(&kms_enc_mek, &kms_region,
                                   &kms_key_id, &kms_salt_mac)) {
    throw EncdbException(ENCDB_EXCEPTION_KMS_META_WRITE_FAILED,
                         "MEK_IMPORT_FROM_KMS: failed to persist KMS metadata");
  }

  ENCDB_LOG(LL_INFO,
            "MEK_IMPORT_FROM_KMS: registered region=%s\n",
            kms_region.c_str());
}

// MEK_DETACH_KMS (request_type=0x33): detach from KMS mode, delete all KMS metadata.
// Request format:
//   { "request_type": 51 }
void encdbExitKmsMode() {
  if (encdb_plugin_kms_meta_delete_all())
    throw EncdbException(ENCDB_EXCEPTION_KMS_META_DELETE_FAILED,
                         "MEK_DETACH_KMS: failed to delete KMS metadata");
  ENCDB_LOG(LL_INFO, "MEK_DETACH_KMS: KMS metadata cleared\n");
}


}  // namespace encdb
