/*
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */

#include "ra.h"
#include <cstring>
#include <string>
#include "EncDBConfig.h"
#include "encdb_hash.h"
#include "encdb_key_exception.h"
#include "helper.hpp"
#include "json.hpp"
#include "key_mgmt.h"

namespace encdb {

const std::string getEnclaveDataJsonStr() {
  json::JSON enclave_data_json;
  enclave_data_json["public_key"] = getPublicKey();
  CipherSuite cipher_suite = getDefaultCipherSuite();
  enclave_data_json["cipher_suite"] = cipher_suite.to_string();
  return enclave_data_json.dump();
}

void prepareRaReportData(RaReportData &report_data,
                         const std::string &user_data_json_str) {
  // prepare enclave data, i.e., enclave public key and cipher suite
  std::string enclave_data_json_str = getEnclaveDataJsonStr();
  prepareRaReportData(report_data, user_data_json_str, enclave_data_json_str);
}

void prepareRaReportData(RaReportData &report_data,
                         const std::string &user_data_json_str,
                         const std::string &enclave_data_json_str) {
  // use digests of user data and enclave data as report data, in order
  // user_data_hash || enclave_data_hash
  CipherSuite cipher_suite = getDefaultCipherSuite();
  bytes_t user_data_hash = Hash::hash(
      cipher_suite.get_hash(),
      bytes_t(reinterpret_cast<const uint8_t *>(user_data_json_str.data()),
              user_data_json_str.size()));
  bytes_t enclave_data_hash = Hash::hash(
      cipher_suite.get_hash(),
      bytes_t(reinterpret_cast<const uint8_t *>(enclave_data_json_str.data()),
              enclave_data_json_str.size()));
  if (user_data_hash.size() + enclave_data_hash.size() > sizeof(RaReportData)) {
    throw EncdbRAException("out of max length of report data, which is " +
                           std::to_string(sizeof(RaReportData)));
  }

  memset(&report_data, 0, sizeof(RaReportData));
  memcpy(&report_data.user_data_hash, user_data_hash.data(),
         user_data_hash.size());
  memcpy(&report_data.enclave_data_hash, enclave_data_hash.data(),
         enclave_data_hash.size());
}

const std::string getRaQuote(const std::string &user_data_json_str) {
  RaReportData report_data;
  memset(&report_data, 0, sizeof(report_data));

  const std::string enclave_data_str = getEnclaveDataJsonStr();
  prepareRaReportData(report_data, user_data_json_str, enclave_data_str);

  // generate quote binding with given report data
#ifdef ENCDB_BUILD_WITH_TEE
  std::vector<uint8_t> quote = tdxGetRaQuote(report_data);
#else
  std::vector<uint8_t> quote = mockGetRaQuote(report_data);
#endif

  // pack result into json string, without user data since the caller already
  // owns
  std::string base64_quote_str = Utils::base64_encode(quote);
  json::JSON ret_json;
  ret_json["quote"] = base64_quote_str;
  ret_json["enclave_data"] = enclave_data_str;
  ret_json["user_data"] = user_data_json_str;

  return ret_json.dump();
}

const std::string verifyRaQuote(const std::string &quote_str,
                                const std::string &user_data_json_str,
                                const std::string &enclave_data_json_str) {
  if (user_data_json_str.empty() || enclave_data_json_str.empty()) {
    throw EncdbRAException("verify remote attestation failed");
  }
  std::vector<uint8_t> quote = Utils::base64_decode(quote_str);
#ifdef ENCDB_BUILD_WITH_TEE
  int ret = tdxVerifyQuote(quote);
#else
  int ret = mockVerifyQuote(quote);
#endif
  if (ret < 0) {
    throw EncdbRAException("verify remote attestation failed");
  }

  // FIXME: do nothing for now
  bytes_t envelope_bytes;
  return Utils::base64_encode(envelope_bytes);
}

std::vector<uint8_t> mockGetRaQuote(const RaReportData &report_data) {
  // FIXME: leave empty
  std::vector<uint8_t> result(sizeof(RaReportData));
  memcpy(result.data(), &report_data, sizeof(RaReportData));

  return result;
}

int mockVerifyQuote(const std::vector<uint8_t> &quote) {
  // FIXME: always success
  if (quote.empty()) {
    throw EncdbRAException("verify remote attestation failed");
  }
  return 0;
}

}  // namespace encdb
