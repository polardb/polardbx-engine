/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#include <cstdint>
#include <string>
#include <vector>

namespace encdb {

struct RaReportData {
  uint8_t user_data_hash[32];
  uint8_t enclave_data_hash[32];
};

const std::string getEnclaveDataJsonStr();
void prepareRaReportData(RaReportData &report_data,
                         const std::string &user_data_json_str);
void prepareRaReportData(RaReportData &report_data,
                         const std::string &user_data_json_str,
                         const std::string &enclave_data_json_str);

// FIXME: use single function for simple. it may be more elegant to use class.
std::vector<uint8_t> mockGetRaQuote(const RaReportData &report_data);
int mockVerifyQuote(const std::vector<uint8_t> &quote);
std::vector<uint8_t> tdxGetRaQuote(const RaReportData &report_data);
int tdxVerifyQuote(const std::vector<uint8_t> &quote);

}  // namespace encdb
