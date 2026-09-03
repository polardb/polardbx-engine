/*
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * Copyright (C) 2011-2021 Intel Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Intel Corporation nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <sgx_dcap_quoteverify.h>
#include <sgx_ql_quote.h>
#include <tdx_attest.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include "encdb_key_exception.h"
#include "logger.h"
#include "ra.h"
#include "storage.h"

namespace encdb {

const std::vector<uint8_t> tdxGetRaQuote(const tdx_report_data_t &report_data) {
  uint32_t quote_size = 0;
  tdx_report_t tdx_report = {{0}};
  tdx_uuid_t selected_att_key_id = {0};
  uint8_t *p_quote_buf = NULL;

  if (TDX_ATTEST_SUCCESS != tdx_att_get_report(&report_data, &tdx_report)) {
    throw EncdbRAException("Failed to get the report");
  }

  if (TDX_ATTEST_SUCCESS != tdx_att_get_quote(&report_data, NULL, 0,
                                              &selected_att_key_id,
                                              &p_quote_buf, &quote_size, 0)) {
    throw EncdbRAException("Failed to get the quote");
  }

  std::vector<uint8_t> quote(quote_size);
  memcpy(quote.data(), p_quote_buf, quote_size);

  tdx_att_free_quote(p_quote_buf);
  return quote;
}

int tdxVerifyQuote(const std::vector<uint8_t> &quote) {
  int ret = 0;
  time_t current_time = 0;
  quote3_error_t dcap_ret = SGX_QL_ERROR_UNEXPECTED;
  uint32_t collateral_expiration_status = 1;
  sgx_ql_qv_result_t quote_verification_result = SGX_QL_QV_RESULT_UNSPECIFIED;

  tee_supp_data_descriptor_t supp_data;

  // You can also set specify a major version in this structure, then we will
  // always return supplemental data of the major version set major verison to 0
  // means always return latest supplemental data
  memset(&supp_data, 0, sizeof(tee_supp_data_descriptor_t));

  typedef union _supp_ver_t {
    uint32_t version;
    struct {
      uint16_t major_version;
      uint16_t minor_version;
    };
  } supp_ver_t;

  supp_ver_t latest_ver;

  // Untrusted quote verification
  // call DCAP quote verify library to get supplemental latest version and data
  // size version is a combination of major_version and minor version you can
  // set the major version in 'supp_data.major_version' to get old version
  // supplemental data only support major_version 3 right now
  dcap_ret = tee_get_supplemental_data_version_and_size(
      quote.data(), (uint32_t)quote.size(), &latest_ver.version,
      &supp_data.data_size);

  if (dcap_ret == SGX_QL_SUCCESS &&
      supp_data.data_size == sizeof(sgx_ql_qv_supplemental_t)) {
    ENCDB_LOG(
        LL_INFO,
        "\tInfo: tee_get_quote_supplemental_data_version_and_size successfully "
        "returned.\n");
    ENCDB_LOG(
        LL_INFO,
        "\tInfo: latest supplemental data major version: %d, minor version: "
        "%d, size: %d\n",
        latest_ver.major_version, latest_ver.minor_version,
        supp_data.data_size);
    supp_data.p_data = (uint8_t *)encdb_plugin_malloc(supp_data.data_size);
    if (supp_data.p_data != NULL) {
      memset(supp_data.p_data, 0, supp_data.data_size);
    }

    // Just print error in sample
    //
    else {
      ENCDB_LOG(LL_ERROR,
                "\tError: Cannot allocate memory for supplemental data.\n");
    }
  } else {
    if (dcap_ret != SGX_QL_SUCCESS)
      ENCDB_LOG(
          LL_ERROR,
          "\tError: tee_get_quote_supplemental_data_size failed: 0x%04x\n",
          dcap_ret);

    if (supp_data.data_size != sizeof(sgx_ql_qv_supplemental_t))
      ENCDB_LOG(
          LL_WARNING,
          "\tWarning: Quote supplemental data size is different between DCAP "
          "QVL and QvE, please make sure you installed DCAP QVL and QvE from "
          "same release.\n");

    supp_data.data_size = 0;
  }

  // set current time. This is only for sample purposes, in production mode a
  // trusted time should be used.
  current_time = time(NULL);

  // call DCAP quote verify library for quote verification
  // here you can choose 'trusted' or 'untrusted' quote verification by
  // specifying parameter '&qve_report_info' if '&qve_report_info' is NOT NULL,
  // this API will call Intel QvE to verify quote if '&qve_report_info' is NULL,
  // this API will call 'untrusted quote verify lib' to verify quote, this mode
  // doesn't rely on SGX capable system, but the results can not be
  // cryptographically authenticated
  dcap_ret = tee_verify_quote(quote.data(), (uint32_t)quote.size(), NULL,
                              current_time, &collateral_expiration_status,
                              &quote_verification_result, NULL, &supp_data);
  if (dcap_ret == SGX_QL_SUCCESS) {
    ENCDB_LOG(LL_INFO,
              "\tInfo: App: tee_verify_quote successfully returned.\n");
  } else {
    ENCDB_LOG(LL_ERROR, "\tError: App: tee_verify_quote failed: 0x%04x\n",
              dcap_ret);
  }

  // check verification result
  //
  switch (quote_verification_result) {
    case SGX_QL_QV_RESULT_OK:
      // check verification collateral expiration status
      // this value should be considered in your own attestation/verification
      // policy
      //
      if (collateral_expiration_status == 0) {
        ENCDB_LOG(LL_INFO,
                  "\tInfo: App: Verification completed successfully.\n");
        ret = 0;
      } else {
        ENCDB_LOG(
            LL_WARNING,
            "\tWarning: App: Verification completed, but collateral is out of "
            "date based on 'expiration_check_date' you provided.\n");
        ret = 1;
      }
      break;
    case SGX_QL_QV_RESULT_CONFIG_NEEDED:
    case SGX_QL_QV_RESULT_OUT_OF_DATE:
    case SGX_QL_QV_RESULT_OUT_OF_DATE_CONFIG_NEEDED:
    case SGX_QL_QV_RESULT_SW_HARDENING_NEEDED:
    case SGX_QL_QV_RESULT_CONFIG_AND_SW_HARDENING_NEEDED:
      ENCDB_LOG(
          LL_WARNING,
          "\tWarning: App: Verification completed with Non-terminal result: "
          "%x\n",
          quote_verification_result);
      ret = 1;
      break;
    case SGX_QL_QV_RESULT_INVALID_SIGNATURE:
    case SGX_QL_QV_RESULT_REVOKED:
    case SGX_QL_QV_RESULT_UNSPECIFIED:
    default:
      ENCDB_LOG(
          LL_ERROR,
          "\tError: App: Verification completed with Terminal result: %x\n",
          quote_verification_result);
      ret = -1;
      break;
  }

  // check supplemental data if necessary
  //
  if (dcap_ret == SGX_QL_SUCCESS && supp_data.p_data != NULL &&
      supp_data.data_size > 0) {
    sgx_ql_qv_supplemental_t *p = (sgx_ql_qv_supplemental_t *)supp_data.p_data;

    // you can check supplemental data based on your own
    // attestation/verification policy here we only print supplemental data
    // version for demo usage
    //
    ENCDB_LOG(LL_INFO, "\tInfo: Supplemental data Major Version: %d\n",
              p->major_version);
    ENCDB_LOG(LL_INFO, "\tInfo: Supplemental data Minor Version: %d\n",
              p->minor_version);

    // print SA list if exist, SA list is supported from version 3.1
    //
    if (p->version > 3 && strlen(p->sa_list) > 0) {
      ENCDB_LOG(LL_INFO, "\tInfo: Advisory ID: %s\n", p->sa_list);
    }
  }

  if (supp_data.p_data != NULL) {
    encdb_plugin_free(supp_data.p_data);
  }

  return ret;
}

std::vector<uint8_t> tdxGetRaQuote(const RaReportData &report_data) {
  static_assert(sizeof(tdx_report_data_t) == sizeof(RaReportData));
  const tdx_report_data_t &tdx_report_data =
      reinterpret_cast<const tdx_report_data_t &>(report_data);
  return tdxGetRaQuote(tdx_report_data);
}

}  // namespace encdb
