/**
 * Copyright (c) 2019-2023 Alibaba Damo DBLab. All rights reserved.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include "encdb_error.h"
#include "encdb_sens_rule.h"

namespace encdb {

// These functions are called by MySQL hooks (use implicit call)
namespace hook {

bool encdbNeedEncryption(const std::string &, const std::string &,
                         const std::string &);
int encdbRuleSpreadInsertOrCreateFrom(const std::string &src_db,
                                      const std::string &src_tbl,
                                      const std::string &src_col,
                                      const std::string &dst_db,
                                      const std::string &dst_tbl,
                                      const std::string &dst_col, bool dst_tmp);
int encdbRuleSpreadRenameTable(const std::string &src_db,
                               const std::string &src_tbl,
                               const std::string &dst_db,
                               const std::string &dst_tbl, bool tmp);
int encdbRuleSpreadRenameColumn(const std::string &src_db,
                                const std::string &src_tbl,
                                const std::string &src_col,
                                const std::string &dst_col, bool tmp);
void encdbReloadAllEncRules();
UserAccess encdbGetUserAccess(const std::string &uname);

}  // namespace hook

// These functions are called by MySQL UDFs (user explicit call)
namespace udf {

std::string encdbExportEncRules();
std::string encdbExportUserAuths();
void encdbDeleteEncRules(const std::string &rule_name);
void encdbAddOrUpdateEncRules(const std::string &rule_json_strs);
void encdbSetUserAuths(const std::string &param);
void encdbSetDefaultUserAuths(const std::string &rights);

}  // namespace udf

}  // namespace encdb
