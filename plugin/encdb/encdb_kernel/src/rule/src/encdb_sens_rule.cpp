/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#include "encdb_sens_rule.h"
#include <string>
#include <tuple>
#include <vector>
#include "../../common/include/encdb_constants.h"
#include "encdb_common_exception.h"
#include "json.hpp"

namespace encdb {
DesensitizationAlgo::DesensitizationAlgo(const std::string &alg_json)
    : DesensitizationAlgo(json::JSON::Load(alg_json)) {}
DesensitizationAlgo::DesensitizationAlgo(const json::JSON &json) {
  this->algo_name_ = json["name"].ToString();
  this->params_ = json["params"];
}
std::string DesensitizationAlgo::getAlgoName() const {
  return this->algo_name_;
}
std::string DesensitizationAlgo::getAlgoParam(
    const std::string &param_name) const {
  return this->params_[param_name].ToString();
}
json::JSON DesensitizationAlgo::toJson() const {
  json::JSON json_obj;
  json_obj["name"] = this->algo_name_;
  json_obj["params"] = this->params_;
  return json_obj;
}

inline bool itemExistInVec(const std::string &item,
                           const std::vector<std::string> &rule_vec) {
  for (const std::string &str : rule_vec) {
    if (item == str || str == "*") {
      return true;
    }
  }
  return false;
}
EncdbSenstiveDataRule::EncdbSenstiveDataRule(const std::string &json_str,
                                             bool tolower)
    : EncdbSenstiveDataRule(json::JSON::Load(json_str), tolower) {}
EncdbSenstiveDataRule::EncdbSenstiveDataRule(const json::JSON &json_rule,
                                             bool tolower) {
  json_rule.checkField(JSON_RULE_NAME);
  json_rule.checkField(JSON_ENABLED);
  json_rule.checkField(JSON_SENSITIVE_DATA);
  json_rule[JSON_SENSITIVE_DATA].checkField(JSON_SENSITIVE_DBS);
  json_rule[JSON_SENSITIVE_DATA].checkField(JSON_SENSITIVE_TBLS);
  json_rule[JSON_SENSITIVE_DATA].checkField(JSON_SENSITIVE_COLS);
  json::JSON rule_name = json_rule[JSON_RULE_NAME];
  json::JSON enabled = json_rule[JSON_ENABLED];
  json::JSON sensitive_dbs = json_rule[JSON_SENSITIVE_DATA][JSON_SENSITIVE_DBS];
  json::JSON sensitive_tbls =
      json_rule[JSON_SENSITIVE_DATA][JSON_SENSITIVE_TBLS];
  json::JSON sensitive_cols =
      json_rule[JSON_SENSITIVE_DATA][JSON_SENSITIVE_COLS];

  // FIXME(yx): these three fields are ignored for now
  // json::JSON algos = json_rule[JSON_ALGO];
  // json::JSON default_algo = json_rule[JSON_DEFAULT_ALGO];
  // json::JSON endpoints = json_rule[JSON_ENDPTS];

  this->rule_name_ = rule_name.ToString();
  this->description_ = json_rule.hasKey(JSON_RULE_DESCRIPTION)
                           ? json_rule[JSON_RULE_DESCRIPTION].ToString()
                           : "";
  this->enabled_ = enabled.ToBool();
  this->sensitive_dbs_ = json::jsonStrArrToVecStr(sensitive_dbs, tolower);
  this->sensitive_tbls_ = json::jsonStrArrToVecStr(sensitive_tbls, tolower);
  this->sensitive_cols_ = json::jsonStrArrToVecStr(sensitive_cols);
  this->algos_ = std::vector<DesensitizationAlgo>();
  this->default_algo_ = "";
  this->endpoints_ = json::jsonStrArrToVecStr("[]");
  this->type_ = RuleType::USER_DEFINED;
}
EncdbSenstiveDataRule::EncdbSenstiveDataRule(
    const std::string &dbs, const std::string &tbls, const std::string &cols,
    const std::string &rule_name, bool enabled, RuleType rt,
    const std::string &description, bool tolower) {
  this->rule_name_ = rule_name;
  this->description_ = description;
  this->enabled_ = enabled;
  this->algos_ = std::vector<DesensitizationAlgo>();
  // FIXME: Algos, default_algo and endpoints are not put to use for now
  // for (const std::string &algo_str :
  //      json::jsonObjArrToVecStr(json::JSON::Load(algos))) {
  //   this->algos_.push_back(DesensitizationAlgo(algo_str));
  // }
  this->default_algo_ = "";
  this->sensitive_dbs_ =
      json::jsonStrArrToVecStr(json::JSON::Load(dbs), tolower);
  this->sensitive_tbls_ =
      json::jsonStrArrToVecStr(json::JSON::Load(tbls), tolower);
  this->sensitive_cols_ = json::jsonStrArrToVecStr(json::JSON::Load(cols));
  this->endpoints_ = json::jsonStrArrToVecStr("[]");
  this->type_ = rt;
}

EncdbSenstiveDataRule::EncdbSenstiveDataRule(const std::string &rule_name,
                                             RuleType rt, bool) {
  this->rule_name_ = rule_name;
  this->description_ = "";
  this->enabled_ = true;
  this->algos_ = std::vector<DesensitizationAlgo>();
  this->default_algo_ = "";
  this->sensitive_dbs_ = std::vector<std::string>();
  this->sensitive_tbls_ = std::vector<std::string>();
  this->sensitive_cols_ = std::vector<std::string>();
  this->endpoints_ = std::vector<std::string>();
  this->type_ = rt;
}

std::string EncdbSenstiveDataRule::getRuleName() const {
  return this->rule_name_;
}
bool EncdbSenstiveDataRule::isEnabled() const { return this->enabled_; }
DesensitizationAlgo EncdbSenstiveDataRule::getAlgo(
    std::string algo_name) const {
  for (const DesensitizationAlgo &alg : this->algos_) {
    if (alg.getAlgoName() == algo_name) {
      return alg;
    }
  }
  throw InvalidSensitiveAlgoException(
      "algo \"" + algo_name + "\" is not found in rule " + this->rule_name_);
}
std::string EncdbSenstiveDataRule::getDefaultAlgoName() const {
  return this->default_algo_;
}
bool EncdbSenstiveDataRule::isDataSensitive(const std::string &db,
                                            const std::string &tbl,
                                            const std::string &col) const {
  return (db.empty() || itemExistInVec(db, this->sensitive_dbs_)) &&
         (tbl.empty() || itemExistInVec(tbl, this->sensitive_tbls_)) &&
         (col.empty() || itemExistInVec(col, this->sensitive_cols_));
}

void EncdbSenstiveDataRule::setEnabled(bool enabled) {
  this->enabled_ = enabled;
}
const std::vector<DesensitizationAlgo> &EncdbSenstiveDataRule::getAlgos()
    const {
  return this->algos_;
}
void EncdbSenstiveDataRule::dump(std::string &dbs, std::string &tbls,
                                 std::string &cols, std::string &algos,
                                 std::string &default_algo,
                                 std::string &endpoints, std::string &rule_name,
                                 bool &enabled, int &type,
                                 std::string &description) const {
  dbs = json::vecStrToJsonArr(this->sensitive_dbs_).dump();
  tbls = json::vecStrToJsonArr(this->sensitive_tbls_).dump();
  cols = json::vecStrToJsonArr(this->sensitive_cols_).dump();
  json::JSON algos_json_obj = json::Array();
  for (const auto &alg : this->algos_) {
    algos_json_obj.append(alg.toJson());
  }
  algos = algos_json_obj.dump();
  default_algo = this->default_algo_;
  endpoints = json::vecStrToJsonArr(this->endpoints_).dump();
  rule_name = this->rule_name_;
  enabled = this->enabled_;
  type = static_cast<int>(this->type_);
  description = this->description_;
}
void EncdbSenstiveDataRule::dump(
    std::vector<std::string> &dbs, std::vector<std::string> &tbls,
    std::vector<std::string> &cols, std::vector<std::string> &algos,
    std::string &default_algo, std::vector<std::string> &endpoints,
    std::string &rule_name, bool &enabled, int &ruletype,
    std::string &description) const {
  dbs = this->sensitive_dbs_;
  tbls = this->sensitive_tbls_;
  cols = this->sensitive_cols_;
  for (const auto &algo : this->algos_) {
    algos.push_back(algo.toJson().dump());
  }
  default_algo = this->default_algo_;
  endpoints = this->endpoints_;
  rule_name = this->rule_name_;
  enabled = this->enabled_;
  ruletype = static_cast<int>(this->type_);
  description = this->description_;
}
json::JSON EncdbSenstiveDataRule::toJson() const {
  json::JSON ret_obj;
  json::JSON algos_json_obj = json::Array();
  for (const auto &algo : this->algos_) {
    algos_json_obj.append(algo.toJson());
  }
  ret_obj[JSON_RULE_NAME] = this->rule_name_;
  if (!this->description_.empty()) {
    ret_obj[JSON_RULE_DESCRIPTION] = this->description_;
  }
  ret_obj[JSON_ENABLED] = this->enabled_;
  ret_obj[JSON_ALGO] = algos_json_obj;
  ret_obj[JSON_DEFAULT_ALGO] = this->default_algo_;
  json::JSON meta_obj;
  meta_obj[JSON_SENSITIVE_COLS] = json::vecStrToJsonArr(this->sensitive_cols_);
  meta_obj[JSON_SENSITIVE_DBS] = json::vecStrToJsonArr(this->sensitive_dbs_);
  meta_obj[JSON_SENSITIVE_TBLS] = json::vecStrToJsonArr(this->sensitive_tbls_);
  ret_obj[JSON_SENSITIVE_DATA] = meta_obj;
  ret_obj[JSON_ENDPTS] = json::vecStrToJsonArr(this->endpoints_);
  return ret_obj;
}
RuleType EncdbSenstiveDataRule::getRuleType() const { return this->type_; }
void EncdbSenstiveDataRule::setRuleName(const std::string &name) {
  this->rule_name_ = name;
}
void EncdbSenstiveDataRule::setDbs(const std::vector<std::string> dbs) {
  this->sensitive_dbs_ = dbs;
}
void EncdbSenstiveDataRule::setTbls(const std::vector<std::string> tbls) {
  this->sensitive_tbls_ = tbls;
}
void EncdbSenstiveDataRule::setCols(const std::vector<std::string> cols) {
  this->sensitive_cols_ = cols;
}
std::vector<std::string> EncdbSenstiveDataRule::getDbs() const {
  return this->sensitive_dbs_;
}
std::vector<std::string> EncdbSenstiveDataRule::getTbls() const {
  return this->sensitive_tbls_;
}
std::vector<std::string> EncdbSenstiveDataRule::getCols() const {
  return this->sensitive_cols_;
}
void EncdbSenstiveDataRule::mergeColsWith(
    const std::vector<std::string> &cols) {
  this->sensitive_cols_ = vector_union(this->sensitive_cols_, cols);
}
bool EncdbSenstiveDataRule::compareName(const EncdbSenstiveDataRule &r1,
                                        const EncdbSenstiveDataRule &r2) {
  return r1.rule_name_.compare(r2.rule_name_) < 0;
}
EncdbSearchTup encdb_generate_search_tup(const std::string &db,
                                         const std::string &tbl,
                                         const std::string &col) {
  return std::make_tuple(db, tbl, col);
}
}  // namespace encdb
