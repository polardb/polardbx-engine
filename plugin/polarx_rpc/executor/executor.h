/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


#pragma once

#include "sql/sql_class.h"

#include "../coders/command_delegate.h"
#include "../coders/protocol_fwd.h"

#include "meta.h"
#include "protocol.h"

namespace rpc_executor {
using KeyPartMap = ::key_part_map;
using KeyRange = ::key_range;
using KeyFlag = enum ha_rkey_function;

class PlanNode;

class Executor {
 public:
  static Executor &instance() {
    static Executor executor;
    return executor;
  }

  int execute(const PolarXRPC::ExecPlan::AnyPlan &plan,
              const ParamsList &params, polarx_rpc::CcommandDelegate &resultset,
              THD *thd, const char *query, uint query_len);

 private:
  int build_and_execute(const PolarXRPC::ExecPlan::AnyPlan &plan_msg,
                        polarx_rpc::CcommandDelegate &resultset, THD *thd);

  int execute_get(const PolarXRPC::ExecPlan::GetPlan &get_plan,
                  polarx_rpc::CcommandDelegate &resultset, THD *thd);
  int execute_batch_get(const PolarXRPC::ExecPlan::GetPlan &batch_get_plan,
                        polarx_rpc::CcommandDelegate &resultset, THD *thd);
  int execute_key_only_scan(
      const PolarXRPC::ExecPlan::KeyOnlyRangeScan &key_only_scan_plan,
      polarx_rpc::CcommandDelegate &resultset, THD *thd);
  int execute_scan(const PolarXRPC::ExecPlan::RangeScan &scan_plan,
                   polarx_rpc::CcommandDelegate &resultset, THD *thd);
  int execute_project(const PolarXRPC::ExecPlan::TableProject &project,
                      polarx_rpc::CcommandDelegate &resultset, THD *thd);
};

class PlanBuilder {
 public:
  static PlanBuilder &instance() {
    static PlanBuilder plan_builder;
    return plan_builder;
  }

  int create_plan_tree(const PolarXRPC::ExecPlan::AnyPlan &plan_msg,
                       polarx_rpc::CcommandDelegate &resultset,
                       InternalDataSet &dataset, THD *thd,
                       std::unique_ptr<PlanNode> &plan);
  int create_project_tree(const PolarXRPC::ExecPlan::TableProject &project_msg,
                          InternalDataSet &dataset, THD *thd,
                          std::unique_ptr<PlanNode> &plan);
  int create_project_tree(const PolarXRPC::ExecPlan::Project &project_msg,
                          InternalDataSet &dataset, THD *thd,
                          std::unique_ptr<PlanNode> &plan);
  int create_filter_tree(const PolarXRPC::ExecPlan::Filter &filter_msg,
                         InternalDataSet &dataset, THD *thd,
                         std::unique_ptr<PlanNode> &plan);
  int create_get_tree(const PolarXRPC::ExecPlan::GetPlan &get_message,
                      InternalDataSet &dataset, THD *thd,
                      std::unique_ptr<PlanNode> &plan);
  int create_scan_tree(const PolarXRPC::ExecPlan::RangeScan &scan_message,
                       InternalDataSet &dataset, THD *thd,
                       std::unique_ptr<PlanNode> &plan);
  int create_scan_tree(const PolarXRPC::ExecPlan::TableScanPlan &scan_message,
                       InternalDataSet &dataset, THD *thd,
                       std::unique_ptr<PlanNode> &plan);

  int create_aggr_tree(const PolarXRPC::ExecPlan::Aggr &aggr_msg,
                       InternalDataSet &dataset, THD *thd,
                       std::unique_ptr<PlanNode> &plan);

 private:
  int create(const PolarXRPC::ExecPlan::AnyPlan &plan_message,
             InternalDataSet &dataset, THD *thd,
             std::unique_ptr<PlanNode> &plan);
};

class PlanNode {
 public:
  virtual ~PlanNode() {}
  virtual int next(InternalDataSet &result) = 0;
  virtual int finish(int ret) = 0;
  std::unique_ptr<PlanNode> left_tree_;
  std::unique_ptr<PlanNode> right_tree_;
  std::unique_ptr<PlanNode> project_info_node_;
};

class ResponseNode : public PlanNode {
 public:
  int init(polarx_rpc::CcommandDelegate &resultset, InternalDataSet &dataset);
  virtual int next(InternalDataSet &dataset) override;
  virtual int finish(int ret) override;

 private:
  std::unique_ptr<Protocol> protocol_;
};

class ProjectNode : public PlanNode {
 public:
  int init(const PolarXRPC::ExecPlan::TableProject &project_message,
           InternalDataSet &dataset);
  int init(const PolarXRPC::ExecPlan::Project &project_msg,
           InternalDataSet &dataset);
  virtual int next(InternalDataSet &dataset) override;
  virtual int finish(int ret) override;

 private:
  ProjectInfo project_exprs_;
};

class AggrNode : public PlanNode {
 public:
  int init(const PolarXRPC::ExecPlan::Aggr &aggr_msg, InternalDataSet &dataset);
  int init_aggr_expr(PolarXRPC::ExecPlan::Aggr::AggrType type, ExprItem *item);
  virtual int next(InternalDataSet &dataset) override;
  virtual int finish(int ret) override;

  bool is_type_valid(PolarXRPC::ExecPlan::Aggr::AggrType type);
  int calculate(InternalDataSet &dataset);

 private:
  PolarXRPC::ExecPlan::Aggr::AggrType type_;

  POS pos_;  // just for placeholder
  std::string aggr_name_;
  Item_sum *aggr_expr_;
};

class FilterNode : public PlanNode {
 public:
  int init(const ::PolarXRPC::ExecPlan::Filter &filter_msg,
           InternalDataSet &dataset);
  virtual int next(InternalDataSet &dataset) override;
  virtual int finish(int ret) override;

 private:
  ExprItem *condition_expr_;
};

class GetNode : public PlanNode {
 public:
  int init(const PolarXRPC::ExecPlan::GetPlan &get_msg,
           InternalDataSet &dataset, THD *thd);
  virtual int next(InternalDataSet &dataset) override;
  virtual int finish(int ret) override;

 private:
  THD *thd_;
  std::unique_ptr<char[]> table_name_ptr_;
  std::unique_ptr<char[]> schema_name_ptr_;
  ExecTable *table_;
  ExecKeyMeta *key_meta_;
  SearchKey search_key_;
  int table_status_ = HA_ERR_INTERNAL_ERROR;
  bool first_next_ = true;
};

class InlineScanNode {
 public:
  int init(const PolarXRPC::ExecPlan::RangeScan &scan_msg,
           InternalDataSet &dataset, THD *thd);
  int init(const PolarXRPC::ExecPlan::KeyOnlyRangeScan &scan_msg,
           InternalDataSet &dataset, THD *thd);
  int init(const PolarXRPC::ExecPlan::TableScanPlan &scan_msg,
           InternalDataSet &dataset, THD *thd);
  int seek(InternalDataSet &dataset);
  int next(InternalDataSet &dataset);

  int index_first(InternalDataSet &dataset);
  int index_next(InternalDataSet &dataset);
  int finish(int ret);

 private:
  int init(const PolarXRPC::ExecPlan::TableInfo &table_info,
           const char *index_name, const RangeInfo &range_info,
           InternalDataSet &dataset, THD *thd);

  THD *thd_;
  std::unique_ptr<char[]> table_name_ptr_;
  std::unique_ptr<char[]> schema_name_ptr_;
  ExecTable *table_;
  ExecKeyMeta *key_meta_;
  RangeSearchKey range_key_;
  int64_t flags_;
  int table_status_ = HA_ERR_INTERNAL_ERROR;
  bool key_only_ = false;
};

class ScanNode : public PlanNode {
 public:
  int init(const PolarXRPC::ExecPlan::RangeScan &scan_msg,
           InternalDataSet &dataset, THD *thd);
  virtual int next(InternalDataSet &dataset) override;
  virtual int finish(int ret) override;

 private:
  InlineScanNode inline_scan_node_;
  bool first_next_;
};

class TableScanNode : public PlanNode {
 public:
  int init(const PolarXRPC::ExecPlan::TableScanPlan &table_scan_msg,
           InternalDataSet &dataset, THD *thd);
  virtual int next(InternalDataSet &dataset) override;
  virtual int finish(int ret) override;

 private:
  InlineScanNode inline_scan_node_;
  bool first_next_;
};

class KeyScanNode : public PlanNode {
 public:
  int init(const PolarXRPC::ExecPlan::RangeScan &scan_msg,
           InternalDataSet &dataset, THD *thd);
  virtual int next(InternalDataSet &dataset) override;
  virtual int finish(int ret) override;

 private:
  InlineScanNode inline_scan_node_;
  bool first_next_;
};

}  // namespace rpc_executor
