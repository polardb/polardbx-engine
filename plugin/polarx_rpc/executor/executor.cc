
#include "../global_defines.h"
#ifndef MYSQL8
#define MYSQL_SERVER
#endif
#include "sql/sql_class.h"

#include "../session/session_base.h"

#include "executor.h"
#include "expr.h"
#include "handler_api.h"
#include "log.h"
#include "meta.h"

using namespace PolarXRPC;

extern MYSQL_PLUGIN_IMPORT CHARSET_INFO *files_charset_info;

namespace rpc_executor {

#ifdef XPLUGIN_LOG_DEBUG
#define DEBUG_PRINT_PLAN(plan) \
  do {                         \
    debug_print_request(plan); \
  } while (0)
#else
#define DEBUG_PRINT_PLAN(plan)
#endif

void debug_print_request(const ExecPlan::GetPlan &get_plan) {
  for (int64_t k = 0; k < get_plan.keys_size(); ++k) {
    log_debug(
        "ExecPlan should execute, table: %s.%s, index: %s, key parts offered: "
        "%d",
        pb2ptr(get_plan.table_info().schema_name()),
        pb2ptr(get_plan.table_info().name()), parse_index_name(get_plan),
        get_plan.keys().Get(k).keys().size());
    for (int64_t i = 0; i < get_plan.keys(k).keys().size(); ++i) {
      switch (get_plan.keys(k).keys(i).value().type()) {
        case PolarXRPC::Datatypes::Scalar::V_SINT:
          log_debug("key[%ld] %s:%ld", i,
                    pb2ptr(get_plan.keys(k).keys(i).field()),
                    get_plan.keys(k).keys(i).value().v_signed_int());
          break;
        case PolarXRPC::Datatypes::Scalar::V_UINT:
          log_debug("key[%ld] %s:%lu", i,
                    pb2ptr(get_plan.keys(k).keys(i).field()),
                    get_plan.keys(k).keys(i).value().v_unsigned_int());
          break;
        case PolarXRPC::Datatypes::Scalar::V_STRING:
          log_debug("key[%ld] %s:%s", i,
                    pb2ptr(get_plan.keys(k).keys(i).field()),
                    get_plan.keys(k).keys(i).value().v_string().value().data());
          break;
        default:
          log_debug("key[%ld] %s:(type(%d))", i,
                    pb2ptr(get_plan.keys(k).keys(i).field()),
                    static_cast<int>(get_plan.keys(k).keys(i).value().type()));
      }
    }
  }
}

void convert_name_to_lowercase(const Datatypes::Scalar &name,
                               std::unique_ptr<char[]> &output) {
  const std::string &name_str = pb2str(name);
  std::unique_ptr<char[]> temp(new char[name_str.size() + 1]);
  // copy string data and terminator 0
  memcpy(temp.get(), name_str.data(), name_str.size() + 1);
  my_casedn_str(files_charset_info, temp.get());
  output = std::move(temp);
}

int create_search_key(const PolarXRPC::ExecPlan::GetExpr &mysqlx_key,
                      ExecKeyMeta *key_meta, SearchKey &search_key) {
  return search_key.init(key_meta, mysqlx_key);
}

int create_range_search_key(const RangeInfo &range_info, ExecKeyMeta *key_meta,
                            RangeSearchKey &range_key) {
  return range_key.init(key_meta, range_info);
}

int InlineScanNode::init(const ExecPlan::RangeScan &scan_msg,
                         InternalDataSet &dataset, THD *thd) {
  key_only_ = false;
  flags_ = scan_msg.flag();
  const ExecPlan::GetExpr *begin_key =
      scan_msg.has_key() ? &(scan_msg.key()) : nullptr;
  const ExecPlan::GetExpr *end_key =
      scan_msg.has_end_key() ? &(scan_msg.end_key()) : nullptr;
  const RangeInfo range_info = {begin_key, end_key, true, true};
  return init(scan_msg.table_info(), parse_index_name(scan_msg), range_info,
              dataset, thd);
}

int InlineScanNode::init(const ExecPlan::KeyOnlyRangeScan &scan_msg,
                         InternalDataSet &dataset, THD *thd) {
  int ret = HA_EXEC_SUCCESS;
  key_only_ = true;
  flags_ = scan_msg.flag();
  const ExecPlan::GetExpr *begin_key =
      scan_msg.has_key() ? &(scan_msg.key()) : nullptr;
  const ExecPlan::GetExpr *end_key =
      scan_msg.has_end_key() ? &(scan_msg.end_key()) : nullptr;
  const RangeInfo range_info = {begin_key, end_key, true, true};
  if ((ret = init(scan_msg.table_info(), parse_index_name(scan_msg), range_info,
                  dataset, thd))) {
  } else if ((ret = handler_set_key_read_only(table_))) {
    log_exec_error("handler_set_key_read_only failed, ret: %d", ret);
  }
  return ret;
}

int InlineScanNode::init(const ExecPlan::TableScanPlan &scan_msg,
                         InternalDataSet &dataset, THD *thd) {
  int ret = HA_EXEC_SUCCESS;
  const ExecPlan::TableInfo &table_info = scan_msg.table_info();
  convert_name_to_lowercase(table_info.name(), table_name_ptr_);
  convert_name_to_lowercase(table_info.schema_name(), schema_name_ptr_);
  const char *table_name = table_name_ptr_.get();
  const char *schema_name = schema_name_ptr_.get();
  const char *index_name = parse_index_name(scan_msg);
  thd_ = thd;
  if ((table_status_ = ret = handler_open_table(thd_, schema_name, table_name,
                                                HDL_READ, table_))) {
    log_exec_error("handler_open_table failed, ret: %d, name: %s.%s", ret,
                   schema_name, table_name);
  } else if (!(handler_lock_table(thd_, table_, TL_READ))) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("handler_lock_table failed, ret: %d, name: %s.%s", ret,
                   schema_name, table_name);
  } else if ((ret = table_->get_key(index_name, key_meta_))) {
    log_exec_error("index not found, ret: %d, name: %s.%s.%s", ret, schema_name,
                   table_name, index_name);
  } else if ((ret = dataset.init(1, table_))) {
    log_exec_error("InternalDataSet init failed, ret: %d, name %s.%s", ret,
                   schema_name, table_name);
  }
  return ret;
}

int InlineScanNode::init(const ExecPlan::TableInfo &table_info,
                         const char *index_name, const RangeInfo &range_info,
                         InternalDataSet &dataset, THD *thd) {
  int ret = HA_EXEC_SUCCESS;
  convert_name_to_lowercase(table_info.name(), table_name_ptr_);
  convert_name_to_lowercase(table_info.schema_name(), schema_name_ptr_);
  const char *table_name = table_name_ptr_.get();
  const char *schema_name = schema_name_ptr_.get();
  thd_ = thd;
  if ((table_status_ = ret = handler_open_table(thd_, schema_name, table_name,
                                                HDL_READ, table_))) {
    log_exec_error("handler_open_table failed, ret: %d, name: %s.%s", ret,
                   schema_name, table_name);
  } else if (!(handler_lock_table(thd_, table_, TL_READ))) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("handler_lock_table failed, ret: %d, name: %s.%s", ret,
                   schema_name, table_name);
  } else if ((ret = table_->get_key(index_name, key_meta_))) {
    log_exec_error("index not found, ret: %d, name: %s.%s.%s", ret, schema_name,
                   table_name, index_name);
  } else if ((ret = dataset.init(1, table_))) {
    log_exec_error("InternalDataSet init failed, ret: %d, name %s.%s", ret,
                   schema_name, table_name);
  } else if ((ret =
                  create_range_search_key(range_info, key_meta_, range_key_))) {
    log_exec_error("create RangeSearchKey failed, ret: %d, name: %s.%s", ret,
                   schema_name, table_name);
  }
  return ret;
}

int InlineScanNode::seek(InternalDataSet &dataset) {
  bool found = false;
  int ret = HA_EXEC_SUCCESS;
  if ((ret = handler_seek(thd_, table_, key_meta_, range_key_, found))) {
    const char *table_name = table_name_ptr_.get();
    const char *schema_name = schema_name_ptr_.get();
    log_exec_error("handler_seek failed, ret: %d, name %s.%s", ret, schema_name,
                   table_name);
  }
  if (found) {
    dataset.set_found();
  }
  return ret;
}

int InlineScanNode::next(InternalDataSet &dataset) {
  bool found = false;
  int ret = HA_EXEC_SUCCESS;

  if ((ret = handler_range_next(thd_, table_, found))) {
    const char *table_name = table_name_ptr_.get();
    const char *schema_name = schema_name_ptr_.get();
    log_exec_error("handler_range_next failed, ret: %d, name %s.%s", ret,
                   schema_name, table_name);
  }
  if (found) {
    dataset.set_found();
  }
  return ret;
}

int InlineScanNode::index_first(InternalDataSet &dataset) {
  bool found = false;
  int ret = HA_EXEC_SUCCESS;

  if ((ret = handler_index_first(thd_, table_, key_meta_, found))) {
    const char *table_name = table_name_ptr_.get();
    const char *schema_name = schema_name_ptr_.get();
    log_exec_error("handler_index_first failed, ret: %d, name %s.%s", ret,
                   schema_name, table_name);
  }
  if (found) {
    dataset.set_found();
  }
  return ret;
}

int InlineScanNode::index_next(InternalDataSet &dataset) {
  bool found = false;
  int ret = HA_EXEC_SUCCESS;

  if ((ret = handler_index_next(thd_, table_, found))) {
    const char *table_name = table_name_ptr_.get();
    const char *schema_name = schema_name_ptr_.get();
    log_exec_error("handler_index_next failed, ret: %d, name %s.%s", ret,
                   schema_name, table_name);
  }
  if (found) {
    dataset.set_found();
  }
  return ret;
}

int InlineScanNode::finish(int error) {
  int ret = HA_EXEC_SUCCESS;
  if (HA_EXEC_SUCCESS == table_status_) {
    // handler_index_end has no requirement of a success seek.
    handler_index_end(thd_, table_);
    if (key_only_) {
      handler_set_no_key_read_only(table_);
    }
    handler_close_table(thd_, table_, HDL_READ);
  }
  return ret;
}

int ScanNode::init(const PolarXRPC::ExecPlan::RangeScan &scan_msg,
                   InternalDataSet &dataset, THD *thd) {
  first_next_ = true;
  return inline_scan_node_.init(scan_msg, dataset, thd);
}

int ScanNode::next(InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;
  if (first_next_) {
    ret = inline_scan_node_.seek(dataset);
    first_next_ = false;
  } else {
    ret = inline_scan_node_.next(dataset);
  }
  return ret;
}

int ScanNode::finish(int error) { return inline_scan_node_.finish(error); }

int TableScanNode::init(const PolarXRPC::ExecPlan::TableScanPlan &scan_msg,
                        InternalDataSet &dataset, THD *thd) {
  first_next_ = true;
  return inline_scan_node_.init(scan_msg, dataset, thd);
}

int TableScanNode::next(InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;
  if (first_next_) {
    ret = inline_scan_node_.index_first(dataset);
    first_next_ = false;
  } else {
    ret = inline_scan_node_.index_next(dataset);
  }
  return ret;
}

int TableScanNode::finish(int error) { return inline_scan_node_.finish(error); }

int Executor::execute(const ExecPlan::AnyPlan &plan, const ParamsList &params,
                      polarx_rpc::CcommandDelegate &resultset, THD *thd,
                      const char *query, uint query_len) {
  int ret = HA_EXEC_SUCCESS;
  tls_params = &params;
  thd->reset_for_next_command();
  polarx_rpc::CsessionBase::begin_query(thd, query, query_len);
  switch (plan.plan_type()) {
    case PolarXRPC::ExecPlan::AnyPlan::GET:
      ret = execute_get(plan.get_plan(), resultset, thd);
      break;
    case PolarXRPC::ExecPlan::AnyPlan::RANGE_SCAN:
      ret = execute_scan(plan.range_scan(), resultset, thd);
      break;
    default:
      ret = build_and_execute(plan, resultset, thd);
      break;
  }
  polarx_rpc::CsessionBase::end_query(thd);
  return ret;
}

int Executor::build_and_execute(const ExecPlan::AnyPlan &plan_msg,
                                polarx_rpc::CcommandDelegate &resultset,
                                THD *thd) {
  int ret = HA_EXEC_SUCCESS;
  // TODO: See if all tree built here is a simple read.
  std::unique_ptr<PlanNode> plan_tree;
  auto &plan_builder = PlanBuilder::instance();
  InternalDataSet dataset;
  if ((ret = plan_builder.create_plan_tree(plan_msg, resultset, dataset, thd,
                                           plan_tree))) {
    log_exec_error("create_plan_tree failed, ret: %d", ret);
  } else {
    while (true) {
      if ((ret = plan_tree->next(dataset))) {
        log_exec_error("plan_tree next failed, ret: %d", ret);
        break;
      } else if (dataset.no_next_row() || (!dataset.found())) {
        break;
      }
    }
    int finish_ret = plan_tree->finish(ret);
    if (finish_ret) {
      log_exec_error("plan tree finish failed, %d", finish_ret);
    }
  }

  return ret;
}

int Executor::execute_get(const ExecPlan::GetPlan &get_plan,
                          polarx_rpc::CcommandDelegate &resultset, THD *thd) {
  DEBUG_PRINT_PLAN(get_plan);

  int ret = HA_EXEC_SUCCESS;

  // THD *thd = handler_create_thd(false);
  if (!thd) {
    ret = HA_ERR_OUT_OF_MEM;
  } else {
    std::unique_ptr<char[]> table_ptr;
    std::unique_ptr<char[]> schema_ptr;
    convert_name_to_lowercase(get_plan.table_info().name(), table_ptr);
    convert_name_to_lowercase(get_plan.table_info().schema_name(), schema_ptr);
    const char *table_name = table_ptr.get();
    const char *schema_name = schema_ptr.get();
    const char *index_name = parse_index_name(get_plan);
    ExecTable *table = nullptr;
    ExecKeyMeta *key_meta = nullptr;
    InternalDataSet dataset;

    if ((ret = handler_open_table(thd, schema_name, table_name, HDL_READ,
                                  table))) {
      log_exec_error("handler_open_table failed, ret: %d, name: %s.%s", ret,
                     schema_name, table_name);
    } else if (!(handler_lock_table(thd, table, TL_READ))) {
      ret = HA_ERR_INTERNAL_ERROR;
      log_exec_error("handler_lock_table failed, ret: %d, name: %s.%s", ret,
                     schema_name, table_name);
    } else if ((ret = table->get_key(index_name, key_meta))) {
      log_exec_error("index not found, ret: %d, name: %s.%s.%s", ret,
                     schema_name, table_name, index_name);
    } else if ((ret = dataset.init(1, table))) {
      log_exec_error("InternalDataSet init failed, ret: %d, name %s.%s", ret,
                     schema_name, table_name);
    } else {
      bool found = false;
      SearchKey search_key;
      if (get_plan.keys_size() <= 0) {
        ret = HA_ERR_KEY_NOT_FOUND;
        log_exec_error("should offer at least one key ret: %d, name: %s.%s",
                       ret, schema_name, table_name);
      } else if ((ret = create_search_key(get_plan.keys(0), key_meta,
                                          search_key))) {
        log_exec_error("create SearchKey failed, ret: %d, name: %s.%s", ret,
                       schema_name, table_name);
      } else if (search_key.is_impossible_where()) {
        // Empty dataset
        Protocol protocol(&resultset);
        if ((ret = protocol.write_metadata(dataset))) {
          log_exec_error("write_metadata in execute_get failed");
        } else {
          protocol.send_and_flush();
        }
      } else if ((ret = dataset.project_all())) {
        log_exec_error("DataSet project all failed, ret: %d", ret);
      } else if ((ret = handler_get(thd, table, key_meta, search_key, found))) {
        log_exec_error("handler_get failed, ret: %d, name: %s.%s", ret,
                       schema_name, table_name);
      } else {
        // TODO: return error message instead of empty message.
        Protocol protocol(&resultset);
        if ((ret = protocol.write_metadata(dataset))) {
          log_exec_error("write_metadata in execute_get failed");
        } else if (found) {
          // When error occurs, found should always be false.
          if ((ret = protocol.write_row(dataset))) {
            log_exec_error("write_row in execute_get failed, ret: %d", ret);
          } else {
            while (true) {
              if ((ret = handler_next_same(thd, table, search_key, found))) {
                log_exec_error("next in execute_get failed, ret: %d", ret);
                break;
              } else if (found) {
                if ((ret = protocol.write_row(dataset))) {
                  log_exec_error("write_row in execute_get failed, ret: %d",
                                 ret);
                  break;
                }
              } else {
                // EOF
                break;
              }
            }
          }
        }
        if (HA_EXEC_SUCCESS == ret) {
          protocol.send_and_flush();
        }
      }
    }
    if (table) {
      handler_index_end(thd, table);
      handler_close_table(thd, table, HDL_READ);
    }
  }
  // handler_close_thd(thd);
  return ret;
}

int Executor::execute_batch_get(const ExecPlan::GetPlan &batch_get_plan,
                                polarx_rpc::CcommandDelegate &resultset,
                                THD *thd) {
  int ret = HA_EXEC_SUCCESS;
  return ret;
}

int Executor::execute_key_only_scan(
    const ExecPlan::KeyOnlyRangeScan &key_scan_plan,
    polarx_rpc::CcommandDelegate &resultset, THD *thd) {
  int ret = HA_EXEC_SUCCESS;
  // THD *thd = handler_create_thd(false);
  if (!thd) {
    ret = HA_ERR_OUT_OF_MEM;
  } else {
    InternalDataSet dataset;
    InlineScanNode scan_node;
    if ((ret = scan_node.init(key_scan_plan, dataset, thd))) {
      log_exec_error("InlineScanNode init failed, ret: %d", ret);
    } else {
      Protocol protocol(&resultset);
      protocol.write_metadata(dataset);

      if ((ret = dataset.project_all())) {
        log_exec_error("Dataset project all failed, ret: %d", ret);
      } else if ((ret = scan_node.seek(dataset))) {
        log_exec_error("InlineScanNode seek failed, ret: %d", ret);
      }
      while (HA_EXEC_SUCCESS == ret && dataset.found()) {
        if ((ret = protocol.write_row(dataset))) {
          log_exec_error("write_row in execute_key_only_scan failed");
          break;
        }
        dataset.reset_found();
        if ((ret = scan_node.next(dataset))) {
          log_exec_error("InlineScanNode next failed, ret: %d", ret);
        }
      }

      // Only when success can we send rows back
      if (HA_EXEC_SUCCESS == ret) {
        protocol.send_and_flush();
      }
    }

    // Cleanup
    int finish_ret = scan_node.finish(ret);
    if (finish_ret) {
      log_exec_error("InlineScanNode finish failed, ret: %d", finish_ret);
    }
  }
  return ret;
}

int Executor::execute_scan(const ExecPlan::RangeScan &scan_plan,
                           polarx_rpc::CcommandDelegate &resultset, THD *thd) {
  int ret = HA_EXEC_SUCCESS;
  // THD *thd = handler_create_thd(false);
  if (!thd) {
    ret = HA_ERR_OUT_OF_MEM;
  } else {
    InternalDataSet dataset;
    InlineScanNode scan_node;
    if ((ret = scan_node.init(scan_plan, dataset, thd))) {
      log_exec_error("InlineScanNode init failed, ret: %d", ret);
    } else {
      Protocol protocol(&resultset);
      protocol.write_metadata(dataset);

      if ((ret = dataset.project_all())) {
        log_exec_error("DataSet project all failed, ret: %d", ret);
      } else if ((ret = scan_node.seek(dataset))) {
        log_exec_error("InlineScanNode seek failed, ret: %d", ret);
      }
      while (HA_EXEC_SUCCESS == ret && dataset.found()) {
        if ((ret = protocol.write_row(dataset))) {
          log_exec_error("write_row in execute_scan failed");
          break;
        }
        dataset.reset_found();
        if ((ret = scan_node.next(dataset))) {
          log_exec_error("InlineScanNode next failed, ret: %d", ret);
        }
      }
      // Only when success can we send rows back
      if (HA_EXEC_SUCCESS == ret) {
        protocol.send_and_flush();
      }
    }
    int finish_ret = scan_node.finish(ret);
    if (finish_ret) {
      log_exec_error("InlineScanNode finish failed, ret: %d", finish_ret);
    }
  }
  return ret;
}

int PlanBuilder::create_plan_tree(const ExecPlan::AnyPlan &plan_msg,
                                  polarx_rpc::CcommandDelegate &resultset,
                                  InternalDataSet &dataset, THD *thd,
                                  std::unique_ptr<PlanNode> &plan) {
  int ret = HA_EXEC_SUCCESS;
  std::unique_ptr<ResponseNode> root_node(new ResponseNode());
  if (!root_node) {
    ret = HA_ERR_OUT_OF_MEM;
  } else if ((ret = create(plan_msg, dataset, thd, root_node->left_tree_))) {
    log_exec_error("create child node of ResponseNode failed, ret: %d", ret);
  } else if ((ret = root_node->init(resultset, dataset))) {
    log_exec_error("ResponseNode init failed, ret: %d", ret);
    int finish_ret = root_node->finish(ret);
    if (finish_ret) {
      log_exec_error("ResponseNode finish failed, ret: %d", finish_ret);
    }
  } else {
    dataset.do_project();
    plan = std::move(root_node);
  }
  return ret;
}

int PlanBuilder::create(const ExecPlan::AnyPlan &plan_msg,
                        InternalDataSet &dataset, THD *thd,
                        std::unique_ptr<PlanNode> &plan_node) {
  int ret = HA_EXEC_SUCCESS;
  switch (plan_msg.plan_type()) {
    case ExecPlan::AnyPlan::GET:
      ret = create_get_tree(plan_msg.get_plan(), dataset, thd, plan_node);
      break;
    case ExecPlan::AnyPlan::TABLE_SCAN:
      ret =
          create_scan_tree(plan_msg.table_scan_plan(), dataset, thd, plan_node);
      break;
    case ExecPlan::AnyPlan::TABLE_PROJECT:
      ret = create_project_tree(plan_msg.table_project(), dataset, thd,
                                plan_node);
      break;
    case ExecPlan::AnyPlan::PROJECT:
      ret = create_project_tree(plan_msg.project(), dataset, thd, plan_node);
      break;
    case ExecPlan::AnyPlan::FILTER:
      ret = create_filter_tree(plan_msg.filter(), dataset, thd, plan_node);
      break;
    case ExecPlan::AnyPlan::RANGE_SCAN:
      ret = create_scan_tree(plan_msg.range_scan(), dataset, thd, plan_node);
      break;
    case ExecPlan::AnyPlan::AGGR:
      ret = create_aggr_tree(plan_msg.aggr(), dataset, thd, plan_node);
      break;
    default:
      assert(0);
  }
  return ret;
}

int PlanBuilder::create_aggr_tree(const ExecPlan::Aggr &aggr_msg,
                                  InternalDataSet &dataset, THD *thd,
                                  std::unique_ptr<PlanNode> &plan_node) {
  int ret = HA_EXEC_SUCCESS;
  std::unique_ptr<AggrNode> aggr_node(new (std::nothrow) AggrNode());
  const ExecPlan::AnyPlan &sub_read_msg = aggr_msg.sub_read_plan();
  if (!aggr_node) {
    ret = HA_ERR_OUT_OF_MEM;
    log_exec_error("aggr node new failed, ret: %d", ret);
  } else if ((ret =
                  create(sub_read_msg, dataset, thd, aggr_node->left_tree_))) {
    log_exec_error("aggr node create sub node failed, ret: %d", ret);
  } else if ((ret = aggr_node->init(aggr_msg, dataset))) {
    log_exec_error("aggr init node failed, ret: %d", ret);
    int finish_ret = aggr_node->finish(ret);
    if (finish_ret) {
      log_exec_error("AggrNode finish failed, ret: %d", finish_ret);
    }
  } else {
    plan_node = std::move(aggr_node);
  }
  return ret;
}

int PlanBuilder::create_project_tree(const ExecPlan::TableProject &project_msg,
                                     InternalDataSet &dataset, THD *thd,
                                     std::unique_ptr<PlanNode> &plan_node) {
  int ret = HA_EXEC_SUCCESS;
  std::unique_ptr<ProjectNode> project_node(new (std::nothrow) ProjectNode());
  const ExecPlan::AnyPlan &sub_read_msg = project_msg.sub_read_plan();
  if (!project_node) {
    ret = HA_ERR_OUT_OF_MEM;
    log_exec_error("project node new failed, ret: %d", ret);
  } else if ((ret = create(sub_read_msg, dataset, thd,
                           project_node->left_tree_))) {
    log_exec_error("project node create sub node failed, ret: %d", ret);
  } else if ((ret = project_node->init(project_msg, dataset))) {
    log_exec_error("project init node failed, ret: %d", ret);
    int finish_ret = project_node->finish(ret);
    if (finish_ret) {
      log_exec_error("ProjectNode finish failed, ret: %d", finish_ret);
    }
  } else {
    plan_node = std::move(project_node->left_tree_);
    plan_node->project_info_node_ = std::move(project_node);
  }
  return ret;
}

int PlanBuilder::create_project_tree(const ExecPlan::Project &project_msg,
                                     InternalDataSet &dataset, THD *thd,
                                     std::unique_ptr<PlanNode> &plan_node) {
  int ret = HA_EXEC_SUCCESS;
  std::unique_ptr<ProjectNode> project_node(new (std::nothrow) ProjectNode());
  const ExecPlan::AnyPlan &sub_msg = project_msg.sub_read_plan();
  if (!project_node) {
    ret = HA_ERR_OUT_OF_MEM;
    log_exec_error("project node new failed, ret: %d", ret);
  } else if ((ret = create(sub_msg, dataset, thd, project_node->left_tree_))) {
    log_exec_error("project node create sub node failed, ret: %d", ret);
  } else if ((ret = project_node->init(project_msg, dataset))) {
    log_exec_error("project init node failed, ret: %d", ret);
    int finish_ret = project_node->finish(ret);
    if (finish_ret) {
      log_exec_error("ProjectNode finish failed, ret: %d", finish_ret);
    }
  } else {
    plan_node = std::move(project_node->left_tree_);
    plan_node->project_info_node_ = std::move(project_node);
  }
  return ret;
}

int PlanBuilder::create_filter_tree(const ExecPlan::Filter &filter_msg,
                                    InternalDataSet &dataset, THD *thd,
                                    std::unique_ptr<PlanNode> &plan_node) {
  int ret = HA_EXEC_SUCCESS;
  std::unique_ptr<FilterNode> filter_node(new (std::nothrow) FilterNode());
  const ExecPlan::AnyPlan &sub_msg = filter_msg.sub_read_plan();
  if (!filter_node) {
    ret = HA_ERR_OUT_OF_MEM;
    log_exec_error("FilterNode new failed, ret: %d", ret);
  } else if ((ret = create(sub_msg, dataset, thd, filter_node->left_tree_))) {
    log_exec_error("FilterNode create sub node failed, ret: %d", ret);
  } else if ((ret = filter_node->init(filter_msg, dataset))) {
    log_exec_error("FilterNode init failed, ret: %d", ret);
    int finish_ret = filter_node->finish(ret);
    if (finish_ret) {
      log_exec_error("FilterNode finish failed, ret: %d", finish_ret);
    }
  } else {
    plan_node = std::move(filter_node);
  }
  return ret;
}

int PlanBuilder::create_get_tree(const ExecPlan::GetPlan &get_msg,
                                 InternalDataSet &dataset, THD *thd,
                                 std::unique_ptr<PlanNode> &plan_node) {
  int ret = HA_EXEC_SUCCESS;
  std::unique_ptr<GetNode> get_node(new (std::nothrow) GetNode());
  if (!get_node) {
    ret = HA_ERR_OUT_OF_MEM;
  } else if ((ret = get_node->init(get_msg, dataset, thd))) {
    log_exec_error("GetNode init failed, ret: %d", ret);
    int finish_ret = get_node->finish(ret);
    if (finish_ret) {
      log_exec_error("GetNode finish failed, finish_ret: %d", finish_ret);
    }
  } else {
    plan_node = std::move(get_node);
  }
  return ret;
}

int PlanBuilder::create_scan_tree(const ExecPlan::RangeScan &scan_msg,
                                  InternalDataSet &dataset, THD *thd,
                                  std::unique_ptr<PlanNode> &plan_node) {
  int ret = HA_EXEC_SUCCESS;
  std::unique_ptr<ScanNode> scan_node(new ScanNode());
  if (!scan_node) {
    ret = HA_ERR_OUT_OF_MEM;
  } else if ((ret = scan_node->init(scan_msg, dataset, thd))) {
    log_exec_error("ScanNode init failed, ret: %d", ret);
    int finish_ret = scan_node->finish(ret);
    if (finish_ret) {
      log_exec_error("ScanNode finish failed, finish_ret: %d", finish_ret);
    }
  } else {
    plan_node = std::move(scan_node);
  }
  return ret;
}

int PlanBuilder::create_scan_tree(const ExecPlan::TableScanPlan &scan_msg,
                                  InternalDataSet &dataset, THD *thd,
                                  std::unique_ptr<PlanNode> &plan_node) {
  int ret = HA_EXEC_SUCCESS;
  std::unique_ptr<TableScanNode> scan_node(new TableScanNode());
  if (!scan_node) {
    ret = HA_ERR_OUT_OF_MEM;
  } else if ((ret = scan_node->init(scan_msg, dataset, thd))) {
    log_exec_error("ScanNode init failed, ret: %d", ret);
    int finish_ret = scan_node->finish(ret);
    if (finish_ret) {
      log_exec_error("ScanNode finish failed, finish_ret: %d", finish_ret);
    }
  } else {
    plan_node = std::move(scan_node);
  }
  return ret;
}

int ResponseNode::init(polarx_rpc::CcommandDelegate &resultset,
                       InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;
  protocol_.reset(new Protocol(&resultset));
  if (!protocol_) {
    ret = HA_ERR_OUT_OF_MEM;
  } else if (!dataset.is_init()) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("result not init when try to response");
  } else {
    protocol_->write_metadata(dataset);
  }

  return ret;
}

int ResponseNode::next(InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;
  // reset for next row
  dataset.reset_found();
  left_tree_->next(dataset);
  // TODO: return error message instead of empty message.
  // When error occurs, found should always be false.
  if (dataset.found() && !dataset.is_aggr()) {
    ret = protocol_->write_row(dataset);
  } else if (!dataset.found() && dataset.is_aggr()) {
    ret = protocol_->write_row(dataset);
  }

  return ret;
}

int ResponseNode::finish(int error) {
  if (!error) {
    protocol_->send_and_flush();
  }
  left_tree_->finish(error);
  return HA_EXEC_SUCCESS;
}

int ProjectNode::init(const ExecPlan::TableProject &project_msg,
                      InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;
  int32_t project_field_count = project_msg.fields_size();
  project_exprs_.reserve(project_field_count);

  auto &expr_parser = ExprParser::instance();
  for (int32_t i = 0; i < project_field_count; ++i) {
    ExprItem *item = nullptr;
    const char *field_name = nullptr;
    if ((ret = expr_parser.parse_field(project_msg.fields(i), dataset, item,
                                       field_name))) {
      log_exec_error("ExprParser parse field failed, ret: %d", ret);
      break;
    }
    // assume field_name has terminator 0
    project_exprs_.emplace_back(field_name, item);
  }
  if (!ret) {
    dataset.project(&project_exprs_);
  }
  return ret;
}

int ProjectNode::init(const ExecPlan::Project &project_msg,
                      InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;
  int32_t project_expr_count = project_msg.exprs_size();
  int32_t project_name_count = project_msg.fields_size();
  project_exprs_.reserve(project_expr_count);

  auto &expr_parser = ExprParser::instance();
  for (int32_t i = 0; i < project_expr_count; ++i) {
    ExprItem *item = nullptr;
    if ((ret = expr_parser.parse(project_msg.exprs(i), dataset, item))) {
      log_exec_error("ExprParser parse failed, ret: %d", ret);
      break;
    }
    if (i < project_name_count) {
      project_exprs_.emplace_back(pb2str(project_msg.fields(i)), item);
    } else {
      project_exprs_.emplace_back(std::to_string(i), item);
    }
  }

  if (!ret) {
    dataset.project(&project_exprs_);
  }
  return ret;
}

int ProjectNode::next(InternalDataSet &dataset) {
  return left_tree_->next(dataset);
}

int ProjectNode::finish(int error) { return left_tree_->finish(error); }

bool AggrNode::is_type_valid(PolarXRPC::ExecPlan::Aggr::AggrType type) {
  bool ret = true;

  if (type < PolarXRPC::ExecPlan::Aggr::COUNT_FUNC ||
      type > PolarXRPC::ExecPlan::Aggr::MAX_FUNC) {
    log_exec_error("type is invalid");
    ret = false;
  }

  return ret;
}

int AggrNode::init(const PolarXRPC::ExecPlan::Aggr &aggr_msg,
                   InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;

  auto &expr_parser = ExprParser::instance();
  ExprItem *item =
      nullptr;  // now memory allocated and deallocated by expr_parser.

  type_ = aggr_msg.type();
  if (!is_type_valid(type_)) {
    ret = HA_EXEC_FAILURE;
    log_exec_error("aggregation type is invalid");
  } else if ((ret = expr_parser.parse(aggr_msg.expr(), dataset, item))) {
    log_exec_error("ExprParser parse filter expr failed, ret: %d", ret);
  } else if ((ret = init_aggr_expr(type_, item))) {
    log_exec_error("Initialize aggr function failed, ret: %d", ret);
  } else {
    aggr_name_ = pb2str(aggr_msg.field());
    dataset.set_aggr(aggr_name_, aggr_expr_);
  }

  return ret;
}

int AggrNode::init_aggr_expr(PolarXRPC::ExecPlan::Aggr::AggrType type,
                             ExprItem *item) {
#ifdef MYSQL8
  return HA_EXEC_FAILURE;
#else
  int ret = HA_EXEC_SUCCESS;

  assert(item != nullptr);

  /*
     Item override operate new, all memory comes from mem_root,
     add free at ~InternalDataSet.
  */
  switch (type) {
    case PolarXRPC::ExecPlan::Aggr::COUNT_FUNC: {
      aggr_expr_ = new Item_sum_count(pos_, item);
      break;
    }
    case PolarXRPC::ExecPlan::Aggr::SUM_FUNC: {
      aggr_expr_ = new Item_sum_sum(pos_, item, false);
      break;
    }
    case PolarXRPC::ExecPlan::Aggr::AVG_FUNC: {
      aggr_expr_ = new Item_sum_avg(pos_, item, false);
      break;
    }
    case PolarXRPC::ExecPlan::Aggr::MIN_FUNC: {
      aggr_expr_ = new Item_sum_min(pos_, item);
      break;
    }
    case PolarXRPC::ExecPlan::Aggr::MAX_FUNC: {
      aggr_expr_ = new Item_sum_max(pos_, item);
      break;
    }
    default: {
      log_exec_error("unsupported aggr functions");
      ret = HA_EXEC_FAILURE;
    }
  }

  aggr_expr_->fix_length_and_dec();
  aggr_expr_->fixed = 1;
  aggr_expr_->set_aggregator(Aggregator::SIMPLE_AGGREGATOR);

  return ret;
#endif
}

int AggrNode::next(InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;

  if ((ret = left_tree_->next(dataset))) {
    log_exec_error("execute error, %d", ret);
  } else {
    // TODO group by
    while (ret == HA_EXEC_SUCCESS && dataset.found()) {
      if ((ret = calculate(dataset))) {
        log_exec_error("run aggr function error, aggr_type:%d, ret:%d", type_,
                       ret);
      } else {
        dataset.reset_found();

        if ((ret = left_tree_->next(dataset))) {
          log_exec_error("run  next child failed, ret: %d", ret);
        }
      }
    }
  }

  return ret;
}

int AggrNode::calculate(InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;

  if (aggr_expr_->aggregator_add()) {
    log_exec_error("run aggr function  failed");
  }

  return ret;
}

int AggrNode::finish(int error) { return left_tree_->finish(error); }

int FilterNode::init(const ExecPlan::Filter &filter_msg,
                     InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;
  auto &expr_parser = ExprParser::instance();
  if ((ret = expr_parser.parse(filter_msg.expr(), dataset, condition_expr_))) {
    log_exec_error("ExprParser parse filter expr failed, ret: %d", ret);
  }
  return ret;
}

int FilterNode::next(InternalDataSet &dataset) {
  int ret = left_tree_->next(dataset);
  if (ret) {
    log_exec_error("FilterNode next child failed(1), ret: %d", ret);
  } else {
    while (dataset.found() && !condition_expr_->val_bool()) {
      dataset.reset_found();
      if (dataset.no_next_row()) {
        break;
      } else if ((ret = left_tree_->next(dataset))) {
        log_exec_error("FilterNode next child failed(2), ret: %d", ret);
      }
    }
  }
  return ret;
}

int FilterNode::finish(int error) { return left_tree_->finish(error); }

int GetNode::init(const ExecPlan::GetPlan &get_msg, InternalDataSet &dataset,
                  THD *thd) {
  DEBUG_PRINT_PLAN(get_msg);
  int ret = HA_EXEC_SUCCESS;
  convert_name_to_lowercase(get_msg.table_info().name(), table_name_ptr_);
  convert_name_to_lowercase(get_msg.table_info().schema_name(),
                            schema_name_ptr_);
  const char *table_name = table_name_ptr_.get();
  const char *schema_name = schema_name_ptr_.get();
  thd_ = thd;
  const char *index_name = parse_index_name(get_msg);
  if ((table_status_ = ret = handler_open_table(thd_, schema_name, table_name,
                                                HDL_READ, table_))) {
    log_exec_error("handler_open_table failed, ret: %d, name: %s.%s", ret,
                   schema_name, table_name);
  } else if (!(handler_lock_table(thd_, table_, TL_READ))) {
    ret = HA_ERR_INTERNAL_ERROR;
    log_exec_error("handler_lock_table failed, ret: %d, name: %s.%s", ret,
                   schema_name, table_name);
  } else if ((ret = table_->get_key(index_name, key_meta_))) {
    log_exec_error("index not found, ret: %d, name: %s.%s.%s", ret, schema_name,
                   table_name, index_name);
  } else if ((ret = dataset.init(1, table_))) {
    log_exec_error("InternalDataSet init failed, ret: %d, name %s.%s", ret,
                   schema_name, table_name);
  } else if ((ret =
                  create_search_key(get_msg.keys(0), key_meta_, search_key_))) {
    log_exec_error("create SearchKey failed, ret: %d, name: %s.%s", ret,
                   schema_name, table_name);
  }
  return ret;
}

int GetNode::next(InternalDataSet &dataset) {
  int ret = HA_EXEC_SUCCESS;
  bool found = false;
  /*
  if (key_meta_->is_unique() && search_key_.using_full_key()) {
    if ((ret = handler_get(thd_, table_, key_meta_, search_key_, found))) {
      log_exec_error("handler_get failed, ret: %d, name: %s.%s",
                     ret, schema_name_, table_name_);
    }
  } else { // if (search_key.used_part_map() > 0) {
    if ((ret = handler_seek(thd_, table_, key_meta_, search_key_, found))) {
      log_exec_error("handler_seek failed, ret: %d, name: %s.%s",
                     ret, schema_name_, table_name_);
    }
  }
  */
  if (search_key_.is_impossible_where()) {
    found = false;
  } else if (first_next_) {
    ret = handler_get(thd_, table_, key_meta_, search_key_, found);
    first_next_ = false;
  } else {
    ret = handler_next_same(thd_, table_, search_key_, found);
  }
  if (ret) {
    log_exec_error("handler_get failed, ret: %d, name: %s.%s", ret,
                   schema_name_ptr_.get(), table_name_ptr_.get());
  }
  if (found) {
    dataset.set_found();
  }
  return ret;
}

int GetNode::finish(int error) {
  int ret = HA_EXEC_SUCCESS;
  if (HA_EXEC_SUCCESS == table_status_) {
    handler_index_end(thd_, table_);
    handler_close_table(thd_, table_, HDL_READ);
  }
  return ret;
}

}  // namespace rpc_executor
