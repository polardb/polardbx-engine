//
// Created by zzy on 2022/8/25.
//

#include <cstring>

#include "plugin/polarx_rpc/utility/perf.h"

#include "sql/sql_class.h"
#include "xrpc_proc.h"

/// use proc to export hist result
namespace im {

LEX_CSTRING XRPC_PROC_SCHEMA = {C_STRING_WITH_LEN("xrpc")};

Proc *Proc_perf_hist::instance() {
  auto proc = new Proc_perf_hist(key_memory_package);
  return proc;
}

Sql_cmd *Proc_perf_hist::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Cmd_perf_hist(thd, list, this);
}

bool Cmd_perf_hist::pc_execute(THD *thd) {
  DBUG_ASSERT(m_proc->get_result_type() == Proc::Result_type::RESULT_SET);

  /// get hist name
  List_iterator_fast<Item> it(*m_list);
  auto name_item = dynamic_cast<Item_string *>(it++);
  String *name = name_item->val_str(nullptr);
  if (!name->is_empty())
    name_ = name->ptr();
  return false;
}

void Cmd_perf_hist::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();

  /* No need to proceed if error occurred */
  if (error) {
    DBUG_ASSERT(thd->is_error());
    return;
  }

  if (m_proc->send_result_metadata(thd))
    return;

  if (0 == ::strcasecmp(name_.c_str(), "work queue")) {
    protocol->start_row();
    protocol->store("work queue", system_charset_info);
    std::string hist("hist:\n");
    hist += polarx_rpc::g_work_queue_hist.histogram();
    protocol->store(hist.c_str(), system_charset_info);
    if (protocol->end_row())
      return;
  } else if (0 == ::strcasecmp(name_.c_str(), "recv first")) {
    protocol->start_row();
    protocol->store("recv first", system_charset_info);
    std::string hist("hist:\n");
    hist += polarx_rpc::g_recv_first_hist.histogram();
    protocol->store(hist.c_str(), system_charset_info);
    if (protocol->end_row())
      return;
  } else if (0 == ::strcasecmp(name_.c_str(), "recv all")) {
    protocol->start_row();
    protocol->store("recv all", system_charset_info);
    std::string hist("hist:\n");
    hist += polarx_rpc::g_recv_all_hist.histogram();
    protocol->store(hist.c_str(), system_charset_info);
    if (protocol->end_row())
      return;
  } else if (0 == ::strcasecmp(name_.c_str(), "schedule")) {
    protocol->start_row();
    protocol->store("schedule", system_charset_info);
    std::string hist("hist:\n");
    hist += polarx_rpc::g_schedule_hist.histogram();
    protocol->store(hist.c_str(), system_charset_info);
    if (protocol->end_row())
      return;
  } else if (0 == ::strcasecmp(name_.c_str(), "run")) {
    protocol->start_row();
    protocol->store("run", system_charset_info);
    std::string hist("hist:\n");
    hist += polarx_rpc::g_run_hist.histogram();
    protocol->store(hist.c_str(), system_charset_info);
    if (protocol->end_row())
      return;
  } else if (0 == ::strcasecmp(name_.c_str(), "all")) {
    /// all
    protocol->start_row();
    protocol->store("work queue", system_charset_info);
    std::string hist("hist:\n");
    hist += polarx_rpc::g_work_queue_hist.histogram();
    protocol->store(hist.c_str(), system_charset_info);
    if (protocol->end_row())
      return;

    protocol->start_row();
    protocol->store("recv first", system_charset_info);
    hist = "hist:\n";
    hist += polarx_rpc::g_recv_first_hist.histogram();
    protocol->store(hist.c_str(), system_charset_info);
    if (protocol->end_row())
      return;

    protocol->start_row();
    protocol->store("recv all", system_charset_info);
    hist = "hist:\n";
    hist += polarx_rpc::g_recv_all_hist.histogram();
    protocol->store(hist.c_str(), system_charset_info);
    if (protocol->end_row())
      return;

    protocol->start_row();
    protocol->store("schedule", system_charset_info);
    hist = "hist:\n";
    hist += polarx_rpc::g_schedule_hist.histogram();
    protocol->store(hist.c_str(), system_charset_info);
    if (protocol->end_row())
      return;

    protocol->start_row();
    protocol->store("run", system_charset_info);
    hist = "hist:\n";
    hist += polarx_rpc::g_run_hist.histogram();
    protocol->store(hist.c_str(), system_charset_info);
    if (protocol->end_row())
      return;
  } else if (0 == ::strcasecmp(name_.c_str(), "reset")) {
    /// reset all
    polarx_rpc::g_work_queue_hist.reset();
    polarx_rpc::g_recv_first_hist.reset();
    polarx_rpc::g_recv_all_hist.reset();
    polarx_rpc::g_schedule_hist.reset();
    polarx_rpc::g_run_hist.reset();

    protocol->start_row();
    protocol->store("reset", system_charset_info);
    protocol->store("ok", system_charset_info);
    if (protocol->end_row())
      return;
  } else {
    protocol->start_row();
    protocol->store("error", system_charset_info);
    protocol->store("Param should be \"work queue\", \"recv\", \"schedule\", "
                    "\"run\" \"all\" or \"reset\".",
                    system_charset_info);
    if (protocol->end_row())
      return;
  }

  my_eof(thd);
}

} // namespace im
