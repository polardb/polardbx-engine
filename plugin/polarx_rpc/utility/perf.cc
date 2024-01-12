//
// Created by zzy on 2022/8/17.
//

#include "perf.h"

namespace polarx_rpc {

static constexpr auto HIST_GRANULARITY = 1024;
static constexpr auto HIST_MIN_VALUE = 1e-9;  /// 1ns
static constexpr auto HIST_MAX_VALUE = 99.;   /// 99s

Chistogram g_work_queue_hist(HIST_GRANULARITY, HIST_MIN_VALUE, HIST_MAX_VALUE);
Chistogram g_recv_first_hist(HIST_GRANULARITY, HIST_MIN_VALUE, HIST_MAX_VALUE);
Chistogram g_recv_all_hist(HIST_GRANULARITY, HIST_MIN_VALUE, HIST_MAX_VALUE);
Chistogram g_decode_hist(HIST_GRANULARITY, HIST_MIN_VALUE, HIST_MAX_VALUE);
Chistogram g_schedule_hist(HIST_GRANULARITY, HIST_MIN_VALUE, HIST_MAX_VALUE);
Chistogram g_run_hist(HIST_GRANULARITY, HIST_MIN_VALUE, HIST_MAX_VALUE);
Chistogram g_timer_hist(HIST_GRANULARITY, HIST_MIN_VALUE, HIST_MAX_VALUE);
Chistogram g_cleanup_hist(HIST_GRANULARITY, HIST_MIN_VALUE, HIST_MAX_VALUE);
Chistogram g_fin_hist(HIST_GRANULARITY, HIST_MIN_VALUE, HIST_MAX_VALUE);
Chistogram g_auth_hist(HIST_GRANULARITY, HIST_MIN_VALUE, HIST_MAX_VALUE);

}  // namespace polarx_rpc
