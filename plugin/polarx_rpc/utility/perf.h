//
// Created by zzy on 2022/8/17.
//

#pragma once

#include "histogram.h"

namespace polarx_rpc {

extern Chistogram g_work_queue_hist;
extern Chistogram g_recv_first_hist;
extern Chistogram g_recv_all_hist;
extern Chistogram g_decode_hist;
extern Chistogram g_schedule_hist;
extern Chistogram g_run_hist;
extern Chistogram g_timer_hist;
extern Chistogram g_cleanup_hist;
extern Chistogram g_fin_hist;
extern Chistogram g_auth_hist;

}  // namespace polarx_rpc
