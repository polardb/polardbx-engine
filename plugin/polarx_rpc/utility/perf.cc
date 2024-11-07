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
