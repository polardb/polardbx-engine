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
// Created by zzy on 2022/7/6.
//

#include <atomic>
#include <cstdint>

#include "mysql/plugin.h"

#include "../global_defines.h"
#include "server_variables.h"

class THD;

namespace polarx_rpc {

namespace defaults {
static constexpr my_bool auto_cpu_affinity = true;
/// allow bind to multi core in group
static constexpr my_bool multi_affinity_in_group = true;
static constexpr my_bool force_all_cores = false;
/// auto calculate by core numbers
static constexpr uint32_t epoll_groups = 0;
#ifdef MYSQL8
/// for open source
static constexpr uint32_t min_auto_epoll_groups = 32;
#else
/// for aliyun
static constexpr uint32_t min_auto_epoll_groups = 16;
#endif
/// only works when 0 == epoll_groups, better performance if set to 0 with auto
/// bind core
static constexpr uint32_t epoll_extra_groups = 0;
static constexpr uint32_t epoll_threads_per_group = 4;
/// should adjust by testing, don't limit if 0
static constexpr uint32_t max_epoll_wait_total_threads = 0;
/// 1 for normal task, 2-4 for simple bench task
static constexpr uint32_t epoll_events_per_thread = 4;
/// read only
static constexpr uint32_t epoll_work_queue_capacity = 256;

/// 10s
static constexpr uint32_t epoll_timeout = 10 * 1000;

static constexpr uint32_t tcp_keep_alive = 30;
static constexpr uint32_t tcp_listen_queue = 128;

static constexpr uint32_t tcp_send_buf = 0;
static constexpr uint32_t tcp_recv_buf = 0;
/// 4KB per TCP
static constexpr uint32_t tcp_fixed_dealing_buf = 0x1000;

static constexpr uint32_t mcs_spin_cnt = 2000;
static constexpr uint32_t session_poll_rwlock_spin_cnt = 1;

/// 10s
static constexpr uint32_t net_write_timeout = 10 * 1000;

static constexpr my_bool galaxy_protocol = false;
static constexpr uint32_t galaxy_version = 0;

/// 64MB
static constexpr uint32_t max_allowed_packet = 67108864;

/// 10*4KB = 40KB per session
static constexpr uint32_t max_cached_output_buffer_pages = 10;

static constexpr uint32_t max_queued_messages = 128;

static constexpr my_bool enable_kill_log = true;
static constexpr my_bool enable_thread_pool_log = true;

static constexpr my_bool enable_perf_hist = false;

/// 10s
static constexpr uint32_t epoll_group_ctx_refresh_time = 10 * 1000;
/// 1min
static constexpr uint32_t shared_session_lifetime = 60 * 1000;

static constexpr uint32_t epoll_group_dynamic_threads = 0;
/// 10s
static constexpr uint32_t epoll_group_dynamic_threads_shrink_time = 10 * 1000;
/// [0, base thread number)
static constexpr uint32_t epoll_group_thread_scale_thresh = 2;
/// 500ms
static constexpr uint32_t epoll_group_thread_deadlock_check_interval = 500;

static constexpr my_bool enable_tasker = true;
/// default 8
static constexpr uint32_t epoll_group_tasker_multiply = 3;
/// default 2
static constexpr uint32_t epoll_group_tasker_extend_step = 2;
static constexpr my_bool enable_epoll_in_tasker = true;

static constexpr uint32_t request_cache_number = 1024;
static constexpr uint32_t request_cache_instances = 16;
/// only cache sql/plan smaller than 1MB
static constexpr uint32_t request_cache_max_length = 1024 * 1024;
}  // namespace defaults

my_bool auto_cpu_affinity = defaults::auto_cpu_affinity;
my_bool multi_affinity_in_group = defaults::multi_affinity_in_group;
my_bool force_all_cores = defaults::force_all_cores;
uint32_t epoll_groups = defaults::epoll_groups;
uint32_t min_auto_epoll_groups = defaults::min_auto_epoll_groups;
uint32_t epoll_extra_groups = defaults::epoll_extra_groups;
uint32_t epoll_threads_per_group = defaults::epoll_threads_per_group;
uint32_t max_epoll_wait_total_threads = defaults::max_epoll_wait_total_threads;
uint32_t epoll_events_per_thread = defaults::epoll_events_per_thread;
uint32_t epoll_work_queue_capacity = defaults::epoll_work_queue_capacity;

uint32_t epoll_timeout = defaults::epoll_timeout;

uint32_t tcp_keep_alive = defaults::tcp_keep_alive;
uint32_t tcp_listen_queue = defaults::tcp_listen_queue;

uint32_t tcp_send_buf = defaults::tcp_send_buf;
uint32_t tcp_recv_buf = defaults::tcp_recv_buf;
uint32_t tcp_fixed_dealing_buf = defaults::tcp_fixed_dealing_buf;

uint32_t mcs_spin_cnt = defaults::mcs_spin_cnt;
uint32_t session_poll_rwlock_spin_cnt = defaults::session_poll_rwlock_spin_cnt;

uint32_t net_write_timeout = defaults::net_write_timeout;

my_bool galaxy_protocol = defaults::galaxy_protocol;
uint32_t galaxy_version = defaults::galaxy_version;

uint32_t max_allowed_packet = defaults::max_allowed_packet;

uint32_t max_cached_output_buffer_pages =
    defaults::max_cached_output_buffer_pages;

uint32_t max_queued_messages = defaults::max_queued_messages;

my_bool enable_kill_log = defaults::enable_kill_log;
my_bool enable_thread_pool_log = defaults::enable_thread_pool_log;

my_bool enable_perf_hist = defaults::enable_perf_hist;

uint32_t epoll_group_ctx_refresh_time = defaults::epoll_group_ctx_refresh_time;
uint32_t shared_session_lifetime = defaults::shared_session_lifetime;

uint32_t epoll_group_dynamic_threads = defaults::epoll_group_dynamic_threads;
uint32_t epoll_group_dynamic_threads_shrink_time =
    defaults::epoll_group_dynamic_threads_shrink_time;
uint32_t epoll_group_thread_scale_thresh =
    defaults::epoll_group_thread_scale_thresh;
uint32_t epoll_group_thread_deadlock_check_interval =
    defaults::epoll_group_thread_deadlock_check_interval;

my_bool enable_tasker = defaults::enable_tasker;
uint32_t epoll_group_tasker_multiply = defaults::epoll_group_tasker_multiply;
uint32_t epoll_group_tasker_extend_step =
    defaults::epoll_group_tasker_extend_step;
my_bool enable_epoll_in_tasker = defaults::enable_epoll_in_tasker;

uint32_t request_cache_number = defaults::request_cache_number;
uint32_t request_cache_instances = defaults::request_cache_instances;
uint32_t request_cache_max_length = defaults::request_cache_max_length;

/**
 * Global Variables
 */

std::atomic<uint64_t> g_tcp_id_generator(0);

/**
 * Variables defines.
 */

static void update_func_u32(THD *, SYS_VAR *, void *tgt, const void *save) {
  *(uint32_t *)tgt = *(const unsigned int *)save;
}

static void update_func_b(THD *, SYS_VAR *, void *tgt, const void *save) {
  *(my_bool *)tgt = *(const my_bool *)save;
}

/// Read-Only param
static MYSQL_SYSVAR_BOOL(auto_cpu_affinity, ::polarx_rpc::auto_cpu_affinity,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Enable auto thread CPU affinity(RO)", nullptr,
                         nullptr, defaults::auto_cpu_affinity);
static MYSQL_SYSVAR_BOOL(multi_affinity_in_group,
                         ::polarx_rpc::multi_affinity_in_group,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Enable base thread bind to all cores in group(RO)",
                         nullptr, nullptr, defaults::multi_affinity_in_group);
static MYSQL_SYSVAR_BOOL(force_all_cores, ::polarx_rpc::force_all_cores,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Enable thread CPU affinity to all CPU cores(RO)",
                         nullptr, nullptr, defaults::force_all_cores);
static MYSQL_SYSVAR_UINT(epoll_groups, ::polarx_rpc::epoll_groups,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Epoll group number(auto calculate if set to 0)(RO)",
                         nullptr, nullptr, defaults::epoll_groups, 0, 128, 0);
static MYSQL_SYSVAR_UINT(min_auto_epoll_groups,
                         ::polarx_rpc::min_auto_epoll_groups,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Minimum epoll group number calculated by auto(RO)",
                         nullptr, nullptr, defaults::min_auto_epoll_groups, 1,
                         128, 0);
static MYSQL_SYSVAR_UINT(epoll_extra_groups, ::polarx_rpc::epoll_extra_groups,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Extra epoll groups(RO)", nullptr, nullptr,
                         defaults::epoll_extra_groups, 0, 32, 0);
static MYSQL_SYSVAR_UINT(epoll_threads_per_group,
                         ::polarx_rpc::epoll_threads_per_group,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Base threads number for each epoll group(RO)",
                         nullptr, nullptr, defaults::epoll_threads_per_group, 1,
                         128, 0);
static MYSQL_SYSVAR_UINT(max_epoll_wait_total_threads,
                         ::polarx_rpc::max_epoll_wait_total_threads,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Number of base threads which wait on epoll in each "
                         "group(don't limit it if set to 0)(RO)",
                         nullptr, nullptr,
                         defaults::max_epoll_wait_total_threads, 0, 128, 0);
static MYSQL_SYSVAR_UINT(epoll_work_queue_capacity,
                         ::polarx_rpc::epoll_work_queue_capacity,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Capacity of work queue for each epoll group(RO)",
                         nullptr, nullptr, defaults::epoll_work_queue_capacity,
                         128, 4096, 0);
static MYSQL_SYSVAR_UINT(tcp_listen_queue, ::polarx_rpc::tcp_listen_queue,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "TCP listen queue(RO)", nullptr, nullptr,
                         defaults::tcp_listen_queue, 128, 4096, 0);
static MYSQL_SYSVAR_UINT(request_cache_number,
                         ::polarx_rpc::request_cache_number,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Total cache number of XRPC requests(RO)", nullptr,
                         nullptr, defaults::request_cache_number, 128, 16384,
                         0);
static MYSQL_SYSVAR_UINT(request_cache_instances,
                         ::polarx_rpc::request_cache_instances,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                         "Instances number of request cache(RO)", nullptr,
                         nullptr, defaults::request_cache_instances, 1, 128, 0);
static MYSQL_SYSVAR_UINT(
    request_cache_max_length, ::polarx_rpc::request_cache_max_length,
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
    "Max length of sql/plan which will store in request cache(RO)", nullptr,
    nullptr, defaults::request_cache_max_length, 128, 1024 * 1024 * 1024, 0);

/// Dynamic param
static MYSQL_SYSVAR_UINT(epoll_events_per_thread,
                         ::polarx_rpc::epoll_events_per_thread,
                         PLUGIN_VAR_OPCMDARG,
                         "Number of event slots per epoll_wait invoking(RW)",
                         nullptr, update_func_u32,
                         defaults::epoll_events_per_thread, 1, 16, 0);
static MYSQL_SYSVAR_UINT(epoll_timeout, ::polarx_rpc::epoll_timeout,
                         PLUGIN_VAR_OPCMDARG, "Timeout of epoll_wait in ms(RW)",
                         nullptr, update_func_u32, defaults::epoll_timeout, 1,
                         60 * 1000, 0);
static MYSQL_SYSVAR_UINT(tcp_keep_alive, ::polarx_rpc::tcp_keep_alive,
                         PLUGIN_VAR_OPCMDARG, "TCP keep alive thresh in s(RW)",
                         nullptr, update_func_u32, defaults::tcp_keep_alive, 1,
                         2 * 3600, 0);
static MYSQL_SYSVAR_UINT(tcp_send_buf, ::polarx_rpc::tcp_send_buf,
                         PLUGIN_VAR_OPCMDARG,
                         "TCP send buffer size(not change if set to 0)(RW)",
                         nullptr, update_func_u32, defaults::tcp_send_buf, 0,
                         2 * 1024 * 1024, 0);
static MYSQL_SYSVAR_UINT(tcp_recv_buf, ::polarx_rpc::tcp_recv_buf,
                         PLUGIN_VAR_OPCMDARG,
                         "TCP receive buffer size(not change if set to 0)(RW)",
                         nullptr, update_func_u32, defaults::tcp_recv_buf, 0,
                         2 * 1024 * 1024, 0);
static MYSQL_SYSVAR_UINT(tcp_fixed_dealing_buf,
                         ::polarx_rpc::tcp_fixed_dealing_buf,
                         PLUGIN_VAR_OPCMDARG,
                         "TCP fixed dealing buffer size(RW)", nullptr,
                         update_func_u32, defaults::tcp_fixed_dealing_buf, 4096,
                         64 * 1024, 0);
static MYSQL_SYSVAR_UINT(mcs_spin_cnt, ::polarx_rpc::mcs_spin_cnt,
                         PLUGIN_VAR_OPCMDARG,
                         "MCS spin lock spin count before snooze(RW)", nullptr,
                         update_func_u32, defaults::mcs_spin_cnt, 1, 10000, 0);
static MYSQL_SYSVAR_UINT(
    session_poll_rwlock_spin_cnt, ::polarx_rpc::session_poll_rwlock_spin_cnt,
    PLUGIN_VAR_OPCMDARG,
    "Spin count of RW-lock before snooze for session pool(RW)", nullptr,
    update_func_u32, defaults::session_poll_rwlock_spin_cnt, 1, 10000, 0);
static MYSQL_SYSVAR_UINT(net_write_timeout, ::polarx_rpc::net_write_timeout,
                         PLUGIN_VAR_OPCMDARG, "Timeout for TCP write in ms(RW)",
                         nullptr, update_func_u32, defaults::net_write_timeout,
                         1, 2 * 3600 * 1000, 0);
static MYSQL_SYSVAR_BOOL(galaxy_protocol, ::polarx_rpc::galaxy_protocol,
                         PLUGIN_VAR_OPCMDARG, "Is galaxy protocol(RW)", nullptr,
                         update_func_b, defaults::galaxy_protocol);
static MYSQL_SYSVAR_UINT(galaxy_version, ::polarx_rpc::galaxy_version,
                         PLUGIN_VAR_OPCMDARG, "Version of galaxy protocol(RW)",
                         nullptr, update_func_u32, defaults::galaxy_version, 0,
                         127, 0);
static MYSQL_SYSVAR_UINT(max_allowed_packet, ::polarx_rpc::max_allowed_packet,
                         PLUGIN_VAR_OPCMDARG,
                         "Max size of packet which can receive(RW)", nullptr,
                         update_func_u32, defaults::max_allowed_packet, 4096,
                         1024 * 1024 * 1024, 0);
static MYSQL_SYSVAR_UINT(max_cached_output_buffer_pages,
                         ::polarx_rpc::max_cached_output_buffer_pages,
                         PLUGIN_VAR_OPCMDARG,
                         "Max cached page number for output(RW)", nullptr,
                         update_func_u32,
                         defaults::max_cached_output_buffer_pages, 1, 256, 0);
static MYSQL_SYSVAR_UINT(max_queued_messages, ::polarx_rpc::max_queued_messages,
                         PLUGIN_VAR_OPCMDARG,
                         "Max queued message for each session(RW)", nullptr,
                         update_func_u32, defaults::max_queued_messages, 16,
                         4096, 0);
static MYSQL_SYSVAR_BOOL(enable_kill_log, ::polarx_rpc::enable_kill_log,
                         PLUGIN_VAR_OPCMDARG, "Enable session kill log(RW)",
                         nullptr, update_func_b, defaults::enable_kill_log);
static MYSQL_SYSVAR_BOOL(enable_thread_pool_log,
                         ::polarx_rpc::enable_thread_pool_log,
                         PLUGIN_VAR_OPCMDARG, "Enable thread pool log(RW)",
                         nullptr, update_func_b,
                         defaults::enable_thread_pool_log);
static MYSQL_SYSVAR_BOOL(enable_perf_hist, ::polarx_rpc::enable_perf_hist,
                         PLUGIN_VAR_OPCMDARG,
                         "Enable performance histogram(RW)", nullptr,
                         update_func_b, defaults::enable_perf_hist);
static MYSQL_SYSVAR_UINT(
    epoll_group_ctx_refresh_time, ::polarx_rpc::epoll_group_ctx_refresh_time,
    PLUGIN_VAR_OPCMDARG,
    "Epoll group context(shared session) refresh time in ms(RW)", nullptr,
    update_func_u32, defaults::epoll_group_ctx_refresh_time, 1000, 60 * 1000,
    0);
static MYSQL_SYSVAR_UINT(shared_session_lifetime,
                         ::polarx_rpc::shared_session_lifetime,
                         PLUGIN_VAR_OPCMDARG,
                         "Life time for shared session in ms(RW)", nullptr,
                         update_func_u32, defaults::shared_session_lifetime,
                         1000, 3600 * 1000, 0);
static MYSQL_SYSVAR_UINT(epoll_group_dynamic_threads,
                         ::polarx_rpc::epoll_group_dynamic_threads,
                         PLUGIN_VAR_OPCMDARG,
                         "Expected non-base threads for each epoll group(RW)",
                         nullptr, update_func_u32,
                         defaults::epoll_group_dynamic_threads, 0, 16, 0);
static MYSQL_SYSVAR_UINT(
    epoll_group_dynamic_threads_shrink_time,
    ::polarx_rpc::epoll_group_dynamic_threads_shrink_time, PLUGIN_VAR_OPCMDARG,
    "Time of shrinking the thread pool when no stall occurs in ms(RW)", nullptr,
    update_func_u32, defaults::epoll_group_dynamic_threads_shrink_time, 1000,
    600 * 1000, 0);
static MYSQL_SYSVAR_UINT(epoll_group_thread_scale_thresh,
                         ::polarx_rpc::epoll_group_thread_scale_thresh,
                         PLUGIN_VAR_OPCMDARG,
                         "Thresh(number of stalled threads in one thread pool "
                         "group) of scaling the thread pool(RW)",
                         nullptr, update_func_u32,
                         defaults::epoll_group_thread_scale_thresh, 0, 100, 0);
static MYSQL_SYSVAR_UINT(
    epoll_group_thread_deadlock_check_interval,
    ::polarx_rpc::epoll_group_thread_deadlock_check_interval,
    PLUGIN_VAR_OPCMDARG, "Interval of checking thread pool deadlock in ms(RW)",
    nullptr, update_func_u32,
    defaults::epoll_group_thread_deadlock_check_interval, 1, 10 * 1000, 0);
static MYSQL_SYSVAR_BOOL(enable_tasker, ::polarx_rpc::enable_tasker,
                         PLUGIN_VAR_OPCMDARG, "Enable balance tasker(RW)",
                         nullptr, update_func_b, defaults::enable_tasker);
static MYSQL_SYSVAR_UINT(epoll_group_tasker_multiply,
                         ::polarx_rpc::epoll_group_tasker_multiply,
                         PLUGIN_VAR_OPCMDARG,
                         "Workload factor of one thread to n queued tasks(RW)",
                         nullptr, update_func_u32,
                         defaults::epoll_group_tasker_multiply, 1, 50, 0);
static MYSQL_SYSVAR_UINT(epoll_group_tasker_extend_step,
                         ::polarx_rpc::epoll_group_tasker_extend_step,
                         PLUGIN_VAR_OPCMDARG, "Tasker threads extend step(RW)",
                         nullptr, update_func_u32,
                         defaults::epoll_group_tasker_extend_step, 1, 50, 0);
static MYSQL_SYSVAR_BOOL(enable_epoll_in_tasker,
                         ::polarx_rpc::enable_epoll_in_tasker,
                         PLUGIN_VAR_OPCMDARG, "Enable tasker to do epoll(RW)",
                         nullptr, update_func_b,
                         defaults::enable_epoll_in_tasker);

struct SYS_VAR *polarx_rpc_system_variables[] = {
    MYSQL_SYSVAR(auto_cpu_affinity),
    MYSQL_SYSVAR(multi_affinity_in_group),
    MYSQL_SYSVAR(force_all_cores),
    MYSQL_SYSVAR(epoll_groups),
    MYSQL_SYSVAR(min_auto_epoll_groups),
    MYSQL_SYSVAR(epoll_extra_groups),
    MYSQL_SYSVAR(epoll_threads_per_group),
    MYSQL_SYSVAR(max_epoll_wait_total_threads),
    MYSQL_SYSVAR(epoll_work_queue_capacity),
    MYSQL_SYSVAR(tcp_listen_queue),
    MYSQL_SYSVAR(request_cache_number),
    MYSQL_SYSVAR(request_cache_instances),
    MYSQL_SYSVAR(request_cache_max_length),

    MYSQL_SYSVAR(epoll_events_per_thread),
    MYSQL_SYSVAR(epoll_timeout),
    MYSQL_SYSVAR(tcp_keep_alive),
    MYSQL_SYSVAR(tcp_send_buf),
    MYSQL_SYSVAR(tcp_recv_buf),
    MYSQL_SYSVAR(tcp_fixed_dealing_buf),
    MYSQL_SYSVAR(mcs_spin_cnt),
    MYSQL_SYSVAR(session_poll_rwlock_spin_cnt),
    MYSQL_SYSVAR(net_write_timeout),
    MYSQL_SYSVAR(galaxy_protocol),
    MYSQL_SYSVAR(galaxy_version),
    MYSQL_SYSVAR(max_allowed_packet),
    MYSQL_SYSVAR(max_cached_output_buffer_pages),
    MYSQL_SYSVAR(max_queued_messages),
    MYSQL_SYSVAR(enable_kill_log),
    MYSQL_SYSVAR(enable_thread_pool_log),
    MYSQL_SYSVAR(enable_perf_hist),
    MYSQL_SYSVAR(epoll_group_ctx_refresh_time),
    MYSQL_SYSVAR(shared_session_lifetime),
    MYSQL_SYSVAR(epoll_group_dynamic_threads),
    MYSQL_SYSVAR(epoll_group_dynamic_threads_shrink_time),
    MYSQL_SYSVAR(epoll_group_thread_scale_thresh),
    MYSQL_SYSVAR(epoll_group_thread_deadlock_check_interval),
    MYSQL_SYSVAR(enable_tasker),
    MYSQL_SYSVAR(epoll_group_tasker_multiply),
    MYSQL_SYSVAR(epoll_group_tasker_extend_step),
    MYSQL_SYSVAR(enable_epoll_in_tasker),
    nullptr};
}  // namespace polarx_rpc
