//
// Created by zzy on 2022/7/6.
//

#pragma once

#include <atomic>
#include <cstdint>

#include "../global_defines.h"

#ifdef MYSQL8
#include "mysql/plugin.h"
#else
#include "my_global.h"
#define SYS_VAR st_mysql_sys_var
#endif

struct st_mysql_sys_var;

namespace polarx_rpc {

#ifdef MYSQL8
typedef bool my_bool;
#endif

extern my_bool auto_cpu_affinity;
extern my_bool multi_affinity_in_group;
extern my_bool force_all_cores;
extern uint32_t epoll_groups;
extern uint32_t min_auto_epoll_groups;
extern uint32_t epoll_extra_groups;
extern uint32_t epoll_threads_per_group;
extern uint32_t max_epoll_wait_total_threads;
extern uint32_t epoll_events_per_thread;
extern uint32_t epoll_work_queue_capacity;

extern uint32_t epoll_timeout;

extern uint32_t tcp_keep_alive;
extern uint32_t tcp_listen_queue;

extern uint32_t tcp_send_buf;
extern uint32_t tcp_recv_buf;
extern uint32_t tcp_fixed_dealing_buf;

extern uint32_t mcs_spin_cnt;
extern uint32_t session_poll_rwlock_spin_cnt;

extern uint32_t net_write_timeout;

extern my_bool galaxy_protocol;
extern uint32_t galaxy_version;

extern uint32_t max_allowed_packet;

extern uint32_t max_cached_output_buffer_pages;

extern uint32_t max_queued_messages;

extern my_bool enable_kill_log;
extern my_bool enable_thread_pool_log;

extern my_bool enable_perf_hist;

extern uint32_t epoll_group_ctx_refresh_time;
extern uint32_t shared_session_lifetime;

extern uint32_t epoll_group_dynamic_threads;
extern uint32_t epoll_group_dynamic_threads_shrink_time;
extern uint32_t epoll_group_thread_scale_thresh;
extern uint32_t epoll_group_thread_deadlock_check_interval;

extern my_bool enable_tasker;
extern uint32_t epoll_group_tasker_multiply;
extern uint32_t epoll_group_tasker_extend_step;
extern my_bool enable_epoll_in_tasker;

extern uint32_t request_cache_number;
extern uint32_t request_cache_instances;
extern uint32_t request_cache_max_length;

/**
 * Global Variables
 */

extern std::atomic<uint64_t> g_tcp_id_generator;

/**
 * Variables defines.
 */
extern struct SYS_VAR *polarx_rpc_system_variables[];

}  // namespace polarx_rpc
