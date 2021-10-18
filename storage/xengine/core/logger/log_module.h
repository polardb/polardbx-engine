/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "util/to_string.h"

#define K(obj) #obj, obj
#define K_(obj) #obj, obj##_
#define KP(obj) #obj, (reinterpret_cast<const util::MyVoid*>(obj))
#define KP_(obj) #obj, reinterpret_cast<const util::MyVoid*>(obj##_)
#define KE(obj) #obj, ((uint32_t)(obj))
#define KE_(obj) #obj, ((uint32_t)(obj##_))

enum InfoLogModule : long {
  XENGINE_MOD = 0,
  XENGINE_COMPACTION_MOD,
  XENGINE_FLUSH_MOD,
  XENGINE_XLOG_MOD,
  XENGINE_REPLAY_MOD,
  XENGINE_XHANDLER_MOD,
  XRPC_MOD,
  SE_MOD,
  XPAXOS_MOD,
  COMMON_MOD,
  GMS_MOD,
  LMS_MOD,
  XMS_MOD,
  SQL_MOD,
  NUM_INFO_LOG_MODULES,
};

#define LOG ::xengine::logger::Logger::get_log()
#define FILE_NAME __FILE__
#define FUNCTION_NAME __FUNCTION__
#define LINE_NUM __LINE__
#define LOG_LEVEL(level) ::xengine::logger::level##_LEVEL
#define LOG_MOD(mod)	InfoLogModule::mod##_MOD
#define LOG_MOD_SUBMOD(mod, submod) InfoLogModule::mod##_##submod##_MOD

//IF the logger with level is enabled
//#define XLOG(level) LOG.need_print(LOG_LEVEL(level))
#define XLOG_MOD(mod, level) LOG.need_print_mod(LOG_MOD(mod), LOG_LEVEL(level))
#define XLOG_MOD_ORIGIN(mod, level)	LOG.need_print_mod(LOG_MOD(mod), level)
#define XLOG_MOD_SUBMOD(mod, submod, level) LOG.need_print_mod(LOG_MOD_SUBMOD(mod, submod), LOG_LEVEL(level))

#define MOD_LOG(mod, level, info_string, ...) \
  ( XLOG_MOD(mod, level) \
  ? LOG.print_log_kv("["#mod"]", LOG_LEVEL(level), FILE_NAME, FUNCTION_NAME, LINE_NUM, info_string, ##__VA_ARGS__) : (void)(0))
#define __MOD_LOG(mod, level, fmt, ...) \
  ( XLOG_MOD(mod, level) \
  ? LOG.print_log_fmt("["#mod"]", LOG_LEVEL(level), FILE_NAME, FUNCTION_NAME, LINE_NUM, fmt, ##__VA_ARGS__) : (void)(0))
#define __MOD_LOG_OLD(mod, level, fmt, ap) \
  LOG.print_log_fmt("["#mod"]", level, FILE_NAME, FUNCTION_NAME, LINE_NUM, fmt, ap)
#define SUB_MOD_LOG(mod, submod, level, info_string, ...) \
  ( XLOG_MOD_SUBMOD(mod, submod, level) \
  ? LOG.print_log_kv("["#mod"."#submod"]", LOG_LEVEL(level), FILE_NAME, FUNCTION_NAME, LINE_NUM, info_string, ##__VA_ARGS__) : (void)(0))
#define __SUB_MOD_LOG(mod, submod, level, fmt, ...) \
  ( XLOG_MOD_SUBMOD(mod, submod, level) \
  ? LOG.print_log_fmt("["#mod"."#submod"]", LOG_LEVEL(level), FILE_NAME, FUNCTION_NAME, LINE_NUM, fmt, ##__VA_ARGS__) : (void)(0))
#define __ORIGIN_LOG(mod, level, fmt, ...) \
  ( XLOG_MOD_ORIGIN(mod, level) \
  ? LOG.print_log_fmt("["#mod"]", (level), FILE_NAME, FUNCTION_NAME, LINE_NUM, fmt, ##__VA_ARGS__) : (void)(0))

//xengine mod and submod
#define XENGINE_LOG(level, info_string, ...) MOD_LOG(XENGINE, level, info_string, ##__VA_ARGS__)
#define XENGINE_LOG_OLD(level, fmt, ap) __MOD_LOG_OLD(XENGINE, level, fmt, ap)
#define __XENGINE_LOG(level, fmt, ...) __MOD_LOG(XENGINE, level, fmt, ##__VA_ARGS__)
#define COMPACTION_LOG(level, info_string, ...) SUB_MOD_LOG(XENGINE, COMPACTION, level, info_string, ##__VA_ARGS__)
#define __COMPACTION_LOG(level, fmt, ...) __SUB_MOD_LOG(XENGINE, COMPACTION, level, fmt, ##__VA_ARGS__)
#define FLUSH_LOG(level, info_string, ...) SUB_MOD_LOG(XENGINE, FLUSH, level, info_string, ##__VA_ARGS__)
#define __FLUSH_LOG(level, fmt, ...) __SUB_MOD_LOG(XENGINE, FLUSH, level, fmt, ##__VA_ARGS__)

#define XLOG_LOG(level, info_string, ...) SUB_MOD_LOG(XENGINE, XLOG, level, info_string, ##__VA_ARGS__)
#define __XLOG_LOG(level, fmt, ...) __SUB_MOD_LOG(XENGINE, XLOG, level, fmt, ##__VA_ARGS__)
#define REPLAY_LOG(level, info_string, ...) SUB_MOD_LOG(XENGINE, REPLAY, level, info_string, ##__VA_ARGS__)
#define __REPLAY_LOG(level, fmt, ...) __SUB_MOD_LOG(XENGINE, REPLAY, level, fmt, ##__VA_ARGS__)

//xengine xhandler
#define XHANDLER_LOG(level, info_string, ...) SUB_MOD_LOG(XENGINE, XHANDLER, level, info_string, ##__VA_ARGS__)
#define __XHANDLER_LOG(level, fmt, ...) __SUB_MOD_LOG(XENGINE, XHANDLER, level, fmt, ##__VA_ARGS__)

//IS modules
#define XRPC_LOG(level, info_string, ...) MOD_LOG(XRPC, level, info_string, ##__VA_ARGS__)
#define __XRPC_LOG(level, fmt, ...) __MOD_LOG(XRPC, level, fmt, ##__VA_ARGS__)
#define SE_LOG(level, info_string, ...) MOD_LOG(SE, level, info_string, ##__VA_ARGS__)
#define __SE_LOG(level, fmt, ...) __MOD_LOG(SE, level, fmt, ##__VA_ARGS__)
#define XPAXOS_LOG(level, info_string, ...) MOD_LOG(XPAXOS, level, info_string, ##__VA_ARGS__)
#define __XPAXOS_LOG(level, fmt, ...) __MOD_LOG(XPAXOS, level, fmt, ##__VA_ARGS__)
#define COMMON_LOG(level, info_string, ...) MOD_LOG(COMMON, level, info_string, ##__VA_ARGS__)
#define __COMMON_LOG(level, fmt, ap) __MOD_LOG(COMMON, level, fmt, ap)

//GMS modules
#define GMS_LOG(level, info_string, ...) MOD_LOG(GMS, level, info_string, ##__VA_ARGS__)
#define __GMS_LOG(level, fmt, ...) __MOD_LOG(GMS, level, fmt, ##__VA_ARGS__)

//LMS modules
#define LMS_LOG(level, info_string, ...) MOD_LOG(LMS, level, info_string, ##__VA_ARGS__)
#define __LMS_LOG(level, fmt, ...) __MOD_LOG(LMS, level, fmt, ##__VA_ARGS__)

//XMS modules
#define XMS_LOG(level, info_string, ...) MOD_LOG(XMS, level, info_string, ##__VA_ARGS__)
#define __XMS_LOG(level, fmt, ...) __MOD_LOG(XMS, level, fmt, ##__VA_ARGS__)

//SQL modules
#define SQL_LOG(level, info_string, ...) MOD_LOG(SQL, level, info_string, ##__VA_ARGS__)
#define __SQL_LOG(level, fmt, ...) __MOD_LOG(SQL, level, fmt, ##__VA_ARGS__)
