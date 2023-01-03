/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef LOG_EVENT_EXT_H_INCLUDED
#define LOG_EVENT_EXT_H_INCLUDED

#include "libbinlogevents/include/control_events_ext.h"
#include "sql/log_event.h"

/**
  A Gcn event is written to the binary log whenever the database is
  modified on the master, unless row based logging is used.

  Gcn_log_event is created for logging, and is called after Gtid_log_event.

  Virtual inheritance is required here to handle the diamond problem in
  the class @c Execute_load_query_log_event.
  The diamond structure is explained in @c Excecute_load_query_log_event

  @internal
  The inheritance structure is as follows:

Binary_log_event  
                   \       
         Gcn_event    Log_event
                    \         /
                Gcn_log_event
  @endinternal
*/

class Gcn_log_event : public binary_log::Gcn_event, public Log_event {
 public:
#ifdef MYSQL_SERVER
  Gcn_log_event(THD *thd_arg);

  int pack_info(Protocol *) override;
#endif

  Gcn_log_event(const char *buffer,
                const Format_description_event *description_event)
      : binary_log::Gcn_event(buffer, description_event),
        Log_event(header(), footer()) {}

  ~Gcn_log_event() override {}

  size_t get_data_size() override {
    return POST_HEADER_LENGTH;
  }

  static size_t get_event_length(enum_binlog_checksum_alg alg) {
    return LOG_EVENT_HEADER_LEN + POST_HEADER_LENGTH +
           (alg ? BINLOG_CHECKSUM_LEN : 0);
  }

#ifdef MYSQL_SERVER
 public:
  bool write_data_header(Basic_ostream *ostream) override;
  uint32 write_data_header_to_memory(uchar *buffer);
  bool write(Basic_ostream *ostream) override;
#endif

 public:
#ifndef MYSQL_SERVER
  void print(FILE *file, PRINT_EVENT_INFO *print_event_info) const override;
#endif

#ifdef MYSQL_SERVER
  int do_apply_event(Relay_log_info const *rli) override;
  int do_update_pos(Relay_log_info *rli) override;
  enum_skip_reason do_shall_skip(Relay_log_info *rli) override;
#endif
};

inline bool is_gcn_event(Log_event *evt) {
  return (evt->get_type_code() == binary_log::GCN_LOG_EVENT);
}
#endif