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

#include "sql/log_event_ext.h"
#include "sql/log_event.h"


#ifdef MYSQL_SERVER
Gcn_log_event::Gcn_log_event(THD *thd_arg)
    : binary_log::Gcn_event(),
      Log_event(thd_arg,
                LOG_EVENT_IGNORABLE_F,
                Log_event::EVENT_TRANSACTIONAL_CACHE,
                Log_event::EVENT_NORMAL_LOGGING, header(), footer()){
  DBUG_TRACE;

  Log_event_type event_type = binary_log::GCN_LOG_EVENT;
  common_header->type_code = event_type;
  common_header->set_is_valid(true);
  common_header->flags |= LOG_EVENT_IGNORABLE_F;
}

int Gcn_log_event::pack_info(Protocol *protocol) {
  String str_buf;
  str_buf.append("SET @@SESSION.INNODB_COMMIT_SEQ=");
  
  char gcn_buf[64];
  longlong10_to_str(commit_gcn, gcn_buf, 10);
  str_buf.append(gcn_buf);

  protocol->store_string(str_buf.ptr(), str_buf.length(), &my_charset_bin);
  return 0;
}
#endif  // MYSQL_SERVER

#ifdef MYSQL_SERVER

uint32 Gcn_log_event::write_data_header_to_memory(uchar *buffer) {
  DBUG_TRACE;
  uchar *ptr_buffer = buffer;

	if (thd && thd->get_commit_gcn() != MYSQL_GCN_NULL) {
    flags |= FLAG_HAVE_COMMITTED_GCN;
    commit_gcn = thd->get_commit_gcn();
  }

  if (thd != nullptr && thd->variables.innodb_commit_gcn != MYSQL_GCN_NULL) {
    flags |= FLAG_HAVE_COMMITTED_SEQ;
  }

  if (thd != nullptr && thd->variables.innodb_snapshot_gcn != MYSQL_GCN_NULL) {
    flags |= FLAG_HAVE_SNAPSHOT_SEQ;
  }

  //DBUG_ASSERT(flags != 0);

  *ptr_buffer = flags;
  ptr_buffer += FLAGS_LENGTH;

  if (flags & FLAG_HAVE_COMMITTED_GCN) {
    int8store(ptr_buffer, commit_gcn);
    ptr_buffer += COMMITTED_GCN_LENGTH;
  }

  return ptr_buffer - buffer;
}

bool Gcn_log_event::write_data_header(Basic_ostream *ostream) {
  DBUG_TRACE;
  uchar buffer[POST_HEADER_LENGTH];
  write_data_header_to_memory(buffer);
  return wrapper_my_b_safe_write(ostream, (uchar *)buffer, POST_HEADER_LENGTH);
}

bool Gcn_log_event::write(Basic_ostream *ostream) {
  return (write_header(ostream, get_data_size()) ||
          write_data_header(ostream) || write_footer(ostream));
}

#endif

#ifndef MYSQL_SERVER
void Gcn_log_event::print(FILE *, PRINT_EVENT_INFO *print_event_info) const {
  DBUG_ASSERT(flags != 0);
  IO_CACHE *const head = &print_event_info->head_cache;

  if (!print_event_info->short_form)
  {
    print_header(head, print_event_info, false);
    my_b_printf(head, "\tGcn\thave_snapshot_seq=%s\thave_commit_seq=%s\n",
                (flags & FLAG_HAVE_SNAPSHOT_SEQ) ? "true" : "false", 
                (flags & FLAG_HAVE_COMMITTED_SEQ) ? "true" : "false");
  }

  if (flags & FLAG_HAVE_COMMITTED_GCN) {
    my_b_printf(head, "SET @@session.innodb_commit_seq=%llu%s\n",
                (ulonglong)commit_gcn, print_event_info->delimiter);
  }
}
#endif


#ifdef MYSQL_SERVER
int Gcn_log_event::do_apply_event(Relay_log_info const *rli) {
  DBUG_TRACE;
  DBUG_ASSERT(rli->info_thd == thd);

  if (flags & FLAG_HAVE_COMMITTED_GCN) {
    thd->variables.innodb_commit_gcn = commit_gcn;
  }

  return 0;
}

int Gcn_log_event::do_update_pos(Relay_log_info *rli) {
  /*
    This event does not increment group positions. This means
    that if there is a failure after it has been processed,
    it will be automatically re-executed.
  */
  rli->inc_event_relay_log_pos();
  return 0;
}

Log_event::enum_skip_reason Gcn_log_event::do_shall_skip(Relay_log_info *rli) {
  Log_event::enum_skip_reason ret = Log_event::continue_group(rli);

  if (ret != EVENT_SKIP_NOT) {
    thd->m_extra_desc.m_commit_gcn = MYSQL_GCN_NULL;
  }

  return ret;
}
#endif  // MYSQL_SERVER
