/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

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

#include "sql/consensus_log_event.h"
#include "sql/log_event.h"

const char *Consensus_log_event::SET_STRING_FLAG = "##CONSENSUS FLAG: ";
const char *Consensus_log_event::SET_STRING_TERM = " TERM: ";
const char *Consensus_log_event::SET_STRING_INDEX = " INDEX: ";
const char *Consensus_log_event::SET_STRING_LENGTH = " LENGTH: ";
const char *Consensus_log_event::SET_STRING_RESERVE = " RESERVE: ";

Consensus_log_event::Consensus_log_event(uint32 flag_arg, uint64 term_arg,
                                         uint64 index_arg, uint64 length_arg)
    : binary_log::Consensus_event(flag_arg, term_arg, index_arg, length_arg),
#ifdef MYSQL_SERVER
      Log_event(header(), footer(), Log_event::EVENT_TRANSACTIONAL_CACHE,
                Log_event::EVENT_NORMAL_LOGGING)
#else
      Log_event(header(), footer())
#endif
{
  DBUG_ENTER(
      "Consensus_log_event::Consensus_log_event(int, uint64, uint64, uint64)");
  Log_event_type event_type = binary_log::CONSENSUS_LOG_EVENT;
  common_header->type_code = event_type;
  common_header->data_written = LOG_EVENT_HEADER_LEN + get_data_size();
  common_header->set_is_valid(true);
  common_header->flags |= LOG_EVENT_IGNORABLE_F;
  DBUG_VOID_RETURN;
}

Consensus_log_event::Consensus_log_event(
    const char *buffer, uint event_len,
    const Format_description_event *description_event)
    : binary_log::Consensus_event(buffer, event_len, description_event),
      Log_event(header(), footer()) {
  DBUG_ENTER(
      "Consensus_log_event::Consensus_log_event(const char *,"
      " uint, const Format_description_log_event *");
#ifndef DBUG_OFF
  uint8_t const common_header_len = description_event->common_header_len;
  MY_UNUSED(common_header_len);
  uint8 const post_header_len =
      description_event->post_header_len[binary_log::CONSENSUS_LOG_EVENT - 1];
  MY_UNUSED(post_header_len);
  DBUG_PRINT("info",
             ("event_len: %u; common_header_len: %d; post_header_len: %d",
              event_len, common_header_len, post_header_len));
#endif
  DBUG_VOID_RETURN;
}
size_t Consensus_log_event::to_string(char *buffer) const {
  char *p = buffer;
  assert(strlen(SET_STRING_FLAG) == SET_STRING_FLAG_LENGTH);
  assert(strlen(SET_STRING_TERM) == SET_STRING_TERM_LENGTH);
  assert(strlen(SET_STRING_INDEX) == SET_STRING_INDEX_LENGTH);
  assert(strlen(SET_STRING_LENGTH) == SET_STRING_LENGTH_LENGTH);
  assert(strlen(SET_STRING_RESERVE) == SET_STRING_RESERVE_LENGTH);
  strncpy(p, SET_STRING_FLAG, SET_STRING_FLAG_LENGTH);
  p += SET_STRING_FLAG_LENGTH;
  int flag_len = (int)(ll2str(flag, p, 10, 1) - p);
  p += flag_len;
  strncpy(p, SET_STRING_TERM, SET_STRING_TERM_LENGTH);
  p += SET_STRING_TERM_LENGTH;
  int term_len = int(ll2str(term, p, 10, 1) - p);
  p += term_len;
  strncpy(p, SET_STRING_INDEX, SET_STRING_INDEX_LENGTH);
  p += SET_STRING_INDEX_LENGTH;
  int index_len = int(ll2str(index, p, 10, 1) - p);
  p += index_len;
  strncpy(p, SET_STRING_LENGTH, SET_STRING_LENGTH_LENGTH);
  p += SET_STRING_LENGTH_LENGTH;
  int length_len = int(ll2str(length, p, 10, 1) - p);
  p += length_len;
  strncpy(p, SET_STRING_RESERVE, SET_STRING_RESERVE_LENGTH);
  p += SET_STRING_RESERVE_LENGTH;
  int reserve_len = int(ll2str(reserve, p, 10, 1) - p);
  p += reserve_len;
  *p++ = '\'';
  *p = '\0';
  return p - buffer;
}
#ifdef MYSQL_SERVER
uint32 Consensus_log_event::write_data_header_to_memory(uchar *buffer) {
  DBUG_ENTER("Consensus_log_event::write_data_header_to_memory");
  uchar *ptr_buffer = buffer;
  memcpy(ptr_buffer, &flag, sizeof(flag));
  ptr_buffer += ENCODED_FLAG_LENGTH;
  memcpy(ptr_buffer, &term, sizeof(term));
  ptr_buffer += ENCODED_TERM_LENGTH;
  memcpy(ptr_buffer, &index, sizeof(index));
  ptr_buffer += ENCODED_INDEX_LENGTH;
  memcpy(ptr_buffer, &length, sizeof(length));
  ptr_buffer += ENCODED_LENGTH_LENGTH;
  memcpy(ptr_buffer, &reserve, sizeof(reserve));
  ptr_buffer += ENCODED_RESERVE_LENGTH;
#ifndef DBUG_OFF
  DBUG_PRINT("info", ("flag=%u, term=%llu index=%llu length=%llu reserve=%llu",
                      flag, term, index, length, reserve));
#endif
  assert(ptr_buffer == (buffer + POST_HEADER_LENGTH));
  DBUG_RETURN(POST_HEADER_LENGTH);
}
bool Consensus_log_event::write_data_header(Basic_ostream *ostream) {
  DBUG_ENTER("Consensus_log_event::write_data_header");
  uchar buffer[POST_HEADER_LENGTH];
  write_data_header_to_memory(buffer);
  DBUG_RETURN(
      wrapper_my_b_safe_write(ostream, (uchar *)buffer, POST_HEADER_LENGTH));
}
#endif
#ifdef MYSQL_SERVER
int Consensus_log_event::pack_info(Protocol *protocol) {
  char buffer[MAX_SET_STRING_LENGTH + 2];
  size_t len = to_string(buffer);
  protocol->store_string(buffer, len, &my_charset_bin);
  return 0;
}
#else
void Consensus_log_event::print(FILE *file __attribute__((unused)),
                                PRINT_EVENT_INFO *print_event_info) const {
  char buffer[MAX_SET_STRING_LENGTH + 2];
  IO_CACHE *const head = &print_event_info->head_cache;
  if (!print_event_info->short_form) {
    print_header(head, print_event_info, false);
    my_b_printf(head,
                "\tConsensus flag=%u\tterm=%llu\tindex=%llu\tlength=%llu\n",
                flag, term, index, length);
  }
  to_string(buffer);
  my_b_printf(head, "%s%s\n", buffer, print_event_info->delimiter);
}
#endif
#if defined(MYSQL_SERVER)
int Consensus_log_event::do_apply_event(Relay_log_info const *rli) {
  DBUG_ENTER("Consensus_log_event::do_apply_event");
  MY_UNUSED(rli);
  assert(rli->info_thd == thd);
  DBUG_RETURN(0);
}
int Consensus_log_event::do_update_pos(Relay_log_info *rli) {
  rli->inc_event_relay_log_pos();
  return 0;
}
#endif
const char *Previous_consensus_index_log_event::SET_STRING_PREFIX =
    "##PREV_CONSENSUS_INDEX: ";
Previous_consensus_index_log_event::Previous_consensus_index_log_event(
    uint64 index_arg)
    : binary_log::Previous_consensus_index_event(index_arg),
#ifdef MYSQL_SERVER
      Log_event(header(), footer(), Log_event::EVENT_TRANSACTIONAL_CACHE,
                Log_event::EVENT_NORMAL_LOGGING)
#else
      Log_event(header(), footer())
#endif
{
  DBUG_ENTER("Consensus_log_event::Consensus_log_event(bool, uint64, uint64)");
  Log_event_type event_type = binary_log::PREVIOUS_CONSENSUS_INDEX_LOG_EVENT;
  common_header->type_code = event_type;
  common_header->data_written = LOG_EVENT_HEADER_LEN + get_data_size();
  common_header->set_is_valid(true);
  common_header->flags |= LOG_EVENT_IGNORABLE_F;
  DBUG_VOID_RETURN;
}
Previous_consensus_index_log_event::Previous_consensus_index_log_event(
    const char *buffer, uint event_len,
    const Format_description_event *description_event)
    : binary_log::Previous_consensus_index_event(buffer, event_len,
                                                 description_event),
      Log_event(header(), footer()) {
  DBUG_ENTER(
      "Consensus_log_event::Consensus_log_event(const char *,"
      " uint, const Format_description_log_event *");
#ifndef DBUG_OFF
  uint8_t const common_header_len = description_event->common_header_len;
  MY_UNUSED(common_header_len);
  uint8 const post_header_len =
      description_event
          ->post_header_len[binary_log::PREVIOUS_CONSENSUS_INDEX_LOG_EVENT - 1];
  MY_UNUSED(post_header_len);
  DBUG_PRINT("info",
             ("event_len: %u; common_header_len: %d; post_header_len: %d",
              event_len, common_header_len, post_header_len));
#endif
  DBUG_VOID_RETURN;
}
size_t Previous_consensus_index_log_event::to_string(char *buffer) const {
  char *p = buffer;
  assert(strlen(SET_STRING_PREFIX) == SET_STRING_PREFIX_LENGTH);
  strncpy(p, SET_STRING_PREFIX, SET_STRING_PREFIX_LENGTH);
  p += SET_STRING_PREFIX_LENGTH;
  int index_len = int(ll2str(index, p, 10, 1) - p);
  p += index_len;
  *p++ = '\'';
  *p = '\0';
  return p - buffer;
}
#ifdef MYSQL_SERVER
uint32 Previous_consensus_index_log_event::write_data_header_to_memory(
    uchar *buffer) {
  DBUG_ENTER("Consensus_log_event::write_data_header_to_memory");
  uchar *ptr_buffer = buffer;
  memcpy(ptr_buffer, &index, sizeof(index));
  ptr_buffer += ENCODED_INDEX_LENGTH;
#ifndef DBUG_OFF
  DBUG_PRINT("info", ("index=%lld", index));
#endif
  assert(ptr_buffer == (buffer + POST_HEADER_LENGTH));
  DBUG_RETURN(POST_HEADER_LENGTH);
}
bool Previous_consensus_index_log_event::write_data_header(
    Basic_ostream *ostream) {
  DBUG_ENTER("Consensus_log_event::write_data_header");
  uchar buffer[POST_HEADER_LENGTH];
  write_data_header_to_memory(buffer);
  DBUG_RETURN(
      wrapper_my_b_safe_write(ostream, (uchar *)buffer, POST_HEADER_LENGTH));
}
#endif
#ifdef MYSQL_SERVER
int Previous_consensus_index_log_event::pack_info(Protocol *protocol) {
  char buffer[MAX_SET_STRING_LENGTH + 1];
  size_t len = to_string(buffer);
  protocol->store_string(buffer, len, &my_charset_bin);
  return 0;
}
#else
void Previous_consensus_index_log_event::print(
    FILE *file __attribute__((unused)),
    PRINT_EVENT_INFO *print_event_info) const {
  char buffer[MAX_SET_STRING_LENGTH + 1];
  IO_CACHE *const head = &print_event_info->head_cache;
  if (!print_event_info->short_form) {
    print_header(head, print_event_info, false);
    my_b_printf(head, "\tPrevious Consensus index=%llu\n", index);
  }
  to_string(buffer);
  my_b_printf(head, "%s%s\n", buffer, print_event_info->delimiter);
}
#endif
#if defined(MYSQL_SERVER)
int Previous_consensus_index_log_event::do_apply_event(
    Relay_log_info const *rli) {
  DBUG_ENTER("Consensus_log_event::do_apply_event");
  MY_UNUSED(rli);
  assert(rli->info_thd == thd);
  DBUG_RETURN(0);
}
int Previous_consensus_index_log_event::do_update_pos(Relay_log_info *rli) {
  rli->inc_event_relay_log_pos();
  return 0;
}
#endif
const char *Consensus_cluster_info_log_event::SET_STRING_INFO_LENGTH =
    "##CONSENSUS CLUSTER INFO LENGTH: ";
Consensus_cluster_info_log_event::Consensus_cluster_info_log_event(
    uint32 info_length_arg, char *info_arg)
    : binary_log::Consensus_cluster_info_event(info_length_arg, info_arg),
#ifdef MYSQL_SERVER
      Log_event(header(), footer(), Log_event::EVENT_TRANSACTIONAL_CACHE,
                Log_event::EVENT_NORMAL_LOGGING)
#else
      Log_event(header(), footer())
#endif
{
  DBUG_ENTER("Consensus_log_event::Consensus_log_event(int, uint64, uint64)");
  Log_event_type event_type = binary_log::CONSENSUS_CLUSTER_INFO_EVENT;
  common_header->type_code = event_type;
  common_header->data_written = LOG_EVENT_HEADER_LEN + get_data_size();
  common_header->set_is_valid(true);
  common_header->flags |= LOG_EVENT_IGNORABLE_F;
  DBUG_VOID_RETURN;
}
Consensus_cluster_info_log_event::Consensus_cluster_info_log_event(
    const char *buffer, uint event_len,
    const Format_description_event *description_event)
    : binary_log::Consensus_cluster_info_event(buffer, event_len,
                                               description_event),
      Log_event(header(), footer()) {
  DBUG_ENTER(
      "Consensus_log_event::Consensus_log_event(const char *,"
      " uint, const Format_description_log_event *");
#ifndef DBUG_OFF
  uint8_t const common_header_len = description_event->common_header_len;
  MY_UNUSED(common_header_len);
  uint8 const post_header_len =
      description_event
          ->post_header_len[binary_log::CONSENSUS_CLUSTER_INFO_EVENT - 1];
  MY_UNUSED(post_header_len);
  DBUG_PRINT("info",
             ("event_len: %u; common_header_len: %d; post_header_len: %d",
              event_len, common_header_len, post_header_len));
#endif
  DBUG_VOID_RETURN;
}
size_t Consensus_cluster_info_log_event::to_string(char *buffer) const {
  char *p = buffer;
  assert(strlen(SET_STRING_INFO_LENGTH) == SET_STRING_INFO_LENGTH_LENGTH);
  strncpy(p, SET_STRING_INFO_LENGTH, SET_STRING_INFO_LENGTH_LENGTH);
  p += SET_STRING_INFO_LENGTH_LENGTH;
  int flag_len = (int)(ll2str(info_length, p, 10, 1) - p);
  p += flag_len;
  *p++ = '\'';
  *p = '\0';
  return p - buffer;
}
#ifdef MYSQL_SERVER
uint32 Consensus_cluster_info_log_event::write_data_header_to_memory(
    uchar *buffer) {
  DBUG_ENTER("Consensus_log_event::write_data_header_to_memory");
  uchar *ptr_buffer = buffer;
  memcpy(ptr_buffer, &info_length, sizeof(info_length));
  ptr_buffer += ENCODED_INFO_LENGTH_LENGTH;
#ifndef DBUG_OFF
  DBUG_PRINT("info", ("info_length=%u", info_length));
#endif
  assert(ptr_buffer == (buffer + POST_HEADER_LENGTH));
  DBUG_RETURN(POST_HEADER_LENGTH);
}
bool Consensus_cluster_info_log_event::write_data_header(
    Basic_ostream *ostream) {
  DBUG_ENTER("Consensus_log_event::write_data_header");
  uchar buffer[POST_HEADER_LENGTH];
  write_data_header_to_memory(buffer);
  DBUG_RETURN(
      wrapper_my_b_safe_write(ostream, (const uchar *)buffer,
                              POST_HEADER_LENGTH) ||
      wrapper_my_b_safe_write(ostream, (const uchar *)info, info_length));
}
#endif
#ifdef MYSQL_SERVER
int Consensus_cluster_info_log_event::pack_info(Protocol *protocol) {
  char buffer[MAX_SET_STRING_LENGTH + 2];
  size_t len = to_string(buffer);
  protocol->store_string(buffer, len, &my_charset_bin);
  return 0;
}
#else
void Consensus_cluster_info_log_event::print(
    FILE *file __attribute__((unused)),
    PRINT_EVENT_INFO *print_event_info) const {
  char buffer[MAX_SET_STRING_LENGTH + 2];
  IO_CACHE *const head = &print_event_info->head_cache;
  if (!print_event_info->short_form) {
    print_header(head, print_event_info, false);
    my_b_printf(head, "\tConsensus cluster info \tlength=%u\n", info_length);
  }
  to_string(buffer);
  my_b_printf(head, "%s%s\n", buffer, print_event_info->delimiter);
}
#endif
#if defined(MYSQL_SERVER)
int Consensus_cluster_info_log_event::do_apply_event(
    Relay_log_info const *rli) {
  DBUG_ENTER("Consensus_cluster_info_log_event::do_apply_event");
  MY_UNUSED(rli);
  assert(rli->info_thd == thd);
  DBUG_RETURN(0);
}
int Consensus_cluster_info_log_event::do_update_pos(Relay_log_info *rli) {
  rli->inc_event_relay_log_pos();
  return 0;
}
#endif
const char *Consensus_empty_log_event::SET_STRING = "##CONSENSUS EMPTY";
Consensus_empty_log_event::Consensus_empty_log_event()
    : binary_log::Consensus_empty_event(),
#ifdef MYSQL_SERVER
      Log_event(header(), footer(), Log_event::EVENT_TRANSACTIONAL_CACHE,
                Log_event::EVENT_NORMAL_LOGGING)
#else
      Log_event(header(), footer())
#endif
{
  DBUG_ENTER("Consensusempty_log_event::Consensusempty_log_event()");
  Log_event_type event_type = binary_log::CONSENSUS_EMPTY_EVENT;
  common_header->type_code = event_type;
  common_header->data_written = LOG_EVENT_HEADER_LEN;
  common_header->set_is_valid(true);
  common_header->flags |= LOG_EVENT_IGNORABLE_F;
  DBUG_VOID_RETURN;
}
Consensus_empty_log_event::Consensus_empty_log_event(
    const char *buffer, uint event_len,
    const Format_description_event *description_event)
    : binary_log::Consensus_empty_event(buffer, event_len, description_event),
      Log_event(header(), footer()) {
  DBUG_ENTER(
      "Consensus_empty_log_event::Consensus_empty_log_event(const char *,"
      " uint, const Format_description_log_event *");
#ifndef DBUG_OFF
  uint8_t const common_header_len = description_event->common_header_len;
  MY_UNUSED(common_header_len);
  uint8 const post_header_len =
      description_event->post_header_len[binary_log::CONSENSUS_EMPTY_EVENT - 1];
  MY_UNUSED(post_header_len);
  DBUG_PRINT("info",
             ("event_len: %u; common_header_len: %d; post_header_len: %d",
              event_len, common_header_len, post_header_len));
#endif
  DBUG_VOID_RETURN;
}
size_t Consensus_empty_log_event::to_string(char *buffer) const {
  char *p = buffer;
  assert(strlen(SET_STRING) == SET_STRING_LENGTH);
  strncpy(p, SET_STRING, SET_STRING_LENGTH);
  p += SET_STRING_LENGTH;
  *p++ = '\'';
  *p = '\0';
  return p - buffer;
}
#ifdef MYSQL_SERVER
int Consensus_empty_log_event::pack_info(Protocol *protocol) {
  char buffer[MAX_SET_STRING_LENGTH + 2];
  size_t len = to_string(buffer);
  protocol->store_string(buffer, len, &my_charset_bin);
  return 0;
}
#else
void Consensus_empty_log_event::print(
    FILE *file __attribute__((unused)),
    PRINT_EVENT_INFO *print_event_info) const {
  char buffer[MAX_SET_STRING_LENGTH + 2];
  IO_CACHE *const head = &print_event_info->head_cache;
  if (!print_event_info->short_form) {
    print_header(head, print_event_info, false);
    my_b_printf(head, "\tConsensus empty\n");
  }
  to_string(buffer);
  my_b_printf(head, "%s%s\n", buffer, print_event_info->delimiter);
}
#endif
#if defined(MYSQL_SERVER)
int Consensus_empty_log_event::do_apply_event(Relay_log_info const *rli) {
  DBUG_ENTER("Consensus_empty_log_event::do_apply_event");
  MY_UNUSED(rli);
  assert(rli->info_thd == thd);
  DBUG_RETURN(0);
}
int Consensus_empty_log_event::do_update_pos(Relay_log_info *rli) {
  rli->inc_event_relay_log_pos();
  return 0;
}

Log_event::enum_skip_reason Stop_log_event::do_shall_skip(Relay_log_info *rli) {
  /*
   Events from ourself should be skipped, but they should not
   decrease the slave skip counter.
  */
  if (Multisource_info::is_xpaxos_channel(rli)) {
#ifndef DBUG_OFF
    assert(Multisource_info::is_xpaxos_replication_channel_name(
        rli->get_channel()));
#endif
    return Log_event::EVENT_SKIP_NOT;
  }

  if (this->server_id == ::server_id)
    return Log_event::EVENT_SKIP_IGNORE;
  else
    return Log_event::EVENT_SKIP_NOT;
}
#endif
