/* Copyright (c) 2018, Alibaba Cloud and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "plain_serializer.h"
#include <sys/types.h>

uint Plain_serializer::serialize_connection_event(
  const struct mysql_event_rds_connection *event, char *buf, uint buf_len) {
  uint len;
  len = snprintf(buf, buf_len,
    "MYSQL_V1\t" /* Rds audit log version, keep compatible with rds 5.6 */
    "%ld\t"      /* thread id */
    "%s\t"       /* host or ip */
    "%s\t"       /* user */
    "%s\t"       /* db */
    "%llu\t"     /* start_utime */
    "%llu\t"     /* transaction utime */
    "%d\t"       /* error code */
    "%llu\t"     /* time cost*/
    "%llu\t"     /* sent_rows*/
    "%llu\t"     /* updated_rows*/
    "%llu\t"     /* examnied_rows*/
    "%lld\t"     /* memory_used*/
    "%lld\t"     /* memory_used_by_query */
    "%llu\t"     /* logical read */
    "%llu\t"     /* physical sync read */
    "%llu\t"     /* physical async read */
    "%llu\t"     /* temp user table size */
    "%llu\t"     /* temp sort table size */
    "%llu\t"     /* temp sort file size */
    "%d\t"       /* sql command, this is different from rds 5.6 */
    "%d\t"       /* is super */
    "%llu\t"     /* lock wait time */
    "%s"         /* message */
    "%s",        /* terminator */
    event->thread_id,
    event->ip.length ? event->ip.str : "nulll",
    event->user.length ? event->user.str : "null",
    event->db.length ? event->db.str : "null",
    event->start_utime,
    (ulonglong)0,
    event->error_code,
    event->cost_utime,
    (ulonglong)0,
    (ulonglong)0,
    (ulonglong)0,
    (ulonglong)0,
    (ulonglong)0,
    (ulonglong)0,
    (ulonglong)0,
    (ulonglong)0,
    (ulonglong)0,
    (ulonglong)0,
    (ulonglong)0,
    /* 3 for login, 4 for logout as defined in RDS MySQL 5.6 */
    event->event_subclass == MYSQL_AUDIT_RDS_CONNECTION_CONNECT ? 3 : 4,
    event->is_super,
    (ulonglong)0,
    event->message.str,
    "\1\n");

  /*
    If buf_len is not big enough, the output will be trucated, in that
    case, the return value has a different meaning.
  */
  return len >= buf_len ? buf_len : len;
}

uint Plain_serializer::serialize_query_event(
    const struct mysql_event_rds_query* event, char *buf, uint buf_len) {
  uint len;
  uint command = 0;

  /*
    Command column value meanings as defined in RDS MySQL 5.6:
    0 : for normal query
    1 : for statement execute (COM or SQLCOM)
    2 : for statement prepare (COM or SQLCOM)
  */
  switch (event->command) {
  case COM_QUERY:
    command = 0;
    if (event->sql_command == SQLCOM_PREPARE) {
      command = 2;
    } else if (event->sql_command == SQLCOM_EXECUTE) {
      command = 1;
    }
    break;
  case COM_STMT_PREPARE:
    command = 2;
    break;
  case COM_STMT_EXECUTE:
    command = 1;
    break;
  default:
    DBUG_ASSERT(0);
    break;
  }

  len = snprintf(buf, buf_len,
    "MYSQL_V1\t" /* RDS audit log version, keep compatible with rds 5.6 */
    "%ld\t"      /* thread id */
    "%s\t"       /* host or ip */
    "%s\t"       /* user */
    "%s\t"       /* db */
    "%llu\t"     /* start_utime */
    "%llu\t"     /* transaction utime */
    "%d\t"       /* error code */
    "%llu\t"     /* time cost*/
    "%llu\t"     /* sent_rows*/
    "%llu\t"     /* updated_rows*/
    "%llu\t"     /* examnied_rows*/
    "%lld\t"     /* memory_used*/
    "%lld\t"     /* memory_used_by_query */
    "%llu\t"     /* logical read */
    "%llu\t"     /* physical sync read */
    "%llu\t"     /* physical async read */
    "%llu\t"     /* temp user table size */
    "%llu\t"     /* temp sort table size */
    "%llu\t"     /* temp sort file size */
    "%d\t"       /* sql command */
    "%d\t"       /* is super */
    "%llu\t",     /* lock wait time */

    event->thread_id,
    event->ip.length ? event->ip.str : "nulll",
    event->user.length ? event->user.str : "null",
    event->db.length ? event->db.str : "null",
    event->start_utime,
    event->trx_utime,
    event->error_code,
    event->cost_utime,
    event->sent_rows,
    event->updated_rows,
    event->examined_rows,
    event->memory_used,
    event->query_memory_used,
    event->logical_reads,
    event->physical_sync_reads,
    event->physical_async_reads,
    event->temp_user_table_size,
    event->temp_sort_table_size,
    event->temp_sort_file_size,
    command,
    event->is_super,
    event->lock_utime);

  /*
    If buf_len is not big enough, the output will be trucated, in that
    case, the return value has a different meaning.
  */
  if (len >= buf_len) {
    len = buf_len;
  }

  uint remain_len = buf_len - len;
  uint copy_len =
    event->query.length < remain_len ? event->query.length : remain_len;

  /* Use memcpy here because query may contain '\0' character. */
  memcpy(buf + len, event->query.str, copy_len);
  len += copy_len;

  /* Use "\1\n" as audit log record separator */
  len += snprintf(buf + len, buf_len - len, "\1\n");

  /* Make sure audit log record always end with "\1\n" */
  if (len >= buf_len) {
    len = buf_len;
    memcpy(&buf[len - 2], "\1\n", 2);
  }

  return len;
}
