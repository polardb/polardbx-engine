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

/* connection event v1 version */
uint Plain_serializer::serialize_connection_event_v1(
    const struct mysql_event_rds_connection *event, char *buf, uint buf_len) {
  uint len;
  len = snprintf(
      buf, buf_len,
      "MYSQL_V1\t" /* 0. Rds audit log version, keep compatible with rds 5.6 */
      "%ld\t"      /* 1. thread id */
      "%s\t"       /* 2. host or ip */
      "%s\t"       /* 3. user */
      "%s\t"       /* 4. db */
      "%llu\t"     /* 5. start_utime */
      "%llu\t"     /* 6. transaction utime */
      "%d\t"       /* 7. error code */
      "%llu\t"     /* 8. time cost*/
      "%llu\t"     /* 9. sent_rows*/
      "%llu\t"     /* 10. updated_rows*/
      "%llu\t"     /* 11. examnied_rows*/
      "%lld\t"     /* 12. memory_used*/
      "%lld\t"     /* 13. memory_used_by_query */
      "%llu\t"     /* 14. logical read */
      "%llu\t"     /* 15. physical sync read */
      "%llu\t"     /* 16. physical async read */
      "%llu\t"     /* 17. temp user table size */
      "%llu\t"     /* 18. temp sort table size */
      "%llu\t"     /* 19. temp sort file size */
      "%d\t"       /* 20. sql command, this is different from rds 5.6 */
      "%d\t"       /* 21. is super */
      "%llu\t"     /* 22. lock wait time */
      "%s"         /* 23. message */
      "%s",        /* 24. terminator */
      event->thread_id,                              /* 1 */
      event->ip.length ? event->ip.str : "nulll",    /* 2 */
      event->user.length ? event->user.str : "null", /* 3 */
      event->db.length ? event->db.str : "null",     /* 4 */
      event->start_utime,                            /* 5 */
      (ulonglong)0,                                  /* 6 */
      event->error_code,                             /* 7 */
      event->cost_utime,                             /* 8 */
      (ulonglong)0,                                  /* 9 */
      (ulonglong)0,                                  /* 10 */
      (ulonglong)0,                                  /* 11 */
      (ulonglong)0,                                  /* 12 */
      (ulonglong)0,                                  /* 13 */
      (ulonglong)0,                                  /* 14 */
      (ulonglong)0,                                  /* 15 */
      (ulonglong)0,                                  /* 16 */
      (ulonglong)0,                                  /* 17 */
      (ulonglong)0,                                  /* 18 */
      (ulonglong)0,                                  /* 19 */
      /* 20 */
      /* 3 for login, 4 for logout as defined in RDS MySQL 5.6 */
      event->event_subclass == MYSQL_AUDIT_RDS_CONNECTION_CONNECT ? 3 : 4,
      event->is_super,    /* 21 */
      (ulonglong)0,       /* 22 */
      event->message.str, /* 23 */
      "\1\n");            /* 24 */

  /*
    If buf_len is not big enough, the output will be trucated, in that
    case, the return value has a different meaning.
  */
  return len >= buf_len ? buf_len : len;
}

/* query event v1 version */
uint Plain_serializer::serialize_query_event_v1(
    const struct mysql_event_rds_query *event, char *buf, uint buf_len) {
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
      assert(0);
      break;
  }

  len = snprintf(
      buf, buf_len,
      "MYSQL_V1\t" /* 0. RDS audit log version, keep compatible with rds 5.6 */
      "%ld\t"      /* 1. thread id */
      "%s\t"       /* 2. host or ip */
      "%s\t"       /* 3. user */
      "%s\t"       /* 4. db */
      "%llu\t"     /* 5. start_utime */
      "%llu\t"     /* 6. transaction utime */
      "%d\t"       /* 7. error code */
      "%llu\t"     /* 8. time cost*/
      "%llu\t"     /* 9. sent_rows*/
      "%llu\t"     /* 10. updated_rows*/
      "%llu\t"     /* 11. examnied_rows*/
      "%lld\t"     /* 12. memory_used*/
      "%lld\t"     /* 13. memory_used_by_query */
      "%llu\t"     /* 14. logical read */
      "%llu\t"     /* 15. physical sync read */
      "%llu\t"     /* 16. physical async read */
      "%llu\t"     /* 17. temp user table size */
      "%llu\t"     /* 18. temp sort table size */
      "%llu\t"     /* 19. temp sort file size */
      "%d\t"       /* 20. sql command */
      "%d\t"       /* 21. is super */
      "%llu\t",    /* 22. lock wait time */
      event->thread_id,                              /* 1 */
      event->ip.length ? event->ip.str : "nulll",    /* 2 */
      event->user.length ? event->user.str : "null", /* 3 */
      event->db.length ? event->db.str : "null",     /* 4 */
      event->start_utime,                            /* 5 */
      event->trx_utime,                              /* 6 */
      event->error_code,                             /* 7 */
      event->cost_utime,                             /* 8 */
      event->sent_rows,                              /* 9 */
      event->updated_rows,                           /* 10 */
      event->examined_rows,                          /* 11 */
      event->memory_used,                            /* 12 */
      event->query_memory_used,                      /* 13 */
      event->logical_reads,                          /* 14 */
      event->physical_sync_reads,                    /* 15 */
      event->physical_async_reads,                   /* 16 */
      event->temp_user_table_size,                   /* 17 */
      event->temp_sort_table_size,                   /* 18 */
      event->temp_sort_file_size,                    /* 19 */
      command,                                       /* 20 */
      event->is_super,                               /* 21 */
      event->lock_utime);                            /* 22 */

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

const char *Plain_serializer::s_ext_str_ = "sql_cutoff_flag:%d\t";

/* connection event v3 version */
uint Plain_serializer::serialize_connection_event_v3(
    const struct mysql_event_rds_connection *event, char *buf, uint buf_len) {
  uint len;
  len = snprintf(
      buf, buf_len,
      "MYSQL_V3\t" /* 0. Rds audit log version, keep compatible with rds 5.6 */
      "%ld\t"      /* 1. thread id */
      "%s\t"       /* 2. host or ip */
      "%s\t"       /* 3. user */
      "%s\t"       /* 4. db */
      "%llu\t"     /* 5. start_utime */
      "%llu\t"     /* 6. transaction utime */
      "%llu\t"     /* 7. latency, sql execution utime */
      "%llu\t"     /* 8. cpu time */
      "%llu\t"     /* 9. lock wait time, server and engine are both included */
      "%llu\t"     /* 10. server lock time */
      "%llu\t"     /* 11. engine lock time */
      "%d\t"       /* 12. sql error code */
      "%d\t"       /* 13. whether the user has super privilege */
      /*
        sql command type:
        0: normal, 1: execute, 2: prepare,
        3: login,  4: logout,  5: proc (procedure, function or event)
      */
      "%d\t"            /* 14. sql command type */
      "%llu\t"          /* 15. rows returned(sent to client) */
      "%llu\t"          /* 16. rows updated */
      "%llu\t"          /* 17. rows examined */
      "%llu\t"          /* 18. memory used by thread */
      "%llu\t"          /* 19. memory used by SQL */
      "%llu\t"          /* 20. logical reads */
      "%llu\t"          /* 21. physical sync reads */
      "%llu\t"          /* 22. physical async reads */
      "%llu\t"          /* 23. mutex_spins */
      "%llu\t"          /* 24. mutex_waits */
      "%llu\t"          /* 25. rw_spin_waits */
      "%llu\t"          /* 26. rw_spin_rounds */
      "%llu\t"          /* 27. rw_os_waits */
      "%llu\t"          /* 28. data_read_bytes */
      "%llu\t"          /* 29. data_read_time */
      "%llu\t"          /* 30. data_write_bytes */
      "%llu\t"          /* 31. data_write_time */
      "%llu\t"          /* 32. redo_write_bytes */
      "%llu\t"          /* 33. redo_write_time */
      "%llu\t"          /* 34. temp_file_read_bytes */
      "%llu\t"          /* 35. temp_file_read_time */
      "%llu\t"          /* 36. temp_file_write_bytes */
      "%llu\t"          /* 37. temp_file_write_time */
      "%llu\t"          /* 38. temp_table_read_bytes */
      "%llu\t"          /* 39. temp_table_read_time */
      "%llu\t"          /* 40. temp_table_write_bytes */
      "%llu\t"          /* 41. temp_table_write_time */
      "%llu\t"          /* 42. binlog_write_bytes */
      "%llu\t"          /* 43. binlog_write_time */
      "%llu\t"          /* 44. bytes_sent */
      "%llu\t"          /* 45. bytes_received */
      "%s\t"            /* 46. client_endpoint_ip */
      "%llu\t",         /* 47. trx_id */
      event->thread_id, /* 1 */
      event->ip.length ? event->ip.str : "null",     /* 2 */
      event->user.length ? event->user.str : "null", /* 3 */
      event->db.length ? event->db.str : "null",     /* 4 */
      event->start_utime,                            /* 5 */
      (ulonglong)0,                                  /* 6 */
      event->cost_utime,                             /* 7 */
      (ulonglong)0,                                  /* 8 */
      (ulonglong)0,                                  /* 9 */
      (ulonglong)0,                                  /* 10 */
      (ulonglong)0,                                  /* 11 */
      event->error_code,                             /* 12 */
      event->is_super,                               /* 13 */
      event->event_subclass == MYSQL_AUDIT_RDS_CONNECTION_CONNECT ? 3
                                                                  : 4, /* 14 */
      (ulonglong)0,                                                    /* 15 */
      (ulonglong)0,                                                    /* 16 */
      (ulonglong)0,                                                    /* 17 */
      (ulonglong)0,                                                    /* 18 */
      (ulonglong)0,                                                    /* 19 */
      (ulonglong)0,                                                    /* 20 */
      (ulonglong)0,                                                    /* 21 */
      (ulonglong)0,                                                    /* 22 */
      (ulonglong)0,                                                    /* 23 */
      (ulonglong)0,                                                    /* 24 */
      (ulonglong)0,                                                    /* 25 */
      (ulonglong)0,                                                    /* 26 */
      (ulonglong)0,                                                    /* 27 */
      (ulonglong)0,                                                    /* 28 */
      (ulonglong)0,                                                    /* 29 */
      (ulonglong)0,                                                    /* 30 */
      (ulonglong)0,                                                    /* 31 */
      (ulonglong)0,                                                    /* 32 */
      (ulonglong)0,                                                    /* 33 */
      (ulonglong)0,                                                    /* 34 */
      (ulonglong)0,                                                    /* 35 */
      (ulonglong)0,                                                    /* 36 */
      (ulonglong)0,                                                    /* 37 */
      (ulonglong)0,                                                    /* 38 */
      (ulonglong)0,                                                    /* 39 */
      (ulonglong)0,                                                    /* 40 */
      (ulonglong)0,                                                    /* 41 */
      (ulonglong)0,                                                    /* 42 */
      (ulonglong)0,                                                    /* 43 */
      (ulonglong)0,                                                    /* 44 */
      (ulonglong)0,                                                    /* 45 */
      event->endpoint_ip.length ? event->endpoint_ip.str : "null",     /* 46 */
      (ulonglong)0                                                     /* 47 */
  );

  /*
    If buf_len is not big enough, the output will be trucated, in that
    case, the return value has a different meaning.
  */
  if (len >= buf_len) {
    len = buf_len;
  }
  uint remain_len = buf_len - len;
  len += fill_ext_and_msg(buf + len, remain_len, event->message.str,
                          event->message.length);
  /* Make sure audit log record always end with "\1\n" */
  if (len >= buf_len) {
    len = buf_len;
    memcpy(&buf[len - 2], "\1\n", 2);
  }
  return len;
}

/* query event v3 version */
uint Plain_serializer::serialize_query_event_v3(
    const struct mysql_event_rds_query *event, char *buf, uint buf_len) {
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
      assert(0);
      break;
  }

  len = snprintf(
      buf, buf_len,
      "MYSQL_V3\t" /* 0. Rds audit log version, keep compatible with rds 5.6 */
      "%ld\t"      /* 1. thread id */
      "%s\t"       /* 2. host or ip */
      "%s\t"       /* 3. user */
      "%s\t"       /* 4. db */
      "%llu\t"     /* 5. start_utime */
      "%llu\t"     /* 6. transaction utime */
      "%llu\t"     /* 7. latency, sql execution utime */
      "%llu\t"     /* 8. cpu time */
      "%llu\t"     /* 9. lock wait time, server and engine are both included */
      "%llu\t"     /* 10. server lock time */
      "%llu\t"     /* 11. engine lock time */
      "%d\t"       /* 12. sql error code */
      "%d\t"       /* 13. whether the user has super privilege */
      /*
        sql command type:
        0: normal, 1: execute, 2: prepare,
        3: login,  4: logout,  5: proc (procedure, function or event)
      */
      "%d\t"            /* 14. sql command type */
      "%llu\t"          /* 15. rows returned(sent to client) */
      "%llu\t"          /* 16. rows updated */
      "%llu\t"          /* 17. rows examined */
      "%llu\t"          /* 18. memory used by thread */
      "%llu\t"          /* 19. memory used by SQL */
      "%llu\t"          /* 20. logical reads */
      "%llu\t"          /* 21. physical sync reads */
      "%llu\t"          /* 22. physical async reads */
      "%llu\t"          /* 23. mutex_spins */
      "%llu\t"          /* 24. mutex_waits */
      "%llu\t"          /* 25. rw_spin_waits */
      "%llu\t"          /* 26. rw_spin_rounds */
      "%llu\t"          /* 27. rw_os_waits */
      "%llu\t"          /* 28. data_read_bytes */
      "%llu\t"          /* 29. data_read_time */
      "%llu\t"          /* 30. data_write_bytes */
      "%llu\t"          /* 31. data_write_time */
      "%llu\t"          /* 32. redo_write_bytes */
      "%llu\t"          /* 33. redo_write_time */
      "%llu\t"          /* 34. temp_file_read_bytes */
      "%llu\t"          /* 35. temp_file_read_time */
      "%llu\t"          /* 36. temp_file_write_bytes */
      "%llu\t"          /* 37. temp_file_write_time */
      "%llu\t"          /* 38. temp_table_read_bytes */
      "%llu\t"          /* 39. temp_table_read_time */
      "%llu\t"          /* 40. temp_table_write_bytes */
      "%llu\t"          /* 41. temp_table_write_time */
      "%llu\t"          /* 42. binlog_write_bytes */
      "%llu\t"          /* 43. binlog_write_time */
      "%llu\t"          /* 44. bytes_sent */
      "%llu\t"          /* 45. bytes_received */
      "%s\t"            /* 46. client_endpoint_ip */
      "%llu\t",         /* 47. trx_id */
      event->thread_id, /* 1 */
      event->ip.length ? event->ip.str : "null",                   /* 2 */
      event->user.length ? event->user.str : "null",               /* 3 */
      event->db.length ? event->db.str : "null",                   /* 4 */
      event->start_utime,                                          /* 5 */
      event->trx_utime,                                            /* 6 */
      event->cost_utime,                                           /* 7 */
      (ulonglong)0,                                                /* 8 */
      event->lock_utime,                                           /* 9 */
      event->server_lock_wait,                                     /* 10 */
      event->engine_lock_wait,                                     /* 11 */
      event->error_code,                                           /* 12 */
      event->is_super,                                             /* 13 */
      command,                                                     /* 14 */
      event->sent_rows,                                            /* 15 */
      event->updated_rows,                                         /* 16 */
      event->examined_rows,                                        /* 17 */
      event->memory_used,                                          /* 18 */
      event->query_memory_used,                                    /* 19 */
      event->logical_reads,                                        /* 20 */
      event->physical_sync_reads,                                  /* 21 */
      event->physical_async_reads,                                 /* 22 */
      (ulonglong)0,                                                /* 23 */
      (ulonglong)0,                                                /* 24 */
      event->rw_spin_waits,                                        /* 25 */
      event->rw_spin_rounds,                                       /* 26 */
      event->rw_os_waits,                                          /* 27 */
      (ulonglong)0,                                                /* 28 */
      (ulonglong)0,                                                /* 29 */
      (ulonglong)0,                                                /* 30 */
      (ulonglong)0,                                                /* 31 */
      (ulonglong)0,                                                /* 32 */
      (ulonglong)0,                                                /* 33 */
      (ulonglong)0,                                                /* 34 */
      (ulonglong)0,                                                /* 35 */
      (ulonglong)0,                                                /* 36 */
      (ulonglong)0,                                                /* 37 */
      (ulonglong)0,                                                /* 38 */
      (ulonglong)0,                                                /* 39 */
      (ulonglong)0,                                                /* 40 */
      (ulonglong)0,                                                /* 41 */
      (ulonglong)0,                                                /* 42 */
      (ulonglong)0,                                                /* 43 */
      (ulonglong)0,                                                /* 44 */
      (ulonglong)0,                                                /* 45 */
      event->endpoint_ip.length ? event->endpoint_ip.str : "null", /* 46 */
      event->trx_id                                                /* 47 */
  );

  /*
If buf_len is not big enough, the output will be trucated, in that
case, the return value has a different meaning.
*/
  if (len >= buf_len) {
    len = buf_len;
  }
  uint remain_len = buf_len - len;
  len += fill_ext_and_msg(buf + len, remain_len, event->query.str,
                          event->query.length);
  /* Make sure audit log record always end with "\1\n" */
  if (len >= buf_len) {
    len = buf_len;
    memcpy(&buf[len - 2], "\1\n", 2);
  }

  return len;
}

/*
  fill the ext and msg field for connection or query event iff version of event
  is no less than V3. For connection event, msg is the connection message.
  For query event, msg is the query str.
*/
uint Plain_serializer::fill_ext_and_msg(char *buf, uint limit, const char *msg,
                                        uint msg_size) {
  static const uint ext_size = strlen(s_ext_str_);
  uint len = 0;
  /*
    In audit log, sql_cutoff_flag would be 0 or 1 which occupies 1 byte,
    however "%d" occupies 2 byte, so we must minus 1 from ext_size, therefore
    we set ext_recoup_size = -1 as a recoupment.
    If some future fields with unpredictable length would be added here which
    would be influenced by query str, it would be a complex calculation for
    "ext_recoup_size".
  */
  int ext_recoup_size = -1;

  /* +2 for separator "\1\n". */
  len = snprintf(buf, limit, s_ext_str_,
                 limit >= ext_size + ext_recoup_size + msg_size + 2 ? 0 : 1);

  uint remain_len = limit > len ? limit - len : 0;

  uint copy_len = msg_size < remain_len ? msg_size : remain_len;

  /* Use memcpy here because query may contain '\0' character. */
  memcpy(buf + len, msg, copy_len);
  len += copy_len;

  /* Use "\1\n" as audit log record separator */
  len += snprintf(buf + len, limit - len, "\1\n");
  return len;
}
