-- Copyright (c) 2014, 2022, Oracle and/or its affiliates.
--
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License, version 2.0,
-- as published by the Free Software Foundation.
--
-- This program is also distributed with certain software (including
-- but not limited to OpenSSL) that is licensed under separate terms,
-- as designated in a particular file or component or in included license
-- documentation.  The authors of MySQL hereby grant you an additional
-- permission to link the program and your derivative works with the
-- separately licensed software that they have included with MySQL.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License, version 2.0, for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

--
-- View: host_summary_by_file_io_type
--
-- Summarizes file IO by event type per host.
--
-- When the host found is NULL, it is assumed to be a "background" thread.
--
-- mysql> select * from host_summary_by_file_io_type;
-- +------------+--------------------------------------+-------+---------------+-------------+
-- | host       | event_name                           | total | total_latency | max_latency |
-- +------------+--------------------------------------+-------+---------------+-------------+
-- | hal1       | wait/io/file/sql/FRM                 |   871 | 168.15 ms     | 18.48 ms    |
-- | hal1       | wait/io/file/innodb/innodb_data_file |   173 | 129.56 ms     | 34.09 ms    |
-- | hal1       | wait/io/file/innodb/innodb_log_file  |    20 | 77.53 ms      | 60.66 ms    |
-- | hal1       | wait/io/file/myisam/dfile            |    40 | 6.54 ms       | 4.58 ms     |
-- | hal1       | wait/io/file/mysys/charset           |     3 | 4.79 ms       | 4.71 ms     |
-- | hal1       | wait/io/file/myisam/kfile            |    67 | 4.38 ms       | 300.04 us   |
-- | hal1       | wait/io/file/sql/ERRMSG              |     5 | 2.72 ms       | 1.69 ms     |
-- | hal1       | wait/io/file/sql/pid                 |     3 | 266.30 us     | 185.47 us   |
-- | hal1       | wait/io/file/sql/casetest            |     5 | 246.81 us     | 150.19 us   |
-- | hal1       | wait/io/file/sql/global_ddl_log      |     2 | 21.24 us      | 18.59 us    |
-- | hal2       | wait/io/file/sql/file_parser         |  1422 | 4.80 s        | 135.14 ms   |
-- | hal2       | wait/io/file/sql/FRM                 |   865 | 85.82 ms      | 9.81 ms     |
-- | hal2       | wait/io/file/myisam/kfile            |  1073 | 37.14 ms      | 15.79 ms    |
-- | hal2       | wait/io/file/myisam/dfile            |  2991 | 25.53 ms      | 5.25 ms     |
-- | hal2       | wait/io/file/sql/dbopt               |    20 | 1.07 ms       | 153.07 us   |
-- | hal2       | wait/io/file/sql/misc                |     4 | 59.71 us      | 33.75 us    |
-- | hal2       | wait/io/file/archive/data            |     1 | 13.91 us      | 13.91 us    |
-- +------------+--------------------------------------+-------+---------------+-------------+
--

CREATE OR REPLACE
  ALGORITHM = MERGE
  DEFINER = 'mysql.sys'@'localhost'
  SQL SECURITY INVOKER 
VIEW host_summary_by_file_io_type (
  host,
  event_name,
  total,
  total_latency,
  max_latency
) AS
SELECT IF(host IS NULL, 'background', host) AS host,
       event_name,
       count_star AS total,
       sys.format_time(sum_timer_wait) AS total_latency,
       sys.format_time(max_timer_wait) AS max_latency
  FROM performance_schema.events_waits_summary_by_host_by_event_name
 WHERE event_name LIKE 'wait/io/file%'
   AND count_star > 0
 ORDER BY IF(host IS NULL, 'background', host), sum_timer_wait DESC;
