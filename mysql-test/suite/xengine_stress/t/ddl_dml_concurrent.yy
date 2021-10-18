# Copyright (c) 2012 Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301
# USA
#

start_delay:
   # Avoid that worker threads cause a server crash before reporters are started.
   # This leads often to STATUS_ENVIRONMENT_ERROR though a crash happened.
   { sleep 5; return undef };

query_init:
   start_delay; SET AUTOCOMMIT = 0; SET @fill_amount = (@@innodb_page_size / 2 ) + 1; USE rqg_ddl_dml_concurrent;

query:
   ddl |
   dml |
   dml |
   dml |
   dml |
   dml |
   dml |
   dml |
   dml |
   dml |
   dml |
   dml |
   dml;

dml:
   # Ensure that the table does not grow endless.                                                                       |
   delete ; COMMIT                                                                                                      |
   # Make likely: Get duplicate key based on the two row INSERT only.                                                   |
   enforce_duplicate1 ;                                                                                 commit_rollback |
   # Make likely: Get duplicate key based on two row UPDATE only.                                                       |
   enforce_duplicate2 ;                                                                                 commit_rollback |
   # Make likely: Get duplicate key based on the row INSERT and the already committed data.                             |
   insert_part ( my_digit , $my_digit, $my_digit, REPEAT(CAST($my_digit AS CHAR(1)),@fill_amount), REPEAT(CAST($my_digit AS CHAR(1)), 80), REPEAT(CAST($my_digit AS CHAR(1)), 1200));     commit_rollback |
   insert_part ( my_digit , $my_digit - 1, $my_digit, REPEAT(CAST($my_digit AS CHAR(1)),@fill_amount), REPEAT(CAST($my_digit AS CHAR(1)), 80), REPEAT(CAST($my_digit AS CHAR(1)), 1200)); commit_rollback |
   insert_part ( my_digit , $my_digit, $my_digit - 1, REPEAT(CAST($my_digit AS CHAR(1)),@fill_amount), REPEAT(CAST($my_digit AS CHAR(1)), 80), REPEAT(CAST($my_digit AS CHAR(1)), 1200)); commit_rollback |
   insert_part ( my_digit , $my_digit, $my_digit, REPEAT(CAST($my_digit - 1 AS CHAR(1)),@fill_amount), REPEAT(CAST($my_digit AS CHAR(1)), 80), REPEAT(CAST($my_digit AS CHAR(1)), 1200)); commit_rollback |
   insert_part ( my_digit , $my_digit, $my_digit, REPEAT(CAST($my_digit AS CHAR(1)),@fill_amount), REPEAT(CAST($my_digit - 1 AS CHAR(1)), 80), REPEAT(CAST($my_digit AS CHAR(1)), 1200)); commit_rollback |
   insert_part ( my_digit , $my_digit, $my_digit, REPEAT(CAST($my_digit AS CHAR(1)),@fill_amount), REPEAT(CAST($my_digit AS CHAR(1)), 80), REPEAT(CAST($my_digit - 1 AS CHAR(1)), 1200)); commit_rollback ;

enforce_duplicate1:
   delete ; insert_part /* my_digit */ some_record , some_record ;

enforce_duplicate2:
   UPDATE ddl_dml_concurrent SET column_name_int = my_digit LIMIT 2 ;

insert_part:
   INSERT INTO ddl_dml_concurrent (col1,col2,col3,col4,col5,col6) VALUES ;

some_record:
   ($my_digit,$my_digit,$my_digit,REPEAT(CAST($my_digit AS CHAR(1)),@fill_amount),REPEAT(CAST($my_digit AS CHAR(1)),80),REPEAT(CAST($my_digit AS CHAR(1)),1200)) ;

delete:
   DELETE FROM ddl_dml_concurrent WHERE column_name_int = my_digit OR $column_name_int IS NULL;

my_digit:
   { $my_digit= 1 }      |
   { $my_digit= 2 }      |
   { $my_digit= 3 }      |
   { $my_digit= 4 }      |
   { $my_digit= 5 }      |
   { $my_digit= 6 }      |
   { $my_digit= 7 }      |
   { $my_digit= 8 }      |
   { $my_digit= 9 }      |
   { $my_digit= 'NULL' } ;

commit_rollback:
   COMMIT   |
   ROLLBACK ;

ddl:
   ALTER TABLE ddl_dml_concurrent add_accelerator                     |
   ALTER TABLE ddl_dml_concurrent add_accelerator                     |
   ALTER TABLE ddl_dml_concurrent add_accelerator                     |
   ALTER TABLE ddl_dml_concurrent add_accelerator                     |
   ALTER TABLE ddl_dml_concurrent drop_accelerator                    |
   ALTER TABLE ddl_dml_concurrent drop_accelerator                    |
   ALTER TABLE ddl_dml_concurrent drop_accelerator                    |
   ALTER TABLE ddl_dml_concurrent drop_accelerator                    |
   ALTER TABLE ddl_dml_concurrent add_accelerator  , add_accelerator  |
   ALTER TABLE ddl_dml_concurrent drop_accelerator , drop_accelerator |
   ALTER TABLE ddl_dml_concurrent drop_accelerator , add_accelerator  |
   check_table                                        |
   replace_column                                     ;

add_accelerator:
   ADD  UNIQUE   KEY  uidx ( column_name_list ) |
   ADD           KEY   idx ( column_name_list ) |
   ADD  PRIMARY  KEY       ( column_name_list );

drop_accelerator:
   DROP         KEY  uidx |
   DROP         KEY   idx |
   DROP PRIMARY KEY       ;

check_table:
   CHECK TABLE ddl_dml_concurrent ;

column_name_int:
   { $column_name_int= 'col1' } |
   { $column_name_int= 'col2' } |
   { $column_name_int= 'col3' } ;

column_name_list:
   column_name_int           |
   column_name_int           |
   column_name_int           |
   column_name_int           |
   col4(10)                  |
   col5                      |
   col6(10)                  |
   col4(10), column_name_int |
   col4(10), col5            |
   col4(10), col6(10)        |
   col5, column_name_int     |
   col5, col6(10)            |
   col6(10), column_name_int |
   col4(10), column_name_int, col5 |
   col5, column_name_int, col6(10) |
   col4(10), column_name_int, col6(10) ;

replace_column:
   ALTER TABLE ddl_dml_concurrent ADD COLUMN extra_i INT; UPDATE ddl_dml_concurrent SET extra_i = column_name_int; ALTER TABLE ddl_dml_concurrent DROP COLUMN $column_name_int, RENAME COLUMN extra_i TO $column_name_int |
   ALTER TABLE ddl_dml_concurrent ADD COLUMN extra_c CHAR(80); UPDATE ddl_dml_concurrent SET extra_c = col5; ALTER TABLE ddl_dml_concurrent DROP COLUMN col5, RENAME COLUMN extra_c TO col5 |
   ALTER TABLE ddl_dml_concurrent ADD COLUMN extra_v VARCHAR(1200); UPDATE ddl_dml_concurrent SET extra_v = col6; ALTER TABLE ddl_dml_concurrent DROP COLUMN col6, RENAME COLUMN extra_v TO col6 ;
