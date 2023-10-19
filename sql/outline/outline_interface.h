/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_OUTLINE_OUTLINE_INTERFACE_INCLUDED
#define SQL_OUTLINE_OUTLINE_INTERFACE_INCLUDED

#include "my_inttypes.h"
#include "sql/sql_class.h"

class Parser_state;

namespace im {

/* Initialize outline system. */
void outline_init();

/* Destroy outline system. */
void outline_destroy();

/**
  Init the statement outlines when mysqld reboot

  It should log error message if failed, reported client error
  will be ingored.

  @param[in]      bootstrap     Whether initialize or restart.
*/
void statement_outline_init(bool bootstrap);

/**
  Invoke the outline hint by db and sql digest.


  @param[in]    thd               thread context
  @param[in]    db                current db
  @param[in]    digest storage    sql digest storage by parser
  @param[in]    strip length      number of bytes stripped for explain token
*/
void invoke_outlines(THD *thd, const char *db,
                     const sql_digest_storage *digest_storage,
                     const uint strip_length = 0);

/**
  Enable the digest compute in parser that required by outline.

  @param[in]      parser state
*/
void enable_digest_by_outline(Parser_state *ps);

/* System outline structure partition count */
extern ulonglong outline_partitions;

/* The switch of statement outline */
extern bool opt_outline_enabled;

/**
  Global select query block list within LEX structure.
*/
class Query_blocks_list {
 public:
  Query_block *all_query_blocks;
  Query_block **all_query_blocks_last;

  Query_blocks_list() { reset_query_blocks_list(); }
  /**
    Reset the query blocks list.
  */
  void reset_query_blocks_list();
  /**
    Add the select query block into the global list.
  */
  void add_to_query_blocks(Query_block *select_lex);
};

} /* namespace im */

#endif
