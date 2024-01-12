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

#ifndef SQL_RECYCLE_BIN_RECYCLE_PARSE_INCLUDED
#define SQL_RECYCLE_BIN_RECYCLE_PARSE_INCLUDED

class THD;
namespace im {

namespace recycle_bin {

class Recycle_process_context;

/**
  It's the recycle logic entrance, we allowed to recycle table
  when DROP TABLE or DATABASE by setting recycle state flag.

  @param[in]      thd       thread context
  @param[in]      context   Recycle process context
*/
extern void recycle_state_rewrite(const THD *thd,
                                  const Recycle_process_context *context);

} /* namespace recycle_bin */

} /* namespace im */

#endif
