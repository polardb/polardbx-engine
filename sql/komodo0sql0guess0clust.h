/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/komodo0sql0optimizer.h
  Classes used for query optimizations.

 Created 2025-11-11 by yuchen
 *******************************************************/

 #ifndef KOMODO_SQL_GUESS_CLUST_INCLUDED
 #define KOMODO_SQL_GUESS_CLUST_INCLUDED
 #include <functional>
 #include "my_inttypes.h"
class handler;
namespace komodo {

/** Helper function to compare rowid with GPP info. */
extern "C" int refpos_order_cmp_with_gpp(const void *arg, const void *a,
                                         const void *b);

/** Helper function to get the cmp function with or without GPP info. */
std::function<int(const uchar *a, const uchar *b)> cmp_func(handler *h);

}  // namespace komodo

#endif
 
