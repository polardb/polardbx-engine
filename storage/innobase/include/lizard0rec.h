/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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
ANY WARRANTY; without even the lizardplied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License,
version 2.0, for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/lizard0rec.h
 lizard record format.

 Created 2020-04-06 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0rec_h
#define lizard0rec_h

namespace lizard {

/**
  lizard Record Format:

  Based on the innodb compact record format, add scn, undo_ptr system columns:

  Table [id, name, row_id, trx_id, roll_ptr, scn, undo_ptr];

  1) Cluster index:
     [id, trx_id, roll_ptr, scn, undo_ptr, name];

  2) Secondary index:
     [name, id]

  Both durable and temporary table will add two new columns expect of
  intrinsic temproary table.
*/

} /* namespace lizard */

#endif
