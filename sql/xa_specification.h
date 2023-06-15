/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file sql/xa_specification.h

  Transaction coordinator log structure specification.

  Created 2023-06-14 by Jianwei.zhao
 *******************************************************/

#ifndef SQL_XA_SPECIFICATION_H
#define SQL_XA_SPECIFICATION_H

#include "sql/xa.h"

/**
 * Transaction coordinator should write log to complete XA,
 * except of xid for all TC_LOG category,  Other specifications are maybe
 * record according to different implemention.
 */

/**
 * transaction coordinator log specification, except of xid.
 */
class XA_specification {
 public:
  XA_specification() {}

  virtual ~XA_specification() {}

  virtual std::string print() = 0;
};
#endif
