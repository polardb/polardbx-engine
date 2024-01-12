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
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/lizard0data0types.h
 lizard record data.

 Created 2020-04-06 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0datatypes_h
#define lizard0datatypes_h

#include "univ.i"

/** lizard scn id, the third system column */
constexpr size_t DATA_SCN_ID = 3;
/** lizard scn occupy 8 bytes */
constexpr size_t DATA_SCN_ID_LEN = 8;

/** lizard UBA, the fourth system column */
constexpr size_t DATA_UNDO_PTR = 4;
/** lizard UBA occupy 7 bytes */
constexpr size_t DATA_UNDO_PTR_LEN = 8;

/** gcn id, firth system column */
constexpr size_t DATA_GCN_ID = 5;
constexpr size_t DATA_GCN_ID_LEN = 8;

/* The sum of SCN, UBA and GCN length */
constexpr size_t DATA_LIZARD_TOTAL_LEN =
    DATA_SCN_ID_LEN + DATA_UNDO_PTR_LEN + DATA_GCN_ID_LEN;

#define DATA_N_LIZARD_COLS 3 /** number of lizard system column */

/** number of lizard system column for
intrinsic tempooary table. Intrinsic table didn't support
rollback, so didn't have UBA and no meaningful of SCN */
#define DATA_ITT_N_LIZARD_COLS 0

namespace lizard {} /* namespace lizard */

#endif
