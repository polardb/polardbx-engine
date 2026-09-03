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

/** @file include/lizard0spc0types.h
  Metadata area of Secondary Prefix Compression.

 Created 2025-09-28 by Yichang Song
 *******************************************************/

#ifndef lizard0spc0types_h
#define lizard0spc0types_h

#include "univ.i"

/**********************************/
/** Page dict type */
/**********************************/
/** Page dict data */
typedef byte pd_data_t;

/** Page dict record. */
typedef pd_data_t dict_rec_t;

/**********************************/
/** Record pack type */
/**********************************/
/** Pack id type */
typedef uint8_t pack_id_t;

/** Max pack id (7bits)*/
constexpr pack_id_t PACK_ID_MAX = 127;

/** Undefined pack id. */
constexpr pack_id_t PACK_ID_NULL = PACK_ID_MAX + 1;

/** Pack length type. */
typedef uint16_t pack_len_t;

/** Max pack length (15bits) */
constexpr pack_len_t PACK_LEN_MAX = 32767;

#endif
