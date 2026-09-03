/*****************************************************************************

Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

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
/** @file include/enc_checksum/enc_checksum_base.h

  Base definitions of encrypted page checksum service bridge.
 *******************************************************/

#ifndef ENC_CHECKSUM_ENC_CHECKSUM_BASE_H_INCLUDED
#define ENC_CHECKSUM_ENC_CHECKSUM_BASE_H_INCLUDED

#include <cstddef>
#include <cstdint>

namespace enc_checksum {

typedef unsigned char byte;
typedef std::size_t ulint;

typedef void (*write_enc_checksum_func)(byte *page, ulint page_size,
                                        ulint data_offset,
                                        ulint trailer_size,
                                        const byte *tablespace_key,
                                        ulint tablespace_key_len);

typedef bool (*verify_enc_checksum_func)(byte *page, ulint page_size,
                                         ulint data_offset,
                                         ulint trailer_size,
                                         const byte *tablespace_key,
                                         ulint tablespace_key_len);

struct Enc_checksum_service_t {
  write_enc_checksum_func write_enc_checksum;
  verify_enc_checksum_func verify_enc_checksum;
};

extern Enc_checksum_service_t Enc_checksum_service_noop;

}  // namespace enc_checksum

#endif
