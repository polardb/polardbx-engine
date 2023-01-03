/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef PLUGIN_X_NGS_INCLUDE_NGS_GALAXY_SESSION_H
#define PLUGIN_X_NGS_INCLUDE_NGS_GALAXY_SESSION_H

#include "plugin/x/ngs/include/ngs/galaxy_protocol.h"

namespace gx {

typedef uint64_t GSession_id;

typedef int8_t GVersion;

const GSession_id DEFAULT_GSESSION_ID = 1UL;

const GSession_id AUTO_COMMIT_GSESSION_ID_MASK = static_cast<GSession_id>(0x8000000000000000ULL);

const GVersion INVALID_GVERSION = -1;

/** Version history */
/*

Revision history:
---------------------------------
First version : 0

*/
const GVersion GVERSION_FIRST = 0;

/** Occupy 8 bytes to save galaxy session id. */
const uint8_t GSESSION_SIZE = 8;

/** Occupy 1 bytes to save galaxy X-protocol version. */
const uint8_t GVERSION_SIZE = 1;

const uint8_t GREQUEST_SIZE = GSESSION_SIZE + GVERSION_SIZE;

extern uint32_t header_size(Protocol_type ptype);

/**
  The message request header for GALAXY protocol.
*/
typedef struct GRequest {
  Protocol_type ptype{Protocol_type::MYSQLX};
  GSession_id sid{DEFAULT_GSESSION_ID};
  GVersion version{INVALID_GVERSION};

 public:
  explicit GRequest() { reset(); }

  explicit GRequest(Protocol_type ptype_arg, GSession_id sid_arg,
                    GVersion version_arg)
      : ptype(ptype_arg), sid(sid_arg), version(version_arg) {}

  void reset() {
    ptype = Protocol_type::MYSQLX;
    sid = DEFAULT_GSESSION_ID;
    version = INVALID_GVERSION;
  }

  void init(Protocol_type ptype_arg, GSession_id sid_arg) {
    ptype = ptype_arg;
    sid = sid_arg;

    if (ptype == Protocol_type::MYSQLX)
      version = INVALID_GVERSION;
    else
      version = GVERSION_FIRST;
  }

  void init(const GRequest &another) {
    ptype = another.ptype;
    sid = another.sid;
    version = another.version;
  }

  uint8_t size() { return header_size(ptype); }

  bool is_galaxy() { return ptype == Protocol_type::GALAXYX; }

} GRequest;

/**
  The message response header for GALAXY protocol.
*/
typedef struct GRequest GHeader;

}  // namespace gx

#endif
