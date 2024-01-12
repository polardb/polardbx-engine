/* Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.

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

#ifndef PLUGIN_X_CLIENT_MYSQLXCLIENT_XMESSAGE_H_
#define PLUGIN_X_CLIENT_MYSQLXCLIENT_XMESSAGE_H_

#ifdef WIN32
#pragma warning(push, 0)
#undef DELETE
#endif  // WIN32

#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/wire_format_lite.h>

#include "protobuf/polarx.pb.h"
#include "protobuf/polarx_connection.pb.h"
#include "protobuf/polarx_datatypes.pb.h"
#include "protobuf/polarx_exec_plan.pb.h"
#include "protobuf/polarx_expect.pb.h"
#include "protobuf/polarx_notice.pb.h"
#include "protobuf/polarx_physical_backfill.pb.h"
#include "protobuf/polarx_resultset.pb.h"
#include "protobuf/polarx_session.pb.h"
#include "protobuf/polarx_sql.pb.h"

#ifdef WIN32
#pragma warning(pop)
#endif  // WIN32

#define POLARX_COLUMN_BYTES_CONTENT_TYPE_GEOMETRY \
  0x0001                                              // GEOMETRY (WKB encoding)
#define POLARX_COLUMN_BYTES_CONTENT_TYPE_JSON 0x0002  // JSON (text encoding)
#define POLARX_COLUMN_BYTES_CONTENT_TYPE_XML 0x0003   // XML (text encoding)

#define POLARX_COLUMN_DATETIME_CONTENT_TYPE_DATE 0x0001
#define POLARX_COLUMN_DATETIME_CONTENT_TYPE_DATETIME 0x0002

#define POLARX_COLUMN_FLAGS_UINT_ZEROFILL 0x0001     // UINT zerofill
#define POLARX_COLUMN_FLAGS_DOUBLE_UNSIGNED 0x0001   // DOUBLE 0x0001 unsigned
#define POLARX_COLUMN_FLAGS_FLOAT_UNSIGNED 0x0001    // FLOAT  0x0001 unsigned
#define POLARX_COLUMN_FLAGS_DECIMAL_UNSIGNED 0x0001  // DECIMAL 0x0001 unsigned
#define POLARX_COLUMN_FLAGS_BYTES_RIGHTPAD 0x0001    // BYTES  0x0001 rightpad
#define POLARX_COLUMN_FLAGS_DATETIME_TIMESTAMP \
  0x0001  // DATETIME 0x0001 timestamp

#define POLARX_COLUMN_FLAGS_NOT_NULL 0x0010
#define POLARX_COLUMN_FLAGS_PRIMARY_KEY 0x0020
#define POLARX_COLUMN_FLAGS_UNIQUE_KEY 0x0040
#define POLARX_COLUMN_FLAGS_MULTIPLE_KEY 0x0080
#define POLARX_COLUMN_FLAGS_AUTO_INCREMENT 0x0100

#endif  // PLUGIN_X_CLIENT_MYSQLXCLIENT_XMESSAGE_H_
