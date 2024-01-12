//
// Created by zzy on 2022/7/26.
//

#pragma once

#ifdef WIN32
#pragma warning(push, 0)
#endif  // WIN32

#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/wire_format_lite.h>

#include "protobuf_lite/polarx.pb.h"
#include "protobuf_lite/polarx_connection.pb.h"
#include "protobuf_lite/polarx_datatypes.pb.h"
#include "protobuf_lite/polarx_exec_plan.pb.h"
#include "protobuf_lite/polarx_expect.pb.h"
#include "protobuf_lite/polarx_notice.pb.h"
#include "protobuf_lite/polarx_physical_backfill.pb.h"
#include "protobuf_lite/polarx_resultset.pb.h"
#include "protobuf_lite/polarx_session.pb.h"
#include "protobuf_lite/polarx_sql.pb.h"

#ifdef WIN32
#pragma warning(pop)
#endif  // WIN32

typedef ::google::protobuf::MessageLite ProtoMsg;

#define POLARX_COLUMN_BYTES_CONTENT_TYPE_GEOMETRY \
  0x0001                                              // GEOMETRY (WKB encoding)
#define POLARX_COLUMN_BYTES_CONTENT_TYPE_JSON 0x0002  // JSON (text encoding)
#define POLARX_COLUMN_BYTES_CONTENT_TYPE_XML 0x0003   // XML (text encoding)

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
