//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "xdb_comparator.h"
#include "xengine/ldb_tool.h"

int main(int argc, char **argv) {
  xengine::common::Options db_options;
  const myx::Xdb_pk_comparator pk_comparator;
  db_options.comparator = &pk_comparator;

  xengine::tools::LDBTool tool;
  tool.Run(argc, argv, db_options);
  return 0;
}
