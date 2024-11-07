/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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


#pragma once

#include <memory>
#include <string>
#include <vector>

#include "../global_defines.h"

#include "meta.h"
#include "murmurhash3.h"

namespace rpc_executor {
class BloomFilterItem : public ::Item_bool_func {
 public:
  BloomFilterItem() : Item_bool_func() {}

#ifdef MYSQL8
  virtual bool resolve_type(THD *) override {
    set_data_type(MYSQL_TYPE_LONG);
    return false;
  }
#else
  virtual enum_field_types field_type() const override {
    return MYSQL_TYPE_LONG;
  }

  virtual Item_result result_type() const override { return INT_RESULT; }
#endif

  virtual const char *func_name() const override { return "bloomfilter"; }

  virtual longlong val_int() override {
    bool contain = true;
    int ret = may_contain(contain);
    if (!ret) {
      return contain ? 1 : 0;
    }
    return 1;
  }

  int init(std::vector<ExprItem *> &param_exprs);

  // Paramenter should be set from init.
  int may_contain(bool &contain);

 private:
  ExprItem *target_item_;
  int64_t total_bits_;
  int64_t number_hash_;
  std::string strategy_;
  int64_t bitmap_size_;
  const char *bitmap_;
};
}  // namespace rpc_executor