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