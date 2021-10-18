/**
 * A MergeOperator for rocksdb that implements string append.
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#include "xengine/merge_operator.h"
#include "xengine/slice.h"

namespace xengine {
namespace util {

class StringAppendOperator : public db::AssociativeMergeOperator {
 public:
  // Constructor: specify delimiter
  explicit StringAppendOperator(char delim_char);

  virtual bool Merge(const common::Slice& key,
                     const common::Slice* existing_value,
                     const common::Slice& value,
                     std::string* new_value) const override;

  virtual const char* Name() const override;

 private:
  char delim_;  // The delimiter is inserted between elements
};

}  //  namespace util
}  //  namespace xengine
