/**
 * A TEST MergeOperator for rocksdb that implements string append.
 * It is built using the MergeOperator interface rather than the simpler
 * AssociativeMergeOperator interface. This is useful for testing/benchmarking.
 * While the two operators are semantically the same, all production code
 * should use the StringAppendOperator defined in stringappend.{h,cc}. The
 * operator defined in the present file is primarily for testing.
 *
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#include <deque>
#include <string>

#include "xengine/merge_operator.h"
#include "xengine/slice.h"

namespace xengine {
namespace util {

class StringAppendTESTOperator : public db::MergeOperator {
 public:
  // Constructor with delimiter
  explicit StringAppendTESTOperator(char delim_char);

  virtual bool FullMergeV2(
      const db::MergeOperator::MergeOperationInput& merge_in,
      db::MergeOperator::MergeOperationOutput* merge_out) const override;

  virtual bool PartialMergeMulti(const common::Slice& key,
                                 const std::deque<common::Slice>& operand_list,
                                 std::string* new_value) const override;

  virtual const char* Name() const override;

 private:
  // A version of PartialMerge that actually performs "partial merging".
  // Use this to simulate the exact behaviour of the StringAppendOperator.
  bool _AssocPartialMergeMulti(const common::Slice& key,
                               const std::deque<common::Slice>& operand_list,
                               std::string* new_value) const;

  char delim_;  // The delimiter is inserted between elements
};

}  //  namespace util
}  //  namespace xengine
