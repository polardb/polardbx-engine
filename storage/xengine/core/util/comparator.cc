// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "xengine/comparator.h"
#include <stdint.h>
#include <algorithm>
#include <memory>
#include "db/dbformat.h"
#include "port/port.h"
#include "storage/storage_manager.h"
#include "xengine/slice.h"

using namespace xengine::port;
using namespace xengine::db;
using namespace xengine::storage;
using namespace xengine::common;

namespace xengine {
namespace util {

Comparator::~Comparator() {}

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() {}

  virtual const char* Name() const override {
    return "leveldb.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    return a.compare(b);
  }

  virtual bool Equal(const Slice& a, const Slice& b) const override {
    return a == b;
  }

  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const override {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t start_byte = static_cast<uint8_t>((*start)[diff_index]);
      uint8_t limit_byte = static_cast<uint8_t>(limit[diff_index]);
      if (start_byte >= limit_byte || (diff_index == start->size() - 1)) {
        // Cannot shorten since limit is smaller than start or start is
        // already the shortest possible.
        return;
      }
      assert(start_byte < limit_byte);

      if (diff_index < limit.size() - 1 || start_byte + 1 < limit_byte) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
      } else {
        //     v
        // A A 1 A A A
        // A A 2
        //
        // Incrementing the current byte will make start bigger than limit, we
        // will skip this byte, and find the first non 0xFF byte in start and
        // increment it.
        diff_index++;

        while (diff_index < start->size()) {
          // Keep moving until we find the first non 0xFF byte to
          // increment it
          if (static_cast<uint8_t>((*start)[diff_index]) <
              static_cast<uint8_t>(0xff)) {
            (*start)[diff_index]++;
            start->resize(diff_index + 1);
            break;
          }
          diff_index++;
        }
      }
      assert(Compare(*start, limit) < 0);
    }
  }

  virtual void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i + 1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};

class ReverseBytewiseComparatorImpl : public BytewiseComparatorImpl {
 public:
  ReverseBytewiseComparatorImpl() {}

  virtual const char* Name() const override {
    return "xengine.ReverseBytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    return -a.compare(b);
  }
};

/*
class MetaKeyComparatorImpl : public BytewiseComparatorImpl {
 private:
  // support special comparator
  const Comparator* user_comparator_;

 public:
  explicit MetaKeyComparatorImpl() : user_comparator_(BytewiseComparator()) {}
  explicit MetaKeyComparatorImpl(const Comparator* c) : user_comparator_(c) {}
  explicit MetaKeyComparatorImpl(const MetaKeyComparatorImpl& keycomp)
      : user_comparator_(keycomp.user_comparator_) {}
  virtual const char* Name() const override {
    return "xengine.MetaKeyComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    int size_cfd_level = sizeof(int32_t) + sizeof(int32_t);
    int size_cfd_level_sequence = size_cfd_level + sizeof(int64_t);
    assert(a.size() >= static_cast<size_t>(size_cfd_level_sequence));
    assert(b.size() >= static_cast<size_t>(size_cfd_level_sequence));
    // order by metakey columnfamilyid + level increasing
    //                  sequence number decreasing
    //                  user_key increasing
    const storage::MetaKey* akey =
        reinterpret_cast<const storage::MetaKey*>(a.data());
    const storage::MetaKey* bkey =
        reinterpret_cast<const storage::MetaKey*>(b.data());
    int r = 0;
    if (((r = akey->column_family_id_ - bkey->column_family_id_) != 0) ||
        ((r = akey->level_ - bkey->level_) != 0)) {
      return r;
    }

    int64_t r64 = akey->sequence_number_ - bkey->sequence_number_;
    if (r64 == 0) {
      return compare_internal_key(Slice(a.data() + size_cfd_level_sequence,
                                        a.size() - size_cfd_level_sequence),
                                  Slice(b.data() + size_cfd_level_sequence,
                                        b.size() - size_cfd_level_sequence));
    } else if (r64 > 0) {
      return -1;
    } else {
      return +1;
    }
  }

 private:
  // like InternalKeyComparator
  int compare_internal_key(const Slice& akey, const Slice& bkey) const {
    // Order by:
    //    increasing user key (according to user-supplied comparator)
    //    decreasing sequence number
    //    decreasing type (though sequence# should be enough to disambiguate)
    // int r = ExtractUserKey(akey).compare(ExtractUserKey(bkey));
    int r =
        user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
    if (r == 0) {
      const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
      const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
      if (anum > bnum) {
        r = -1;
      } else if (anum < bnum) {
        r = +1;
      }
    }
    return r;
  }
};
*/

}  // namespace

const Comparator* BytewiseComparator() {
  static BytewiseComparatorImpl bytewise;
  return &bytewise;
}

const Comparator* ReverseBytewiseComparator() {
  static ReverseBytewiseComparatorImpl rbytewise;
  return &rbytewise;
}

/*
const Comparator* MetaKeyComparator(const Comparator* c) {
  //FIXME: not support change user comparator
  // of static instance
  static MetaKeyComparatorImpl metakeycomp(c);
  return &metakeycomp;
}
*/

}  // namespace util
}  // namespace xengine
