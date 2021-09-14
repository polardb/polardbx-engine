/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "db/memtable.h"
#include "memtable/art.h"
#include "memtable/art_node.h"
#include "util/arena.h"
#include "xengine/memtablerep.h"

using namespace xengine;
using namespace xengine::common;
using namespace xengine::util;
using namespace xengine::db;

namespace xengine {
namespace memtable {

class ARTRep : public MemTableRep {
  ART art_;
  const MemTableRep::KeyComparator& cmp_;

 public:
  explicit ARTRep(const MemTableRep::KeyComparator& compare,
                       MemTableAllocator* allocator)
      : MemTableRep(allocator),
        art_(compare, allocator),
        cmp_(compare) {}

  void init() { art_.init(); }

  virtual KeyHandle Allocate(const size_t len, char** buf) override {
    ARTValue *artvalue = art_.allocate_art_value(len);
    *buf = reinterpret_cast<char *>(artvalue) + sizeof(ARTValue);
    return static_cast<KeyHandle>(artvalue);
  }

  virtual void Insert(KeyHandle handle) override {
    art_.insert(reinterpret_cast<ARTValue *>(handle));
  }

  virtual void InsertConcurrently(KeyHandle handle) override {
    art_.insert(reinterpret_cast<ARTValue *>(handle));
  }

  virtual bool Contains(const char* key) const override {
    return art_.contains(key);
  }

  virtual size_t ApproximateMemoryUsage() override {
    return art_.approximate_memory_usage();
  }

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg,
                                         const char* entry)) override {
    ARTRep::Iterator iter(&art_);
    Slice dummy_slice;
    for (iter.Seek(dummy_slice, k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, iter.key());
         iter.Next()) {
    }
  }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    return art_.estimate_count(start_ikey, end_ikey);
  }

  virtual ~ARTRep() override {}

  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
    ART::Iterator iter_;

   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(ART* art_)
        : iter_(art_) {}

    virtual ~Iterator() override {}

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const override { return iter_.valid(); }

    virtual const char* key() const override { return iter_.entry(); }

    virtual void Next() override { iter_.next(); }

    virtual void Prev() override { iter_.prev(); }

    virtual void Seek(const Slice& user_key,
                      const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.seek(memtable_key);
      } else {
        iter_.seek(user_key);
      }
    }

    // Retreat to the last entry with a key <= target
    virtual void SeekForPrev(const Slice& user_key,
                             const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.seek_for_prev(memtable_key);
      } else {
        iter_.seek_for_prev(user_key);
      }
    }

    virtual void SeekToFirst() override { iter_.seek_to_first(); }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() override { iter_.seek_to_last(); }
  };

  virtual MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    void* mem = arena ? arena->AllocateAligned(sizeof(ARTRep::Iterator))
                      :
                      operator new(sizeof(ARTRep::Iterator));
    return new (mem) ARTRep::Iterator(&art_);
  }
};

MemTableRep* ARTFactory::CreateMemTableRep(const MemTableRep::KeyComparator& compare, 
                                            memtable::MemTableAllocator* allocator,
                                            const common::SliceTransform*) {
  ARTRep *rep = new memtable::ARTRep(compare, allocator);
  rep->init();
  return rep;
}

}  // namespace memtable
}  // namespace xengine