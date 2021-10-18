#include <cstdlib>
#include "cache/lru_cache.h"
#include "cache/row_cache.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "table/extent_table_factory.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace table;
using namespace cache;
using namespace memtable;
using namespace monitor;
using namespace storage;
using namespace db;
namespace xengine {
namespace cache {

class RowCacheTest : public DBTestBase {
 public:
  RowCacheTest()
    : DBTestBase("/row_cache_test"),
      cache_add_(0),
      cache_hit_(0),
      cache_miss_(0),
      cache_evict_(0) {
  }

  void record_trace_count() {
    cache_add_ = TestGetGlobalCount(CountPoint::ROW_CACHE_ADD);
    cache_hit_ = TestGetGlobalCount(CountPoint::ROW_CACHE_HIT);
    cache_miss_ = TestGetGlobalCount(CountPoint::ROW_CACHE_MISS);
    cache_evict_ = TestGetGlobalCount(CountPoint::ROW_CACHE_EVICT);
  }

//  void set_tmp_schema(int cf) {
    // set tmp schema
//    ColumnFamilyData *cfd = get_column_family_data(cf);
//    assert(cfd);
//    XengineSchema tmp_schema;
//    tmp_schema.schema_version = 1;
//    cfd->mem()->add_schema(tmp_schema);
//  }

  Options get_options() {
    option_config_ = kRowCache;
    Options options = CurrentOptions();
    return options;
  }
 public:
  int64_t cache_add_;
  int64_t cache_hit_;
  int64_t cache_miss_;
  int64_t cache_evict_;
};

TEST_F(RowCacheTest, normal_test) {
  Options options = get_options();
  CreateAndReopenWithCF({"row_cache"}, options);
  const Snapshot* snapshot1 = db_->GetSnapshot();
  ASSERT_OK(Put(1, "key1", "val1"));
  // set tmp schema
//  set_tmp_schema(1);
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact(); // wait flush
  record_trace_count();
  // range[1,1], can read, not write
  Get(1, "key1", snapshot1);
    // not found
  ASSERT_EQ(cache_add_ + 0, TestGetGlobalCount(CountPoint::ROW_CACHE_ADD));
  // range[1,2], can read, can write
  ASSERT_OK(Put(1, "key2", "val2"));
  Get(1, "key1", nullptr);
  ASSERT_EQ(cache_hit_ + 0, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));
  ASSERT_EQ(cache_add_ + 1, TestGetGlobalCount(CountPoint::ROW_CACHE_ADD));
  ASSERT_EQ("val1", Get(1, "key1", nullptr));
  ASSERT_EQ(cache_hit_ + 1, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));

  // test update
  record_trace_count();
  ASSERT_OK(Put(1, "key1", "val1_update"));
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact(); // wait flush
  Get(1, "key1", nullptr);
  ASSERT_EQ(cache_evict_ + 1, TestGetGlobalCount(CountPoint::ROW_CACHE_EVICT));
  ASSERT_EQ(cache_hit_ + 0, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));
  ASSERT_EQ(cache_add_ + 1, TestGetGlobalCount(CountPoint::ROW_CACHE_ADD)); // no imm, true add
  ASSERT_OK(Put(1, "key2", "val2_update"));
  Get(1, "key1", nullptr);
  ASSERT_EQ(cache_add_ + 1, TestGetGlobalCount(CountPoint::ROW_CACHE_ADD));
  ASSERT_EQ("val1_update", Get(1, "key1", nullptr));
  ASSERT_EQ(cache_hit_ + 2, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));
}

TEST_F(RowCacheTest, snapshot_test) {
  Options options = get_options();
  CreateAndReopenWithCF({"row_cache"}, options);
  const Snapshot* snapshot1 = db_->GetSnapshot();
  ASSERT_OK(Put(1, "key1", "val1"));
  ASSERT_OK(Put(1, "key2", "val2"));
  const Snapshot* snapshot2 = db_->GetSnapshot();
  ASSERT_OK(Put(1, "key3", "val3"));

  // set tmp schema
//  set_tmp_schema(1);
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact(); // wait flush
  record_trace_count();

  ASSERT_OK(Put(1, "key4", "val5"));
  Get(1, "key1", nullptr);
  ASSERT_EQ(cache_add_ + 1, TestGetGlobalCount(CountPoint::ROW_CACHE_ADD));
  Get(1, "key1", nullptr);
  ASSERT_EQ(cache_hit_ + 1, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));
  Get(1, "key1", snapshot1); // can't read
  ASSERT_EQ(cache_hit_ + 1, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));
  Get(1, "key1", snapshot2); // can read,
  Get(1, "key2", snapshot2); // can't write
  ASSERT_EQ(cache_hit_ + 2, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));
  Get(1, "key2", snapshot2); // not in cache
  ASSERT_EQ(cache_hit_ + 2, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));
  Get(1, "key2", nullptr); // can write
  Get(1, "key2", snapshot2);
  ASSERT_EQ(cache_hit_ + 3, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));
}

TEST_F(RowCacheTest, lru_cache_test) {
  Options options = get_options();
  // charge = RowHandle+key+value=32
  CreateAndReopenWithCF({"row_cache"}, options);
  ASSERT_OK(Put(1, "a", "a"));
  ASSERT_OK(Put(1, "b", "b"));
  ASSERT_OK(Put(1, "c", "c"));
  ASSERT_OK(Put(1, "d", "d"));
  // set tmp schema
//  set_tmp_schema(1);
  record_trace_count();
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact(); // wait flush
  ASSERT_EQ(cache_evict_ + 0, TestGetGlobalCount(CountPoint::ROW_CACHE_EVICT));
  ASSERT_OK(Put(1, "e", "e"));
  ASSERT_OK(Put(1, "f", "f"));
  Get(1, "a", nullptr);
  ASSERT_EQ(34, options.row_cache.get()->GetUsage());
  Get(1, "b", nullptr);
  ASSERT_EQ(68, options.row_cache.get()->GetUsage());
  Get(1, "c", nullptr);
  ASSERT_EQ(102, options.row_cache.get()->GetUsage());
  Get(1, "d", nullptr);
  ASSERT_EQ(136, options.row_cache.get()->GetUsage());
  ASSERT_EQ(cache_hit_ + 0, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));
  ASSERT_EQ(cache_add_ + 4, TestGetGlobalCount(CountPoint::ROW_CACHE_ADD));
  Get(1, "a", nullptr);
  Get(1, "b", nullptr);
  Get(1, "c", nullptr);
  Get(1, "d", nullptr);
  ASSERT_EQ(cache_hit_ + 4, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));

  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact(); // wait flush
  ASSERT_OK(Put(1, "c", "c1")); // evict
  ASSERT_OK(Put(1, "d", "d1")); // evict
  ASSERT_OK(Put(1, "g", "g"));
  Get(1, "e", nullptr);
  ASSERT_EQ(170, options.row_cache.get()->GetUsage());
  Get(1, "f", nullptr); 
  ASSERT_EQ(204, options.row_cache.get()->GetUsage());
  ASSERT_EQ(cache_add_ + 6, TestGetGlobalCount(CountPoint::ROW_CACHE_ADD));
  Get(1, "e", nullptr);
  Get(1, "a", nullptr); 
  //Get(1, "b", nullptr);
  ASSERT_EQ(cache_hit_ + 6, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));

  record_trace_count();
  ASSERT_OK(Flush(1));
  dbfull()->TEST_wait_for_filter_build();
  dbfull()->TEST_WaitForCompact(); // wait flush
  ASSERT_EQ(cache_evict_ + 2, TestGetGlobalCount(CountPoint::ROW_CACHE_EVICT));
  ASSERT_OK(Put(1, "d", "d1"));
  Get(1, "g", nullptr);
  ASSERT_EQ(cache_add_ + 1, TestGetGlobalCount(CountPoint::ROW_CACHE_ADD));
  Get(1, "g", nullptr);
  ASSERT_EQ(cache_hit_ + 1, TestGetGlobalCount(CountPoint::ROW_CACHE_HIT));
  ASSERT_EQ("d1", Get(1, "d", nullptr));
  ASSERT_EQ("c1", Get(1, "c", nullptr));
}
}
}
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  std::string log_path = test::TmpDir() + "/row_cache_test.log";
  xengine::logger::Logger::get_log().init(log_path.c_str(), xengine::logger::WARN_LEVEL);
  return RUN_ALL_TESTS();
}
