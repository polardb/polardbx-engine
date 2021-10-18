/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_ROCKSDB_INCLUDE_STATISTICS_H_
#define STORAGE_ROCKSDB_INCLUDE_STATISTICS_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace xengine {
namespace monitor {

/**
 * Keep adding ticker's here.
 *  1. Any ticker should be added before TICKER_ENUM_MAX.
 *  2. Add a readable string in TickersNameMap below for the newly added ticker.
 */
enum Tickers : uint32_t {
  // total block cache misses
  // REQUIRES: BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS +
  //                               BLOCK_CACHE_FILTER_MISS +
  //                               BLOCK_CACHE_DATA_MISS;
  BLOCK_CACHE_MISS = 0,
  // total block cache hit
  // REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
  //                              BLOCK_CACHE_FILTER_HIT +
  //                              BLOCK_CACHE_DATA_HIT;
  BLOCK_CACHE_HIT,
  // # of blocks added to block cache.
  BLOCK_CACHE_ADD,
  // # of failures when adding blocks to block cache.
  BLOCK_CACHE_ADD_FAILURES,
  // # of times cache miss when accessing index block from block cache.
  BLOCK_CACHE_INDEX_MISS,
  // # of times cache hit when accessing index block from block cache.
  BLOCK_CACHE_INDEX_HIT,
  // # of index blocks added to block cache.
  BLOCK_CACHE_INDEX_ADD,
  // # of bytes of index blocks inserted into cache
  BLOCK_CACHE_INDEX_BYTES_INSERT,
  // # of bytes of index block erased from cache
  BLOCK_CACHE_INDEX_BYTES_EVICT,
  // # of times cache miss when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_MISS,
  // # of times cache hit when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_HIT,
  // # of filter blocks added to block cache.
  BLOCK_CACHE_FILTER_ADD,
  // # of bytes of bloom filter blocks inserted into cache
  BLOCK_CACHE_FILTER_BYTES_INSERT,
  // # of bytes of bloom filter block erased from cache
  BLOCK_CACHE_FILTER_BYTES_EVICT,
  // # of times cache miss when accessing data block from block cache.
  BLOCK_CACHE_DATA_MISS,
  // # of times cache hit when accessing data block from block cache.
  BLOCK_CACHE_DATA_HIT,
  // # of data blocks added to block cache.
  BLOCK_CACHE_DATA_ADD,
  // # of bytes of data blocks inserted into cache
  BLOCK_CACHE_DATA_BYTES_INSERT,
  // # of bytes read from cache.
  BLOCK_CACHE_BYTES_READ,
  // # of bytes written into cache.
  BLOCK_CACHE_BYTES_WRITE,

  // # of times bloom filter has avoided file reads.
  BLOOM_FILTER_USEFUL,

  // # persistent cache hit
  PERSISTENT_CACHE_HIT,
  // # persistent cache miss
  PERSISTENT_CACHE_MISS,

  // # total simulation block cache hits
  SIM_BLOCK_CACHE_HIT,
  // # total simulation block cache misses
  SIM_BLOCK_CACHE_MISS,

  // # of memtable hits.
  MEMTABLE_HIT,
  // # of memtable misses.
  MEMTABLE_MISS,

  // # of Get() queries served by L0
  GET_HIT_L0,
  // # of Get() queries served by L1
  GET_HIT_L1,
  // # of Get() queries served by L2 and up
  GET_HIT_L2_AND_UP,

  /**
   * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
   * There are 4 reasons currently.
   */
  COMPACTION_KEY_DROP_NEWER_ENTRY,  // key was written with a newer value.
                                    // Also includes keys dropped for range del.
  COMPACTION_KEY_DROP_OBSOLETE,     // The key is obsolete.
  COMPACTION_KEY_DROP_RANGE_DEL,    // key was covered by a range tombstone.
  COMPACTION_KEY_DROP_USER,  // user compaction function has dropped the key.

  COMPACTION_RANGE_DEL_DROP_OBSOLETE,  // all keys in range were deleted.

  // Number of keys written to the database via the Put and Write call's
  NUMBER_KEYS_WRITTEN,
  // Number of Keys read,
  NUMBER_KEYS_READ,
  // Number keys updated, if inplace update is enabled
  NUMBER_KEYS_UPDATED,
  // The number of uncompressed bytes issued by DB::Put(), DB::Delete(),
  // DB::Merge(), and DB::Write().
  BYTES_WRITTEN,
  // The number of uncompressed bytes read from DB::Get().  It could be
  // either from memtables, cache, or table files.
  // For the number of logical bytes read from DB::MultiGet(),
  // please use NUMBER_MULTIGET_BYTES_READ.
  BYTES_READ,
  // The number of calls to seek/next/prev
  NUMBER_DB_SEEK,
  NUMBER_DB_NEXT,
  NUMBER_DB_PREV,
  // The number of calls to seek/next/prev that returned data
  NUMBER_DB_SEEK_FOUND,
  NUMBER_DB_NEXT_FOUND,
  NUMBER_DB_PREV_FOUND,
  // The number of uncompressed bytes read from an iterator.
  // Includes size of key and value.
  ITER_BYTES_READ,
  NO_FILE_CLOSES,
  NO_FILE_OPENS,
  NO_FILE_ERRORS,
  // DEPRECATED Time system had to wait to do LO-L1 compactions
  STALL_L0_SLOWDOWN_MICROS,
  // DEPRECATED Time system had to wait to move memtable to L1.
  STALL_MEMTABLE_COMPACTION_MICROS,
  // DEPRECATED write throttle because of too many files in L0
  STALL_L0_NUM_FILES_MICROS,
  // Writer has to wait for compaction or flush to finish.
  STALL_MICROS,
  // The wait time for db mutex.
  // Disabled by default. To enable it set stats level to kAll
  DB_MUTEX_WAIT_MICROS,
  RATE_LIMIT_DELAY_MILLIS,
  NO_ITERATORS,  // number of iterators currently open

  // Number of MultiGet calls, keys read, and bytes read
  NUMBER_MULTIGET_CALLS,
  NUMBER_MULTIGET_KEYS_READ,
  NUMBER_MULTIGET_BYTES_READ,

  // Number of deletes records that were not required to be
  // written to storage because key does not exist
  NUMBER_FILTERED_DELETES,
  NUMBER_MERGE_FAILURES,

  // number of times bloom was checked before creating iterator on a
  // file, and the number of times the check was useful in avoiding
  // iterator creation (and thus likely IOPs).
  BLOOM_FILTER_PREFIX_CHECKED,
  BLOOM_FILTER_PREFIX_USEFUL,

  // Number of times we had to reseek inside an iteration to skip
  // over large number of keys with same userkey.
  NUMBER_OF_RESEEKS_IN_ITERATION,

  // Record the number of calls to GetUpadtesSince. Useful to keep track of
  // transaction log iterator refreshes
  GET_UPDATES_SINCE_CALLS,
  BLOCK_CACHE_COMPRESSED_MISS,  // miss in the compressed block cache
  BLOCK_CACHE_COMPRESSED_HIT,   // hit in the compressed block cache
  // Number of blocks added to comopressed block cache
  BLOCK_CACHE_COMPRESSED_ADD,
  // Number of failures when adding blocks to compressed block cache
  BLOCK_CACHE_COMPRESSED_ADD_FAILURES,
  WAL_FILE_SYNCED,  // Number of times WAL sync is done
  WAL_FILE_BYTES,   // Number of bytes written to WAL

  // Writes can be processed by requesting thread or by the thread at the
  // head of the writers queue.
  WRITE_DONE_BY_SELF,
  WRITE_DONE_BY_OTHER,  // Equivalent to writes done for others
  WRITE_TIMEDOUT,       // Number of writes ending up with timed-out.
  WRITE_WITH_WAL,       // Number of Write calls that request WAL
  COMPACT_READ_BYTES,   // Bytes read during compaction
  COMPACT_WRITE_BYTES,  // Bytes written during compaction
  FLUSH_WRITE_BYTES,    // Bytes written during flush

  // Number of table's properties loaded directly from file, without creating
  // table reader object.
  NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
  NUMBER_SUPERVERSION_ACQUIRES,
  NUMBER_SUPERVERSION_RELEASES,
  NUMBER_SUPERVERSION_CLEANUPS,

  // # of compressions/decompressions executed
  NUMBER_BLOCK_COMPRESSED,
  NUMBER_BLOCK_DECOMPRESSED,

  NUMBER_BLOCK_NOT_COMPRESSED,
  MERGE_OPERATION_TOTAL_TIME,
  FILTER_OPERATION_TOTAL_TIME,

  // Row cache.
  ROW_CACHE_HIT,
  ROW_CACHE_MISS,
  ROW_CACHE_ADD,
  ROW_CACHE_EVICT,

  // Read amplification statistics.
  // Read amplification can be calculated using this formula
  // (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
  //
  // REQUIRES: ReadOptions::read_amp_bytes_per_bit to be enabled
  READ_AMP_ESTIMATE_USEFUL_BYTES,  // Estimate of total bytes actually used.
  READ_AMP_TOTAL_READ_BYTES,       // Total size of loaded data blocks.

  // Number of refill intervals where rate limiter's bytes are fully consumed.
  NUMBER_RATE_LIMITER_DRAINS,

  TICKER_ENUM_MAX
};

// The order of items listed in  Tickers should be the same as
// the order listed in TickersNameMap
const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
    {BLOCK_CACHE_MISS, "xengine.block.cache.miss"},
    {BLOCK_CACHE_HIT, "xengine.block.cache.hit"},
    {BLOCK_CACHE_ADD, "xengine.block.cache.add"},
    {BLOCK_CACHE_ADD_FAILURES, "xengine.block.cache.add.failures"},
    {BLOCK_CACHE_INDEX_MISS, "xengine.block.cache.index.miss"},
    {BLOCK_CACHE_INDEX_HIT, "xengine.block.cache.index.hit"},
    {BLOCK_CACHE_INDEX_ADD, "xengine.block.cache.index.add"},
    {BLOCK_CACHE_INDEX_BYTES_INSERT, "xengine.block.cache.index.bytes.insert"},
    {BLOCK_CACHE_INDEX_BYTES_EVICT, "xengine.block.cache.index.bytes.evict"},
    {BLOCK_CACHE_FILTER_MISS, "xengine.block.cache.filter.miss"},
    {BLOCK_CACHE_FILTER_HIT, "xengine.block.cache.filter.hit"},
    {BLOCK_CACHE_FILTER_ADD, "xengine.block.cache.filter.add"},
    {BLOCK_CACHE_FILTER_BYTES_INSERT,
     "xengine.block.cache.filter.bytes.insert"},
    {BLOCK_CACHE_FILTER_BYTES_EVICT, "xengine.block.cache.filter.bytes.evict"},
    {BLOCK_CACHE_DATA_MISS, "xengine.block.cache.data.miss"},
    {BLOCK_CACHE_DATA_HIT, "xengine.block.cache.data.hit"},
    {BLOCK_CACHE_DATA_ADD, "xengine.block.cache.data.add"},
    {BLOCK_CACHE_DATA_BYTES_INSERT, "xengine.block.cache.data.bytes.insert"},
    {BLOCK_CACHE_BYTES_READ, "xengine.block.cache.bytes.read"},
    {BLOCK_CACHE_BYTES_WRITE, "xengine.block.cache.bytes.write"},
    {BLOOM_FILTER_USEFUL, "xengine.bloom.filter.useful"},
    {PERSISTENT_CACHE_HIT, "xengine.persistent.cache.hit"},
    {PERSISTENT_CACHE_MISS, "xengine.persistent.cache.miss"},
    {SIM_BLOCK_CACHE_HIT, "xengine.sim.block.cache.hit"},
    {SIM_BLOCK_CACHE_MISS, "xengine.sim.block.cache.miss"},
    {MEMTABLE_HIT, "xengine.memtable.hit"},
    {MEMTABLE_MISS, "xengine.memtable.miss"},
    {GET_HIT_L0, "xengine.l0.hit"},
    {GET_HIT_L1, "xengine.l1.hit"},
    {GET_HIT_L2_AND_UP, "xengine.l2andup.hit"},
    {COMPACTION_KEY_DROP_NEWER_ENTRY, "xengine.compaction.key.drop.new"},
    {COMPACTION_KEY_DROP_OBSOLETE, "xengine.compaction.key.drop.obsolete"},
    {COMPACTION_KEY_DROP_RANGE_DEL, "xengine.compaction.key.drop.range_del"},
    {COMPACTION_KEY_DROP_USER, "xengine.compaction.key.drop.user"},
    {COMPACTION_RANGE_DEL_DROP_OBSOLETE,
     "xengine.compaction.range_del.drop.obsolete"},
    {NUMBER_KEYS_WRITTEN, "xengine.number.keys.written"},
    {NUMBER_KEYS_READ, "xengine.number.keys.read"},
    {NUMBER_KEYS_UPDATED, "xengine.number.keys.updated"},
    {BYTES_WRITTEN, "xengine.bytes.written"},
    {BYTES_READ, "xengine.bytes.read"},
    {NUMBER_DB_SEEK, "xengine.number.db.seek"},
    {NUMBER_DB_NEXT, "xengine.number.db.next"},
    {NUMBER_DB_PREV, "xengine.number.db.prev"},
    {NUMBER_DB_SEEK_FOUND, "xengine.number.db.seek.found"},
    {NUMBER_DB_NEXT_FOUND, "xengine.number.db.next.found"},
    {NUMBER_DB_PREV_FOUND, "xengine.number.db.prev.found"},
    {ITER_BYTES_READ, "xengine.db.iter.bytes.read"},
    {NO_FILE_CLOSES, "xengine.no.file.closes"},
    {NO_FILE_OPENS, "xengine.no.file.opens"},
    {NO_FILE_ERRORS, "xengine.no.file.errors"},
    {STALL_L0_SLOWDOWN_MICROS, "xengine.l0.slowdown.micros"},
    {STALL_MEMTABLE_COMPACTION_MICROS, "xengine.memtable.compaction.micros"},
    {STALL_L0_NUM_FILES_MICROS, "xengine.l0.num.files.stall.micros"},
    {STALL_MICROS, "xengine.stall.micros"},
    {DB_MUTEX_WAIT_MICROS, "xengine.db.mutex.wait.micros"},
    {RATE_LIMIT_DELAY_MILLIS, "xengine.rate.limit.delay.millis"},
    {NO_ITERATORS, "xengine.num.iterators"},
    {NUMBER_MULTIGET_CALLS, "xengine.number.multiget.get"},
    {NUMBER_MULTIGET_KEYS_READ, "xengine.number.multiget.keys.read"},
    {NUMBER_MULTIGET_BYTES_READ, "xengine.number.multiget.bytes.read"},
    {NUMBER_FILTERED_DELETES, "xengine.number.deletes.filtered"},
    {NUMBER_MERGE_FAILURES, "xengine.number.merge.failures"},
    {BLOOM_FILTER_PREFIX_CHECKED, "xengine.bloom.filter.prefix.checked"},
    {BLOOM_FILTER_PREFIX_USEFUL, "xengine.bloom.filter.prefix.useful"},
    {NUMBER_OF_RESEEKS_IN_ITERATION, "xengine.number.reseeks.iteration"},
    {GET_UPDATES_SINCE_CALLS, "xengine.getupdatessince.calls"},
    {BLOCK_CACHE_COMPRESSED_MISS, "xengine.block.cachecompressed.miss"},
    {BLOCK_CACHE_COMPRESSED_HIT, "xengine.block.cachecompressed.hit"},
    {BLOCK_CACHE_COMPRESSED_ADD, "xengine.block.cachecompressed.add"},
    {BLOCK_CACHE_COMPRESSED_ADD_FAILURES,
     "xengine.block.cachecompressed.add.failures"},
    {WAL_FILE_SYNCED, "xengine.wal.synced"},
    {WAL_FILE_BYTES, "xengine.wal.bytes"},
    {WRITE_DONE_BY_SELF, "xengine.write.self"},
    {WRITE_DONE_BY_OTHER, "xengine.write.other"},
    {WRITE_TIMEDOUT, "xengine.write.timeout"},
    {WRITE_WITH_WAL, "xengine.write.wal"},
    {COMPACT_READ_BYTES, "xengine.compact.read.bytes"},
    {COMPACT_WRITE_BYTES, "xengine.compact.write.bytes"},
    {FLUSH_WRITE_BYTES, "xengine.flush.write.bytes"},
    {NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
     "xengine.number.direct.load.table.properties"},
    {NUMBER_SUPERVERSION_ACQUIRES, "xengine.number.superversion_acquires"},
    {NUMBER_SUPERVERSION_RELEASES, "xengine.number.superversion_releases"},
    {NUMBER_SUPERVERSION_CLEANUPS, "xengine.number.superversion_cleanups"},
    {NUMBER_BLOCK_COMPRESSED, "xengine.number.block.compressed"},
    {NUMBER_BLOCK_DECOMPRESSED, "xengine.number.block.decompressed"},
    {NUMBER_BLOCK_NOT_COMPRESSED, "xengine.number.block.not_compressed"},
    {MERGE_OPERATION_TOTAL_TIME, "xengine.merge.operation.time.nanos"},
    {FILTER_OPERATION_TOTAL_TIME, "xengine.filter.operation.time.nanos"},
    {ROW_CACHE_HIT, "xengine.row.cache.hit"},
    {ROW_CACHE_MISS, "xengine.row.cache.miss"},
    {ROW_CACHE_EVICT, "xengine.row.cache.evict"},
    {READ_AMP_ESTIMATE_USEFUL_BYTES, "xengine.read.amp.estimate.useful.bytes"},
    {READ_AMP_TOTAL_READ_BYTES, "xengine.read.amp.total.read.bytes"},
    {NUMBER_RATE_LIMITER_DRAINS, "xengine.number.rate_limiter.drains"},
};

/**
 * Keep adding histogram's here.
 * Any histogram whould have value less than HISTOGRAM_ENUM_MAX
 * Add a new Histogram by assigning it the current value of HISTOGRAM_ENUM_MAX
 * Add a string representation in HistogramsNameMap below
 * And increment HISTOGRAM_ENUM_MAX
 */
enum Histograms : uint32_t {
  DB_GET = 0,
  DB_WRITE,
  COMPACTION_TIME,
  SUBCOMPACTION_SETUP_TIME,
  TABLE_SYNC_MICROS,
  COMPACTION_OUTFILE_SYNC_MICROS,
  WAL_FILE_SYNC_MICROS,
  MANIFEST_FILE_SYNC_MICROS,
  // TIME SPENT IN IO DURING TABLE OPEN
  TABLE_OPEN_IO_MICROS,
  DB_MULTIGET,
  READ_BLOCK_COMPACTION_MICROS,
  READ_BLOCK_GET_MICROS,
  WRITE_RAW_BLOCK_MICROS,
  STALL_L0_SLOWDOWN_COUNT,
  STALL_MEMTABLE_COMPACTION_COUNT,
  STALL_L0_NUM_FILES_COUNT,
  HARD_RATE_LIMIT_DELAY_COUNT,
  SOFT_RATE_LIMIT_DELAY_COUNT,
  NUM_FILES_IN_SINGLE_COMPACTION,
  DB_SEEK,
  WRITE_STALL,
  SST_READ_MICROS,
  // The number of subcompactions actually scheduled during a compaction
  NUM_SUBCOMPACTIONS_SCHEDULED,
  // Value size distribution in each operation
  BYTES_PER_READ,
  BYTES_PER_WRITE,
  BYTES_PER_MULTIGET,

  // number of bytes compressed/decompressed
  // number of bytes is when uncompressed; i.e. before/after respectively
  BYTES_COMPRESSED,
  BYTES_DECOMPRESSED,
  COMPRESSION_TIMES_NANOS,
  DECOMPRESSION_TIMES_NANOS,
  ENTRY_PER_LOG_COPY,
  BYTES_PER_LOG_COPY,
  TIME_PER_LOG_COPY,
  BYTES_PER_LOG_WRITE,
  TIME_PER_LOG_WRITE,
  PIPLINE_GROUP_SIZE,
  PIPLINE_LOOP_COUNT,
  PIPLINE_TRY_LOG_COPY_COUNT,
  PIPLINE_TRY_LOG_WRITE_COUNT,
  PIPLINE_CONCURRENT_RUNNING_WORKER_THERADS,
  PIPLINE_LOG_QUEUE_LENGTH,
  PIPLINE_MEM_QUEUE_LENGTH,
  PIPLINE_COMMIT_QUEUE_LENGTH,
  DEMO_WATCH_TIME_NANOS,
//COMPACTION statisic
  COMPACTION_FPGA_WAIT_PROCESS_TIME,
  COMPACTION_FPGA_PROCESS_TIME,
  COMPACTION_FPGA_WAIT_DONE_TIME,
  COMPACTION_FPGA_DONE_TIME,
  COMPACTION_MINOR_WAIT_PROCESS_TIME,
  COMPACTION_MINOR_PROCESS_TIME,
  COMPACTION_MINOR_WAIT_DONE_TIME,
  COMPACTION_MINOR_DONE_TIME,
  COMPACTION_MINOR_WAY_NUM,
  COMPACTION_MINOR_BLOCKS_NUM,
  COMPACTION_MINOR_BLOCKS_BYTES,
  HISTOGRAM_ENUM_MAX,  // TODO(ldemailly): enforce HistogramsNameMap match
};

const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap = {
    {DB_GET, "xengine.db.get.micros"},
    {DB_WRITE, "xengine.db.write.micros"},
    {COMPACTION_TIME, "xengine.compaction.times.micros"},
    {SUBCOMPACTION_SETUP_TIME, "xengine.compactionjob.setup.times.micros"},
    {TABLE_SYNC_MICROS, "xengine.table.sync.micros"},
    {COMPACTION_OUTFILE_SYNC_MICROS, "xengine.compaction.outfile.sync.micros"},
    {WAL_FILE_SYNC_MICROS, "xengine.wal.file.sync.micros"},
    {MANIFEST_FILE_SYNC_MICROS, "xengine.manifest.file.sync.micros"},
    {TABLE_OPEN_IO_MICROS, "xengine.table.open.io.micros"},
    {DB_MULTIGET, "xengine.db.multiget.micros"},
    {READ_BLOCK_COMPACTION_MICROS, "xengine.read.block.compaction.micros"},
    {READ_BLOCK_GET_MICROS, "xengine.read.block.get.micros"},
    {WRITE_RAW_BLOCK_MICROS, "xengine.write.raw.block.micros"},
    {STALL_L0_SLOWDOWN_COUNT, "xengine.l0.slowdown.count"},
    {STALL_MEMTABLE_COMPACTION_COUNT, "xengine.memtable.compaction.count"},
    {STALL_L0_NUM_FILES_COUNT, "xengine.num.files.stall.count"},
    {HARD_RATE_LIMIT_DELAY_COUNT, "xengine.hard.rate.limit.delay.count"},
    {SOFT_RATE_LIMIT_DELAY_COUNT, "xengine.soft.rate.limit.delay.count"},
    {NUM_FILES_IN_SINGLE_COMPACTION, "xengine.numextents.in.singlecompaction"},
    {DB_SEEK, "xengine.db.seek.micros"},
    {WRITE_STALL, "xengine.db.write.stall"},
    {SST_READ_MICROS, "xengine.sst.read.micros"},
    {NUM_SUBCOMPACTIONS_SCHEDULED, "xengine.num.compactiontasks.scheduled"},
    {BYTES_PER_READ, "xengine.bytes.per.read"},
    {BYTES_PER_WRITE, "xengine.bytes.per.write"},
    {BYTES_PER_MULTIGET, "xengine.bytes.per.multiget"},
    {BYTES_COMPRESSED, "xengine.bytes.compressed"},
    {BYTES_DECOMPRESSED, "xengine.bytes.decompressed"},
    {COMPRESSION_TIMES_NANOS, "xengine.compression.times.nanos"},
    {DECOMPRESSION_TIMES_NANOS, "xengine.decompression.times.nanos"},
    {ENTRY_PER_LOG_COPY, "xengine.log.per.copy.num"},
    {BYTES_PER_LOG_COPY, "xengine.log.per.copy.bytes"},
    {TIME_PER_LOG_COPY, "xengine.log.per.copy.time.nanos"},
    {BYTES_PER_LOG_WRITE, "xengine.log.per.write.bytes"},
    {TIME_PER_LOG_WRITE, "xengine.log.per.write.time.nanos"},
    {PIPLINE_GROUP_SIZE, "xengine.pipline.group.size"},
    {PIPLINE_LOOP_COUNT, "xengine.pipline.loop.count"},
    {PIPLINE_TRY_LOG_COPY_COUNT, "xengine.pipline.loop.try.log.copy.count"},
    {PIPLINE_TRY_LOG_WRITE_COUNT, "xengine.pipline.loop.try.log.write.count"},
    {PIPLINE_CONCURRENT_RUNNING_WORKER_THERADS,
     "xengine.pipline.concurrent.running.worker.threads"},
    {PIPLINE_LOG_QUEUE_LENGTH, "xengine.pipline.log.quque.length"},
    {PIPLINE_MEM_QUEUE_LENGTH, "xengine.pipline.mem.queue.length"},
    {PIPLINE_COMMIT_QUEUE_LENGTH, "xengine.pipline.commit.queue.length"},
    {DEMO_WATCH_TIME_NANOS, "xengine.demo.time.nanos"},
    {COMPACTION_FPGA_WAIT_PROCESS_TIME, "xengine.compaction.fpga.wait.process.time"}, 
    {COMPACTION_FPGA_PROCESS_TIME, "xengine.compaction.fpga.process.time"},
    {COMPACTION_FPGA_WAIT_DONE_TIME, "xengine.compaction.fpga.wait.done.time"},
    {COMPACTION_FPGA_DONE_TIME, "xengine.compaction.fpga.done.time"},
    {COMPACTION_MINOR_WAIT_PROCESS_TIME,"xengine.compaction.minor.wait.process.time"}, 
    {COMPACTION_MINOR_PROCESS_TIME, "xengine.compaction.minor.process.time"},
    {COMPACTION_MINOR_WAIT_DONE_TIME,"xengine.compaction.minor.wait.done.time"},
    {COMPACTION_MINOR_DONE_TIME, "xengine.compaction.minor.done.time"},
    {COMPACTION_MINOR_WAY_NUM, "xengine.compaction.minor.way.num"},
    {COMPACTION_MINOR_BLOCKS_NUM, "xengine.compaction.minor.blocks.num"},
    {COMPACTION_MINOR_BLOCKS_BYTES, "xengine.compaction.minor.blocks.bytes"},
};

struct HistogramData {
  double median;
  double percentile95;
  double percentile99;
  double average;
  double standard_deviation;
  // zero-initialize new members since old Statistics::histogramData()
  // implementations won't write them.
  double max = 0.0;
};

enum StatsLevel {
  // Collect all stats except time inside mutex lock AND time spent on
  // compression.
  kExceptDetailedTimers,
  // Collect all stats except the counters requiring to get time inside the
  // mutex lock.
  kExceptTimeForMutex,
  // Collect all stats, including measuring duration of mutex operations.
  // If getting time is expensive on the platform to run, it can
  // reduce scalability to more threads, especially for writes.
  All,
};

// Analyze the performance of a db
class Statistics {
 public:
  virtual ~Statistics() {}

  virtual uint64_t getTickerCount(uint32_t tickerType) const = 0;
  virtual void histogramData(uint32_t type,
                             HistogramData* const data) const = 0;
  virtual std::string getHistogramString(uint32_t type) const { return ""; }
  virtual void recordTick(uint32_t tickerType, uint64_t count = 0) = 0;
  virtual void setTickerCount(uint32_t tickerType, uint64_t count) = 0;
  virtual uint64_t getAndResetTickerCount(uint32_t tickerType) = 0;
  virtual void measureTime(uint32_t histogramType, uint64_t time) = 0;

  // String representation of the statistic object.
  virtual std::string ToString() const {
    // Do nothing by default
    return std::string("ToString(): not implemented");
  }

  // Override this function to disable particular histogram collection
  virtual bool HistEnabledForType(uint32_t type) const {
    return type < HISTOGRAM_ENUM_MAX;
  }

  StatsLevel stats_level_ = kExceptDetailedTimers;
};

// Create a concrete DBStatistics object
std::shared_ptr<Statistics> CreateDBStatistics();

}  // namespace common
}  // namespace xengine
#endif  // STORAGE_ROCKSDB_INCLUDE_STATISTICS_H_
