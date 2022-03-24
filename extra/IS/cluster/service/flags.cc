#include <gflags/gflags.h>

DEFINE_string(rpc_members, "", "ip:port for raft rpc, e.g. myb11111.sqa.zmf:1234");
DEFINE_string(server_members, "", "ip:port for client server, e.g. myb11111.sqa.zmf:1234");
DEFINE_string(raft_groups, "", "ip:port for raft group, e.g. 0.0.0.0:1,myb145142.sqa.zmf:10000,myb145142.sqa.zmf:10001,myb145142.sqa.zmf:10002;0.0.0.0:2,e010101083011.zmf:10000,e010101083011.zmf:10001,e010101083011.zmf:10002");
DEFINE_int32(server_id, 1, "the offset in members of this node");

DEFINE_string(data_dir, "data", "local directory which store pesistent information");
DEFINE_string(raftlog_dir, "raftlog", "write-ahead log directory path");

// For RocksDB
DEFINE_bool(data_compress, false, "enable snappy compression on rocksdb storage");
DEFINE_uint64(data_write_buffer_size, 64, "for data, rocksdb write_buffer_size, MB");
DEFINE_bool(raftlog_sync, true, "do sync when raft log is writen");
DEFINE_uint64(max_write_buffer_number, 5, "the maximum number of write buffers that are built up in memory");
DEFINE_uint64(min_write_buffer_number_to_merge, 1, "the minimum number of write buffers that will be merged together before writing to storage");
DEFINE_uint64(max_background_compactions, 6, "maximum number of concurrent background compaction jobs, submitted to the default LOW priority thread pool.");
DEFINE_uint64(max_bytes_for_level_base, 64, "control maximum total data size for base level (level 1), MB");
DEFINE_uint64(target_file_size_base, 64, "target file size for compaction, MB");
DEFINE_uint64(level0_slowdown_writes_trigger, 12, "soft limit on number of level-0 files. We start slowing down writes at this point.");
DEFINE_uint64(level0_stop_writes_trigger, 16, "Maximum number of level-0 files. We stop writes at this point.");
DEFINE_bool(enable_statistics, false, "Rocksdb Statistics provides cumulative stats over time.");
DEFINE_uint64(stats_dump_period_sec, 600, "Dump statistics periodically in information logs. Same as rocksdb's default value (10 min).");
DEFINE_uint64(block_cache_size, 5, "block-cache used to cache uncompressed blocks, G");
DEFINE_uint64(block_size, 64, "Approximate size of user data packed per block, K");
DEFINE_uint64(bloom_filter_bits_per_key, 10, "number of bits per key for bloom filter");
DEFINE_bool(block_based_bloom_filter, false, "false means one sst file one bloom filter, true means evry block has a corresponding bloom filter");

// For client
DEFINE_string(cli_cmd, "", "the command of cli shell");
DEFINE_string(cli_key, "", "key");
DEFINE_string(cli_value, "", "value");
DEFINE_uint64(cli_cas, 0, "cas unique");

// For libeasy
DEFINE_uint64(io_thread_num, 16, "libeasy IO thread number");
DEFINE_uint64(work_thread_num, 16, "libeasy work thread number");
