#ifndef _COMPSTAT_H_
#define _COMPSTAT_H_
#include <cstdint>
#include <string>
#include <vector>

struct CompStats {
    uint64_t elapsed_micros_;		// the elapsed time in micro of this compaction.
    uint64_t elapsed_micros_fpga_;	// the elapsed time in micro of compaction on FPGA
    uint64_t num_input_records_;	// the number of compaction input records.
    uint64_t num_output_records_;	// the number of compaction output records.

    // this could be a new value or a deletion entry for that key so this field
    // sums up all updated and deleted keys
    uint64_t num_records_replaced_;	 		// number of records being replaced by newer record associated with same key.
    uint64_t num_records_delete_;			// number of delete entry of input records
    uint64_t total_input_raw_key_bytes_;	// the sum of the uncompressed input keys in bytes.
    uint64_t total_input_raw_value_bytes_;	// the sum of the uncompressed input values in bytes.
    std::vector<uint64_t> largest_seq_no_blocks_;	// vector of each output block's largest seq no.
    std::vector<uint64_t> smallest_seq_no_blocks_;	// vector of each output block's samllest seq no.
    std::vector<std::string> first_keys_;		// vector of each output block's first key.
    std::vector<std::string> last_keys_;		// vector of each output block's last key.
    std::vector<size_t> data_size_blocks_;		// vector of each output block's data size.
    std::vector<size_t> key_size_blocks_;		 // vector of each output block's key size.
    std::vector<size_t> value_size_blocks_;		// vector of each output block's value size.
    std::vector<size_t> rows_blocks_;		// vector of each output block's number of rows.
    std::vector<size_t> entry_put_blocks_;	// vector of each output block's number of entry whose type = kTypeValue.
    std::vector<size_t> entry_delete_blocks_;	// vector of each output block's number of entry whose type = kTypeDeletion.
    std::vector<size_t> entry_merge_blocks_;	// vector of each output block's number of entry whose type = kTypeMerge.
    std::vector<size_t> entry_others_blocks_;	// vector of each output block's number of entry whose type = kTypeValueLarge and other
};

#endif
