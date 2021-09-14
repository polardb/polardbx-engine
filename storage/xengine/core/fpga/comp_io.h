#ifndef _COMP_IO_H_
#define _COMP_IO_H_

#include <stdint.h>
#include <stddef.h>
#include <inttypes.h>
#include <mutex>
#include <condition_variable>
#include "comp_stats.h"

typedef void(*CBFunc)(size_t	level_type,
                      uint64_t	min_ref_seqno,
                      char	**input_blocks,
                      size_t	**input_blocks_size,
                      size_t	*num_input_blocks,
                      size_t	num_ways,
                      char	*output_blocks,
                      size_t	*out_blocks_size,
                      size_t	*num_output_blocks,
                      void	*group,
                      CompStats	*stats,
#ifdef SELF_TEST
                      int id,
                      std::mutex *mtx,
                      std::condition_variable *cv,
#endif
                      int		rc);

/*
 * Entry of multi-block compaction.
 * @level_type: input level, ranges from 0 to N-1.
 * @min_ref_seqno: snapshot reference sequence number.
 * @input_blocks: input_blocks[i] represents content of the ith way of blocks.
 * @input_blocks_size: input_block_size[i][j] represents byte length of jth input block in ith way.
 * @num_input_blocks: num_input_blocks[i] represents block count in ith way.
 * @num_ways: the number of ways to be merged.
 * @output_blocks: content of output blocks.
 * @out_blocks_size: out_blocks_size[i] represents byte length of ith output block.
 * @num_output_blocks: num_output_blocks[i] represents block count in ith way.
 * @in_buf_sz: total byte length of all input blocks.
 * @out_buf_sz: total byte length of all output blocks.
 * @est_out_buf_sz: estimated byte length of all output blocks.
 * @total_num_in_blocks: total count of input blocks.
 * @est_num_out_blocks: estimated count of output blocks.
 * @stats: compaction job statistics.
*/

struct CompIO {
    size_t	level_type;
    uint64_t	min_ref_seqno;
    char	**input_blocks;
    size_t	**input_blocks_size;
    size_t	*num_input_blocks;
    size_t	num_ways;
    char	*output_blocks;
    size_t	*out_blocks_size;
    size_t *num_output_blocks;
    size_t	in_buf_sz;
    size_t	out_buf_sz;
    size_t	est_out_buf_sz;
    size_t	total_num_in_blocks;
    size_t	est_num_out_blocks;
    void	*group;
    CompStats	*stats;
#ifdef SELF_TEST
    int id;
    std::mutex *mtx;
    std::condition_variable *cv;
#endif
    CBFunc	pfunc;
};

#endif
