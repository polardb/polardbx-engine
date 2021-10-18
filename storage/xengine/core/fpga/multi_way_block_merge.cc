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
#include <ctime>
#include <fstream>
#include <iostream>
#include "xengine/comparator.h"
#include "db/dbformat.h"
#include "multi_way_block_merge.h"

namespace xengine{
namespace fpga{
void MultiWayBlockMerge::MultiBlockMerge() {

  assert(num_ways_ > 1);

  //auto t = clock();

  GetInputBlocksStats();
  // CheckKeyRange(input_blocks, input_blocks_size, num_input_blocks, num_ways);
  const Comparator *cmp = BytewiseComparator();
#if 0
/*db提供的合并代码*/
  std::vector<std::string> input_buffer_way;
  for(size_t i=0;i<num_ways;++i)
  {
    BlockBuilder builder(RESTART_INTERVAL);

    size_t num_blocks = *(num_input_blocks + i);
    auto block_ptr = *(input_blocks + i);
    // each data block way can be formed as a larger "block"
    // redundant decode/encode, not efficient
    for(size_t j=0;j<num_blocks;++j)
    {
      size_t size = *(*(input_blocks_size + i) + j);
      BlockContents contents;
      contents.data = Slice(block_ptr, size);
      Block reader(std::move(contents), kDisableGlobalSequenceNumber);
      BlockIter* iter;
      reader.NewIterator(cmp, iter);
      for (iter->SeekToFirst(); iter->Valid(); iter->Next())
      {
        builder.Add(iter->key(), iter->value());
      }

      block_ptr += size;
    }
    auto raw_block = builder.Finish();
    std::string buffer(raw_block.data(),raw_block.size());
    input_buffer_way.emplace_back(buffer);
  }

  std::string res = input_buffer_way.front();

  for(size_t i =1;i<num_ways;++i)
  {
    std::string res_ = input_buffer_way.at(i);
    Slice block_2(res_);
    Slice cur(res);
    res = SingleBlockMerge(min_ref_seq_no, &cur, &block_2, stats);
  }
  PutRes(res);
#else
/*适合FPGA逻辑的多路合并*/
#if 1
  Slice **blocks[num_ways_];
  for(size_t i = 0; i < num_ways_; ++i)
  {
    char *block_ptr = input_blocks_[i];
    Slice **block = new Slice*[num_input_blocks_[i]];
    if(NULL == block)
    {
      abort();
    }else
    {
      blocks[i] = block;
    }

    for(size_t j = 0; j < num_input_blocks_[i]; ++j)
    {
      size_t size = input_blocks_size_[i][j];
      blocks[i][j] = new Slice(block_ptr, size);
      block_ptr += size;
    }
  }
  std::string res = MultiBlockMergeKWay(blocks);
  for(size_t i = 0; i < num_ways_; ++i)
  {
    for (size_t j = 0; j < num_input_blocks_[i]; ++j){
      delete blocks[i][j];
    }
    delete[] blocks[i];
  }
#endif
  //assert(VerifyResEquality(res));
#endif
  // break down into data blocks
  // record each output block first key and last key
  // for CPU index block construction

#if 1
  BlockContents contents;
  contents.data = Slice(res.c_str(), res.size());
  Block reader(std::move(contents), kDisableGlobalSequenceNumber);
  BlockIter *iter = new BlockIter();
  // BlockIter* iter = reader.NewIterator(cmp);
  reader.NewIterator(cmp, iter);

  BlockBuilder builder(RESTART_INTERVAL);

  std::string out_buf;
  num_output_blocks_ = 0;

  std::string last_key;
  // std::string last_keys_str;
  // std::string first_keys_str;

  size_t data_size = 0;
  size_t key_size = 0;
  size_t value_size = 0;
  size_t rows = 0;
  size_t entry_put = 0;
  size_t entry_others = 0;
  size_t entry_delete = 0;
  size_t entry_single_delete = 0;
  size_t entry_merge = 0;
  uint64_t largest_seq_no = 0;
  uint64_t smallest_seq_no = kMaxSequenceNumber;


  iter->SeekToFirst();
  // first_keys_str.append(std::string(iter->key().data(),iter->key().size()));
  // first_keys_length_[num_output_blocks_] = iter->key().size();

  multi_way_block_merge_stats_.first_keys_.emplace_back(std::string(iter->key().data(), iter->key().size()));
  for(iter->SeekToFirst();iter->Valid();iter->Next())
  {
    multi_way_block_merge_stats_.num_output_records_++;

    if(builder.EstimateSizeAfterKV(iter->key().size(),iter->value().size()) > SIZE_PER_BLOCK)
    {
      // exceeds size of block
      auto raw_block = builder.Finish();
      out_buf.append(std::string(raw_block.data(),raw_block.size()));
      output_blocks_size_[num_output_blocks_] = raw_block.size();

      // last_keys_str.append(last_key);
      // last_keys_length_[num_output_blocks_] = last_key.size();
      multi_way_block_merge_stats_.last_keys_.emplace_back(last_key);

      multi_way_block_merge_stats_.data_size_blocks_.emplace_back(data_size);
      multi_way_block_merge_stats_.key_size_blocks_.emplace_back(key_size);
      multi_way_block_merge_stats_.value_size_blocks_.emplace_back(value_size);
      multi_way_block_merge_stats_.largest_seq_no_blocks_.emplace_back(largest_seq_no);
      multi_way_block_merge_stats_.smallest_seq_no_blocks_.emplace_back(smallest_seq_no);
      multi_way_block_merge_stats_.entry_put_blocks_.emplace_back(entry_put);
      multi_way_block_merge_stats_.entry_delete_blocks_.emplace_back(entry_delete);
      multi_way_block_merge_stats_.entry_merge_blocks_.emplace_back(entry_merge);
      multi_way_block_merge_stats_.entry_others_blocks_.emplace_back(entry_others);
      multi_way_block_merge_stats_.rows_blocks_.emplace_back(rows);

      num_output_blocks_ ++;
      // first_keys_str.append(std::string(iter->key().data(),iter->key().size()));
      // first_keys_length_[num_output_blocks_] = iter->key().size();
      multi_way_block_merge_stats_.first_keys_.emplace_back(std::string(iter->key().data(), iter->key().size()));

      builder.Reset();

      // reset block stats
      data_size = 0;
      key_size = 0;
      value_size = 0;
      rows = 0;
      entry_put = 0;
      entry_delete = 0;
      entry_single_delete = 0;
      entry_merge = 0;
      entry_others = 0;
      smallest_seq_no = kMaxSequenceNumber;
      largest_seq_no = 0;
    }

    builder.Add(iter->key(), iter->value());
    key_size += iter->key().size();
    value_size += iter->value().size();
    data_size = data_size + iter->key().size() + iter->value().size();
    rows += 1;

    // seq and type
    ParsedInternalKey ikey("", 0, kTypeValue);
    ParseInternalKey(iter->key(), &ikey);
    smallest_seq_no = std::min(smallest_seq_no, ikey.sequence);
    largest_seq_no = std::max(largest_seq_no, ikey.sequence);

    switch(ikey.type){
      case kTypeValue:
        entry_put++;
        break;
      case kTypeValueLarge:
        entry_others++;
        break;
      case kTypeDeletion:
        entry_delete++;
        break;
      case kTypeSingleDeletion:
        entry_single_delete++;
        break;
      case kTypeMerge:
        entry_merge++;
        break;
      default:
        entry_others++;
        break;
    }

    last_key = std::string(iter->key().data(), iter->key().size());
  }
  // last_keys_str.append(std::string(last_key.data(),last_key.size()));
  // multi_way_block_merge_stats_.last_keys_.emplace_back(last_key);

  if(!builder.empty())
  {
    auto raw_block = builder.Finish();
    out_buf.append(std::string(raw_block.data(),raw_block.size()));
    //builder.Reset();

    output_blocks_size_[num_output_blocks_] = raw_block.size();
    num_output_blocks_++;

    multi_way_block_merge_stats_.last_keys_.emplace_back(last_key);
    multi_way_block_merge_stats_.data_size_blocks_.emplace_back(data_size);
    multi_way_block_merge_stats_.key_size_blocks_.emplace_back(key_size);
    multi_way_block_merge_stats_.value_size_blocks_.emplace_back(value_size);
    multi_way_block_merge_stats_.largest_seq_no_blocks_.emplace_back(largest_seq_no);
    multi_way_block_merge_stats_.smallest_seq_no_blocks_.emplace_back(smallest_seq_no);
    multi_way_block_merge_stats_.entry_put_blocks_.emplace_back(entry_put);
    multi_way_block_merge_stats_.entry_delete_blocks_.emplace_back(entry_delete);
    multi_way_block_merge_stats_.entry_merge_blocks_.emplace_back(entry_merge);
    multi_way_block_merge_stats_.entry_others_blocks_.emplace_back(entry_others);
    multi_way_block_merge_stats_.rows_blocks_.emplace_back(rows);

  }
  delete iter;

  out_buf.copy(output_blocks_, out_buf.size());
  // first_keys_str.copy(first_keys_, first_keys_str.size());
  // last_keys_str.copy(last_keys_, last_keys_str.size());

  //multi_way_block_merge_stats_.elapsed_micros_ = clock() - t;
#endif
}

struct Record {
  std::string key;
  std::string value;
  ParsedInternalKey  pkey;
};
/* 功能：
 * 合并多路数据;
 * 输入：
 * min_ref_seq_no，表示快照sequence no.;
 * blocks为二维数组，block[i][j]对应第i路，第j个KV块指针;
 * num_input_blocks为一维数组，num_input_blocks[i]表示第i路的KV块数量;
 * 输出：
 * 多路合并结果;
 */
#if 1
std::string MultiWayBlockMerge::MultiBlockMergeKWay(Slice ***blocks)
{
  const Comparator *cmp = BytewiseComparator();
  BlockIter *iters[num_ways_];
  std::vector<std::pair<std::pair<int, int>, BlockIter*>> candidate_iterators;

/*为每一路第一个KV块生成迭代器，并添加到候选迭代器数组中*/
  for(size_t i = 0; i < num_ways_; ++i)
  {
    iters[i] = NULL;
    BlockContents contents;
    contents.data = Slice(blocks[i][0]->data(), blocks[i][0]->size());
    Block reader(std::move(contents), kDisableGlobalSequenceNumber);
    BlockIter* iterator = new BlockIter();
    reader.NewIterator(cmp, iterator); /*为第way_id路，第block_id KV块生成迭代器*/
    iters[i] =  iterator;
    iters[i]->SeekToFirst();
    assert(iters[i]->Valid());
    candidate_iterators.emplace_back(std::make_pair(std::make_pair(i, 0), iters[i]));
  }

/*循环遍历每一路的每一块*/
  BlockBuilder builder(RESTART_INTERVAL);
  while(1)
  {
    /*没有候选KV块*/
    if(candidate_iterators.empty())
      break;
    ParsedInternalKey pkeys[candidate_iterators.size()];
    for(size_t i = 0; i < candidate_iterators.size(); ++i)
    {
      /*获取候选KV块的迭代器*/
      BlockIter *iterator = candidate_iterators[i].second;
      ParseInternalKey(iterator->key(), &pkeys[i]);
    }
    std::set<size_t> index_set;
    index_set.insert(0);
    /*找到Key最小的记录所属KV块在候选KV块中的索引号*/
    size_t index_1;
    size_t index_2 = 1;
    while(index_2 < candidate_iterators.size())
    {
      index_1 = *index_set.begin();
      assert(pkeys[index_1].user_key != pkeys[index_2].user_key 
             || pkeys[index_1].sequence != pkeys[index_2].sequence);
      int compare = pkeys[index_1].user_key.compare(pkeys[index_2].user_key);
      if(compare > 0){
        index_set.clear();
        index_set.insert(index_2);
      }else if(0 == compare){
        index_set.insert(index_2);
      }
      index_2++;
    }
    
    // order index set by decreasing seq
    std::vector<size_t> index_vec;
    //(1) collect records with the same user_key and different seq
    //Slice target_user_key = pkeys[*index_set.begin()].user_key;
    std::string target_user_key = pkeys[*index_set.begin()].user_key.ToString();
    //fprintf(stderr, "\nnew loop set target_key=%s\n",
    //                target_user_key.data());
    std::vector<Record> record_list;
    
    //TODO use slice to avoid copy 
    std::string tmp_key, tmp_value;
    ParsedInternalKey tmp_pkey("", 0, kTypeValue);
    for(auto it = index_set.begin(); it != index_set.end(); ++it) {
      size_t index = *it;
      BlockIter* block_iter = candidate_iterators[index].second;
      while (true) { 
        std::pair<int, int> way_block = candidate_iterators[index].first;
        int way_id = way_block.first;
        size_t block_id = way_block.second + 1;
        if (!block_iter->Valid() && 
            block_id < num_input_blocks_[way_id]) {//get next blocks
            BlockContents contents;
            contents.data = Slice(blocks[way_id][block_id]->data(), blocks[way_id][block_id]->size());
            Block reader(std::move(contents), kDisableGlobalSequenceNumber);
            BlockIter *iterator = new BlockIter();
            reader.NewIterator(cmp, iterator); /*为第way_id路，第block_id KV块生成迭代器*/
            iterator->SeekToFirst();
            assert(iterator->Valid());
            delete block_iter;
            block_iter = iterator;
            iters[way_id] = block_iter;
            way_block = std::make_pair(way_id, block_id);
            std::pair<std::pair<int, int>, BlockIter*> candidate = std::make_pair(way_block, iterator);

            candidate_iterators[index] = candidate; 
        }
        //still invalid exit this way
        if (!block_iter->Valid()) {
          break;
        }

        ParsedInternalKey ikey("", 0, kTypeValue);
        ParseInternalKey(block_iter->key(), &ikey);
        if (ikey.user_key.compare(target_user_key) != 0) {
          //encounter new user_key, exit this way
          assert(ikey.user_key.compare(target_user_key) > 0);
          break;
        }
        //we should always sucdeed
        bool res = GetNextKV(block_iter, tmp_key, tmp_value);
        assert(res == true);
        ParseInternalKey(tmp_key, &tmp_pkey);
        //collect all valid key with the same user_key;
        if (tmp_pkey.user_key.compare(target_user_key) == 0) {
          Record rt;
          rt.key = tmp_key;
          rt.pkey = tmp_pkey;
          rt.value = tmp_value;
          //fprintf(stderr, "add user_key=%s target_key=%s seq=%ld\n", 
          //                 tmp_pkey.user_key.data(),
          //                 target_user_key.data(),
          //                 tmp_pkey.sequence);
          record_list.push_back(rt);
        } else {
          //fprintf(stderr, "skip user_key=%s target_key=%s seq=%ld\n", 
          //                 tmp_pkey.user_key.data(),
          //                 target_user_key.data(),
          //                 tmp_pkey.sequence);
          break;
        }
      }
    }

    //(2) sort the list by seq order des
    std::sort(record_list.begin(), 
              record_list.end(), 
              [](Record a, Record b) { 
                assert(a.pkey.user_key == b.pkey.user_key); 
                return a.pkey.sequence > b.pkey.sequence;
              });

    std::string keys[record_list.size()];
    std::string values[record_list.size()];
    ParsedInternalKey parsed_keys[record_list.size()];
    index_vec.clear();
    for (size_t index = 0; index < record_list.size(); ++index) {
      index_vec.push_back(index);
      keys[index] = record_list[index].key;
      values[index] = record_list[index].value;
      parsed_keys[index] = record_list[index].pkey;
      if (index > 0) {
        assert(parsed_keys[index].sequence < parsed_keys[index - 1].sequence);
        assert(parsed_keys[index].user_key.compare(parsed_keys[index - 1].user_key) == 0);
      }
    } 

    bool left_part= false;
    PartBlockMerge(index_vec, left_part, parsed_keys, keys, values, &builder);
    left_part = true;
    PartBlockMerge(index_vec, left_part, parsed_keys, keys, values, &builder);
   
    std::vector<std::pair<std::pair<int,int>, BlockIter*>> new_candidate_iterators;

    /*生成候选KV块迭代器*/
    for(auto it = candidate_iterators.begin(); it != candidate_iterators.end(); ++it)
    {
      std::pair<int, int> way_block = (*it).first;
      int way_id = way_block.first;
      size_t block_id = way_block.second + 1;
      BlockIter *cur_iterator = (*it).second;
      if(cur_iterator->Valid()) /*当前KV块还未完成迭代*/
      {
        new_candidate_iterators.emplace_back(*it); /*将第way_id路，第block_id-1个KV块加入候选迭代器数组*/
      } else if(block_id < num_input_blocks_[(*it).first.first]) /*第way_id路还有未处理的KV块*/
      {
        delete (*it).second;
        (*it).second = nullptr;
        BlockContents contents;
        contents.data = Slice(blocks[way_id][block_id]->data(), blocks[way_id][block_id]->size());
        Block reader(std::move(contents), kDisableGlobalSequenceNumber);
        BlockIter *iterator = new BlockIter();
        reader.NewIterator(cmp, iterator); /*为第way_id路，第block_id KV块生成迭代器*/
        iterator->SeekToFirst();
        assert(iterator->Valid());
        way_block = std::make_pair(way_id, block_id);
        iters[way_id] = iterator;
        std::pair<std::pair<int, int>, BlockIter*> candidate = std::make_pair(way_block, iterator);
        new_candidate_iterators.emplace_back(candidate); /*将新的KV块迭代器加入候选迭代器数组*/
      }
    }
    candidate_iterators = new_candidate_iterators; /*用新生成的迭代器数组替换旧的迭代器数组*/
  }

  for (size_t i = 0; i < num_ways_; ++i) {
    delete iters[i];
  }
  //for(auto p : candidate_iterators){
  //    delete p.second;
  //}

  auto raw_block = builder.Finish();
  std::string ret(raw_block.data(), raw_block.size());
  return ret;
}

/*
 * left_part=true，处理sequenc no.小于或等于快照sequence no.的KV记录；
 * left_part=false，处理sequence no.大于快照sequenc no.的KV记录
 */
void MultiWayBlockMerge::PartBlockMerge(
    std::vector<size_t> index_vec,
    bool left_part,
    ParsedInternalKey *pkeys,
    std::string *keys,
    std::string *values,
    BlockBuilder *builder)
{
  std::vector<size_t> part_index_vec;
  uint64_t cover_index = 0;
  uint64_t max_seq_no = 0;
  /* left_part=true时，将sequence no小于或等于快照
   * sequence no.的KV记录对应的编号加入part_index_set;
   * left_part=false时，将sequence no大于快照
   * sequence no.的KV记录对应的编号加入part_index_set*/
  for(auto it = index_vec.begin(); it != index_vec.end(); ++it)
  {
    /*
     * left_part=true时，跳过比快照sequence no.大的KV记录；
     * left_part=false时，跳过小于或等于快照sequence no.的KV记录；
     */
    if((left_part && (min_ref_seq_no_ < pkeys[*it].sequence)) || (!left_part && (min_ref_seq_no_ >= pkeys[*it].sequence)))
      continue;
    if(max_seq_no <= pkeys[*it].sequence)
    {
      max_seq_no = pkeys[*it].sequence;
      cover_index = *it;
    }
    part_index_vec.emplace_back(*it);
  }
  /*
   * 如果找到需要处理的记录
   */
  if(!part_index_vec.empty())
  {
    if(left_part)
    {
      auto type = pkeys[cover_index].type;
      if(kTypeDeletion == type || kTypeSingleDeletion == type) /*KV记录类型为删除*/
      {
        // if level 0 data block exists, we must reserve the delete entry
        if (level_type_ == 0) {
          builder->Add(keys[cover_index], values[cover_index]);
        }
        multi_way_block_merge_stats_.num_records_replaced_ += part_index_vec.size();
      }else if(kTypeMerge == type || kTypeValue == type) /*KV记录类型为覆盖*/
      {
        builder->Add(keys[cover_index], values[cover_index]);

        multi_way_block_merge_stats_.num_records_replaced_ += part_index_vec.size() - 1;
      }
    }else /*对于sequence no.大于快照sequence no.的记录，全部保留*/
    {
      //fprintf(stderr, ">seq_no new index_vec\n");
      for(auto it = part_index_vec.begin(); it != part_index_vec.end(); ++it)
      {
        ParsedInternalKey ikey("", 0, kTypeValue);
        ParseInternalKey(keys[*it], &ikey);
       // fprintf(stderr, ">seq_no add user_key:%s\tseq:%ld\n", ikey.user_key.data(), ikey.sequence);
        builder->Add(keys[*it], values[*it]);
      }
    }
  }
}
#endif
#if 0
std::string MultiWayBlockMerge::SingleBlockMerge(
    Slice* block_1,
    Slice* block_2)
{
  const Comparator* cmp = BytewiseComparator();
  BlockContents contents_1;
  contents_1.data = Slice(block_1->data(), block_1->size());
  Block reader_1(std::move(contents_1), kDisableGlobalSequenceNumber);
  BlockIter *iter1 = new BlockIter();
  reader_1.NewIterator(cmp, iter1);

  BlockContents contents_2;
  contents_2.data = Slice(block_2->data(), block_2->size());
  Block reader_2(std::move(contents_2), kDisableGlobalSequenceNumber);
  BlockIter *iter2 = new BlockIter();
  reader_2.NewIterator(cmp, iter2);

  // merge two blocks
  // construct blockbuilder to add new KV pair
  BlockBuilder builder(RESTART_INTERVAL);

  iter1->SeekToFirst();
  iter2->SeekToFirst();
  std::string key_1, value_1;
  std::string key_2, value_2;
  bool hasnext_1, hasnext_2;
  hasnext_1 = GetNextKV(iter1, key_1, value_1);
  hasnext_2 = GetNextKV(iter2, key_2, value_2);

  while(true)
  {
    ParsedInternalKey parsed_key_1("", 0, kTypeValue);
    ParseInternalKey(key_1, &parsed_key_1);

    ParsedInternalKey parsed_key_2("", 0, kTypeValue);
    ParseInternalKey(key_2, &parsed_key_2);

    if(parsed_key_1.user_key.compare(parsed_key_2.user_key) > 0)
    {
      builder.Add(key_2, value_2);
      hasnext_2 = GetNextKV(iter2, key_2, value_2);
    }
    else if(parsed_key_1.user_key.compare(parsed_key_2.user_key) < 0)
    {
      builder.Add(key_1, value_1);
      hasnext_1 = GetNextKV(iter1, key_1, value_1);
    }
    else
    {
      auto seqno_1 = parsed_key_1.sequence;
      auto seqno_2 = parsed_key_2.sequence;
      auto type_1 = parsed_key_1.type;
      auto type_2 = parsed_key_2.type;
      assert(seqno_1 != seqno_2);
      if(seqno_1 > seqno_2)
      {
        if (seqno_1 > min_ref_seq_no_) {
          builder.Add(key_2, value_2);
          builder.Add(key_1, value_1);
        }
        else if(kTypeDeletion == type_1 || kTypeSingleDeletion == type_1)
        {
          // delete entry
          multi_way_block_merge_stats_.num_records_replaced_++;
        }
        else if(kTypeMerge == type_1 || kTypeValue == type_1)
        {
          builder.Add(key_1, value_1);
          multi_way_block_merge_stats_.num_records_replaced_++;
        } else {
          abort();
        }
      }
      else
      {//seq_no_1 < seqno_2
        if (seqno_2 > min_ref_seq_no_) {
          builder.Add(key_1, value_1);
          builder.Add(key_2, value_2);
        }
        else if (kTypeDeletion == type_2 || kTypeSingleDeletion == type_2)
        {
          multi_way_block_merge_stats_.num_records_replaced_++;
        }
        else if (kTypeMerge == type_2 || kTypeValue == type_2)
        {
          builder.Add(key_2, value_2);
          multi_way_block_merge_stats_.num_records_replaced_++;
        } else {
          abort();
        }
      }

      hasnext_1 = GetNextKV(iter1, key_1, value_1);
      hasnext_2 = GetNextKV(iter2, key_2, value_2);

    }
    if(hasnext_1 && hasnext_2)
    {
      continue;
    }
    if(!iter1->Valid() || !iter2->Valid())
    {
      break;
    }
  }
  if(hasnext_1)
  {
    builder.Add(key_1, value_1);
  }
  if(hasnext_2)
  {
    builder.Add(key_2, value_2);
  }
  while (GetNextKV(iter1, key_1, value_1))
  {
    builder.Add(key_1, value_1);
  }

  while (GetNextKV(iter2, key_2, value_2))
  {
    builder.Add(key_2, value_2);
  }
  delete iter1;
  delete iter2;
  auto raw_block = builder.Finish();
  cmp = nullptr;
  return std::string(raw_block.data(), raw_block.size());
}
#endif

bool MultiWayBlockMerge::GetNextKV(BlockIter *iter, std::string &r_key, std::string &r_value)
{
  if (!iter->Valid())
  {
    return false;
  }
  std::string key(iter->key().data(),iter->key().size());
  std::string value(iter->value().data(), iter->value().size());

  ParsedInternalKey parsed_key("", 0, kTypeValue);
  ParsedInternalKey parsed_key_("", 0, kTypeValue);

  while (true)
  {
    iter->Next();
    if(!iter->Valid())
    {
      break;
    }
    std::string key_(iter->key().data(), iter->key().size());
    std::string value_(iter->value().data(), iter->value().size());

    ParseInternalKey(key, &parsed_key);
    ParseInternalKey(key_, &parsed_key_);

    if(parsed_key.sequence >= min_ref_seq_no_)
    {
      r_key = key;
      r_value = value;
      return true;
    }
    //reserve the one with biggest seq
    //until we find the next different user_key
    if (parsed_key_.user_key.compare(parsed_key.user_key) == 0)
    {
      //key = key;
      //value = value;
      multi_way_block_merge_stats_.num_records_replaced_++;
    }
    else
    {
      r_key = key;
      r_value = value;
      return true;
    }
  }

  r_key = key;
  r_value = value;
  return true;
}

/*
 * avoid unnecessary memory copy
 */
bool MultiWayBlockMerge::GetNextKV(BlockIter *iter, Slice &r_key, Slice &r_value)
{
  if (!iter->Valid())
  {
    return false;
  }
  //std::string key(iter->key().data(),iter->key().size());
  //std::string value(iter->value().data(), iter->value().size());

  Slice key = iter->key();
  Slice value = iter->value();

  ParsedInternalKey parsed_key("", 0, kTypeValue);
  ParsedInternalKey parsed_key_("", 0, kTypeValue);

  while (true)
  {
    iter->Next();
    if(!iter->Valid())
    {
      break;
    }
    //std::string key_(iter->key().data(), iter->key().size());
    //std::string value_(iter->value().data(), iter->value().size());

    Slice key_ = iter->key();
    //Slice value_ = iter->value();

    ParseInternalKey(key, &parsed_key);
    ParseInternalKey(key_, &parsed_key_);

    if(parsed_key.sequence >= min_ref_seq_no_)
    {
      r_key = key;
      r_value = value;
      return true;
    }
    //parsed_key.sequence < min_ref_seq_no_
    if (parsed_key_.user_key.compare(parsed_key.user_key) == 0)
    {
      assert(parsed_key.sequence > parsed_key_.sequence);
      key = key;
      value = value;
      multi_way_block_merge_stats_.num_records_replaced_++;
    }
    else
    {
      r_key = key;
      r_value = value;
      return true;
    }
  }

  r_key = key;
  r_value = value;
  return true;
}
void MultiWayBlockMerge::GetInputBlocksStats()
{
  const Comparator *cmp = BytewiseComparator();
  for(size_t i=0;i<num_ways_;++i)
  {
    size_t num_blocks = num_input_blocks_[i];

    auto block_ptr = input_blocks_[i];
    for(size_t j=0;j<num_blocks;++j)
    {
      size_t size = input_blocks_size_[i][j];
      BlockContents contents;
      contents.data = Slice(block_ptr, size);
      Block reader(std::move(contents), kDisableGlobalSequenceNumber);
      BlockIter *iter = new BlockIter();
      reader.NewIterator(cmp, iter);

      for (iter->SeekToFirst(); iter->Valid(); iter->Next())
      {
        multi_way_block_merge_stats_.num_input_records_++;
        multi_way_block_merge_stats_.total_input_raw_key_bytes_ += iter->key().size();
        multi_way_block_merge_stats_.total_input_raw_value_bytes_ += iter->value().size();

        ParsedInternalKey ikey("", 0, kTypeValue);
        ParseInternalKey(iter->key(), &ikey);
        if(ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion){
          multi_way_block_merge_stats_.num_records_delete_++;
        }
      }
      block_ptr += size;
      delete iter;
    }
  }
  //delete cmp;
  cmp = nullptr;
}

#if 0
void MultiWayBlockMerge::PutRes(std::string res)
{
  ofstream res_ofs;
  res_ofs.open("res.bin", std::ofstream::out | std::fstream::binary);
  res_ofs.write((char*)res.data(), res.size());
}

bool MultiWayBlockMerge::VerifyResEquality(std::string reconstruct_res)
{
  ifstream res_ifs;
  res_ifs.open("res.bin", std::ifstream::in | std::fstream::binary);
  res_ifs.seekg(0, ios_base::end);
  size_t res_size = res_ifs.tellg();
  res_ifs.seekg(0);
  char res[res_size+1];
  res[res_size] = '\0';
  res_ifs.read(res, res_size);
  if(memcmp(res, (char*)reconstruct_res.data(), res_size) == 0 && res_size == reconstruct_res.size())
  {
    std::cout << "Reconstructed block merge pass verify\n";
    return 1;
  }
  return 0;
}
#endif

#if 0
void Compaction::CheckKeyRange(
    char** input_blocks,
    size_t** input_blocks_size,
    size_t* num_input_blocks,
    size_t  num_ways)
{
  ofstream ofs;
  ofs.open("key_range.txt", std::fstream::out);
  Comparator cmp;
  ParsedInternalKey pkey_1, pkey_2;
  for(auto i = 0; i < num_ways; ++i)
  {
    ofs << i << "th way\n";
    auto block_ptr = input_blocks[i];
    for(auto j = 0; j < num_input_blocks[i]; ++j)
    {
      size_t size = input_blocks_size[i][j];
      Block reader(block_ptr, size);
      auto iter = reader.NewIterator(&cmp);
      iter->SeekToFirst();
      if(i==0 && j==0)
      {
        ParseInternalKey(iter->Key(), &pkey_1);
      }
      if(i==1 && j==0)
      {
        ParseInternalKey(iter->Key(), &pkey_2);
      }
      for(; iter->Valid(); iter->Next())
      {
        ParsedInternalKey pkey;
        ParseInternalKey(iter->Key(), &pkey);
        ofs << "key=" << pkey.user_key.data_ << ", sequence=" << pkey.sequence << std::endl;
      }
      block_ptr += size;
    }
  }
  if(IsKeyRangeOverlap(&pkey_1, &pkey_2))
    std::cout << "key range overlap between 1th way and 2nd way\n";
}

bool Compaction::IsKeyRangeOverlap(ParsedInternalKey *pkey_1, ParsedInternalKey *pkey_2)
{
  if(0 == pkey_1->user_key.compare(pkey_2->user_key))
    return true;
}
#endif
}
}
