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
#include "memtable/art.h"

namespace xengine {

using namespace common;
using namespace memory;
using namespace util;
using namespace logger;

namespace memtable {

void ART::calc_prefix(Slice &lhs, Slice &rhs, uint32_t &byte_pos, uint32_t limit) {
  const char *ldata = lhs.data();
  const char *rdata = rhs.data();
  while (byte_pos < limit && ldata[byte_pos] == rdata[byte_pos]) {
    byte_pos++;
  }
}

int ART::alloc_newnode(ARTNodeBase *&new_node, const uint8_t *prefix, uint16_t prefix_len) {
  int ret = Status::kOk;
  if (ISNULL(new_node = MOD_NEW_OBJECT(ModId::kMemtable, ARTNode4))) { // TODO(nanlong.ynl): use memory pool for alloc
    XENGINE_LOG(ERROR, "failed to alloc memory for new node", KP(new_node));
    ret = Status::kMemoryLimit;
  } else if (FAILED(new_node->init(prefix, prefix_len))) {
    XENGINE_LOG(ERROR, "failed to init new node", KP(new_node));
  } else {
    memory_usage_.fetch_add(new_node->memory_usage());
  }
  return ret;
}

// 'precursor' must be less than 'insert_artvalue', but may not be the max artvalue which is less than 'insert_artvalue',
// because some artvalues which are less than 'insert_artvalue' but greater than 'precursor'
// may be inserted into the value list after we found 'precursor'.
// As a result we still need to scan forward to find the right pos to insert from 'precursor',
// and this time we will take a lock when doing scan.
void ART::insert_into_value_list(ARTValue *insert_artvalue, ARTValue *precursor, Slice &insert_key) {
  ARTValue *next_pos = nullptr;
  // During scan we only need to lock 'precursor' 
  // to prevent other threads inserting artvalue bwtween 'precursor' and 'next_pos'.
  precursor->lock();
  next_pos = precursor->next();
  if (next_pos != &dummy_node_) {
    while (cmp_(next_pos->entry(), insert_key) < 0) {
      precursor->unlock();
      precursor = next_pos;
      precursor->lock();
      next_pos = precursor->next();
      if (next_pos == &dummy_node_) {
        break;
      }
    }
  }
  insert_artvalue->set_next(next_pos);
  insert_artvalue->set_prev(precursor);
  precursor->set_next(insert_artvalue);
  next_pos->set_prev(insert_artvalue);
  insert_artvalue->commit();
  precursor->unlock();
}

ARTValue *ART::scan_backward_for_less_than(ARTValue *start, const Slice &target) const {
  ARTValue *pos = start;
  if (pos == &dummy_node_) {
    return pos;
  }
  while (cmp_(pos->entry(), target) >= 0) {
    pos = pos->prev();
    if (pos == &dummy_node_) {
      break;
    }
  }
  return pos;
}

ARTValue *ART::scan_backward_for_greater_than_or_equal(ARTValue *start, const Slice &target) const {
  ARTValue *pos = start;
  ARTValue *prev = pos->prev();
  if (prev == &dummy_node_) {
    return pos;
  }
  while (cmp_(prev->entry(), target) >= 0) {
    pos = prev;
    prev = pos->prev();
    if (prev == &dummy_node_) {
      break;
    }
  }
  return pos;
}

ARTValue *ART::scan_forward_for_greater_than_or_equal(ARTValue *start, const Slice &target) const {
  ARTValue *pos = start;
  if (pos == &dummy_node_) {
    return pos;
  }
  while (cmp_(pos->entry(), target) < 0) {
    pos = pos->next();
    if (pos == &dummy_node_) {
      break;
    }
  }
  return pos;
}

// When calling this function, we must have read or write lock of root's parent node.
// If we have write lock of root's parent node, p_node will be nullptr.
int ART::largest_artvalue_in_subtrie(void *root, ARTValue *&largest_artvalue, 
                                void *p_node, uint64_t p_version_snapshot) const {
  int ret = Status::kOk;
  void *curr_node = p_node;
  void *parent_node = nullptr;
  void *next_node = root;
  uint64_t curr_version_snapshot = p_version_snapshot;
  uint64_t parent_version_snapshot = 0;

  while (IS_ARTVALUE(next_node) == false) {
    if (ISNULL(next_node)) {
      XENGINE_LOG(ERROR, "next node should never be null");
      ret = Status::kErrorUnexpected;
      break;
    }
    parent_node = curr_node;
    parent_version_snapshot = curr_version_snapshot;
    curr_node = next_node;
    // we should get read lock of curr node before accessing it
    if (FAILED(VOID2INNERNODE(curr_node)->try_rdlock(curr_version_snapshot))) {
      break;
    }
    // we can unlock parent node after getting read lock of curr node
    if (parent_node != nullptr) {
      if (FAILED(VOID2INNERNODE(parent_node)->rdunlock(parent_version_snapshot))) {
        break;
      }
    }
    next_node = VOID2INNERNODE(curr_node)->maximum();
    // check whether the curr node was modified during accessing
    if (FAILED(VOID2INNERNODE(curr_node)->check_version(curr_version_snapshot))) {
      break;
    }
  }
  
  if (SUCCED(ret)) {
    largest_artvalue = VOID2ARTVALUE(next_node);
  }
  return ret;
}

// When calling this function, we must have read or write lock of root's parent node.
// If we have write lock of root's parent node, p_node will be nullptr.
int ART::smallest_artvalue_in_subtrie(void *root, ARTValue *&smallest_artvalue,
                                  void *p_node, uint64_t p_version_snapshot) const {
  int ret = Status::kOk;
  void *curr_node = p_node;
  void *parent_node = nullptr;
  void *next_node = root;
  uint64_t curr_version_snapshot = p_version_snapshot;
  uint64_t parent_version_snapshot = 0;

  while (IS_ARTVALUE(next_node) == false) {
    if (ISNULL(next_node)) {
      XENGINE_LOG(ERROR, "next node should never be null");
      ret = Status::kErrorUnexpected;
      break;
    }
    parent_node = curr_node;
    parent_version_snapshot = curr_version_snapshot;
    curr_node = next_node;
    // we should get read lock of curr node before accessing it
    if (FAILED(VOID2INNERNODE(curr_node)->try_rdlock(curr_version_snapshot))) {
      break;
    }
    // we can unlock parent node after getting read lock of curr node
    if (parent_node != nullptr) {
      if (FAILED(VOID2INNERNODE(parent_node)->rdunlock(parent_version_snapshot))) {
        break;
      }
    }
    next_node = VOID2INNERNODE(curr_node)->minimum();
    // check whether the curr node was modified during accessing
    if (FAILED(VOID2INNERNODE(curr_node)->check_version(curr_version_snapshot))) {
      break;
    }
  }

  if (SUCCED(ret)) {
    smallest_artvalue = VOID2ARTVALUE(next_node);
  }
  return ret;
}

/*
 * This two functions are used when we have taken the write lock of root's parent node.
 * If we need try largest_artvalue_in_subtrie again, there is no need to try again from root of ART,
 * since the parent node of root is write locked and 
 * the link between root and its parent node cannot be modified.
 */
int ART::largest_artvalue_in_subtrie_wrap(void *root, ARTValue *&largest_artvalue) const {
  int ret = Status::kTryAgain;
  int restart_count = 0;
  while (ret == Status::kTryAgain) {
    if (FAILED(largest_artvalue_in_subtrie(root, largest_artvalue, nullptr, 0))) {
      yield(restart_count++);
    }
  }
  return ret;
}

int ART::smallest_artvalue_in_subtrie_wrap(void *root, ARTValue *&smallest_artvalue) const {
  int ret = Status::kTryAgain;
  int restart_count = 0;
  while (ret == Status::kTryAgain) {
    if (FAILED(smallest_artvalue_in_subtrie(root, smallest_artvalue, nullptr, 0))) {
      yield(restart_count++);
    }
  }
  return ret;
}

int ART::insert_maybe_overflow(uint8_t curr_key, ARTValue *insert_artvalue, ARTNodeBase *curr_node, uint64_t curr_version_snapshot,
                          uint8_t parent_key, ARTNodeBase *parent_node, uint64_t parent_version_snapshot) {
  int ret = Status::kOk;
  int restart_count = 0;
  ARTValue *precursor = nullptr;
  ARTNodeBase *new_node = nullptr;
  Slice insert_key = insert_artvalue->key();

  // We should find the max artvalue which is less than 'insert_artvalue' in the value list,
  // then we can insert 'insert_artvalue' into the value list after that artvalue.
  std::function<int()> find_precursor_of_insert_artvalue = [&]() {
    ARTValue *successor = nullptr;
    void *child_less_than_curr_key = curr_node->less_than(curr_key);
    // All artvalues in the subtrie of 'curr_node' are greater than 'insert_artvalue',
    // so we need to find the smallest artvalue in the subtrie of 'curr_node',
    // and scan backward to find the max artvalue which is less than 'insert_artvalue'.
    if (child_less_than_curr_key == nullptr) {
      void *minimum_child = curr_node->minimum();
      if (ISNULL(minimum_child)) {
        precursor = &dummy_node_;
      } else if (FAILED(smallest_artvalue_in_subtrie_wrap(minimum_child, successor))) {
        XENGINE_LOG(ERROR, "failed to find smallest artvalue in subtrie", KP(minimum_child), KP(successor));
      } else {
        precursor = scan_backward_for_less_than(successor, insert_key);
      }
    // The max artvalue in the subtrie of 'child_less_than_curr_key' is the max artvalue 
    // which is less than 'insert_artvalue' in the value list.
    } else if (FAILED(largest_artvalue_in_subtrie_wrap(child_less_than_curr_key, precursor))) {
      XENGINE_LOG(ERROR, "failed to find largest artvalue in subtrie", KP(child_less_than_curr_key), KP(precursor));
    }
    return ret;
  };
  if (curr_node->is_full()) {
    // Get write lock of parent node and curr node before expanding curr node
    if (FAILED(parent_node->try_upgrade_rdlock(parent_version_snapshot))) {
      // Do nothing
    } else if (FAILED(curr_node->try_upgrade_rdlock(curr_version_snapshot))) {
      parent_node->wrunlock();
    } else if (FAILED(find_precursor_of_insert_artvalue())) {
      XENGINE_LOG(ERROR, "failed to find right pos to insert");
    } else if (FAILED(curr_node->expand(new_node))) {
      XENGINE_LOG(ERROR, "failed to expand curr node", KP(curr_node));
    } else if (FAILED(new_node->insert(curr_key, ARTVALUE2VOID(insert_artvalue)))) {
      XENGINE_LOG(ERROR, "failed to insert into new node", KP(new_node));
    } else if (FAILED(parent_node->update(parent_key, new_node))) {
      XENGINE_LOG(ERROR, "failed to update parent node", KP(parent_node));
    } else {
      memory_usage_.fetch_add(new_node->memory_usage());
      memory_usage_.fetch_sub(curr_node->memory_usage());
      curr_node->wrunlock_with_obsolete();
      // TODO(nanlong.ynl): mark delete use ebr
      EBR_REMOVE(curr_node, ebr_delete);
      parent_node->wrunlock();
      insert_into_value_list(insert_artvalue, precursor, insert_key);
    }
  } else {
    // No need to expand curr node, just unlock the parent node and get write lock of curr node
    if (parent_node != nullptr && FAILED(parent_node->rdunlock(parent_version_snapshot))) {
      // Do nothing
    } else if (FAILED(curr_node->try_upgrade_rdlock(curr_version_snapshot))) {
      // Do nothing
    } else if (FAILED(find_precursor_of_insert_artvalue())) {
      XENGINE_LOG(ERROR, "failed to find right pos to insert");
    } else if (FAILED(curr_node->insert(curr_key, ARTVALUE2VOID(insert_artvalue)))) {
      XENGINE_LOG(ERROR, "failed to insert into curr node", KP(curr_node));
    } else {
      curr_node->wrunlock();
      insert_into_value_list(insert_artvalue, precursor, insert_key);
    }
  }
  return ret;
}

/*
 * This function deals with the case which needs to split the prefix of the curr node.
 * An example of this case is like below:
 * 
 * New artvalue to be inserted: 'ABC'.
 * 
 * Original ART:
 *     ____________________
 *    |_prefix_|_'ABD'_____|
 *    |_key____|_'E'_|_'F'_|  curr node
 *    |_child__|__/__|__\__|  
 *               /       \
 *          artvalue    artvalue
 * 
 * After insert:
 *     ____________________
 *    |_prefix_|_'AB'______|
 *    |_key____|_'C'_|_'D'_|  new node(ARTNode4)
 *    |_child__|__/__|__\__|  
 *               /       \
 *          artvalue      \
 *                   ______\_____________
 *                  |_prefix_|_''________|
 *                  |_key____|_'E'_|_'F'_|  curr node
 *                  |_child__|__/__|__\__|  
 *                             /       \
 *                        artvalue    artvalue
 */
int ART::split_prefix_with_normal_case(Slice &insert_key, uint32_t start_byte_pos, uint32_t shared_prefix, 
                                  ARTValue *insert_artvalue, ARTNodeBase *curr_node, uint64_t current_version_snapshot,
                                  uint8_t parent_key, ARTNodeBase *parent_node, uint64_t parent_version_snapshot) {
  int ret = Status::kOk;
  uint32_t next_byte_pos = start_byte_pos + shared_prefix;
  const uint8_t *insert_raw_key = reinterpret_cast<const uint8_t *>(insert_key.data());
  const uint8_t *raw_prefix = curr_node->prefix();
  ARTNodeBase *new_node = nullptr;
  ARTValue *precursor = nullptr;
  
  // Get the write lock of parent node and curr node before split the curr node's prefix
  if (FAILED(parent_node->try_upgrade_rdlock(parent_version_snapshot))) {
    // Do nothing
  } else if (FAILED(curr_node->try_upgrade_rdlock(current_version_snapshot))) {
    parent_node->wrunlock();
  } else if (insert_raw_key[next_byte_pos] < raw_prefix[shared_prefix]) {
    // 'insert_artvalue' is less than the prefix of curr node,
    // which means that 'insert_artvalue' is less than all artvalues in the subtrie of curr node,
    // so we should find the smallest artvalue in the subtrie of curr node,
    // and scan backward to find the precursor.
    ARTValue *successor = nullptr;
    if (FAILED(smallest_artvalue_in_subtrie_wrap(curr_node->minimum(), successor))) {
      XENGINE_LOG(ERROR, "failed to find smallest artvalue in subtrie", KP(curr_node), KP(successor));
    } else {
      precursor = scan_backward_for_less_than(successor, insert_key);
    }
  } else {
    // 'insert_artvalue' is greater than the prefix of curr node,
    // so we just need to find the largest artvalue in the subtrie of curr node.
    if (FAILED(largest_artvalue_in_subtrie_wrap(curr_node->maximum(), precursor))) {
      XENGINE_LOG(ERROR, "failed to find largest artvalue in subtrie", KP(curr_node), KP(precursor));
    }
  }
  
  if (FAILED(ret)) {
    // Do nothing
  } else if (FAILED(alloc_newnode(new_node, raw_prefix, shared_prefix))) {
    XENGINE_LOG(ERROR, "failed to init new node");
  } else if (FAILED(new_node->insert(insert_raw_key[next_byte_pos], ARTVALUE2VOID(insert_artvalue)))) {
    XENGINE_LOG(ERROR, "failed to insert new artvalue into new node", KP(new_node));
  } else if (FAILED(new_node->insert(raw_prefix[shared_prefix], curr_node))) {
    XENGINE_LOG(ERROR, "failed to insert curr node into new node", KP(new_node));
  } else if (FAILED(curr_node->set_prefix(raw_prefix + shared_prefix + 1, curr_node->prefix_len() - shared_prefix - 1))) {
    XENGINE_LOG(ERROR, "failed to change the prefix of curr node", 
        KP(curr_node), K(raw_prefix), K(shared_prefix), K(curr_node->prefix_len()));
  } else if (FAILED(parent_node->update(parent_key, new_node))) {
    XENGINE_LOG(ERROR, "failed to update parent node", KP(parent_node));
  } else {
    curr_node->wrunlock();
    parent_node->wrunlock();
    insert_into_value_list(insert_artvalue, precursor, insert_key);
  }

  return ret;
}

/*
 * This function deals with the case where new artvalue and inner node share the same prefix but have no differentiable suffix
 * An example of this case is like below:
 * 
 * New artvalue to be inserted: 'AB'.
 * 
 * Original ART:
 *     ________________
 *    |_prefix_|_'ABC'_|
 *    |_key____|__'D'__|  curr node
 *    |_child__|___/___| 
 *                /       
 *       (artvalue 'ABCDE')
 * 
 * After insert:
 *     ________________
 *    |_prefix_|__'A'__|
 *    |_key____|__'B'__|  new node
 *    |_child__|___\___| 
 *                  \    
 *              _____\_____________
 *             |___prefix__|__''___|
 *             |_value_ptr_|____---|---(artvalue 'AB')
 *             |____key____|__'C'__|  new node2
 *             |___child___|___\___|    
 *                              \
 *                         ______\_______
 *                        |_prefix_|_''__|
 *                        |__key___|_'D'_| curr node
 *                        |_child__|__/__|
 *                                   /
 *                           (artvalue 'ABCDE')
 */
int ART::split_prefix_with_no_differentiable_suffix(Slice &insert_key, uint32_t start_byte_pos, uint32_t shared_prefix, 
                                                ARTValue *insert_artvalue, ARTNodeBase *curr_node, uint64_t current_version_snapshot,
                                                uint8_t parent_key, ARTNodeBase *parent_node, uint64_t parent_version_snapshot) {
  int ret = Status::kOk;
  ARTValue *precursor = nullptr;
  const uint8_t *raw_prefix = curr_node->prefix();
  uint32_t raw_prefix_len = curr_node->prefix_len();

  std::function<int()> find_precursor_of_insert_artvalue = [&]() {
    ARTValue *successor = nullptr;
    if (FAILED(smallest_artvalue_in_subtrie_wrap(curr_node->minimum(), successor))) {
      XENGINE_LOG(ERROR, "failed to find smallest artvalue in subtrie", KP(curr_node));
    } else {
      precursor = scan_backward_for_less_than(successor, insert_key);
    }
    return ret;
  };

  if (curr_node->prefix_len() == 0) {
    if (FAILED(parent_node->rdunlock(parent_version_snapshot))) {
      // Do nothing
    } else if (FAILED(curr_node->try_upgrade_rdlock(current_version_snapshot))) {
      // Do nothing
    } else if (FAILED(find_precursor_of_insert_artvalue())) {
      XENGINE_LOG(ERROR, "failed to find precursor of insert artvalue");
    } else {
      if (curr_node->has_value_ptr()) {
        Slice value_ptr_key = VOID2ARTVALUE(curr_node->value_ptr())->key();
        uint64_t insert_seq = DecodeFixed64(insert_key.data() + insert_key.size() - 8);
        uint64_t value_ptr_seq = DecodeFixed64(value_ptr_key.data() + value_ptr_key.size() - 8);
        if (insert_seq > value_ptr_seq) {
          curr_node->set_value_ptr(ARTVALUE2VOID(insert_artvalue));
        }
      } else {
        curr_node->set_value_ptr(ARTVALUE2VOID(insert_artvalue));
      }
      curr_node->wrunlock();
      insert_into_value_list(insert_artvalue, precursor, insert_key);
    }
  } else {
    if (FAILED(parent_node->try_upgrade_rdlock(parent_version_snapshot))) {
      // Do nothing
    } else if (FAILED(curr_node->try_upgrade_rdlock(current_version_snapshot))) {
      // Curr node has been modified, we should unlock parent node.
      parent_node->wrunlock();
    } else if (FAILED(find_precursor_of_insert_artvalue())) {
      XENGINE_LOG(ERROR, "failed to find precursor of insert artvalue");
    } else {
      if (shared_prefix == 0) {
        ARTNodeBase *new_node = nullptr;
        if (FAILED(alloc_newnode(new_node, nullptr, 0))) {
          XENGINE_LOG(ERROR, "failed to init new node");
        } else if (FAILED(new_node->insert(raw_prefix[0], curr_node))) {
          XENGINE_LOG(ERROR, "failed to insert curr node into new node", KP(new_node));
        } else if (FAILED(curr_node->set_prefix(raw_prefix + 1, raw_prefix_len - 1))) {
          XENGINE_LOG(ERROR, "failed to set prefix of curr node", KP(curr_node));
        } else if (FAILED(parent_node->update(parent_key, new_node))) {
          XENGINE_LOG(ERROR, "failed to update parent", KP(parent_node));
        } else {
          new_node->set_value_ptr(ARTVALUE2VOID(insert_artvalue));
          curr_node->wrunlock();
          parent_node->wrunlock();
          insert_into_value_list(insert_artvalue, precursor, insert_key);
        }
      } else if (shared_prefix == raw_prefix_len) {
        ARTNodeBase *new_node = nullptr;
        if (FAILED(alloc_newnode(new_node, raw_prefix, shared_prefix - 1))) {
          XENGINE_LOG(ERROR, "failed to init new node");
        } else if (new_node->insert(raw_prefix[shared_prefix - 1], curr_node)) {
          XENGINE_LOG(ERROR, "failed to insert curr node into new node", 
              KP(new_node), K(raw_prefix), K(shared_prefix), KP(curr_node));
        } else if (parent_node->update(parent_key, new_node)) {
          XENGINE_LOG(ERROR, "failed to update parent node", KP(parent_node));
        } else if (curr_node->set_prefix(nullptr, 0)) {
          XENGINE_LOG(ERROR, "failed to set prefix of curr node", KP(curr_node));
        } else {
          curr_node->set_value_ptr(ARTVALUE2VOID(insert_artvalue));
          curr_node->wrunlock();
          parent_node->wrunlock();
          insert_into_value_list(insert_artvalue, precursor, insert_key);
        }
      } else {
        ARTNodeBase *new_node = nullptr;
        ARTNodeBase *new_node2 = nullptr;
        if (FAILED(alloc_newnode(new_node, raw_prefix, shared_prefix - 1))) {
          XENGINE_LOG(ERROR, "failed to init new node");
        } else if (FAILED(alloc_newnode(new_node2, nullptr, 0))) {
          XENGINE_LOG(ERROR, "failed to init new node");
        } else if (FAILED(new_node->insert(raw_prefix[shared_prefix - 1], new_node2))) {
          XENGINE_LOG(ERROR, "failed to insert new node2 into new node", 
              KP(new_node), K(raw_prefix), K(shared_prefix), KP(new_node2));
        } else if (FAILED(new_node2->insert(raw_prefix[shared_prefix], curr_node))) {
          XENGINE_LOG(ERROR, "failed to insert curr node into new node2", KP(new_node2));
        } else if (FAILED(parent_node->update(parent_key, new_node))) {
          XENGINE_LOG(ERROR, "failed to update parent node", KP(parent_node));
        } else if (FAILED(curr_node->set_prefix(raw_prefix + shared_prefix + 1, raw_prefix_len - shared_prefix - 1))) {
          XENGINE_LOG(ERROR, "failed to set prefix of curr node", KP(curr_node), K(raw_prefix), K(shared_prefix), K(raw_prefix_len));
        } else {
          new_node2->set_value_ptr(ARTVALUE2VOID(insert_artvalue));
          curr_node->wrunlock();
          parent_node->wrunlock();
          insert_into_value_list(insert_artvalue, precursor, insert_key);
        }
      }
    }
  }

  return ret;
}

int ART::insert_maybe_split_artvalue(Slice &insert_key, ARTValue *insert_artvalue, uint32_t start_byte_pos,
                              uint8_t curr_key, ARTNodeBase *curr_node, uint64_t curr_version_snapshot,
                              ARTNodeBase *parent_node, uint64_t parent_version_snapshot) {
  int ret = Status::kOk;

  if (parent_node != nullptr && FAILED(parent_node->rdunlock(parent_version_snapshot))) {
    // Do nothing
  } else if (FAILED(curr_node->try_upgrade_rdlock(curr_version_snapshot))) {
    // Do nothing
  } else {
    ARTValue *split_artvalue = VOID2ARTVALUE(curr_node->get(curr_key));
    Slice split_key = split_artvalue->key();
    const uint8_t *insert_raw_key = reinterpret_cast<const uint8_t *>(insert_key.data());
    const uint8_t *split_raw_key = reinterpret_cast<const uint8_t *>(split_key.data());
    uint32_t limit = insert_key.size() > split_key.size() ? split_key.size() - 8 : insert_key.size() - 8;
    uint32_t next_byte_pos = start_byte_pos;
    calc_prefix(insert_key, split_key, next_byte_pos, limit);

    if (next_byte_pos < limit) {
      ret = split_artvalue_with_normal_case(insert_key, insert_artvalue, start_byte_pos, next_byte_pos - start_byte_pos,
                                          split_key, split_artvalue, curr_key, curr_node);
    } else if (insert_key.size() == split_key.size()) {
      ret = insert_with_no_split_artvalue(insert_key, insert_artvalue, split_key, split_artvalue, curr_key, curr_node);
    } else {
      ret = split_artvalue_with_no_differentiable_suffix(insert_key, insert_artvalue, start_byte_pos, next_byte_pos - start_byte_pos,
                                                          split_key, split_artvalue, curr_key, curr_node);
    }
  }

  return ret;
}

/*
 * This function deals with the normal case which needs to split the artvalue.
 * An example of this case is like below:
 * 
 * New artvalue to be inserted: 'ABCDE'.
 * 
 * Original ART:
 *     ________________
 *    |_prefix_|_'AB'__|
 *    |_key____|__'C'__|  curr node
 *    |_child__|___/___| 
 *                /       
 *        (artvalue 'ABCDF')
 * 
 * After insert:
 *     ________________
 *    |_prefix_|_'AB'__|
 *    |_key____|__'C'__|  curr node
 *    |_child__|___\___| 
 *                  \
 *             ______\_____________
 *            |_prefix_|___'D'_____|
 *            |_key____|_'E'_|_'F'_|  new node(ARTNode 4)
 *            |_child__|__/__|__\__|  
 *                       /       \
 *        (artvalue 'ABCDE')<-->(artvalue 'ABCDF')
 */
int ART::split_artvalue_with_normal_case(Slice &insert_key, ARTValue *insert_artvalue, uint32_t start_byte_pos, uint32_t shared_prefix, 
                                  Slice &split_key, ARTValue *split_artvalue, uint8_t curr_key, ARTNodeBase *curr_node) {
  int ret = Status::kOk;
  uint32_t next_byte_pos = start_byte_pos + shared_prefix;
  const uint8_t *insert_raw_key = reinterpret_cast<const uint8_t *>(insert_key.data());
  const uint8_t *split_raw_key = reinterpret_cast<const uint8_t *>(split_key.data());
  ARTNodeBase *new_node = nullptr;
  ARTValue *precursor = nullptr;

  if (insert_raw_key[next_byte_pos] < split_raw_key[next_byte_pos]) {
    precursor = scan_backward_for_less_than(split_artvalue, insert_key);
  } else {
    precursor = split_artvalue;
  }
  if (FAILED(alloc_newnode(new_node, insert_raw_key + start_byte_pos, shared_prefix))) {
    XENGINE_LOG(ERROR, "failed to init new node");
  } else if (FAILED(new_node->insert(insert_raw_key[next_byte_pos], ARTVALUE2VOID(insert_artvalue)))) {
    XENGINE_LOG(ERROR, "failed to insert insert_artvalue into new node", KP(new_node));
  } else if (FAILED(new_node->insert(split_raw_key[next_byte_pos], ARTVALUE2VOID(split_artvalue)))) {
    XENGINE_LOG(ERROR, "failed to insert split_artvalue into new node", KP(new_node));
  } else if (FAILED(curr_node->update(curr_key, new_node))) {
    XENGINE_LOG(ERROR, "failed to update curr_node", KP(curr_node));
  } else {
    curr_node->wrunlock();
    insert_into_value_list(insert_artvalue, precursor, insert_key);
  }

  return ret;
}

/*
 * This function deals with the case where two artvalues share the same prefix but have no differentiable suffix
 * An example of this case is like below:
 * 
 * New artvalue to be inserted: 'ABCDE'.
 * 
 * Original ART:
 *     ________________
 *    |_prefix_|_'AB'__|
 *    |_key____|__'C'__|  curr node
 *    |_child__|___/___| 
 *                /       
 *       (artvalue 'ABCDEF')
 * 
 * After insert:
 *     ________________
 *    |_prefix_|_'AB'__|
 *    |_key____|__'C'__|  curr node
 *    |_child__|___\___| 
 *                  \    
 *              _____\__________
 *             |_prefix_|__'D'__|
 *             |_key____|__'E'__|  new node
 *             |_child__|___\___|    
 *                           \
 *                      ______\__________
 *                     |___prefix__|_''__|
 *                     |_value_ptr_|__---|---(artvalue 'ABCDE')
 *                     |____key____|_'F'_|   new node2
 *                     |___child___|__/__|
 *                                   /
 *                           (artvalue 'ABCDEF')
 */
int ART::split_artvalue_with_no_differentiable_suffix(Slice &insert_key, ARTValue *insert_artvalue, 
                                                uint32_t start_byte_pos, uint32_t shared_prefix, 
                                                Slice &split_key, ARTValue *split_artvalue, 
                                                uint8_t curr_key, ARTNodeBase *curr_node) {
  int ret = Status::kOk;
  uint32_t next_byte_pos = start_byte_pos + shared_prefix;
  const uint8_t *insert_raw_key = reinterpret_cast<const uint8_t *>(insert_key.data());
  const uint8_t *split_raw_key = reinterpret_cast<const uint8_t *>(split_key.data());
  ARTValue *precursor = nullptr;

  std::function<void(const uint8_t *shorter_raw_key, const uint8_t *longer_raw_key, 
      ARTValue *shorter_artvalue, ARTValue *longer_artvalue)> do_actual_split 
      = [&](const uint8_t *shorter_raw_key, const uint8_t *longer_raw_key, 
          ARTValue *shorter_artvalue, ARTValue *longer_artvalue) {
    if (shared_prefix == 0) {
      ARTNodeBase *new_node = nullptr;
      if (FAILED(alloc_newnode(new_node, nullptr, 0))) {
        XENGINE_LOG(ERROR, "failed to init new node");
      } else if (new_node->insert(longer_raw_key[next_byte_pos], ARTVALUE2VOID(longer_artvalue))) {
        XENGINE_LOG(ERROR, "failed to insert longer artvalue into new node", KP(new_node));
      } else if (curr_node->update(curr_key, new_node)) {
        XENGINE_LOG(ERROR, "failed to update curr_node", KP(curr_node));
      } else {
        new_node->set_value_ptr(ARTVALUE2VOID(shorter_artvalue));
        curr_node->wrunlock();
        insert_into_value_list(insert_artvalue, precursor, insert_key);
      }
    } else {
      ARTNodeBase *new_node = nullptr;
      ARTNodeBase *new_node2 = nullptr;
      if (FAILED(alloc_newnode(new_node, longer_raw_key + start_byte_pos, shared_prefix - 1))) {
        XENGINE_LOG(ERROR, "failed to init new node");
      } else if (FAILED(alloc_newnode(new_node2, nullptr, 0))) {
        XENGINE_LOG(ERROR, "failed to init new node2");
      } else if (FAILED(curr_node->update(curr_key, new_node))) {
        XENGINE_LOG(ERROR, "failed to update curr_node", KP(curr_node));
      } else if (FAILED(new_node->insert(longer_raw_key[next_byte_pos - 1], new_node2))) {
        XENGINE_LOG(ERROR, "failed to insert new node2 into new node", KP(new_node));
      } else if (FAILED(new_node2->insert(longer_raw_key[next_byte_pos], ARTVALUE2VOID(longer_artvalue)))) {
        XENGINE_LOG(ERROR, "failed to insert longer artvalue into new node2", KP(new_node2));
      } else {
        new_node2->set_value_ptr(ARTVALUE2VOID(shorter_artvalue));
        curr_node->wrunlock();
        insert_into_value_list(insert_artvalue, precursor, insert_key);
      }
    }
  };

  if (insert_key.size() > split_key.size()) {
    precursor = split_artvalue;
    do_actual_split(split_raw_key, insert_raw_key, split_artvalue, insert_artvalue);
  } else {
    precursor = scan_backward_for_less_than(split_artvalue, insert_key);
    do_actual_split(insert_raw_key, split_raw_key, insert_artvalue, split_artvalue);
  }

  return ret;
}

/*
 * This function deals with the case which doesn't need to split the artvalue.
 * An example of this case is like below:
 * 
 * New artvalue to be inserted: 'ABCDF seq:1'.
 * 
 * Original ART:
 *     ________________
 *    |_prefix_|_'AB'__|
 *    |_key____|__'C'__|  curr node
 *    |_child__|___/___| 
 *                /       
 *     (artvalue 'ABCDF seq:0')
 * 
 * After insert:
 *     ________________
 *    |_prefix_|_'AB'__|
 *    |_key____|__'C'__|  curr node
 *    |_child__|___/___| 
 *                /       
 *     (artvalue 'ABCDF seq:1')<--->(artvalue 'ABCDF seq:0')
 */
int ART::insert_with_no_split_artvalue(Slice &insert_key, ARTValue *insert_artvalue, Slice &split_key, ARTValue *split_artvalue, 
                                uint8_t curr_key, ARTNodeBase *curr_node) {
  int ret = Status::kOk;

  uint64_t insert_seq = DecodeFixed64(insert_key.data() + insert_key.size() - 8);
  uint64_t split_seq = DecodeFixed64(split_key.data() + split_key.size() - 8);
  if (insert_seq > split_seq) {
    ARTValue *precursor = scan_backward_for_less_than(split_artvalue, insert_key);

    if (FAILED(curr_node->update(curr_key, ARTVALUE2VOID(insert_artvalue)))) {
      XENGINE_LOG(ERROR, "failed to update curr node", KP(curr_node));
    } else {
      curr_node->wrunlock();
      insert_into_value_list(insert_artvalue, precursor, insert_key);
    }
  } else {
    ARTValue *precursor = split_artvalue;
    curr_node->wrunlock();
    insert_into_value_list(insert_artvalue, precursor, insert_key);
  }

  return ret;
}

int ART::insert_inner(ARTValue *insert_artvalue) {
  int ret = Status::kOk;
  Slice insert_key = insert_artvalue->key();
  const uint8_t *insert_raw_key = reinterpret_cast<const uint8_t *>(insert_key.data());
  uint32_t insert_raw_key_len = insert_key.size();
  uint32_t user_key_len = insert_raw_key_len - 8;
  uint32_t byte_pos = 0;

  ARTNodeBase *curr_node = nullptr;
  ARTNodeBase *parent_node = nullptr;
  void *next_node = root_;
  uint8_t parent_key = 0;
  uint8_t curr_key = 0;
  uint64_t parent_version_snapshot = 0;
  uint64_t curr_version_snapshot = 0;
  while (ret == Status::kOk) {
    parent_key = curr_key;
    parent_node = curr_node;
    parent_version_snapshot = curr_version_snapshot;
    curr_node = VOID2INNERNODE(next_node);
    // get read lock before accessing the node
    if (FAILED(curr_node->try_rdlock(curr_version_snapshot))) {
      continue;
    }
    // Compare the raw key with the prefix of the curr node from byte pos
    uint32_t next_byte_pos = byte_pos;
    int cmp = curr_node->check_prefix(insert_raw_key, next_byte_pos, user_key_len);
    if (cmp == 0 && next_byte_pos < user_key_len) {
      curr_key = insert_raw_key[next_byte_pos];
      next_node = curr_node->get(curr_key);
      if (FAILED(curr_node->check_version(curr_version_snapshot))) {
        continue;
      }
      if (next_node == nullptr) {
        ret = insert_maybe_overflow(curr_key, insert_artvalue, curr_node, curr_version_snapshot, 
                                    parent_key, parent_node, parent_version_snapshot);
        break;
      } else if (IS_ARTVALUE(next_node)) {
        ret = insert_maybe_split_artvalue(insert_key, insert_artvalue, next_byte_pos + 1, curr_key, curr_node, curr_version_snapshot,
                                          parent_node, parent_version_snapshot);
        break;
      }
    } else if (cmp == 0) {
      ret = split_prefix_with_no_differentiable_suffix(insert_key, byte_pos, next_byte_pos - byte_pos, insert_artvalue,
                                                        curr_node, curr_version_snapshot, parent_key, parent_node, parent_version_snapshot);
      break;
    } else {
      ret = split_prefix_with_normal_case(insert_key, byte_pos, next_byte_pos - byte_pos, insert_artvalue, 
                                          curr_node, curr_version_snapshot, parent_key, parent_node, parent_version_snapshot);
      break;
    }

    // unlock the parent node before go to next level
    if (parent_node != nullptr && FAILED(parent_node->rdunlock(parent_version_snapshot))) {
      continue;
    }
    byte_pos = next_byte_pos + 1;
  }
  return ret;
}

int ART::estimate_lower_bound_count_inner(const Slice &target, int64_t &count) const {
  int ret = Status::kOk;
  const uint8_t *target_raw_key = reinterpret_cast<const uint8_t *>(target.data());
  uint32_t target_raw_key_len = target.size();
  uint32_t user_key_len = target_raw_key_len - 8;
  uint32_t byte_pos = 0;
  double total_percent = 0.0;
  double remain_percent = 1.0;

  ARTNodeBase *curr_node = nullptr;
  ARTNodeBase *parent_node = nullptr;
  void *next_node = root_;
  uint64_t curr_version_snapshot = 0;
  uint64_t parent_version_snapshot = 0;
  while (ret == Status::kOk) {
    parent_node = curr_node;
    curr_node = VOID2INNERNODE(next_node);
    parent_version_snapshot = curr_version_snapshot;
    if (FAILED(curr_node->try_rdlock(curr_version_snapshot))) {
      continue;
    }
    if (parent_node != nullptr && FAILED(parent_node->rdunlock(parent_version_snapshot))) {
      continue;
    }
    uint32_t next_byte_pos = byte_pos;
    int cmp = curr_node->check_prefix(target_raw_key, next_byte_pos, user_key_len);
    if (cmp == 0 && next_byte_pos < user_key_len) {
      double less_than_percent = 0.0;
      next_node = curr_node->get_with_less_than_percent(target_raw_key[next_byte_pos], less_than_percent);
      if (FAILED(curr_node->check_version(curr_version_snapshot))) {
        continue;
      } else if (next_node == nullptr || IS_ARTVALUE(next_node)) {
        total_percent += remain_percent * less_than_percent;
        break;
      } else {
        byte_pos = next_byte_pos + 1;
        total_percent += remain_percent * less_than_percent;
        remain_percent *= (1.0 / curr_node->count());
      }
    } else if (cmp <= 0) {
      break;
    } else {
      total_percent += remain_percent;
      break;
    }
  }

  if (SUCCED(ret)) {
    count = num_entries_.load() * total_percent;
  }
  return ret;
}

int ART::lower_bound_inner(const Slice &target, ARTValue *&smallest_successor) const {
  int ret = Status::kOk;
  const uint8_t *target_raw_key = reinterpret_cast<const uint8_t *>(target.data());
  uint32_t target_raw_key_len = target.size();
  uint32_t user_key_len = target_raw_key_len - 8;
  uint32_t byte_pos = 0;

  ARTNodeBase *curr_node = nullptr;
  ARTNodeBase *parent_node = nullptr;
  void *next_node = root_;
  uint64_t curr_version_snapshot = 0;
  uint64_t parent_version_snapshot = 0;
  while (ret == Status::kOk) {
    parent_node = curr_node;
    curr_node = VOID2INNERNODE(next_node);
    parent_version_snapshot = curr_version_snapshot;
    if (FAILED(curr_node->try_rdlock(curr_version_snapshot))) {
      continue;
    }
    if (parent_node != nullptr && FAILED(parent_node->rdunlock(parent_version_snapshot))) {
      continue;
    }
    uint32_t next_byte_pos = byte_pos;
    int cmp = curr_node->check_prefix(target_raw_key, next_byte_pos, user_key_len);
    if (cmp == 0 && next_byte_pos < user_key_len) {
      next_node = curr_node->get(target_raw_key[next_byte_pos]);
      if (FAILED(curr_node->check_version(curr_version_snapshot))) {
        continue;
      } else if (next_node == nullptr) {
        ARTValue *precursor = nullptr;
        void *child_less_than_curr = curr_node->less_than(target_raw_key[next_byte_pos]);
        if (FAILED(curr_node->check_version(curr_version_snapshot))) {
          continue;
        } else if (child_less_than_curr == nullptr) {
          void *minimum_child = curr_node->minimum();
          if (FAILED(curr_node->check_version(curr_version_snapshot))) {
            continue;
          } else if (ISNULL(minimum_child)) {
            smallest_successor = scan_forward_for_greater_than_or_equal(dummy_node_.next(), target);
            break;
          } else if (FAILED(smallest_artvalue_in_subtrie(minimum_child, precursor, curr_node, curr_version_snapshot))) {
            continue;
          } else {
            smallest_successor = scan_backward_for_greater_than_or_equal(precursor, target);
            break;
          }
        } else if (FAILED(largest_artvalue_in_subtrie(child_less_than_curr, precursor, curr_node, curr_version_snapshot))) {
          continue;
        } else {
          smallest_successor = scan_forward_for_greater_than_or_equal(precursor, target);
          break;
        }
      } else if (IS_ARTVALUE(next_node)) {
        smallest_successor = scan_forward_for_greater_than_or_equal(VOID2ARTVALUE(next_node), target);
        break;
      } else {
        byte_pos = next_byte_pos + 1;
      }
    } else if (cmp <= 0) {
      ARTValue *successor = nullptr;
      next_node = curr_node->minimum();
      uint16_t prefix_len = curr_node->prefix_len();
      bool has_value_ptr = curr_node->has_value_ptr();
      if (FAILED(curr_node->check_version(curr_version_snapshot))) {
        continue;
      } else if (FAILED(smallest_artvalue_in_subtrie(next_node, successor, curr_node, curr_version_snapshot))) {
        continue;
      } else {
        if (prefix_len == 0 && has_value_ptr) {
          if (cmp_(successor->entry(), target) < 0) {
            smallest_successor = scan_forward_for_greater_than_or_equal(successor, target);
          } else {
            smallest_successor = successor;
          }
        } else {
          smallest_successor = scan_backward_for_greater_than_or_equal(successor, target);
        }
        break;
      }
    } else {
      ARTValue *precursor = nullptr;
      next_node = curr_node->maximum();
      if (FAILED(curr_node->check_version(curr_version_snapshot))) {
        continue;
      } else if (FAILED(largest_artvalue_in_subtrie(next_node, precursor, curr_node, curr_version_snapshot))) {
        continue;
      } else {
        smallest_successor = scan_forward_for_greater_than_or_equal(precursor, target);
        break;
      }
    }
  }

  return ret;
}

int ART::init() {
  int ret = Status::kOk;
  if (ISNULL(root_ = MOD_NEW_OBJECT(ModId::kMemtable, ARTNode256))) { // TODO(nanlong.ynl): use memory pool for alloc
    XENGINE_LOG(ERROR, "failed to alloc memory for root", KP(root_));
  } else if (FAILED(root_->init(nullptr, 0))) {
    XENGINE_LOG(ERROR, "failed to init root");
  } else {
    dummy_node_.set_next(&dummy_node_);
    dummy_node_.set_prev(&dummy_node_);
    dummy_node_.commit();
    memory_usage_.fetch_add(root_->memory_usage());
  }
  return ret;
}

void ART::insert(ARTValue *art_value) {
  ENTER_CRITICAL;
  int ret = Status::kTryAgain;
  int restart_count = 0;
  if (ISNULL(art_value)) {
    XENGINE_LOG(ERROR, "invalid argument");
    ret = Status::kInvalidArgument;
  } else {
    while (ret == Status::kTryAgain) {
      if (FAILED(insert_inner(art_value))) {
        yield(restart_count++);
      }
    }
  }
  if (FAILED(ret)) {
    XENGINE_LOG(ERROR, "failed to insert artvalue", KP(art_value));
    abort();
  }
  num_entries_.fetch_add(1);
  LEAVE_CRITICAL;
}

void ART::lower_bound(const Slice &target, ARTValue *&smallest_successor) const {
  ENTER_CRITICAL;
  int ret = Status::kTryAgain;
  int restart_count = 0;
  while (ret == Status::kTryAgain) {
    if (FAILED(lower_bound_inner(target, smallest_successor))) {
      yield(restart_count++);
    }
  }
  if (FAILED(ret)) {
    XENGINE_LOG(ERROR, "failed to find lower bound", K(target));
    abort();
  }
  LEAVE_CRITICAL;
}

void ART::estimate_lower_bound_count(const Slice &target, int64_t &count) const {
  ENTER_CRITICAL;
  int ret = Status::kTryAgain;
  int restart_count = 0;
  while (ret == Status::kTryAgain) {
    if (FAILED(estimate_lower_bound_count_inner(target, count))) {
      yield(restart_count++);
    }
  }
  if (FAILED(ret)) {
    XENGINE_LOG(ERROR, "failed to estimate count", K(target));
    abort();
  }
  LEAVE_CRITICAL;
}

uint64_t ART::estimate_count(const Slice &start_ikey, const Slice &end_ikey) const {
  int64_t less_than_start_count = 0;
  int64_t less_than_end_count = 0;
  estimate_lower_bound_count(start_ikey, less_than_start_count);
  estimate_lower_bound_count(end_ikey, less_than_end_count);
  if (less_than_start_count > less_than_end_count) {
    XENGINE_LOG(WARN, "Maybe somthing wrong with estimate count", K(start_ikey), K(end_ikey));
    return 0;
  } else {
    return less_than_end_count - less_than_start_count;
  }
}

bool ART::contains(const char *target) const {
  ARTValue *result = nullptr;
  Slice key = GetLengthPrefixedSlice(target);
  lower_bound(key, result);
  if (result != &dummy_node_ && cmp_(result->entry(), key) == 0) {
    return true;
  } else {
    return false;
  }
}

/*
 * This function is only used when debug.
 * This function is not thread safe.
 */
void ART::dump_art_struct() {
  static int64_t dump_count = 0;
  FILE *dump_fp = fopen("/tmp/art_struct.log", "a+");
  std::queue<ARTNodeBase *> bfs_queue;
  int64_t last_node_id = 0;
  int64_t last_allocated_node_id = 0;
  int64_t level = 0;

  fprintf(dump_fp, "DUMP#%ld\n", dump_count++);
  bfs_queue.push(root_);
  last_allocated_node_id = 1;
  while (!bfs_queue.empty()) {
    fprintf(dump_fp, "LEVEL#%ld\n", level);
    for (int32_t i = bfs_queue.size() - 1; i >= 0; i--) {
      ARTNodeBase *node = bfs_queue.front();
      node->dump_struct_info(dump_fp, level, last_node_id, last_allocated_node_id, bfs_queue);
      bfs_queue.pop();
    }
    level++;
    fsync(fileno(dump_fp));
  }

  fclose(dump_fp);
}

} // namespace memtable
} // namespace xengine
