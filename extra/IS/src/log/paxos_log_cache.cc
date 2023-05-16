/************************************************************************
 *
 * Copyright (c) 2017 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos_log_cache.cc,v 1.0 01/16/2017 07:03:27 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos_log_cache.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 01/16/2017 07:03:27 PM
 * @version 1.0
 * @brief paxos log cache
 *
 **/

#include "paxos_log_cache.h"

namespace alisql {

void PaxosLogCache::put(
    uint64_t beginIndex, uint64_t endIndex,
    ::google::protobuf::RepeatedPtrField<LogEntry> &entries) {
  put(new PaxosLogCacheNode(beginIndex, endIndex, entries));
}

void PaxosLogCache::put(PaxosLogCacheNode *newNode) {
  for (auto it = logCache_.begin(); it != logCache_.end();) {
    PaxosLogCacheNode *node = *it;
    if (node->endIndex < newNode->beginIndex) {
      if (node->endIndex + 1 != newNode->beginIndex) {
        ++it;
        continue;
      } else {
        // continue append case
        byteSize_ -= node->getByteSize();
        node->entries.MergeFrom(newNode->entries);
        node->endIndex = newNode->endIndex;

        delete newNode;
        it = logCache_.erase(it);
        newNode = node;
        continue;
      }
    } else if (node->beginIndex > newNode->endIndex) {
      if (node->beginIndex != newNode->endIndex + 1) {
        byteSize_ += newNode->getByteSize();
        logCache_.insert(it, newNode);
        return;
      } else {
        // continue preppend case
        byteSize_ += newNode->getByteSize();
        newNode->entries.MergeFrom(node->entries);
        newNode->endIndex = node->endIndex;

        delete node;
        *it = newNode;
        return;
      }
    } else {
      // overlap case, need merge.
      int bs = (*it)->getByteSize();
      PaxosLogCacheNode *leftNode = node, *rightNode = newNode;

      if (node->beginIndex > newNode->beginIndex) {
        /*
        uint64_t tmp;
        node->entries.Swap(&(newNode->entries));
        tmp= newNode->beginIndex;
        newNode->beginIndex= node->beginIndex;
        node->beginIndex= tmp;
        tmp= newNode->endIndex;
        newNode->endIndex= node->endIndex;
        node->endIndex= tmp;
        */
        leftNode = newNode;
        rightNode = node;
      } else {
        // node->beginIndex <= newNode->beginIndex
      }

      assert(leftNode->beginIndex <= rightNode->beginIndex);
      assert(static_cast<uint64_t>(leftNode->entries.size()) ==
             (leftNode->endIndex - leftNode->beginIndex + 1));
      assert(static_cast<uint64_t>(rightNode->entries.size()) ==
             (rightNode->endIndex - rightNode->beginIndex + 1));

      if (leftNode->endIndex < rightNode->endIndex) {
        // exclude case
        rightNode->entries.DeleteSubrange(
            0, leftNode->endIndex - rightNode->beginIndex + 1);
        leftNode->entries.MergeFrom(rightNode->entries);
        leftNode->endIndex = rightNode->endIndex;
      } else {
        // include case
        if (node == leftNode) {
          delete rightNode;
          return;
        }
      }

      delete rightNode;
      byteSize_ -= bs;
      it = logCache_.erase(it);
      newNode = leftNode;
    }
  }
  byteSize_ += newNode->getByteSize();
  logCache_.push_back(newNode);
}

PaxosLogCacheNode *PaxosLogCache::get(uint64_t beginIndex) {
  for (auto it = logCache_.begin(); it != logCache_.end();) {
    PaxosLogCacheNode *node = *it;
    if (node->beginIndex == beginIndex) {
      byteSize_ -= (*it)->getByteSize();
      logCache_.erase(it);
      return node;
    } else if (node->beginIndex < beginIndex) {
      if (node->endIndex < beginIndex) {
        byteSize_ -= (*it)->getByteSize();
        it = logCache_.erase(it);
        delete node;
      } else {
        byteSize_ -= (*it)->getByteSize();
        logCache_.erase(it);
        node->entries.DeleteSubrange(0, beginIndex - node->beginIndex);
        node->beginIndex = beginIndex;
        return node;
      }
    } else {
      // node->beginIndex > beginIndex
      return NULL;
    }
  }
  return NULL;
}

void PaxosLogCache::clear() {
  for (auto &it : logCache_) delete it;
  byteSize_ = 0;
  logCache_.clear();
}

PaxosLogCacheNode *PaxosLogCache::debugGet(uint64_t i) {
  uint64_t j = 0;
  for (auto &it : logCache_) {
    if (j == i) return it;

    ++j;
  }
  return NULL;
}

int PaxosLogCache::debugGetByteSize() {
  int ret = 0;
  for (auto &it : logCache_) {
    ret += it->getByteSize();
  }
  return ret;
}

}  // namespace alisql
