/************************************************************************
 *
 * Copyright (c) 2019 Alibaba.com, Inc. All Rights Reserved
 * $Id:  msg_compress.h,v 1.0 01/24/2019 02:05:00 PM
 *aili.xp(aili.xp@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file msg_compress.h
 * @author aili.xp(aili.xp@alibaba-inc.com)
 * @date 01/24/2019 02:05:00 PM
 * @version 1.0
 * @brief paxos msg compression module
 *
 **/

#ifndef msg_compress_INC
#define msg_compress_INC

#include <atomic>

#include "paxos.pb.h"

namespace alisql {

typedef enum MsgCompressionType {
  None = 0,
  LZ4 = 1,   // very fast but with low compression rate
  ZSTD = 2,  // high compression rate but slower
} MsgCompressionType;

class MsgCompressOption {
 public:
  MsgCompressOption(MsgCompressionType type = None, size_t sizeThreshold = 0,
                    bool checksum = false)
      : type_((int)type), sizeThreshold_(sizeThreshold), checksum_(checksum) {
    if (type < None || type > ZSTD) type_.store((int)None);
    if ((MsgCompressionType)type_.load() != None &&
        sizeThreshold_.load() < 4 * 1024)
      sizeThreshold_.store(
          4 * 1024);  // we don't want the threshold to be too small
    if ((MsgCompressionType)type_.load() != None &&
        sizeThreshold_.load() > 128 * 1024)
      sizeThreshold_.store(128 *
                           1024);  // we don't want the threshold to be too big
  }

  MsgCompressOption(const MsgCompressOption &that)
      : type_(that.type_.load()),
        sizeThreshold_(that.sizeThreshold_.load()),
        checksum_(that.checksum_.load()) {}

  MsgCompressOption &operator=(const MsgCompressOption &that) {
    type_.store(that.type_.load());
    sizeThreshold_.store(that.sizeThreshold_.load());
    checksum_.store(that.checksum_.load());
    return *this;
  }

  MsgCompressionType type() const { return (MsgCompressionType)type_.load(); }
  size_t sizeThreshold() const { return sizeThreshold_.load(); }
  bool checksum() const { return checksum_.load(); }

 private:
  std::atomic<int> type_;
  std::atomic<size_t> sizeThreshold_;
  std::atomic<bool> checksum_;
};

// return how many bytes compressed
size_t msgCompress(const MsgCompressOption &option, PaxosMsg &msg,
                   size_t sizeHint);

// return true if no compression or decompression is successful
bool msgDecompress(PaxosMsg &msg);

}  // namespace alisql

#endif  // msg_compress_INC
