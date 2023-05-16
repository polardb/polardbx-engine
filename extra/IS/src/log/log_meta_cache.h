/************************************************************************
 *
 * Copyright (c) 2019 Alibaba.com, Inc. All Rights Reserved
 * $Id:  log_meta_cache.h,v 1.0 07/13/2019 17:27:00 PM
 *aili.xp(aili.xp@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file log_meta_cache.h
 * @author aili.xp(aili.xp@alibaba-inc.com)
 * @date 07/13/2019 17:27:00 PM
 * @version 1.0
 * @brief paxos log meta cache
 *
 **/

#ifndef log_meta_cache_INC
#define log_meta_cache_INC

#include <cstddef>
#include <cstdint>
#include <vector>

namespace alisql {

class LogMetaCache {
 public:
  LogMetaCache();

  void init();

  void reset();

  // return false if `index` is not continuous
  bool putLogMeta(uint64_t index, uint64_t term, uint64_t optype,
                  uint64_t info);

  // return false if `index` is not included in the cache
  bool getLogMeta(uint64_t index, uint64_t *term, uint64_t *optype,
                  uint64_t *info);

 private:
  struct LogMetaEntry {
    uint64_t index;
    uint64_t term;
    uint64_t optype;
    uint64_t info;
  };

  static const size_t MaxEntrySize = 8192;

  uint64_t count_;
  uint64_t left_;
  uint64_t right_;
  uint64_t maxIndex_;
  std::vector<LogMetaEntry> array_;
};

}  // namespace alisql

#endif /* log_meta_cache_INC */
