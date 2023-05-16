#pragma once
#include <stddef.h>
#include <stdint.h>

namespace alisql {

// Return the crc32c of concat(A, data[0,n-1]) where init_crc is the
// crc32c of some string A.  Extend() is often used to maintain the
// crc32c of a stream of data.
extern uint32_t Extend(uint32_t init_crc, const char *data, size_t n);

// Return the crc32c of data[0,n-1]
inline uint32_t calculateCRC32(const char *data, size_t n) {
  return Extend(0, data, n);
}

}  // namespace alisql
