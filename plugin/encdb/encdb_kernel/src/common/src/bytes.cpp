/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: wrapped bytes: 1) ref to existing bytes without malloc, 2) wrap malloc
 * bytes with std::unique_ptr
 */
#include "bytes.h"
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include "encdb_common_exception.h"
#include "xxh3.h"  // for XXH3_64bits fash-hash from third-party

namespace encdb {

bytes_t::bytes_t(bytes_t &&other) noexcept {
  len = other.len;
  dataPtr = std::move(other.dataPtr);
  dataRef = other.dataRef;
  isRef = other.isRef;

  other.set(nullptr, 0, true);
}

bytes_t::bytes_t(uint32_t nlen) : dataRef(nullptr), len(nlen), isRef(false) {
  try {
    if (nlen == 0) {
      set(nullptr, 0, true);
    } else {
      dataPtr = std::shared_ptr<uint8_t>(new uint8_t[nlen],
                                         std::default_delete<uint8_t[]>());
    }
  } catch (const std::exception &e) {
    throw OutOfMemoryException("try new " + std::to_string(nlen) +
                               " bytes fail: " + e.what());
  }
}

bytes_t::bytes_t(const uint8_t *ndata, uint32_t nlen, bool ref) {
  // init len
  len = 0;
  isRef = ref;
  set(const_cast<uint8_t *>(ndata), nlen, isRef);
}

bytes_t::bytes_t() noexcept { set(nullptr, 0, true); }

bytes_t &bytes_t::operator=(bytes_t &&other) noexcept {
  // avoid self move
  if (this == &other) {
    return *this;
  }

  // free old unique ptr if it is not referenced by itself
  if (dataPtr.get() != other.dataRef) {
    dataPtr.reset();
  }

  // use placement new to avoid allocate
  ::new (this) bytes_t(std::move(other));
  return *this;
}

bytes_t::~bytes_t() {}

/**
 * @brief set bytes_t to given address
 * @param [in]bRef  true - \c ndata can be used as an reference
 *                  false - \c ndata should be used as copy since it may be
 * freed during using
 */
void bytes_t::set(const uint8_t *ndata, uint32_t nlen, bool bRef) {
  if (bRef) {
    dataRef = const_cast<uint8_t *>(ndata);
    len = nlen;
    isRef = true;
    dataPtr = nullptr;
  } else {
    resize(nlen);
    memcpy(data(), ndata, nlen);
  }
}

void bytes_t::fill(const uint8_t val) { memset(data(), val, size()); }

/**
 * @brief whether bytes_t contains data
 */
bytes_t::operator bool() const noexcept {
  if (isRef) {
    return dataRef && len;
  } else if (dataPtr) {
    return dataPtr.get() && len;
  } else {
    return false;
  }
}

uint8_t *bytes_t::data() noexcept {
  if (isRef) {
    return dataRef;
  } else if (dataPtr) {
    return dataPtr.get();
  } else {
    return nullptr;
  }
}

const uint8_t *bytes_t::data() const noexcept {
  if (isRef) {
    return dataRef;
  } else if (dataPtr) {
    return dataPtr.get();
  } else {
    return nullptr;
  }
}

uint32_t bytes_t::size() const noexcept { return len; }

void bytes_t::resize(uint32_t nlen) {
  if (nlen <= len) {
    len = nlen;  // just shrink len
  } else {
    std::shared_ptr<uint8_t> __data = nullptr;
    try {
      __data = std::shared_ptr<uint8_t>(new uint8_t[nlen],
                                        std::default_delete<uint8_t[]>());
    } catch (const std::bad_alloc &e) {
      throw OutOfMemoryException("new[] failure");
    }
    if (isRef) {
      memcpy(__data.get(), dataRef, len);
    } else if (dataPtr) {
      // move old data (if exists) to new address
      memcpy(__data.get(), dataPtr.get(), len);
    }
    dataPtr = std::move(__data);
    len = nlen;
    isRef = false;
    dataRef = nullptr;
  }
}

bool bytes_t::operator==(const bytes_t &other) const {
  return (this->size() == other.size()) &&
         (0 == std::memcmp(this->data(), other.data(), this->size()));
}

bool bytes_t::operator!=(const bytes_t &other) const {
  return !(this->operator==(other));
}

const std::string bytes_t::to_string() const noexcept {
  const char _hextable[] = "0123456789abcdef";
  std::string str = "";
  uint32_t srclen = this->size();
  const uint8_t *src = this->data();

  for (uint32_t i = 0; i < srclen; ++i) {
    str += _hextable[(src[i] >> 4) & 0xf];
    str += _hextable[src[i] & 0xf];
  }

  return str;
}

void bytes_t::swap(bytes_t &rhs) {
  using std::swap;
  swap(len, rhs.len);
  swap(dataPtr, rhs.dataPtr);
  swap(dataRef, rhs.dataRef);
  swap(isRef, rhs.isRef);
}

void swap(bytes_t &lhs, bytes_t &rhs) { lhs.swap(rhs); }

}  // namespace encdb

std::size_t std::hash<encdb::bytes_t>::operator()(
    encdb::bytes_t const &bytes) const noexcept {
  XXH64_hash_t hash = XXH3_64bits(bytes.data(), bytes.size());
  return hash;
#ifdef DISABLED
  std::size_t result = 0;
  const uint8_t *raw_bytes = bytes.data();
  for (size_t i = 0; i < bytes.size() / sizeof(size_t); i++) {
    result ^= *reinterpret_cast<const std::size_t *>(raw_bytes);
    raw_bytes += sizeof(std::size_t);
  }

  // XOR remaining bytes
  uint8_t *result_bytes = reinterpret_cast<uint8_t *>(&result);
  while (raw_bytes < bytes.data() + bytes.size()) {
    *result_bytes ^= *raw_bytes;
    raw_bytes++;
  }

  return result;
#endif  // DISABLED
}
