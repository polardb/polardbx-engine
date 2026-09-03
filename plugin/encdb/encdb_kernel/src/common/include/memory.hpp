#pragma once
#include "encdb_common_exception.h"
#include "logger.h"
#include "storage.h"

// Reload new & delete operator for EncDB globally
namespace encdb {

class EncdbBaseObj {
 public:
  static void *operator new(std::size_t n) noexcept(false) {
    void *ret = encdb_plugin_malloc(n);
    if (ret == NULL) {
      ENCDB_LOG(LL_ERROR, "failed to allocate %lu bytes in %s", n,
                __FUNCTION__);
      throw std::bad_alloc();
    }
    return ret;
  }
  static void operator delete(void *p) noexcept(false) { encdb_plugin_free(p); }
  static void *operator new[](std::size_t s) noexcept(false) {
    void *ret = encdb_plugin_malloc(s);
    if (ret == NULL) {
      ENCDB_LOG(LL_ERROR, "failed to allocate %lu bytes in %s", s,
                __FUNCTION__);
      throw std::bad_alloc();
    }
    return ret;
  }
  static void operator delete[](void *p) noexcept(false) {
    encdb_plugin_free(p);
  }
};

}  // namespace encdb
