/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once

#include <assert.h>  // include assert()
#include <cstring>
#include "encdb_error.h"

/*
 * For log
 */

#if defined(__cplusplus)
extern "C" {
#endif

bool encdb_elog(int elevel, const char *filename, int lineno,
                const char *funcname, const char *fmt, ...)
    __attribute__((format(printf, 5, 6)));

#if defined(__cplusplus)
}
#endif

#define __FILENAME__ \
  (strstr(__FILE__, "/src/") ? strstr(__FILE__, "/src/") + 1 : __FILE__)

#define ENCDB_LOG(level, ...)                                              \
  do {                                                                     \
    const int elevel = (level);                                            \
    encdb_elog(elevel, __FILENAME__, __LINE__, __FUNCTION__, __VA_ARGS__); \
  } while (0)

#define REPORT_ENCDB_ERROR(e)                                       \
  ENCDB_LOG(LL_ERROR, "process request message error (0x%x): %s\n", \
            e.get_encdb_status(), e.what());
