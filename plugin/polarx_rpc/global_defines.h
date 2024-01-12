//
// Created by zzy on 2022/9/22.
//

#pragma once

#include "my_config.h"

#if MYSQL_VERSION_MAJOR >= 8
#define MYSQL8
#endif

#if (MYSQL_VERSION_MAJOR > 8) ||                             \
    (MYSQL_VERSION_MAJOR == 8 && MYSQL_VERSION_MINOR > 0) || \
    (MYSQL_VERSION_MAJOR == 8 && MYSQL_VERSION_MINOR == 0 && \
     MYSQL_VERSION_PATCH >= 32)
#define MYSQL8PLUS
#endif
