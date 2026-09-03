/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: common macro defines
 */
#pragma once

#ifndef INT4_LENGTH
#define INT4_LENGTH sizeof(int)
#endif

#ifndef INT8_LENGTH
#define INT8_LENGTH sizeof(int64_t)
#endif

#ifndef FLOAT4_LENGTH
#define FLOAT4_LENGTH sizeof(float)
#endif

#ifndef DOUBLE_LENGTH
#define DOUBLE_LENGTH sizeof(double)
#endif

#ifndef TIMESTAMP
#define TIMESTAMP int64_t
#endif

#ifndef TIMESTAMP_LENGTH
#define TIMESTAMP_LENGTH sizeof(TIMESTAMP)
#endif

#ifndef YEAR_LENGTH
#define YEAR_LENGTH INT4_LENGTH
#endif

/*
 * Limit on the precision (and hence scale) specifiable in a NUMERIC typmod.
 * Note that the implementation limit on the length of a numeric value is
 * much larger --- beware of what you use this for!
 */
#ifndef NUMERIC_MAX_PRECISION
#define NUMERIC_MAX_PRECISION 1000
#endif

#ifndef NUMERIC_MAX_RESULT_SCALE
#define NUMERIC_MAX_RESULT_SCALE (NUMERIC_MAX_PRECISION * 2)
#endif

// typmod = ((PRECISION << 16) | SCALE) + VARHDRS
#ifndef NUMERIC_MAX_TYPMOD
#define NUMERIC_MAX_TYPMOD ((NUMERIC_MAX_PRECISION << 16) + 4)
#endif

#ifndef MAX
#define MAX(x, y) ((x) > (y) ? (x) : (y))
#endif

#ifndef MIN
#define MIN(x, y) ((x) < (y) ? (x) : (y))
#endif

#ifndef decltype_of_member
#define decltype_of_member(type, field) decltype(((type *)0)->field)
#endif
#ifndef size_of_member
#define size_of_member(type, field) sizeof(((type *)0)->field)
#endif

// max length (in bytes) of class bytes_t
#ifndef BYTES_T_MAX_LENGTH
#define BYTES_T_MAX_LENGTH 32
#endif

#ifndef TEXT_INIT_LENGTH
#define TEXT_INIT_LENGTH 1024
#endif

#ifndef ENCDB_KEYNAME_VERSION
#define ENCDB_KEYNAME_VERSION "v2"
#endif
