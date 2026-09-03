/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: definition for sort tuple
 */
#pragma once
#include <stdint.h>
#include "encdb_error.h"

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct EncdbInputTuple {
  int natts;     // number of columns
  int tupindex;  // input tape number

  // Effective when natts == 1
  bool isnull1;     // is the first column NULL?
  int size1;        // byte length of the first column.
  uintptr_t data1;  // value of the first column. Don't mix up with PG's Datum

  // Effective when natts > 1
  bool *isnull;     // is the column NULL?
  int *size;        // byte length of column
  uintptr_t *data;  // column value

} EncdbInputTuple;

typedef struct EncdbSortSupportData {
  uint8_t ssup_type;      // data type. Same semantic as encdb::EncType
  bool ssup_reverse;      // descending-order sort?
  bool ssup_nulls_first;  // sort nulls first?
} EncdbSortSupportData;

typedef EncdbSortSupportData *EncdbSortSupport;

typedef struct EncdbInputRun {
  EncdbInputTuple *tuple;  // array of tuples
  int nTuples;             // number of elements currently stored*/
  int offset;              // index of next tuple to extract
  bool done;               // true if source is known exhausted
} EncdbInputRun;

#if defined(__cplusplus)
}
#endif
