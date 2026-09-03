/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: definition for ecall operation code
 */
#pragma once

// clang-format off
enum OpType: unsigned char { // operating object types
    // management objects
    OT_ROOT_KEY = 0x00, OT_DEK, OT_RA, OT_WORKER_KEY, OT_KMS_MEK, OT_KEY_PAIR, OT_ENCLAVE, OT_ACL, OT_NONCE, OT_ENC_RULE,

    // data types
    OT_INT4 = 0x10, OT_FLOAT4, OT_TEXT, OT_TIMESTAMP, OT_INT8, OT_FLOAT8, OT_DECIMAL, OT_ROARINGBITMAP,

    // auxiliary helper
    OT_AUX = 0x30,

    // index
    OT_BTREE = 0x40,

    // full text search
    OT_ZHPARSER = 0x50, OT_FTS_DICT,

    // wildcard
    OT_ANY = 0xFF,
};

enum OpCmd: unsigned char { // operating command
    // management commands
    INIT = 0x00, GET, PUSH, CLEANUP, ENCRYPT, DECRYPT, SIGN, VERIFY, IMPORT, EXPORT, UPDATE, SET, REENCRYPT,

    // common commands
    CMP = 0x20, EQ, AVG, SUM, MIN, MAX, AVG_ARRAY, AVG_STATE, AVG_FINAL, SUM_ARRAY, SUM_STATE, SUM_FINAL, CMP_BATCH, ADD_BATCH,

    // numeric commands
    ADD = 0x40, MINUS, MULTI, DIV, MOD, POW,
    DIV_O,  // for Polar/Oracle int/long divide
    CAST,

    // text commands
    CONCAT = 0x60, LIKE, SUBSTR, CMP_PREFIX,
    SUBSTR_O,  // for Polar/Oracle substr
    PARSE, SET_DICT, ADD_DICT, SET_DICT_SIMPLE, SET_DICT_SYNONYM, LEXIZE_SIMPLE, LEXIZE_SYNONYM,

    // timestamp commands
    EXTRACT = 0x80,

    // auxiliary commands
    PRINT = 0x90, SHARE, TEST, HASH, CONVERT, SORT, PARTIAL_MERGE, EVAL_EXPR,

    // NOTE: deprecated batch operations
    BATCH_HASH = 0xB0, BATCH_CONVERT, BATCH_CMP, BATCH_BINOP,

    // logical commands
    AND = 0xD0, OR, XOR, ANDNOT, AND_CARD, OR_CARD, XOR_CARD, ANDNOT_CARD, BUILD, ITERATE, AND_AGG, OR_AGG,

    // btree commands
    BTREE_COMPARE = 0xE0, BTREE_BIN_SRCH, BTREE_BIN_SRCH_INSERT, BTREE_READ_PAGE, BTREE_UPDATE_OID,

    // wildcard
    ANY = 0xFF,
};
// clang-format on

struct OpCode {
  enum OpType type;
  enum OpCmd cmd;
};
