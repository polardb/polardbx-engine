/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "../core/port/likely.h"

#include <stdint.h>

/** Max table name length as defined in univ.i */
#define MAX_TABLE_NAME_LEN      320
#define MAX_DATABASE_NAME_LEN   MAX_TABLE_NAME_LEN
#define MAX_FULL_NAME_LEN                               \
        (MAX_TABLE_NAME_LEN + MAX_DATABASE_NAME_LEN + 14)

/** Representation of a byte within X-Engine */
typedef unsigned char xengine_byte_t;
/** Representation of an unsigned long int within X-Engine */

typedef unsigned long int xengine_ulint_t;

/** A signed 8 bit integral type. */
typedef int8_t xengine_i8_t;

/** An unsigned 8 bit integral type. */
typedef uint8_t xengine_u8_t;

/** A signed 16 bit integral type. */
typedef int16_t xengine_i16_t;

/** An unsigned 16 bit integral type. */
typedef uint16_t xengine_u16_t;

/** A signed 32 bit integral type. */
typedef int32_t xengine_i32_t;

/** An unsigned 32 bit integral type. */
typedef uint32_t xengine_u32_t;

/** A signed 64 bit integral type. */
typedef int64_t xengine_i64_t;

/** An unsigned 64 bit integral type. */
typedef uint64_t xengine_u64_t;

/** Generical X-Engine callback prototype. */
typedef void (*xengine_cb_t)(void);

typedef void*			xengine_opaque_t;
typedef xengine_opaque_t		XENGINE_CHARset_t;
typedef xengine_ulint_t		xengine_bool_t;
typedef xengine_u64_t		xengine_id_u64_t;

enum dberr_t {
	DB_SUCCESS_LOCKED_REC = 9, /*!< like DB_SUCCESS, but a new
					explicit record lock was created */
	DB_SUCCESS = 10,

	/* The following are error codes */
	DB_ERROR,
	DB_INTERRUPTED,
	DB_OUT_OF_MEMORY,
	DB_OUT_OF_FILE_SPACE,
	DB_LOCK_WAIT,
	DB_DEADLOCK,
	DB_ROLLBACK,
	DB_DUPLICATE_KEY,
	DB_QUE_THR_SUSPENDED,
	DB_MISSING_HISTORY, /*!< required history data has been
					deleted due to lack of space in
					rollback segment */
	DB_CLUSTER_NOT_FOUND = 30,
	DB_TABLE_NOT_FOUND,
	DB_MUST_GET_MORE_FILE_SPACE, /*!< the database has to be stopped
					and restarted with more file space */
	DB_TABLE_IS_BEING_USED,
	DB_TOO_BIG_RECORD,					 /*!< a record in an index would not fit
					on a compressed page, or it would
					become bigger than 1/2 free space in
					an uncompressed page frame */
	DB_LOCK_WAIT_TIMEOUT,				 /*!< lock wait lasted too long */
	DB_NO_REFERENCED_ROW,				 /*!< referenced key value not found
					for a foreign key in an insert or
					update of a row */
	DB_ROW_IS_REFERENCED,				 /*!< cannot delete or update a row
					because it contains a key value
					which is referenced */
	DB_CANNOT_ADD_CONSTRAINT,		 /*!< adding a foreign key constraint
					to a table failed */
	DB_CORRUPTION,							 /*!< data structure corruption
					noticed */
	DB_CANNOT_DROP_CONSTRAINT,	 /*!< dropping a foreign key constraint
					from a table failed */
	DB_NO_SAVEPOINT,						 /*!< no savepoint exists with the given
					name */
	DB_TABLESPACE_EXISTS,				 /*!< we cannot create a new single-table
					tablespace because a file of the same
					name already exists */
	DB_TABLESPACE_DELETED,			 /*!< tablespace was deleted or is
					being dropped right now */
	DB_TABLESPACE_NOT_FOUND,		 /*<! Attempt to delete a tablespace
					instance that was not found in the
					tablespace hash table */
	DB_LOCK_TABLE_FULL,					 /*!< lock structs have exhausted the
					buffer pool (for big transactions,
					InnoDB stores the lock structs in the
					buffer pool) */
	DB_FOREIGN_DUPLICATE_KEY,		 /*!< foreign key constraints
					activated by the operation would
					lead to a duplicate key in some
					table */
	DB_TOO_MANY_CONCURRENT_TRXS, /*!< when InnoDB runs out of the
					preconfigured undo slots, this can
					only happen when there are too many
					concurrent transactions */
	DB_UNSUPPORTED,							 /*!< when InnoDB sees any artefact or
					a feature that it can't recoginize or
					work with e.g., FT indexes created by
					a later version of the engine. */

	DB_INVALID_NULL, /*!< a NOT NULL column was found to
					be NULL during table rebuild */

	DB_STATS_DO_NOT_EXIST,				 /*!< an operation that requires the
					persistent storage, used for recording
					table and index statistics, was
					requested but this storage does not
					exist itself or the stats for a given
					table do not exist */
	DB_FOREIGN_EXCEED_MAX_CASCADE, /*!< Foreign key constraint related
					cascading delete/update exceeds
					maximum allowed depth */
	DB_CHILD_NO_INDEX,						 /*!< the child (foreign) table does
					not have an index that contains the
					foreign keys as its prefix columns */
	DB_PARENT_NO_INDEX,						 /*!< the parent table does not
					have an index that contains the
					foreign keys as its prefix columns */
	DB_TOO_BIG_INDEX_COL,					 /*!< index column size exceeds
					maximum limit */
	DB_INDEX_CORRUPT,							 /*!< we have corrupted index */
	DB_UNDO_RECORD_TOO_BIG,				 /*!< the undo log record is too big */
	DB_READ_ONLY,									 /*!< Update operation attempted in
					a read-only transaction */
	DB_FTS_INVALID_DOCID,					 /* FTS Doc ID cannot be zero */
	DB_TABLE_IN_FK_CHECK,					 /* table is being used in foreign
					key check */
	DB_ONLINE_LOG_TOO_BIG,				 /*!< Modification log grew too big
					during online index creation */

	DB_IDENTIFIER_TOO_LONG,						/*!< Identifier name too long */
	DB_FTS_EXCEED_RESULT_CACHE_LIMIT, /*!< FTS query memory
					exceeds result cache limit */
	DB_TEMP_FILE_WRITE_FAIL,					/*!< Temp file write failure */
	DB_CANT_CREATE_GEOMETRY_OBJECT,		/*!< Cannot create specified Geometry
					data object */
	DB_CANNOT_OPEN_FILE,							/*!< Cannot open a file */
	DB_FTS_TOO_MANY_WORDS_IN_PHRASE,
	/*< Too many words in a phrase */

	DB_TABLESPACE_TRUNCATED, /*!< tablespace was truncated */

	DB_IO_ERROR = 100, /*!< Generic IO error */

	DB_IO_DECOMPRESS_FAIL, /*!< Failure to decompress a page
					after reading it from disk */

	DB_IO_NO_PUNCH_HOLE, /*!< Punch hole not supported by
					InnoDB */

	DB_IO_NO_PUNCH_HOLE_FS, /*!< The file system doesn't support
					punch hole */

	DB_IO_NO_PUNCH_HOLE_TABLESPACE, /*!< The tablespace doesn't support
					punch hole */

	DB_IO_DECRYPT_FAIL, /*!< Failure to decrypt a page
					after reading it from disk */

	DB_IO_NO_ENCRYPT_TABLESPACE, /*!< The tablespace doesn't support
					encrypt */

	DB_IO_PARTIAL_FAILED, /*!< Partial IO request failed */

	DB_FORCED_ABORT, /*!< Transaction was forced to rollback
					by a higher priority transaction */

	DB_TABLE_CORRUPT, /*!< Table/clustered index is
					corrupted */

	DB_WRONG_FILE_NAME, /*!< Invalid Filename */

	DB_COMPUTE_VALUE_FAILED, /*!< Compute generated value failed */
	DB_NO_FK_ON_S_BASE_COL,	/*!< Cannot add foreign constrain
					placed on the base column of
					stored column */

	/* The following are partial failure codes */
	DB_FAIL = 1000,
	DB_OVERFLOW,
	DB_UNDERFLOW,
	DB_STRONG_FAIL,
	DB_ZIP_OVERFLOW,
	DB_RECORD_NOT_FOUND = 1500,
	DB_END_OF_INDEX,
	DB_NOT_FOUND, /*!< Generic error code for "Not found"
					type of errors */

	/* The following are API only error codes. */
	DB_DATA_MISMATCH = 2000, /*!< Column update or read failed
					because the types mismatch */

	DB_SCHEMA_NOT_LOCKED, /*!< If an API function expects the
					schema to be locked in exclusive mode
					and if it's not then that API function
					will return this error code */

	DB_SNAPSHOT_ERROR, /* Secondary read needs a snapshot, if not returh this */

	DB_UNPACK_ERROR, /* Failed to unpack record */

	DB_GET_ERROR, /* Failed to call DB::Get */
	
	/* The following are only for x_protocol */
	DB_X_PROTOCOL_WRONG_VERSION = 3000,
	DB_X_PROTOCOL_EINVAL,
	DB_X_PROTOCOL_INVISIBLE_CONFIG,
	DB_RES_OVERFLOW,
	/* meta information in container is invalid */
	DB_META_INVAL
};

#define XENGINE_SQL_NULL 0xFFFFFFFF
#define XENGINE_CFG_BINLOG_ENABLED 0x1
#define XENGINE_CFG_MDL_ENABLED 0x2
#define XENGINE_CFG_DISABLE_ROWLOCK 0x4

/** @enum xengine_col_type_t  column types that are supported. */
typedef enum xengine_col_enum {
  XENGINE_VARCHAR = 1, /* Character varying length. The
					column is not padded. */

  XENGINE_CHAR = 2, /* Fixed length character string. The
					column is padded to the right. */

  XENGINE_BINARY = 3, /* Fixed length binary, similar to
					XENGINE_CHAR but the column is not padded
					to the right. */

  XENGINE_VARBINARY = 4, /* Variable length binary */

  XENGINE_BLOB = 5, /* Binary large object, or
					a TEXT type */

  XENGINE_INT = 6, /* Integer: can be any size
					from 1 - 8 bytes. If the size is
					1, 2, 4 and 8 bytes then you can use
					the typed read and write functions. For
					other sizes you will need to use the
					xengine_col_get_value() function and do the
					conversion yourself. */

  XENGINE_SYS = 8, /* System column, this column can
					be one of DATA_TRX_ID, DATA_ROLL_PTR
					or DATA_ROW_ID. */

  XENGINE_FLOAT = 9, /* C (float)  floating point value. */

  XENGINE_DOUBLE = 10, /* C (double) floating point value. */

  XENGINE_DECIMAL = 11, /* Decimal stored as an ASCII string */

  XENGINE_VARCHAR_ANYCHARSET = 12, /* Any charset, varying length */

  XENGINE_CHAR_ANYCHARSET = 13, /* Any charset, fixed length */

	XENGINE_DATETIME = 14,

	XENGINE_DATETIME2 = 15,

	XENGINE_NEWDECIMAL = 16
} xengine_col_type_t;

/** @enum xengine_col_attr_t XDB column attributes */
typedef enum {
  XENGINE_COL_NONE = 0, /* No special attributes. */

  XENGINE_COL_NOT_NULL = 1, /* Column data can't be NULL. */

  XENGINE_COL_UNSIGNED = 2, /* Column is XENGINE_INT and unsigned. */

  XENGINE_COL_NOT_USED = 4, /* Future use, reserved. */

  XENGINE_COL_CUSTOM1 = 8, /* Custom precision type, this is
					a bit that is ignored by XDB and so
					can be set and queried by users. */

  XENGINE_COL_CUSTOM2 = 16, /* Custom precision type, this is
					a bit that is ignored by XDB and so
					can be set and queried by users. */

  XENGINE_COL_CUSTOM3 = 32 /* Custom precision type, this is
					a bit that is ignored by XDB and so
					can be set and queried by users. */
} xengine_col_attr_t;

typedef enum {
  XENGINE_CLUSTERED = 1, /* clustered index */
  XENGINE_UNIQUE = 2,    /* unique index */
  XENGINE_SECONDARY = 8,      /* non-unique secondary */
  XENGINE_CORRUPT = 16,  /* bit to store the corrupted flag
										in SYS_INDEXES.TYPE */
  XENGINE_FTS = 32,      /* FTS index; can't be combined with the
											other flags */
  XENGINE_SPATIAL = 64,  /* SPATIAL index; can't be combined with the
										other flags */
  XENGINE_VIRTUAL = 128  /* Index on Virtual column */
} xengine_index_type_t;

/* Note: must match lock0types.h */
/** @enum ib_lck_mode_t InnoDB lock modes. */
typedef enum {
	XENGINE_LOCK_IS = 0, /*!< Intention shared, an intention
					lock should be used to lock tables */

	XENGINE_LOCK_IX, /*!< Intention exclusive, an intention
					lock should be used to lock tables */

	XENGINE_LOCK_S, /*!< Shared locks should be used to lock rows */

	XENGINE_LOCK_X, /*!< Exclusive locks should be used to lock rows*/

	XENGINE_LOCK_TABLE_X, /*!< exclusive table lock */

	XENGINE_LOCK_NONE, /*!< This is used internally to note consistent read */

	XENGINE_LOCK_NUM = XENGINE_LOCK_NONE /*!< number of lock modes */
} xengine_lck_mode_t;

/** @enum ib_srch_mode_t InnoDB cursor search modes for ib_cursor_moveto().
Note: Values must match those found in page0cur.h */
typedef enum {
	XENGINE_CUR_G = 1, /*!< If search key is not found then
					position the cursor on the row that
					is greater than the search key */

	XENGINE_CUR_GE = 2, /*!< If the search key not found then
					position the cursor on the row that
					is greater than or equal to the search
					key */

	XENGINE_CUR_L = 3, /*!< If search key is not found then
					position the cursor on the row that
					is less than the search key */

	XENGINE_CUR_LE = 4 /*!< If search key is not found then
					position the cursor on the row that
					is less than or equal to the search
					key */
} xengine_srch_mode_t;

typedef void *xengine_opaque_t;
typedef xengine_opaque_t XENGINE_CHARset_t;
/** @struct xengine_col_meta_t XDB column meta data. */
typedef struct {
  uint32_t type; /* Type of the column */

  uint32_t attr; /* Column attributes */

  uint16_t prtype; /* Precise type of the column*/

  uint32_t type_len; /* Length of type */

  uint16_t client_type; /* 16 bits of data relevant only to
					the client. XDB doesn't care */

  XENGINE_CHARset_t *charset; /* Column charset */
} xengine_col_meta_t;

/* Note: Must be in sync with trx0trx.h */
/** @enum xengine_trx_level_t Transaction isolation levels */
typedef enum {
  XENGINE_TRX_READ_UNCOMMITTED = 0, /* Dirty read: non-locking SELECTs are
					performed so that we do not look at a
					possible earlier version of a record;
					thus they are not 'consistent' reads
					under this isolation level; otherwise
					like level 2 */

  XENGINE_TRX_READ_COMMITTED = 1, /* Somewhat Oracle-like isolation,
					except that in range UPDATE and DELETE
					we must block phantom rows with
					next-key locks; SELECT ... FOR UPDATE
					and ...  LOCK IN SHARE MODE only lock
					the index records, NOT the gaps before
					them, and thus allow free inserting;
					each consistent read reads its own
					snapshot */

  XENGINE_TRX_REPEATABLE_READ = 2, /* All consistent reads in the same
					trx read the same snapshot; full
					next-key locking used in locking reads
					to block insertions into gaps */

  XENGINE_TRX_SERIALIZABLE = 3 /* All plain SELECTs are converted to
					LOCK IN SHARE MODE reads */
} xengine_trx_level_t;

/** @enum xengine_match_mode_t Various match modes used by xengine_iter_seek() */
typedef enum {
  XENGINE_CLOSEST_MATCH, /* Closest match possible */

  XENGINE_EXACT_MATCH, /* Search using a complete key
					value */

  XENGINE_EXACT_PREFIX /* Search using a key prefix which
					must match to rows: the prefix may
					contain an incomplete field (the
					last field in prefix may be just
					a prefix of a fixed length column) */
} xengine_match_mode_t;

typedef struct {
	void *db_snapshot_;
} xengine_trx_t;

typedef void *xengine_key_def_t;

struct xengine_table_define {
	int64_t table_name_len_;
	int64_t db_name_len_;
	const char *table_name_;
	const char *db_name_;
	int64_t null_bytes_;
	unsigned char *null_mask_array_;
	int64_t *null_byte_offset_array_;
	int64_t col_buffer_size_;
	int64_t col_array_len_;
	int64_t *col_length_array_;
	void **field_array_;
	const char **col_name_array_;
	void *db_obj_; 	// myx::Xdb_tbl_def *db_obj_
	void *table_; 	// TABLE *table_
	xengine_key_def_t pk_def_;
	int64_t pk_parts_;
};

typedef struct xengine_table_define *xengine_table_def_t;

struct xengine_request {
	void *buf_;
	xengine_table_def_t table_def_;
	xengine_match_mode_t match_mode_;
	xengine_trx_t *read_trx_;
	int64_t lock_type_;
	void *thd_;
};

typedef struct {
	int64_t size_;
	char *data_;
	int64_t *offset_array_;
	uint64_t *length_array_; // use uint64_t to include XENGINE_SQL_NULL
	char **data_array_;
	int64_t *null_offset_array_;
	unsigned char *null_mask_array_;
	int64_t key_parts_;
  void *buffer_;
} xengine_tuple_t;

typedef struct {
	xengine_key_def_t key_def_;
	uint32_t cf_id_;
	uint32_t index_number_;
	void *iter_;
	xengine_tuple_t key_tuple_;  // just a buffer, different from Iterator::key
	xengine_tuple_t value_tuple_;
	char *upper_bound_;
	uint64_t upper_bound_size_;
	char *lower_bound_;
	uint64_t lower_bound_size_;
	xengine_bool_t is_primary_;
	int64_t seek_key_parts_;
} xengine_iter_t;

typedef enum dberr_t xengine_err_t;
typedef struct xengine_request *xengine_request_t;
typedef xengine_tuple_t *xengine_tpl_t;

/* Open an X-Engine table by name and return a cursor handle to it.
@return DB_SUCCESS or err code */
xengine_err_t xengine_open_table(
		const char *db_name,				/* in: db name */
		const char *table_name,			/* in: table name */
		void *thd,
		void *mysql_table,
		int64_t lock_type,
		xengine_request_t *req); /* out, own: X-Engine request context */

/* Start a transaction that's been rolled back. This special function
exists for the case when X-Engine's deadlock detector has rolledack
a transaction. While the transaction has been rolled back the handle
is still valid and can be reused by calling this function. If you
don't want to reuse the transaction handle then you can free the handle
by calling xengine_trx_release().
@return X-Engine txn handle */
xengine_err_t xengine_trx_start(
		xengine_trx_t *trx,						 	/* in: transaction to restart */
		xengine_trx_level_t trx_level, 	/* in: trx isolation level */
		xengine_bool_t read_write,		  /* in: true if read write transaction */
		xengine_bool_t auto_commit,			/* in: auto commit after each single DML */
		void *thd);								 			/* in: THD */

/* Begin a transaction. This will allocate a new transaction handle and
put the transaction in the active state.
@return X-Engine txn handle */
xengine_trx_t *xengine_trx_begin(
		xengine_trx_level_t trx_level, 	/* in: trx isolation level */
		xengine_bool_t read_write,			/* in: true if read write transaction */
		xengine_bool_t auto_commit);		/* in: auto commit after each single DML */

/* Check if the transaction is read_only */
uint32_t xengine_trx_read_only(
		xengine_trx_t *trx); 					 /* in: trx handle */

/* Release the resources of the transaction. If the transaction was
selected as a victim by X-Engine and rolled back then use this function
to free the transaction handle.
@return DB_SUCCESS or err code */
xengine_err_t xengine_trx_release(
		xengine_trx_t *xengine_trx); /* in: trx handle */

/* Commit a transaction. This function will release the schema latches too.
It will also free the transaction handle.
@return DB_SUCCESS or err code */
xengine_err_t xengine_trx_commit(
		xengine_trx_t *xengine_trx); /* in: trx handle */

/* Rollback a transaction. This function will release the schema latches too.
It will also free the transaction handle.
@return DB_SUCCESS or err code */
xengine_err_t xengine_trx_rollback(
		xengine_trx_t *xengine_trx); /* in: trx handle */

/* Open an X-Engine table and return a cursor handle to it.
@return DB_SUCCESS or err code */
xengine_err_t xengine_open_table_using_id(
		xengine_id_u64_t table_id, 	/* in: table id of table to open */
		xengine_trx_t *xengine_trx,	/* in: Current transaction handle can be NULL */
		xengine_request_t *req);		/* out,own: X-Engine cursor */

/* Open an X-Engine secondary index cursor and return a cursor handle to it.
@return DB_SUCCESS or err code */
xengine_err_t xengine_open_index_using_name(
		xengine_request_t req, 				/* in: open/active req */
		const char *index_name, 			/* in: secondary index name */
		xengine_iter_t **iter,					/* out,own: X-Engine index iter */
		int *idx_type,								/* out: index is cluster index */
		xengine_id_u64_t *idx_id);		/* out: index id */

/* Reset the cursor.
@return DB_SUCCESS or err code */
xengine_err_t xengine_reset(
		xengine_request_t xengine_crsr); /* in/out: X-Engine cursor */

/* Close an X-Engine table and free the cursor.
@return DB_SUCCESS or err code */
xengine_err_t xengine_close(
		xengine_request_t xengine_crsr); /* in/out: X-Engine cursor */

/* Close the table, decrement n_ref_count count.
@return DB_SUCCESS or err code */
xengine_err_t xengine_close_table(
		xengine_request_t xengine_crsr); /* in/out: X-Engine cursor */

/* update the cursor with new transactions and also reset the cursor
@return DB_SUCCESS or err code */
xengine_err_t xengine_new_trx(
		xengine_request_t xengine_crsr, 	/* in/out: X-Engine cursor */
		xengine_trx_t *xengine_trx);		/* in: transaction */

/* Commit the transaction in a cursor
@return DB_SUCCESS or err code */
xengine_err_t xengine_commit_trx(
		xengine_request_t xengine_crsr, 	/* in/out: X-Engine cursor */
		xengine_trx_t *xengine_trx);		/* in: transaction */

/* Insert a row to a table.
@return DB_SUCCESS or err code */
xengine_err_t xengine_insert_row(
		xengine_request_t xengine_crsr,			/* in/out: X-Engine cursor instance */
		const xengine_tpl_t xengine_tpl); /* in: tuple to insert */

/*
Update a row in a table.
@return DB_SUCCESS or err code */
xengine_err_t xengine_update_row(
		xengine_request_t xengine_crsr,					/* in: X-Engine cursor instance */
		const xengine_tpl_t xengine_old_tpl,	/* in: Old tuple in table */
		const xengine_tpl_t xengine_new_tpl); /* in: New tuple to update */

/* Delete a row in a table.
@return DB_SUCCESS or err code */
xengine_err_t xengine_delete_row(
		xengine_request_t xengine_crsr); /* in: cursor instance */

xengine_err_t xengine_pk_search(
		xengine_request_t req,
		xengine_tuple_t *key_tuple,
		xengine_tuple_t *value_tuple);

/* Read current row.
@return DB_SUCCESS or err code */
xengine_err_t xengine_read_row(
		xengine_request_t req,
		xengine_iter_t *iter,		 /* in: X-Engine cursor instance */
		xengine_tpl_t cmp_tpl,			 /* in: tuple to compare and stop
					reading */
		int mode,							 /* in: mode determine when to
					stop read */
		void **row_buf,				 	/* in/out: row buffer */
		xengine_ulint_t *row_len,	 	/* in/out: row buffer len */
		xengine_ulint_t *used_len); /* in/out: row buffer len used */

/* Move cursor to the first record in the table.
@return DB_SUCCESS or err code */
xengine_err_t xengine_iter_first(
		xengine_iter_t *iter);

/* Move cursor to the next record in the table.
@return DB_SUCCESS or err code */
xengine_err_t xengine_iter_next(
		xengine_iter_t *iter);

/* Search for key.
@return DB_SUCCESS or err code */
xengine_err_t xengine_iter_seek(
		xengine_request_t req,					 /* in: X-Engine cursor instance */
		xengine_tpl_t xengine_tpl,						 /* in: Key to search for */
		xengine_iter_t *iter,
		xengine_srch_mode_t xengine_srch_mode, /* in: search mode */
		xengine_ulint_t direction);			 /* in: search direction */

/* Set the match mode for xengine_move(). */
void xengine_set_match_mode(
		xengine_request_t xengine_crsr,					 /* in: Cursor instance */
		xengine_match_mode_t match_mode); /* in: xengine_iter_seek match mode */

/* Set a column of the tuple. Make a copy using the tuple's heap.
@return DB_SUCCESS or error code */
xengine_err_t xengine_col_set_value(
		xengine_tpl_t xengine_tpl,		 /* in: tuple instance */
		xengine_ulint_t col_no,	 /* in: column index in tuple */
		const void *src,		 /* in: data value */
		xengine_ulint_t len,			 /* in: data value len */
		xengine_bool_t need_cpy); /* in: if need memcpy */

/* Get the size of the data available in the column the tuple.
@return bytes avail or XENGINE_SQL_NULL */
xengine_ulint_t xengine_col_get_len(
		xengine_tpl_t xengine_tpl, 	/* in: tuple instance */
		xengine_ulint_t i);	 				/* in: column index in tuple */

/* Copy a column value from the tuple.
@return bytes copied or XENGINE_SQL_NULL */
xengine_ulint_t xengine_col_copy_value(
		xengine_tpl_t xengine_tpl, 	/* in: tuple instance */
		xengine_ulint_t i,		 			/* in: column index in tuple */
		void *dst,			 	 					/* out: copied data value */
		xengine_ulint_t len);  			/* in: max data value len to copy */

/* Read a signed int 8 bit column from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_i8(
		xengine_tpl_t xengine_tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i,		 			/* in: column number */
		xengine_i8_t *ival);				/* out: integer value */

/* Read an unsigned int 8 bit column from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_u8(
		xengine_tpl_t xengine_tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i,		 			/* in: column number */
		xengine_u8_t *ival);				/* out: integer value */

/* Read a signed int 16 bit column from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_i16(
		xengine_tpl_t xengine_tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i,		 			/* in: column number */
		xengine_i16_t *ival); 			/* out: integer value */

/* Read an unsigned int 16 bit column from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_u16(
		xengine_tpl_t xengine_tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i,					/* in: column number */
		xengine_u16_t *ival); 			/* out: integer value */

/* Read a signed int 24 bit column from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_i24(
		xengine_tpl_t xengine_tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i,		 	/* in: column number */
		xengine_i32_t *ival); 	/* out: integer value */

/* Read an unsigned int 24 bit column from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_u24(
		xengine_tpl_t xengine_tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i,		 	/* in: column number */
		xengine_u32_t *ival); 	/* out: integer value */

/* Read a signed int 32 bit column from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_i32(
		xengine_tpl_t xengine_tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i,		 	/* in: column number */
		xengine_i32_t *ival); 	/* out: integer value */

/* Read an unsigned int 32 bit column from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_u32(
		xengine_tpl_t xengine_tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i,		 	/* in: column number */
		xengine_u32_t *ival); 	/* out: integer value */

/* Read a signed int 64 bit column from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_i64(
		xengine_tpl_t xengine_tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i,		 			/* in: column number */
		xengine_i64_t *ival); 			/* out: integer value */

/* Read an unsigned int 64 bit column from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_u64(
		xengine_tpl_t xengine_tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i,		 			/* in: column number */
		xengine_u64_t *ival); 			/* out: integer value */

/* Get a column value pointer from the tuple.
@return NULL or pointer to buffer */
const void *xengine_col_get_value(
		xengine_tuple_t *tpl, 	/* in: X-Engine tuple */
		xengine_ulint_t i);	 		/* in: column number */

/* Get a column type, length and attributes from the tuple.
@return len of column data */
xengine_ulint_t xengine_col_get_meta(
		xengine_request_t req,
		xengine_tuple_t *tpl,						 		/* in: X-Engine tuple */
		xengine_ulint_t i,								 	/* in: column number */
		xengine_col_meta_t *col_meta); 			/* out: column meta data */

/* "Clear" or reset an X-Engine tuple. We free the heap and recreate the tuple.
@return new tuple, or NULL */
void xengine_tuple_clear(
		xengine_iter_t **iter,
		xengine_tuple_t **tpl); 				/* in: X-Engine tuple */

/* Create a new cluster key search tuple and copy the contents of  the
secondary index key tuple columns that refer to the cluster index record
to the cluster key. It does a deep copy of the column data.
@return DB_SUCCESS or error code */
xengine_err_t xengine_tuple_get_cluster_key(
		xengine_request_t xengine_crsr,					/* in: secondary index cursor */
		xengine_tpl_t *xengine_dst_tpl,					/* out,own: destination tuple */
		const xengine_tpl_t xengine_src_tpl); 	/* in: source tuple */

/* Create an X-Engine tuple used for index/table search.
@return tuple for current index */
xengine_err_t xengine_key_tuple_create(
		xengine_request_t req,
		xengine_key_def_t key_def,
		int64_t key_parts,
		xengine_tuple_t **tpl); /* in: Cursor instance */

xengine_err_t xengine_value_tuple_create(
		xengine_table_def_t table_def,
    xengine_tuple_t **tuple);

/* Create an X-Engine tuple used for index/table search.
@return tuple for current index */
xengine_err_t xengine_sec_iter_create(
		xengine_request_t req,
		const char *index_name,
		xengine_iter_t **iter); /* in: Cursor instance */

/* Create an X-Engine tuple used for table key operations.
@return tuple for current table */
xengine_err_t xengine_clust_search_tuple_create(
		xengine_request_t req,
		xengine_iter_t *iter); /* in: Cursor instance */

/* Create an X-Engine tuple for table row operations.
@return tuple for current table */
xengine_err_t xengine_clust_iter_create(
		xengine_request_t req,
		xengine_iter_t **iter);

/* Return the number of user columns in the tuple definition.
@return number of user columns */
xengine_ulint_t xengine_iter_get_n_user_cols(
		const xengine_iter_t *iter); /* in: Tuple for current table */

/* Return the number of columns in the tuple definition.
@return number of columns */
xengine_ulint_t xengine_iter_get_n_cols(
		const xengine_iter_t *iter); /* in: Tuple for current table */

/* Sets number of fields used in record comparisons.*/
void xengine_tuple_set_n_fields_cmp(
		const xengine_tpl_t xengine_tpl,	/* in: Tuple for current index */
		uint32_t n_fields_cmp); 	/* in: number of fields used in
					comparisons*/

/* Destroy an X-Engine tuple. */
void xengine_delete_iter(
		xengine_iter_t *iter);	/* in,own: Iter instance to delete */

void xengine_delete_tuple(
		xengine_tuple_t *tuple);

/* Truncate a table. The cursor handle will be closed and set to NULL
on success.
@return DB_SUCCESS or error code */
xengine_err_t xengine_truncate(
		xengine_request_t *xengine_crsr,			/* in/out: cursor for table
					to truncate */
		xengine_id_u64_t *table_id); 	/* out: new table id */

/* Get a table id.
@return DB_SUCCESS if found */
xengine_err_t xengine_table_get_id(
		const char *table_name, 	/* in: table to find */
		xengine_id_u64_t *table_id); 	/* out: table id if found */

/* Check if cursor is positioned.
@return xengine_TRUE if positioned */
xengine_bool_t xengine_is_positioned(
		const xengine_request_t xengine_crsr); /* in: X-Engine cursor instance */

/* Checks if the data dictionary is latched in exclusive mode by a
user transaction.
@return TRUE if exclusive latch */
xengine_bool_t xengine_schema_lock_is_exclusive(
		const xengine_trx_t *xengine_trx); 	/* in: transaction */

/* Lock an X-Engine cursor/table.
@return DB_SUCCESS or error code */
xengine_err_t xengine_lock(
		xengine_request_t xengine_crsr,					/* in/out: X-Engine cursor */
		xengine_lck_mode_t xengine_lck_mode); /* in: X-Engine lock mode */

/* Set the Lock an X-Engine table using the table id.
@return DB_SUCCESS or error code */
xengine_err_t xengine_table_lock(
		xengine_trx_t *xengine_trx,						/* in/out: transaction */
		xengine_id_u64_t table_id,				/* in: table id */
		xengine_lck_mode_t xengine_lck_mode); /* in: X-Engine lock mode */

/* Set the Lock mode of the cursor.
@return DB_SUCCESS or error code */
xengine_err_t xengine_set_lock_mode(
		xengine_request_t xengine_crsr,					/* in/out: X-Engine cursor */
		xengine_lck_mode_t xengine_lck_mode); /* in: X-Engine lock mode */

/* Set need to access clustered index record flag. */
void xengine_set_cluster_access(
		xengine_request_t xengine_crsr); /* in/out: X-Engine cursor */

/* Inform the cursor that it's the start of an SQL statement. */
void xengine_stmt_begin(
		xengine_request_t xengine_crsr); /* in: cursor */

/* Write a double value to a column.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_write_double(
		xengine_tpl_t xengine_tpl, /* in: X-Engine tuple */
		int col_no,			 /* in: column number */
		double val);		 /* in: value to write */

/* Read a double column value from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_double(
		xengine_tpl_t xengine_tpl,	 	/* in: X-Engine tuple */
		xengine_ulint_t col_no, 	/* in: column number */
		double *dval);		 		/* out: double value */

/* Write a float value to a column.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_write_float(
		xengine_tpl_t xengine_tpl, 	/* in/out: tuple to write to */
		int col_no,			 		/* in: column number */
		float val);			 		/* in: value to write */

/* Read a float value from an X-Engine tuple.
@return DB_SUCCESS or error */
xengine_err_t xengine_tuple_read_float(
		xengine_tpl_t xengine_tpl,	 	/* in: X-Engine tuple */
		xengine_ulint_t col_no, 	/* in: column number */
		float *fval);			 		/* out: float value */

/* Get a column type, length and attributes from the tuple.
@return len of column data */
const char *xengine_col_get_name(
		xengine_request_t req, 		/* in: X-Engine request instance */
		xengine_ulint_t i);		 		/* in: column index in tuple */

/* Get an index field name from the cursor.
@return name of the field */
const char *xengine_get_idx_field_name(
		xengine_request_t req, 	/* in: X-Engine req instance */
		xengine_iter_t *iter,		/* iter */
		xengine_ulint_t i);		 	/* in: column index in tuple */

/* Truncate a table.
@return DB_SUCCESS or error code */
xengine_err_t xengine_table_truncate(
		const char *table_name, /* in: table name */
		xengine_id_u64_t *table_id); /* out: new table id */

/* Get generic configure status
@return configure status*/
int xengine_cfg_get_cfg();

/* Increase/decrease the memcached sync count of table to sync memcached
DML with SQL DDLs.
@return DB_SUCCESS or error number */
xengine_err_t xengine_set_memcached_sync(
		xengine_request_t xengine_crsr, 	/* in: cursor */
		xengine_bool_t flag);	 		/* in: true for increasing */

/* Return isolation configuration set by "X-Engine_api_trx_level"
@return trx isolation level*/
xengine_trx_level_t xengine_cfg_trx_level();

/* Return configure value for background commit interval (in seconds)
@return background commit interval (in seconds) */
xengine_ulint_t xengine_cfg_bk_commit_interval();

/* Get a trx start time.
@return trx start_time */
xengine_u64_t xengine_trx_get_start_time(
		xengine_trx_t *xengine_trx); /* in: transaction */

/* Wrapper of ut_strerr() which converts an X-Engine error number to a
human readable text message.
@return string, describing the error */
const char *xengine_ut_strerr(
		xengine_err_t num); /* in: error number */

/** Check the table whether it contains virtual columns.
@param[in]	crsr	X-Engine Cursor
@return true if table contains virtual column else false. */
xengine_bool_t xengine_is_virtual_table(
		xengine_request_t crsr);

/**
 * set the tuple to be key type
 * @param xengine_tpl
 * */
void xengine_set_tuple_key(
		xengine_tpl_t xengine_tpl);
