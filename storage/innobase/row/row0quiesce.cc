/*****************************************************************************

Copyright (c) 2012, Oracle and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA

*****************************************************************************/

/**************************************************//**
@file row/row0quiesce.cc
Quiesce a tablespace.

Created 2012-02-08 by Sunny Bains.
*******************************************************/

#include "row0quiesce.h"

#ifdef UNIV_NONINL
#include "row0quiesce.ic"
#endif

#include "ibuf0ibuf.h"
#include "srv0start.h"
#include "trx0purge.h"

/*********************************************************************//**
Write the meta data config file header. */
static
db_err
row_quiesce_write_meta_data_header(
/*===============================*/
	const dict_table_t*	table,	/*!< in: write the meta data for
					this table */
	FILE*			file,	/*!< in: file to write to */
	void*			thd)	/*!< in/out: session */
{
	byte*			ptr;
	byte			row[sizeof(ib_uint32_t) * 3];

	ptr = row;

	/* Write the meta-data version number. */
	mach_write_to_4(ptr, IB_EXPORT_CFG_VERSION_V1);
	ptr += sizeof(ib_uint32_t);

	/* Write the system page size. */
	mach_write_to_4(ptr, UNIV_PAGE_SIZE);
	ptr += sizeof(ib_uint32_t);

	/* Write the number of indexes in the table. */
	mach_write_to_4(ptr, UT_LIST_GET_LEN(table->indexes));

	if (fwrite(row, 1,  sizeof(row), file) != sizeof(row)) {
		ib_pushf(thd, IB_LOG_LEVEL_ERROR, ER_EXCEPTIONS_WRITE_ERROR,
			"IO error (%lu), writing meta-data "
			"header.\n", (ulint) errno);

		return(DB_IO_ERROR);
	}

	return(DB_SUCCESS);
}

/*********************************************************************//**
Write the meta data config file index information. */
static
db_err
row_quiesce_write_meta_data_indexes(
/*================================*/
	const dict_table_t*	table,	/*!< in: write the meta data for
					this table */
	FILE*			file,	/*!< in: file to write to */
	void*			thd)	/*!< in/out: session */
{
	const dict_index_t*	index;

	/* Write the root page numbers and corresponding index names. */
	for (index = UT_LIST_GET_FIRST(table->indexes);
	     index != 0;
	     index = UT_LIST_GET_NEXT(indexes, index)) {

		byte*		ptr;
		byte		row[sizeof(ib_uint32_t) * 2];

		ptr = row;

		/* Write the root page number. */
		mach_write_to_4(ptr, index->page);
		ptr += sizeof(ib_uint32_t);

		/* Write the length of the index name. */
		ulint	len = ut_strlen(index->name);

		/* NUL byte is included in the length. */
		mach_write_to_4(ptr, len + 1);

		if (fwrite(row, 1, sizeof(row), file) != sizeof(row)) {

			ib_pushf(thd, IB_LOG_LEVEL_ERROR,
				 ER_EXCEPTIONS_WRITE_ERROR,
				 "IO error (%lu), writing index "
				 "meta-data\n", (ulint) errno);

			return(DB_IO_ERROR);
		}

		/* Write out the NUL byte too. */
		if (fwrite(index->name, 1, len + 1, file) != len + 1) {
			ib_pushf(thd, IB_LOG_LEVEL_ERROR,
				 ER_EXCEPTIONS_WRITE_ERROR,
				 "IO error (%lu), writing index name\n",
				 (ulint) errno);

			return(DB_IO_ERROR);
		}
	}

	return(DB_SUCCESS);
}

/*********************************************************************//**
Write the table meta data after quiesce. */
static
db_err
row_quiesce_write_meta_data(
/*========================*/
	const dict_table_t*	table,	/*!< in: write the meta data for
					this table */
	void*			thd)	/*!< in/out: session */
{
	db_err		err;
	char		name[OS_FILE_MAX_PATH];

	srv_get_meta_data_filename(table, name, sizeof(name));

	ib_logf("Writing table metadata to '%s'", name);

	FILE*	file = fopen(name, "w+");

	if (file == NULL) {
		ib_pushf(thd, IB_LOG_LEVEL_ERROR, ER_CANT_CREATE_FILE,
			 "Error (%lu) creating: %s", (ulint) errno, name);

		err = DB_IO_ERROR;
	} else {
		err = row_quiesce_write_meta_data_header(table, file, thd);

		if (err == DB_SUCCESS) {
			err = row_quiesce_write_meta_data_indexes(
				table, file, thd);
		}

		fclose(file);
	}

	return(err);
}

/*********************************************************************//**
Quiesce the tablespace that the table resides in. */
UNIV_INTERN
void
row_quiesce_table_start(
/*====================*/
	dict_table_t*	table,		/*!< in: quiesce this table */
	void*		thd)		/*!< in/out: session */
{
	ut_a(srv_n_purge_threads > 0);

	ib_logf("Sync to disk of %s started.", table->name);

	trx_purge_stop();

	ibuf_contract_in_background(table->id, TRUE);

	ulint	n_pages = buf_flush_list(
		table->space, ULINT_MAX, ULINT_UNDEFINED);

	buf_flush_wait_batch_end(NULL, BUF_FLUSH_LIST);

	ib_logf("Quiesce pages flushed: %lu", n_pages);

	if (row_quiesce_write_meta_data(table, thd) != DB_SUCCESS) {
		ib_logf("There was an error writing to the "
			"meta data file");
	} else {
		ib_logf("%s flushed to disk", table->name);
	}

	row_quiesce_set_state(table, QUIESCE_COMPLETE, thd);
}

/*********************************************************************//**
Cleanup after table quiesce. */
UNIV_INTERN
void
row_quiesce_table_complete(
/*=======================*/
	dict_table_t*	table,		/*!< in: quiesce this table */
	void*		thd)		/*!< in/out: session */
{
	trx_purge_run();

	row_quiesce_set_state(table, QUIESCE_NONE, thd);
}

/*********************************************************************//**
Set a table's quiesce state. */
UNIV_INTERN
void
row_quiesce_set_state(
/*==================*/
	dict_table_t*	table,		/*!< in: quiesce this table */
	ib_quiesce_t	state,		/*!< in: quiesce state to set */
	void*		thd)		/*!< in/out: session */
{
	dict_index_t*	index;

	for (index = dict_table_get_first_index(table);
	     index != NULL;
	     index = dict_table_get_next_index(index)) {

		rw_lock_x_lock(&index->lock);
	}

	if (srv_n_purge_threads == 0) {
		ib_logf("Needs --innodb-purge-threads > 0");
	} else if (table->space == TRX_SYS_SPACE) {
		ib_logf("Can't quiesce system tablespace");
	} else {

		switch (state) {
		case QUIESCE_START:
			ut_a(table->quiesce == QUIESCE_NONE);
			break;

		case QUIESCE_COMPLETE:
			ut_a(table->quiesce == QUIESCE_START);
			break;

		case QUIESCE_NONE:
			ut_a(table->quiesce == QUIESCE_COMPLETE);
			break;
		}

		table->quiesce = state;
	}

	for (index = dict_table_get_first_index(table);
	     index != NULL;
	     index = dict_table_get_next_index(index)) {

		rw_lock_x_unlock(&index->lock);
	}
}

