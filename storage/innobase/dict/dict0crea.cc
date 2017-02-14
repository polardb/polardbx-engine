/*****************************************************************************

Copyright (c) 1996, 2017, Oracle and/or its affiliates. All Rights Reserved.

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
@file dict/dict0crea.cc
Database object creation

Created 1/8/1996 Heikki Tuuri
*******************************************************/

#include "btr0btr.h"
#include "btr0pcur.h"
#include "dict0boot.h"
#include "dict0crea.h"
#include "dict0dict.h"
#include "dict0priv.h"
#include "dict0stats.h"
#include "fsp0space.h"
#include "fsp0sysspace.h"
#include "fts0priv.h"
#include "ha_prototypes.h"
#include "mach0data.h"
#include "my_compiler.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "page0page.h"
#include "pars0pars.h"
#include "que0que.h"
#include "row0ins.h"
#include "row0mysql.h"
#include "srv0start.h"
#include "trx0roll.h"
#include "usr0sess.h"
#include "ut0vec.h"

/** Build a table definition without updating SYSTEM TABLES
@param[in,out]	table	dict table object
@param[in,out]	trx	transaction instance
@return DB_SUCCESS or error code */
dberr_t
dict_build_table_def(
	dict_table_t*	table,
	trx_t*		trx)
{
	dberr_t		err = DB_SUCCESS;

	dict_table_assign_new_id(table, trx);

	err = dict_build_tablespace_for_table(table);

	return(err);
}

/** Build a tablespace to store various objects.
@param[in,out]	tablespace	Tablespace object describing what to build.
@return DB_SUCCESS or error code. */
dberr_t
dict_build_tablespace(
	Tablespace*	tablespace)
{
	dberr_t		err	= DB_SUCCESS;
	mtr_t		mtr;
	space_id_t	space = 0;

	ut_ad(mutex_own(&dict_sys->mutex));
	ut_ad(tablespace);

        DBUG_EXECUTE_IF("out_of_tablespace_disk",
                         return(DB_OUT_OF_FILE_SPACE););
	/* Get a new space id. */
	dict_hdr_get_new_id(NULL, NULL, &space, NULL, false);
	if (space == SPACE_UNKNOWN) {
		return(DB_ERROR);
	}
	tablespace->set_space_id(space);

	Datafile* datafile = tablespace->first_datafile();

	/* We create a new generic empty tablespace.
	We initially let it be 4 pages:
	- page 0 is the fsp header and an extent descriptor page,
	- page 1 is an ibuf bitmap page,
	- page 2 is the first inode page,
	- page 3 will contain the root of the clustered index of the
	first table we create here. */

	err = fil_ibd_create(
		space,
		tablespace->name(),
		datafile->filepath(),
		tablespace->flags(),
		FIL_IBD_FILE_INITIAL_SIZE);
	if (err != DB_SUCCESS) {
		return(err);
	}

	mtr_start(&mtr);
	mtr.set_named_space(space);

	/* Once we allow temporary general tablespaces, we must do this;
	mtr_set_log_mode(&mtr, MTR_LOG_NO_REDO); */
	ut_a(!FSP_FLAGS_GET_TEMPORARY(tablespace->flags()));

	bool ret = fsp_header_init(
		space, FIL_IBD_FILE_INITIAL_SIZE, &mtr, false);
	mtr_commit(&mtr);

	if (!ret) {
		return(DB_ERROR);
	}

	return(err);
}

/** Builds a tablespace to contain a table, using file-per-table=1.
@param[in,out]	table	Table to build in its own tablespace.
@return DB_SUCCESS or error code */
dberr_t
dict_build_tablespace_for_table(
	dict_table_t*	table)
{
	dberr_t		err	= DB_SUCCESS;
	mtr_t		mtr;
	space_id_t	space = 0;
	bool		needs_file_per_table;
	char*		filepath;

	ut_ad(mutex_own(&dict_sys->mutex) || table->is_intrinsic());

	needs_file_per_table
		= DICT_TF2_FLAG_IS_SET(table, DICT_TF2_USE_FILE_PER_TABLE);

	/* Always set this bit for all new created tables */
	DICT_TF2_FLAG_SET(table, DICT_TF2_FTS_AUX_HEX_NAME);
	DBUG_EXECUTE_IF("innodb_test_wrong_fts_aux_table_name",
			DICT_TF2_FLAG_UNSET(table,
					    DICT_TF2_FTS_AUX_HEX_NAME););

	if (needs_file_per_table) {
		/* Temporary table would always reside in the same
		shared temp tablespace. */
		ut_ad(!table->is_temporary());
		/* This table will need a new tablespace. */

		ut_ad(DICT_TF_GET_ZIP_SSIZE(table->flags) == 0
		      || dict_table_has_atomic_blobs(table));

		/* Get a new tablespace ID */
		dict_hdr_get_new_id(NULL, NULL, &space, table, false);

		DBUG_EXECUTE_IF(
			"ib_create_table_fail_out_of_space_ids",
			space = SPACE_UNKNOWN;
		);

		if (space == SPACE_UNKNOWN) {
			return(DB_ERROR);
		}
		table->space = space;

		/* Determine the tablespace flags. */
		bool	has_data_dir = DICT_TF_HAS_DATA_DIR(table->flags);
		bool	is_encrypted = dict_table_is_encrypted(table);
		ulint	fsp_flags = dict_tf_to_fsp_flags(table->flags,
							 is_encrypted);

		/* Determine the full filepath */
		if (has_data_dir) {
			ut_ad(table->data_dir_path);
			filepath = fil_make_filepath(
				table->data_dir_path,
				table->name.m_name, IBD, true);

		} else {
			/* Make the tablespace file in the default dir
			using the table name */
			filepath = fil_make_filepath(
				NULL, table->name.m_name, IBD, false);
		}

		/* We create a new single-table tablespace for the table.
		We initially let it be 4 pages:
		- page 0 is the fsp header and an extent descriptor page,
		- page 1 is an ibuf bitmap page,
		- page 2 is the first inode page,
		- page 3 will contain the root of the clustered index of
		the table we create here. */

		err = fil_ibd_create(
			space, table->name.m_name, filepath, fsp_flags,
			FIL_IBD_FILE_INITIAL_SIZE);

		ut_free(filepath);

		if (err != DB_SUCCESS) {

			return(err);
		}

		mtr_start(&mtr);
		mtr.set_named_space(table->space);

		bool ret = fsp_header_init(
			table->space, FIL_IBD_FILE_INITIAL_SIZE, &mtr, false);
		mtr_commit(&mtr);

		if (!ret) {
			return(DB_ERROR);
		}

		err = btr_sdi_create_indexes(table->space, true);
		return(err);

	} else {
		/* We do not need to build a tablespace for this table. It
		is already built.  Just find the correct tablespace ID. */

		if (DICT_TF_HAS_SHARED_SPACE(table->flags)) {
			ut_ad(table->tablespace != NULL);

			ut_ad(table->space == fil_space_get_id_by_name(
				table->tablespace()));
		} else if (table->is_temporary()) {
			/* Use the shared temporary tablespace.
			Note: The temp tablespace supports all non-Compressed
			row formats whereas the system tablespace only
			supports Redundant and Compact */
			ut_ad(dict_tf_get_rec_format(table->flags)
				!= REC_FORMAT_COMPRESSED);
			table->space = static_cast<uint32_t>(
				srv_tmp_space.space_id());
		} else {
			/* Create in the system tablespace. */
			ut_ad(table->space == TRX_SYS_SPACE);
		}

		DBUG_EXECUTE_IF("ib_ddl_crash_during_tablespace_alloc",
				DBUG_SUICIDE(););
	}

	return(DB_SUCCESS);
}

/***************************************************************//**
Builds an index definition
@return DB_SUCCESS or error code */
void
dict_build_index_def(
/*=================*/
	const dict_table_t*	table,	/*!< in: table */
	dict_index_t*		index,	/*!< in/out: index */
	trx_t*			trx)	/*!< in/out: InnoDB transaction handle */
{
	ut_ad(mutex_own(&dict_sys->mutex) || table->is_intrinsic());

	if (trx->table_id == 0) {
		/* Record only the first table id. */
		trx->table_id = table->id;
	}

	ut_ad((UT_LIST_GET_LEN(table->indexes) > 0)
	      || index->is_clustered());

	if (!table->is_intrinsic()) {
		dict_hdr_get_new_id(NULL, &index->id, NULL, table, false);
	} else {
		/* Index are re-loaded in process of creation using id.
		If same-id is used for all indexes only first index will always
		be retrieved when expected is iterative return of all indexes*/
		if (UT_LIST_GET_LEN(table->indexes) > 0) {
			index->id = UT_LIST_GET_LAST(table->indexes)->id + 1;
		} else {
			index->id = 1;
		}
	}

	/* Inherit the space id from the table; we store all indexes of a
	table in the same tablespace */

	index->space = table->space;

	/* Note that the index was created by this transaction. */
	index->trx_id = trx->id;
}


/***************************************************************//**
Creates an index tree for the index if it is not a member of a cluster.
@return DB_SUCCESS or DB_OUT_OF_FILE_SPACE */
dberr_t
dict_create_index_tree_in_mem(
/*==========================*/
	dict_index_t*	index,	/*!< in/out: index */
	const trx_t*	trx)	/*!< in: InnoDB transaction handle */
{
	mtr_t		mtr;
	ulint		page_no = FIL_NULL;

	ut_ad(mutex_own(&dict_sys->mutex) || index->table->is_intrinsic());

	DBUG_EXECUTE_IF("ib_dict_create_index_tree_fail",
			return(DB_OUT_OF_MEMORY););

	if (index->type == DICT_FTS) {
		/* FTS index does not need an index tree */
		return(DB_SUCCESS);
	}

	const bool      missing =  index->table->ibd_file_missing
		|| dict_table_is_discarded(index->table);

	if (missing) {
		index->page = FIL_NULL;
		index->trx_id= trx->id;

		return(DB_SUCCESS);
	}

	mtr_start(&mtr);

	if (index->table->is_temporary()) {
		mtr_set_log_mode(&mtr, MTR_LOG_NO_REDO);
	} else {
                mtr.set_named_space(index->space);
	}

	dberr_t		err = DB_SUCCESS;

	page_no = btr_create(
		index->type, index->space,
		dict_table_page_size(index->table),
		index->id, index, &mtr);

	index->page = page_no;
	index->trx_id = trx->id;

	if (page_no == FIL_NULL) {
		err = DB_OUT_OF_FILE_SPACE;
	}

	mtr_commit(&mtr);

	return(err);
}

/** Drop an index tree
@param[in]	index		dict index
@param[in]	root_page_no	root page no */
void
dict_drop_index(
	const dict_index_t*	index,
	page_no_t		root_page_no)
{
	ut_ad(mutex_own(&dict_sys->mutex));
	ut_ad(!index->table->is_temporary());

	if (root_page_no == FIL_NULL) {
		ut_ad((index->type & DICT_FTS)
		      || index->table->ibd_file_missing);
		return;
	}

	bool			found;
	const page_size_t	page_size(fil_space_get_page_size(index->space,
								  &found));

	if (!found) {
		/* It is a single table tablespace and the .ibd file is
		missing: do nothing */

		return;
	}

	mtr_t	mtr;
	mtr_start(&mtr);

	btr_free_if_exists(page_id_t(index->space, root_page_no),
			   page_size, index->id, &mtr);

	mtr_commit(&mtr);

	return;
}

/** Drop an index tree belonging to a temporary table.
@param[in]	index		index in a temporary table
@param[in]	root_page_no	index root page number */
void
dict_drop_temporary_table_index(
	const dict_index_t*	index,
	page_no_t		root_page_no)
{
	ut_ad(mutex_own(&dict_sys->mutex) || index->table->is_intrinsic());
	ut_ad(index->table->is_temporary());
	ut_ad(index->page == FIL_NULL);

	space_id_t		space = index->space;
	bool			found;
	const page_size_t	page_size(fil_space_get_page_size(space,
								  &found));

	/* If tree has already been freed or it is a single table
	tablespace and the .ibd file is missing do nothing,
	else free the all the pages */
	if (root_page_no != FIL_NULL && found) {
		btr_free(page_id_t(space, root_page_no), page_size);
	}
}

/** Check whether a column is in an index by the column name
@param[in]	col_name	column name for the column to be checked
@param[in]	index		the index to be searched
@return	true if this column is in the index, otherwise, false */
static
bool
dict_index_has_col_by_name(
	const char*		col_name,
	const dict_index_t*	index)
{
        for (ulint i = 0; i < index->n_fields; i++) {
                dict_field_t*   field = index->get_field(i);

		if (strcmp(field->name, col_name) == 0) {
			return(true);
		}
	}
	return(false);
}

/** Check whether the foreign constraint could be on a column that is
part of a virtual index (index contains virtual column) in the table
@param[in]	fk_col_name	FK column name to be checked
@param[in]	table		the table
@return	true if this column is indexed with other virtual columns */
bool
dict_foreign_has_col_in_v_index(
	const char*		fk_col_name,
	const dict_table_t*	table)
{
	/* virtual column can't be Primary Key, so start with secondary index */
	for (const dict_index_t* index = table->first_index()->next();
	     index;
	     index = index->next()) {

		if (dict_index_has_virtual(index)) {
			if (dict_index_has_col_by_name(fk_col_name, index)) {
				return(true);
			}
		}
	}

	return(false);
}


/** Check whether the foreign constraint could be on a column that is
a base column of some indexed virtual columns.
@param[in]	col_name	column name for the column to be checked
@param[in]	table		the table
@return	true if this column is a base column, otherwise, false */
bool
dict_foreign_has_col_as_base_col(
	const char*		col_name,
	const dict_table_t*	table)
{
	/* Loop through each virtual column and check if its base column has
	the same name as the column name being checked */
	for (ulint i = 0; i < table->n_v_cols; i++) {
		dict_v_col_t*	v_col = dict_table_get_nth_v_col(table, i);

		/* Only check if the virtual column is indexed */
		if (!v_col->m_col.ord_part) {
			continue;
		}

		for (ulint j = 0; j < v_col->num_base; j++) {
			if (strcmp(col_name,
				   table->get_col_name(v_col->base_col[j]->ind))
			    == 0) {
				return(true);
			}
		}
	}

	return(false);
}

/** Check if a foreign constraint is on the given column name.
@param[in]	col_name	column name to be searched for fk constraint
@param[in]	table		table to which foreign key constraint belongs
@return true if fk constraint is present on the table, false otherwise. */
static
bool
dict_foreign_base_for_stored(
	const char*		col_name,
	const dict_table_t*	table)
{
	/* Loop through each stored column and check if its base column has
	the same name as the column name being checked */
	dict_s_col_list::const_iterator it;
	for (it = table->s_cols->begin();
	     it != table->s_cols->end(); ++it) {
		dict_s_col_t	s_col = *it;

		for (ulint j = 0; j < s_col.num_base; j++) {
			if (strcmp(col_name,
				   table->get_col_name(s_col.base_col[j]->ind))
			    == 0) {
				return(true);
			}
		}
	}

	return(false);
}

/** Check if a foreign constraint is on columns served as base columns
of any stored column. This is to prevent creating SET NULL or CASCADE
constraint on such columns
@param[in]	local_fk_set	set of foreign key objects, to be added to
the dictionary tables
@param[in]	table		table to which the foreign key objects in
local_fk_set belong to
@return true if yes, otherwise, false */
bool
dict_foreigns_has_s_base_col(
	const dict_foreign_set& local_fk_set,
	const dict_table_t*	table)
{
	dict_foreign_t* foreign;

	if (table->s_cols == NULL) {
		return (false);
	}

	for (dict_foreign_set::const_iterator it = local_fk_set.begin();
	     it != local_fk_set.end();
	     ++it) {

		foreign = *it;
		ulint	type = foreign->type;

		type &= ~(DICT_FOREIGN_ON_DELETE_NO_ACTION
			  | DICT_FOREIGN_ON_UPDATE_NO_ACTION);

		if (type == 0) {
			continue;
		}

		for (ulint i = 0; i < foreign->n_fields; i++) {
			/* Check if the constraint is on a column that
			is a base column of any stored column */
			if (dict_foreign_base_for_stored(
				foreign->foreign_col_names[i], table)) {
				return(true);
			}
		}
	}

	return(false);
}

/** Check if a column is in foreign constraint with CASCADE properties or
SET NULL
@param[in]	table		table
@param[in]	col_name	name for the column to be checked
@return true if the column is in foreign constraint, otherwise, false */
bool
dict_foreigns_has_this_col(
	const dict_table_t*	table,
	const char*		col_name)
{
	dict_foreign_t*		foreign;
	const dict_foreign_set*	local_fk_set = &table->foreign_set;

	for (dict_foreign_set::const_iterator it = local_fk_set->begin();
	     it != local_fk_set->end();
	     ++it) {
		foreign = *it;
		ut_ad(foreign->id != NULL);
		ulint	type = foreign->type;

		type &= ~(DICT_FOREIGN_ON_DELETE_NO_ACTION
			  | DICT_FOREIGN_ON_UPDATE_NO_ACTION);

		if (type == 0) {
			continue;
		}

		for (ulint i = 0; i < foreign->n_fields; i++) {
			if (strcmp(foreign->foreign_col_names[i],
				   col_name) == 0) {
				return(true);
			}
		}
	}
	return(false);
}

/** Assign a new table ID and put it into the table cache and the transaction.
@param[in,out]	table	Table that needs an ID
@param[in,out]	trx	Transaction */
void
dict_table_assign_new_id(
	dict_table_t*	table,
	trx_t*		trx)
{
	if (table->is_intrinsic()) {
		/* There is no significance of this table->id (if table is
		intrinsic) so assign it default instead of something meaningful
		to avoid confusion.*/
		table->id = ULINT_UNDEFINED;
	} else {
		dict_hdr_get_new_id(&table->id, NULL, NULL, table, false);
	}

	trx->table_id = table->id;
}

/** Create in-memory tablespace dictionary index & table
@param[in]	space		tablespace id
@param[in]	copy_num	copy of sdi table
@param[in]	space_discarded	true if space is discarded
@param[in]	in_flags	space flags to use when space_discarded is true
@return in-memory index structure for tablespace dictionary or NULL */
dict_index_t*
dict_sdi_create_idx_in_mem(
	space_id_t	space,
	uint32_t	copy_num,
	bool		space_discarded,
	ulint		in_flags)
{
	ulint	flags = space_discarded
		? in_flags
		: fil_space_get_flags(space);

	/* This means the tablespace is evicted from cache */
	if (flags == ULINT_UNDEFINED) {
		return(NULL);
	}

	ut_ad(fsp_flags_is_valid(flags));

	rec_format_t rec_format;

	ulint	zip_ssize = FSP_FLAGS_GET_ZIP_SSIZE(flags);
	ulint	atomic_blobs = FSP_FLAGS_HAS_ATOMIC_BLOBS(flags);
	bool	has_data_dir =  FSP_FLAGS_HAS_DATA_DIR(flags);
	bool	has_shared_space = FSP_FLAGS_GET_SHARED(flags);

	/* TODO: Use only REC_FORMAT_DYNAMIC after WL#7704 */
	if (zip_ssize > 0) {
		rec_format = REC_FORMAT_COMPRESSED;
	} else if (atomic_blobs){
		rec_format = REC_FORMAT_DYNAMIC;
	} else {
		rec_format = REC_FORMAT_COMPACT;
	}

	ulint	table_flags;
	dict_tf_set(&table_flags, rec_format, zip_ssize, has_data_dir,
		    has_shared_space);

	/* 28 = strlen(SDI) + Max digits of 4 byte spaceid (10) + Max
	digits of copy_num (10) + 1 */
	char		table_name[28];
	mem_heap_t*	heap = mem_heap_create(DICT_HEAP_SIZE);
	snprintf(table_name, sizeof(table_name),
		"SDI_" SPACE_ID_PF "_" UINT32PF, space, copy_num);

	dict_table_t*	table = dict_mem_table_create(
		table_name, space, 3, 0, table_flags, 0);

	dict_mem_table_add_col(table, heap, "id", DATA_INT,
			       DATA_NOT_NULL|DATA_UNSIGNED, 8);
	dict_mem_table_add_col(table, heap, "type", DATA_INT,
			       DATA_NOT_NULL|DATA_UNSIGNED, 4);
	dict_mem_table_add_col(table, heap, "data", DATA_BLOB, DATA_NOT_NULL,
			       0);

	table->id = dict_sdi_get_table_id(space, copy_num);

	/* Disable persistent statistics on the table */
	dict_stats_set_persistent(table, false, true);

	dict_table_add_system_columns(table, heap);
	dict_table_add_to_cache(table, TRUE, heap);

	/* TODO: After WL#7412, we can use a common name for both
	SDI Indexes. */

	/* 16 =	14(CLUST_IND_SDI_) + 1 (copy_num 0 or 1) + 1 */
	char	index_name[16];
	snprintf(index_name, sizeof(index_name), "CLUST_IND_SDI_" UINT32PF,
		    copy_num);

	dict_index_t*	temp_index = dict_mem_index_create(
		table_name, index_name, space,
		DICT_CLUSTERED |DICT_UNIQUE | DICT_SDI, 2);
	ut_ad(temp_index);

	temp_index->add_field("id", 0, true);
	temp_index->add_field("type", 0, true);

	temp_index->table = table;

	/* Disable AHI on SDI tables */
	temp_index->disable_ahi = true;

	page_no_t	index_root_page_num;

	/* TODO: Remove space_discarded parameter after WL#7412 */
	/* When we do DISCARD TABLESPACE, there will be no fil_space_t
	for the tablespace. In this case, we should not use fil_space_*()
	methods */
	if (!space_discarded) {

		mtr_t	mtr;
		mtr.start();

		index_root_page_num = fsp_sdi_get_root_page_num(
			space, copy_num, page_size_t(flags), &mtr);

		mtr_commit(&mtr);

	} else {
		index_root_page_num = FIL_NULL;
	}

	temp_index->id = dict_sdi_get_index_id(copy_num);

	/* TODO: WL#7141: Do not add the SDI pseudo-tables to the cache */
	dberr_t	error = dict_index_add_to_cache(table, temp_index,
						index_root_page_num, false);

	ut_a(error == DB_SUCCESS);

	mem_heap_free(heap);
	return(table->first_index());
}
