#
# $var_or_sea = var | sea
#

if ($var_or_sea==var) {
  --let $set_var_true=set opt_index_format_gpp_enabled=true
  --let $set_var_false=set opt_index_format_gpp_enabled=false
  --let $set_sea_true=
  --let $set_sea_false=
}
if ($var_or_sea==sea) {
  --let $set_var_true=set opt_index_format_gpp_enabled=false
  --let $set_var_false=set opt_index_format_gpp_enabled=true
  --let $set_sea_true= secondary_engine_attribute '{"gpp":"1"}'
  --let $set_sea_false= secondary_engine_attribute '{"gpp":"0"}'
}

--echo #
--echo # Test gpp options with all kinds of ddl.
--echo #

--let $orig_opt_index_format_gpp_enabled = `SELECT @@global.opt_index_format_gpp_enabled`
--let $orig_innodb_lizard_stat_enabled= `SELECT @@global.innodb_lizard_stat_enabled`
--let $restart_parameters=restart:--loose_innodb_lizard_stat_enabled=1

set global innodb_lizard_stat_enabled=on;
set opt_index_format_gpp_enabled=true;
create database gpp_options;
CREATE TABLE gpp_options.t1 (
  a int NOT NULL,
  b int NOT NULL,
  c int NOT NULL,
  pad varchar(8192),
  primary key (a),
  KEY (b)
) ENGINE=innodb AUTO_INCREMENT=0;
insert into gpp_options.t1 values (1,1,1,'11111');
insert into gpp_options.t1 values (2,2,2,'22222');
insert into gpp_options.t1 values (3,3,3,'33333');
insert into gpp_options.t1 values (4,4,4,'44444');
insert into gpp_options.t1 values (5,5,5,'55555');
insert into gpp_options.t1 values (6,6,6,'66666');
insert into gpp_options.t1 values (7,7,7,'77777');

--echo # index b's gpp option stays true.
--let $SCHEMA_NAME=gpp_options
--let $TABLE_NAME=t1
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=b
--let $INDEX_COL_MIN_VAL=0
--let $INDEX_COL_MAX_VAL=100
--let $NON_INDEX_COL=pad
--let $IS_PARTITION=0
--source suite/lizard/include/gpp_has_options.inc

# ----------------------------------------------------------------------------------------------------- #
--echo # create new index b_c within gpp_opt =false, b_c gpp option stays false, b's gpp option stays true.
--eval $set_var_false
--eval CREATE INDEX b_c ON gpp_options.t1(b,c) $set_sea_false;
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--echo # the following all kinds of ddl test case unit assume that at the beginning, index b has gpp and index b_c doesn't.
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Rename an existing index b_c to bc_renamed, verify GPP option consistency.
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 RENAME INDEX b_c TO bc_renamed;
--let $INDEX_NAME=bc_renamed
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=bc_renamed
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 RENAME INDEX bc_renamed TO b_c;
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Modify column b's data type, indexes b and b_c will be rebuilt.
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 MODIFY COLUMN b BIGINT;
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 MODIFY COLUMN b INT;
--let $INDEX_NAME=b
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
# Restore the original state of index b
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 DROP INDEX b;
ALTER TABLE gpp_options.t1 ADD INDEX b (b);
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Add CHECK constraint, only support Algorithm=copy, indexes b and b_c will be rebuilt.
--echo # Drop CHECK constraint, support Algorithm=inplate, indexes b and b_c stay stable.
SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 ADD CONSTRAINT chk_b CHECK (b > 0);
--let $INDEX_NAME=b
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc

SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 DROP CHECK chk_b;
--let $INDEX_NAME=b
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
# Restore the original state of the indexes
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 DROP INDEX b;
ALTER TABLE gpp_options.t1 ADD INDEX b (b);
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Add a virtual column, indexes b and b_c stay stable. Default algorithm is INSTANT (if possible).
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 ADD COLUMN v_col INT AS (a + b) VIRTUAL;
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc

--echo # Change the expression of the virtual column, default algorithm is copy
ALTER TABLE gpp_options.t1 MODIFY COLUMN v_col INT AS (a * b) VIRTUAL;
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

--echo # Drop the virtual column, default algorithm is INSTANT (if possible)
ALTER TABLE gpp_options.t1 DROP COLUMN v_col;
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

# Restore the original state of the indexes
SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 DROP INDEX b_c;
ALTER TABLE gpp_options.t1 ADD INDEX b_c (b, c);
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Add and drop an AUTO_INCREMENT column as a non-primary key, indexes b and b_c stay stable.
--echo # Default algorithm is INPLACE (if possible).
# Step 1: Add new AUTO_INCREMENT column without making it a primary key. Rebuild the whole table.
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 
  ADD COLUMN tmp_ai INT AUTO_INCREMENT, 
  ADD UNIQUE INDEX tmp_ai_idx(tmp_ai), ALGORITHM=INPLACE;
--let $INDEX_NAME=tmp_ai_idx
--let $CONTAIN_GPP=1
--let $INDEX_COL=tmp_ai
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=tmp_ai_idx
--let $CONTAIN_GPP=1
--let $INDEX_COL=tmp_ai
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

# Step 2: Drop the AUTO_INCREMENT column and the unique index, rebuild the whole table.
SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 
  DROP INDEX tmp_ai_idx, 
  DROP COLUMN tmp_ai, ALGORITHM=INPLACE;
--let $INDEX_NAME=b
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc

# Restore the original state of the indexes
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 DROP INDEX b;
ALTER TABLE gpp_options.t1 ADD INDEX b (b);
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Toggle index visibility and verify the GPP option for index b and b_c.

# Step 1: Set the original state and create a new invisible index.
SET opt_index_format_gpp_enabled=true;
CREATE INDEX tmp_idx ON gpp_options.t1(c) INVISIBLE;
--let $INDEX_NAME=tmp_idx
--let $CONTAIN_GPP=1
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=tmp_idx
--let $CONTAIN_GPP=1
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc

# Step 2: Change index tmp_idx to visible.
SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 ALTER INDEX tmp_idx VISIBLE;
--let $INDEX_NAME=tmp_idx
--let $CONTAIN_GPP=1
--let $INDEX_COL=c
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=tmp_idx
--let $CONTAIN_GPP=1
--let $INDEX_COL=c
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc

# Step 3: Change index b to invisible.
SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 ALTER INDEX b INVISIBLE;
--let $INDEX_NAME=tmp_idx
--let $CONTAIN_GPP=1
--let $INDEX_COL=c
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=tmp_idx
--let $CONTAIN_GPP=1
--let $INDEX_COL=c
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc

# Step 4: Restore
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 DROP INDEX tmp_idx;
ALTER TABLE gpp_options.t1 ALTER INDEX b VISIBLE;
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Add and drop a foreign key, ensure indexes b and b_c are not affected.

# Step 1: Creating a referenced table t2.
CREATE TABLE gpp_options.t2 (
  id INT NOT NULL PRIMARY KEY,
  ref_data VARCHAR(255)
) ENGINE=InnoDB;
INSERT INTO gpp_options.t2 (id, ref_data) VALUES (1, 'data1'), (2, 'data2'), (3, 'data3'), (4, 'data4'), (5, 'data5'), (6, 'data6'), (7, 'data7');

# Step 2: Adding a foreign key constraint. Rebuild the whole table
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 ADD CONSTRAINT fk_t2 FOREIGN KEY (a) REFERENCES gpp_options.t2(id);
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

# Step 3: Dropping the foreign key constraint. No need to rebuild.
SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 DROP CONSTRAINT fk_t2;
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

# Cleanup - drop the referenced table t2.
DROP TABLE gpp_options.t2;

# Restore the original state of indexes.
SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 DROP INDEX b_c;
ALTER TABLE gpp_options.t1 ADD INDEX b_c (b,c);
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Rename table, ensure indexes b and b_c are not affected.
SET opt_index_format_gpp_enabled=true;
RENAME TABLE gpp_options.t1 TO gpp_options.t1_renamed;
--let $TABLE_NAME=t1_renamed
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $TABLE_NAME=t1_renamed
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc

SET opt_index_format_gpp_enabled=false;
RENAME TABLE gpp_options.t1_renamed TO gpp_options.t1;
--let $TABLE_NAME=t1
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Multi-valued indexes can have gpp

SET opt_index_format_gpp_enabled=true;
CREATE TABLE gpp_options.t_multi (
  id INT PRIMARY KEY AUTO_INCREMENT,
  data JSON,
  INDEX multi_idx ((CAST(data->'$[*]' AS UNSIGNED ARRAY)))
);

INSERT INTO gpp_options.t_multi(data) VALUES
  ('[1, 2, 3]'),
  ('[3, 4, 5]'),
  ('[5, 6, 7]'),
  ('[7, 8, 9]'),
  ('[2, 4, 6]');

--let $INDEX_COL=
--let $INDEX_NAME=multi_idx
--let $CONTAIN_GPP=1
--let $TABLE_NAME=t_multi
--source suite/lizard/include/gpp_has_options.inc

--echo # test MEMBER OF() query
call dbms_stat.flush_gpp();
SELECT * FROM gpp_options.t_multi WHERE 3 MEMBER OF (data->'$[*]');
show status like "Lizard_index_scan_guess_clust_hit";
show status like "Lizard_index_scan_guess_clust_miss";

--echo # test JSON_CONTAINS() query
call dbms_stat.flush_gpp();
SELECT * FROM gpp_options.t_multi WHERE JSON_CONTAINS(data->'$[*]', '[3]');
show status like "Lizard_index_scan_guess_clust_hit";
show status like "Lizard_index_scan_guess_clust_miss";

--echo # test JSON_OVERLAPS() query
call dbms_stat.flush_gpp();
SELECT * FROM gpp_options.t_multi WHERE JSON_OVERLAPS(data->'$[*]', '[3, 5]');
show status like "Lizard_index_scan_guess_clust_hit";
show status like "Lizard_index_scan_guess_clust_miss";

--echo # disable GPP and create a new multi-valued index, confirm it does not contain GPP
SET opt_index_format_gpp_enabled=false;
DROP INDEX multi_idx ON gpp_options.t_multi;
CREATE INDEX multi_idx2 ON gpp_options.t_multi ((CAST(data->'$[*]' AS UNSIGNED ARRAY)));
--let $INDEX_NAME=multi_idx2
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc

DROP TABLE gpp_options.t_multi;
# ----------------------------------------------------------------------------------------------------- #

--echo # ------------------------- Clean up --------------------------------------------------
--eval SET opt_index_format_gpp_enabled= $orig_opt_index_format_gpp_enabled
--eval SET GLOBAL innodb_lizard_stat_enabled= $orig_innodb_lizard_stat_enabled
drop database gpp_options;

