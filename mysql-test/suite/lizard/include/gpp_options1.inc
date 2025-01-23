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
--echo # Export and Import test.
CREATE DATABASE gpp_tmp;
# First, create a table with index b and b_c both gpp enabled.
SET opt_index_format_gpp_enabled=true;
CREATE TABLE gpp_tmp.t1 (
  a int NOT NULL,
  b int NOT NULL,
  c int NOT NULL,
  pad varchar(8192),
  primary key (a),
  KEY (b),
  KEY b_c (b,c)
) ENGINE=innodb AUTO_INCREMENT=0;
ALTER TABLE gpp_tmp.t1 DISCARD TABLESPACE;

FLUSH TABLE gpp_options.t1 FOR EXPORT;
LET $MYSQLD_DATADIR = `SELECT @@datadir`;
--copy_file $MYSQLD_DATADIR/gpp_options/t1.ibd $MYSQLTEST_VARDIR/tmp/t1.ibd
--copy_file $MYSQLD_DATADIR/gpp_options/t1.cfg $MYSQLTEST_VARDIR/tmp/t1.cfg
UNLOCK TABLES;
--copy_file $MYSQLTEST_VARDIR/tmp/t1.ibd $MYSQLD_DATADIR/gpp_tmp/t1.ibd
--copy_file $MYSQLTEST_VARDIR/tmp/t1.cfg $MYSQLD_DATADIR/gpp_tmp/t1.cfg

--error ER_TABLE_SCHEMA_MISMATCH
ALTER TABLE gpp_tmp.t1 IMPORT TABLESPACE;

# alter table with index b and b_c both gpp disabled.
ALTER TABLE gpp_tmp.t1 DROP INDEX b, DROP INDEX b_c;
--eval $set_var_false
--eval ALTER TABLE gpp_tmp.t1 ADD INDEX b_c (b,c) $set_sea_false, ADD INDEX b (b) $set_sea_false;
--error ER_TABLE_SCHEMA_MISMATCH
ALTER TABLE gpp_tmp.t1 IMPORT TABLESPACE;

--eval $set_var_true
ALTER TABLE gpp_tmp.t1 DROP INDEX b;
--eval ALTER TABLE gpp_tmp.t1 ADD INDEX b (b) $set_sea_true;

--remove_file $MYSQLD_DATADIR/gpp_tmp/t1.cfg
--error ER_TABLE_CFG_MISSING
ALTER TABLE gpp_tmp.t1 IMPORT TABLESPACE;
--copy_file $MYSQLTEST_VARDIR/tmp/t1.cfg $MYSQLD_DATADIR/gpp_tmp/t1.cfg
ALTER TABLE gpp_tmp.t1 IMPORT TABLESPACE;

--remove_file $MYSQLTEST_VARDIR/tmp/t1.cfg
--remove_file $MYSQLTEST_VARDIR/tmp/t1.ibd
DROP DATABASE gpp_tmp;
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Add index with a new column e, index b and b_c stay stable.
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 ADD COLUMN e INT DEFAULT 1;
CREATE INDEX e_idx ON gpp_options.t1(e);
--let $INDEX_NAME=e_idx
--let $CONTAIN_GPP=1
--let $INDEX_COL=e
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=e_idx
--let $CONTAIN_GPP=1
--let $INDEX_COL=e
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--let $INDEX_COL=b
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc

SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 DROP INDEX e_idx;
ALTER TABLE gpp_options.t1 DROP COLUMN e;
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
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # alter table drop column c, index b_c rebuild, index b stays stable.
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 DROP COLUMN c;
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 ADD COLUMN c int NOT NULL DEFAULT 1;
UPDATE gpp_options.t1 SET c=b;
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 DROP INDEX b_c, ADD INDEX b_c (b,c);
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Add unique index b_unique, index b and b_c stay stable.
--eval $set_var_true
--eval ALTER TABLE gpp_options.t1 ADD UNIQUE INDEX b_unique (b) $set_sea_true;
--let $INDEX_NAME=b_unique
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=b_unique
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 DROP INDEX b_unique;
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Add/drop fulltext index, the whole table was rebuilt. fts always non-gpp, index b and b_c depend on opt_gpp.
# FTS_DOC_ID_INDEX can't be specified in DDL, so we don't test sea for this case.
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 ADD COLUMN tmp_str varchar(10) default 'default';
ALTER TABLE gpp_options.t1 ADD FULLTEXT INDEX tmp_fts (tmp_str);
--let $INDEX_NAME=tmp_fts
--let $CONTAIN_GPP=0
--let $INDEX_COL=tmp_str
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=FTS_DOC_ID_INDEX
--let $CONTAIN_GPP=1
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_COL=b
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=tmp_fts
--let $CONTAIN_GPP=0
--let $INDEX_COL=tmp_str
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=FTS_DOC_ID_INDEX
--let $CONTAIN_GPP=1
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_COL=b
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc


SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 DROP INDEX tmp_fts;
--let $INDEX_NAME=FTS_DOC_ID_INDEX
--let $CONTAIN_GPP=1
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_COL=b
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=FTS_DOC_ID_INDEX
--let $CONTAIN_GPP=1
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_COL=b
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

# Instant DDL can't change the hidden FTS_DOC_ID_INDEX index.
SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 RENAME COLUMN a to aa;
ALTER TABLE gpp_options.t1 RENAME COLUMN aa to a;
--let $INDEX_NAME=FTS_DOC_ID_INDEX
--let $CONTAIN_GPP=1
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_COL=b
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=FTS_DOC_ID_INDEX
--let $CONTAIN_GPP=1
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_COL=b
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

# Can't do instant drop column because table has flag DICT_TF2_FTS_HAS_DOC_ID. Rebuild table.
SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 DROP COLUMN tmp_str;
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc

SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 DROP INDEX b;
ALTER TABLE gpp_options.t1 ADD INDEX b (b);
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------------- #
--echo # Add spatial index, spatial index always non-gpp, index b and b_c stay stable.
ALTER TABLE gpp_options.t1 DROP INDEX b_c;
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 ADD COLUMN tmp_geo GEOMETRY SRID 4326;
UPDATE gpp_options.t1 SET tmp_geo = ST_GeomFromText('POINT(0 0)', 4326) WHERE tmp_geo IS NULL;
ALTER TABLE gpp_options.t1 MODIFY tmp_geo GEOMETRY NOT NULL SRID 4326;
--eval $set_var_false
--eval ALTER TABLE gpp_options.t1 ADD INDEX b_c(b,c) $set_sea_false;
SET opt_index_format_gpp_enabled=true;
ALTER TABLE gpp_options.t1 ADD SPATIAL INDEX tmp_spatial (tmp_geo);
--let $INDEX_NAME=tmp_spatial
--let $CONTAIN_GPP=0
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_COL=b
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
--source include/restart_mysqld.inc
--let $INDEX_NAME=tmp_spatial
--let $CONTAIN_GPP=0
--let $INDEX_COL=
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_COL=b
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc

SET opt_index_format_gpp_enabled=false;
ALTER TABLE gpp_options.t1 DROP INDEX tmp_spatial;
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


ALTER TABLE gpp_options.t1 DROP COLUMN tmp_geo;
--let $INDEX_NAME=b_c
--let $CONTAIN_GPP=0
--source suite/lizard/include/gpp_has_options.inc
--let $INDEX_NAME=b
--let $CONTAIN_GPP=1
--source suite/lizard/include/gpp_has_options.inc
# ----------------------------------------------------------------------------------------------------- #

--echo # ------------------------- Clean up --------------------------------------------------
--eval SET opt_index_format_gpp_enabled= $orig_opt_index_format_gpp_enabled
--eval SET GLOBAL innodb_lizard_stat_enabled= $orig_innodb_lizard_stat_enabled
drop database gpp_options;

