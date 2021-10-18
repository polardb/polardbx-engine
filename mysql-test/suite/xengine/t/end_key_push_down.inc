--disable_warnings
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

eval create table t1 (
  k1 $key_type,
  k2 $key_type,
  c1 $c_type,
  c2 $c_type,
  primary key (k1, k2),
  key i1 (c1, c2)
) engine=xengine;

# hidden pk
eval create table t2 (
  k1 $key_type,
  k2 $key_type,
  c1 $c_type,
  c2 $c_type,
  key i2 (c1, c2)
) engine=xengine;
--enable_warnings

show keys in t1;
show keys in t2;

let $table = t1;
let $index = i1;
--source end_key_push_down_do_select.inc

let $table = t2;
let $index = i2;
--source end_key_push_down_do_select.inc

drop table t1;
drop table t2;
