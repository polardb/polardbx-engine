
delimiter |;
CREATE PROCEDURE populate_tt(IN times INT)
BEGIN
  DECLARE i INT DEFAULT 1;
  truncate table tt;
  while (i <= times) DO
    insert into tt values (i, repeat('a', 255));
    SET i = i + 1;
  END WHILE;
END|
delimiter ;|

delimiter |;
CREATE PROCEDURE test_boundary(IN times INT)
BEGIN
  DECLARE i INT DEFAULT 0;
  while (i <= times) DO
    call populate_tt(i);
    select /*+ sample_percentage(100.0) */ * from tt;
    SET i = i + 1;
  END WHILE;
END|
delimiter ;|

--echo #################
--echo # Record Mode
--echo #################

SET GLOBAL debug="+d,polarx_sample_force_rec_mode";
SET GLOBAL debug="+d,sampling_use_fixed_seed";

truncate table tt;
select /*+ sample_percentage(100.0) */ * from tt;

call populate_tt(1);
select /*+ sample_percentage(100.0) */ * from tt;

call populate_tt(250);
select /*+ sample_percentage(10.0) */ * from tt;
select /*+ sample_percentage(20.0) */ * from tt;
select /*+ sample_percentage(50.0) */ * from tt;
select /*+ sample_percentage(100.0) */ count(*) from tt;
SET GLOBAL debug="-d,sampling_use_fixed_seed";

--echo # Test boundary
--disable_result_log
call test_boundary(150);
--enable_result_log

SET GLOBAL debug="-d,polarx_sample_force_rec_mode";

--echo #################
--echo # Block Mode
--echo #################
SET GLOBAL debug="+d,polarx_sample_force_block_mode";
SET GLOBAL debug="+d,sampling_use_fixed_seed";

truncate table tt;
select /*+ sample_percentage(100.0) */ * from tt;

call populate_tt(1);
select /*+ sample_percentage(100.0) */ * from tt;

call populate_tt(1000);
select /*+ sample_percentage(10.0) */ * from tt;
select /*+ sample_percentage(20.0) */ * from tt;
select /*+ sample_percentage(50.0) */ * from tt;
select /*+ sample_percentage(100.0) */ count(*) from tt;
SET GLOBAL debug="-d,sampling_use_fixed_seed";

--echo # Test boundary
--disable_result_log
call test_boundary(300);
--enable_result_log

SET GLOBAL debug="-d,polarx_sample_force_block_mode";

drop procedure populate_tt;
drop procedure test_boundary;
