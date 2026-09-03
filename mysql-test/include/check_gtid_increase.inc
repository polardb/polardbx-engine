SELECT interval_start, interval_end 
INTO @current_start, @current_end 
FROM mysql.gtid_executed 
LIMIT 1;

if (`SELECT @current_start != 1`)
{
  SELECT 'ERROR: GTID interval_start is not 1! Current:' as message, @current_start as current_value;
  --exit
}

if (`SELECT IFNULL(@last_gtid_end, 0) != 0 AND @current_end <= @last_gtid_end`)
{
  SELECT 'ERROR: GTID end did not increase!' as message, @last_gtid_end as previous, @current_end as current;
  --exit
}

if (`SELECT IFNULL(@last_gtid_end, 0) != 0 AND @current_end > @last_gtid_end`)
{
  SELECT 'GTID end increased' as message;
  SET @last_gtid_end = @current_end;
}

if (`SELECT IFNULL(@last_gtid_end, 0) = 0`)
{
  SET @last_gtid_end = @current_end;
  SELECT 'Initial GTID end' as message;
}