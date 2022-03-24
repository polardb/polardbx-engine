# ==== Purpose ====
# 
# Ensure that the server is running with GTID support on.
#
# ==== Usage ====
#
# --source include/have_gtid.inc

--source include/have_log_bin.inc

--disable_warnings

--let $_have_gtid= query_get_value(SHOW GLOBAL VARIABLES LIKE 'GTID_MODE', Value, 1)
if ($_have_gtid != ON)
{
  --skip Test requires GTID_MODE=ON
}

--enable_warnings
