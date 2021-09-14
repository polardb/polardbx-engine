# ==== Purpose ====
# 
# Ensure that the server is running with GTID support off.
#
# ==== Usage ====
#
# --source include/not_gtid_enabled.inc

--disable_warnings
if (`SELECT @@gtid_mode = 'ON'`)
{
  --skip Test requires GTIDs off.
}
--enable_warnings
