# ==== Purpose ====
# 
# Ensure that the server is running with GTID support on.
#
# ==== Usage ====
#
# --source include/have_gtid.inc

--source include/have_log_bin.inc

--disable_warnings
if (`SELECT @@gtid_mode = 'OFF'`)
{
  --skip Test requires GTID_MODE=ON.
}
--enable_warnings
