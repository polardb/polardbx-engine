#
# Include this script to wait until the connection to the
# server has been dropped
--disable_result_log
--disable_query_log
let $counter= 500;
if ($disconnect_timeout)
{
  # Use --expr (client-side arithmetic, no server connection needed)
  # Original: SELECT $disconnect_timeout * 10 (converts seconds to 0.1s iterations)
  --let $__dt_factor= 10
  --expr $counter= $disconnect_timeout * $__dt_factor
}
if ($ASAN_OPTIONS) {
  # 3x multiplier for ASAN (server shutdown is I/O bound, not CPU bound)
  --let $__asan_mult= 3
  --expr $counter= $counter * $__asan_mult
}
let $mysql_errno= 0;
while (!$mysql_errno)
{
  --error 0,1040,1053,2002,2003,2006,2013,2016,2017,ER_CLIENT_INTERACTION_TIMEOUT
  show session status;

  dec $counter;
  if (!$counter)
  {
    if ($show_rpl_debug_info)
    {
      --source include/show_rpl_debug_info.inc
      --echo current_connectio= $CURRENT_CONNECTION
    }
    --die Server failed to disapear
  }
  --sleep 0.1
}
--enable_query_log
--enable_result_log
