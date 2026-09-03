# include/wait_innodb_purged_gcn.inc
#
# SUMMARY
#
#    Waits until purged_gcn >= $expect_purged_gcn, or operation times out.
#
# USAGE
#
#    --let $wait_timeout = 30
#    --let $expect_can_not_advance = 0
#    --let $expect_purged_gcn = 3000
#    --source include/wait_innodb_all_purged.inc
#

if ($is_debug)
{
--disable_query_log

let $wait_counter_init= 300;
if ($wait_timeout)
{
  let $wait_counter_init= `SELECT $wait_timeout * 10`;
}
# Reset $wait_timeout so that its value won't be used on subsequent
# calls, and default will be used instead.
let $wait_timeout= 0;

let $wait_counter= $wait_counter_init;

# Keep track of how many times the wait condition is tested
let $wait_condition_reps= 0;
while ($wait_counter)
{
  --let $purged_gcn = query_get_value("call dbms_undo.purge_status()", PURGED_GCN, 1)

  let $success= `SELECT $purged_gcn >= $expect_purged_gcn`;
  inc $wait_condition_reps;
  if ($success)
  {
      let $wait_counter= 0;
  }
  if (!$success)
  {
      set global innodb_purge_run_now=ON;
      sleep 0.1;
      dec $wait_counter;
  }
}

if ($expect_can_not_advance) {
  if ($success) {
    CALL DBMS_UNDO.PURGE_STATUS();
    echo $expect_purged_gcn;
    die expect can not advance purge, but actually purge advance.
  }
}

if (!$expect_can_not_advance) {
  if (!$success)
  {
    CALL DBMS_UNDO.PURGE_STATUS();
    echo $expect_purged_gcn;
    die Expect purge advance, but timeout in wait_innodb_purged_gcn.inc;
  }
}

--enable_query_log
}
