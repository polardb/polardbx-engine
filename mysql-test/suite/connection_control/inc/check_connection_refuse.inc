# Following variables should be set:
# $USER                 Name of the user
# $PASSWORD             Password to be supplied
# $SUCCESS              Whether a successful connection is expected or not
# $REFUSE               Whether refuse to authenticate or not
# $REFUSE_STATS         Expected value of Connection_control_refuse_generated
# $USE_AUTH_PLUGIN      Whether an authentication plugin is to be used or not
# $CLIENT_AUTH_PLUGIN   Authentication plugin

connection default;
disable_query_log;
disable_result_log;

if( $REFUSE == 0){
  if ($SUCCESS == 0)
  {
    --echo # Connection attempt should fail.
    if ($USE_AUTH_PLUGIN == 0)
    {
      --error 1
      --exec $MYSQL -u$USER -p$PASSWORD -e "SELECT 1;" 2>&1
    }
    if ($USE_AUTH_PLUGIN == 1)
    {
      --error 1
      --exec $MYSQL -u$USER $CLIENT_AUTH_PLUGIN -p$PASSWORD -e "SELECT 1;" 2>&1
    }
  }

  if ($SUCCESS != 0)
  {
    --echo # Connection attempt should succeed.
      if ($USE_AUTH_PLUGIN == 0)
    {
      --exec $MYSQL -u$USER -p$PASSWORD -e "SELECT 1;" 2>&1
    }
    if ($USE_AUTH_PLUGIN == 1)
    {
      --exec $MYSQL -u$USER $CLIENT_AUTH_PLUGIN -p$PASSWORD -e "SELECT 1;" 2>&1
    }
  }
}

if( $REFUSE != 0 )
{
  --echo # Refuse to authenticate the password
  if ($USE_AUTH_PLUGIN == 0)
  {
    --error ER_CONN_CONTROL_CONN_REFUSE_CONNECTION
    connect(fail_con_u1, localhost, $USER,$PASSWORD,,,,);
  }
  if ($USE_AUTH_PLUGIN == 1)
  {
    --error ER_CONN_CONTROL_CONN_REFUSE_CONNECTION
    connect(fail_con_u1, localhost, $USER,$PASSWORD,,,,);
  }
}
enable_result_log;
--echo Connection_control_refuse_generated should be $REFUSE_STATS
SHOW STATUS LIKE 'Connection_control_refuse_generated';
enable_query_log;
