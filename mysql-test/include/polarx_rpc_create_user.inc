
--disable_query_log

if ($create_user_disable_binlog)
{
  SET SESSION sql_log_bin = 0;
}

CREATE USER 'polarx_root' @'%' IDENTIFIED WITH 'mysql_native_password';
GRANT ALL ON *.* TO 'polarx_root' @'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

if ($create_user_disable_binlog)
{
  SET SESSION sql_log_bin = 1;
}

--enable_query_log
