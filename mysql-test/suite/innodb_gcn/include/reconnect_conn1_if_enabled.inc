# set enable_reconnect flag to enable this

if ($enable_reconnect) {
   let $backup_base_seq= query_get_value(select @base_seq as base_seq, base_seq, 1);
   
   disconnect conn1;
   connect (conn1,localhost,root,,);

   --disable_query_log
   eval SET @base_seq = $backup_base_seq;
   --enable_query_log

   --echo ==== conn1 reconnected ====
}
