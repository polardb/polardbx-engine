--disable_warnings

#
# Assumption:
# 1. root user must have full_access privilege, so we could get original data.
# 2. The table mysql.cipher_text should have no rules.
# 
# Procedure:
# 1. We execute the SQL statement to get all fields using aurora user, and keep 
#    the results in encrypt_data.txt
#
# 2. Then we import the results in encrypt_data.txt into table mysql.cipher_text.
#
# 3. After that, we call $default_decrypt_sql or $decrypt_sql to simulate the 
#    decryption process using JDBC, the results of which are stored in diff_file_prev.
#
#    If all fields are encrypted, the caller could set $decrypt_sql to 0 to use 
#    the default_decrypt_sql. However, if some fields are not encrypted, the caller 
#    should set $decrypt_sql by themselves.
#
# 4. Execute the SQL statement again but using root user, and keep the results in diff_file_after.
#    Compare the results in diff_file_prev and diff_file_after to make sure all works well.
#

let $diff_file_prev=$MYSQL_TMP_DIR/encdb_unencrypted_data.txt;
let $diff_file_after=$MYSQL_TMP_DIR/encdb_original_data.txt;
let $default_decrypt_sql=select encdb_decrypt(c0, 'aurora'), encdb_decrypt(c1, 'aurora'),
                                encdb_decrypt(c2, 'aurora'), encdb_decrypt(c3, 'aurora'),
                                encdb_decrypt(c4, 'aurora'), encdb_decrypt(c5, 'aurora'),
                                encdb_decrypt(c6, 'aurora'), encdb_decrypt(c7, 'aurora'),
                                encdb_decrypt(c8, 'aurora'), encdb_decrypt(c9, 'aurora'),
                                encdb_decrypt(c10, 'aurora'), encdb_decrypt(c11, 'aurora'),
                                encdb_decrypt(c12, 'aurora'), encdb_decrypt(c13, 'aurora'),
                                encdb_decrypt(c14, 'aurora'), encdb_decrypt(c15, 'aurora'),
                                encdb_decrypt(c16, 'aurora'), encdb_decrypt(c17, 'aurora'),
                                encdb_decrypt(c18, 'aurora'), encdb_decrypt(c19, 'aurora'),
                                encdb_decrypt(c20, 'aurora'), encdb_decrypt(c21, 'aurora'),
                                encdb_decrypt(c22, 'aurora'), encdb_decrypt(c23, 'aurora')
                                from mysql.cipher_text;

# Write per-connection provision SQL to a reusable temp file.
# Each --exec $MYSQL spawns a new connection with no CipherContext;
# the connection must do MEK_PROVISION before touching encrypted columns.
--write_file $MYSQL_TMP_DIR/aurora_prov.sql
SELECT from_base64(encdb_process_message(encdb_generate_mek_request(encdb_process_message('{"request_type":0, "version":"1.2.15"}'), '00112233445566778899aabbccddeeff'))) INTO @mek_prov;
EOF

let $select_stmt_orgdata=$select_stmt;

echo # Verify data correctness in plain SQL.;
echo $select_stmt;
--exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
--exec echo "$select_stmt" >> $MYSQL_TMP_DIR/aurora_exec.sql
--exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $MYSQL_TMP_DIR/encrypt_data.txt
--replace_result $MYSQL_TMP_DIR MYSQL_TMP_DIR
eval load data local infile "$MYSQL_TMP_DIR/encrypt_data.txt" into table mysql.cipher_text;

if (!$decrypt_sql){
  --exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
  --exec echo "$default_decrypt_sql" >> $MYSQL_TMP_DIR/aurora_exec.sql
  --exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $diff_file_prev
}
if ($decrypt_sql){
  --exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
  --exec echo "$decrypt_sql" >> $MYSQL_TMP_DIR/aurora_exec.sql
  --exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $diff_file_prev
}
--exec $MYSQL --skip-column-names -uroot -e "$select_stmt_orgdata" test > $diff_file_after;
diff_files $diff_file_prev $diff_file_after;
delete from mysql.cipher_text;

if (!$skip_prepare) {
  echo # Verify data correctness in prepared staments.;
  --exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
  --exec echo "PREPARE stmt_1 FROM '$select_stmt';EXECUTE stmt_1;" >> $MYSQL_TMP_DIR/aurora_exec.sql
  --exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $MYSQL_TMP_DIR/encrypt_data.txt
  --replace_result $MYSQL_TMP_DIR MYSQL_TMP_DIR
  eval load data local infile "$MYSQL_TMP_DIR/encrypt_data.txt" into table mysql.cipher_text;

  if (!$decrypt_sql){
    --exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
    --exec echo "$default_decrypt_sql" >> $MYSQL_TMP_DIR/aurora_exec.sql
    --exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $diff_file_prev
  }
  if ($decrypt_sql){
    --exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
    --exec echo "$decrypt_sql" >> $MYSQL_TMP_DIR/aurora_exec.sql
    --exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $diff_file_prev
  }
  --exec $MYSQL --skip-column-names -uroot -e "$select_stmt_orgdata" test > $diff_file_after;
  diff_files $diff_file_prev $diff_file_after;
  delete from mysql.cipher_text;
}


echo # Verify data correctness in store procedure.;
delimiter |;
eval CREATE PROCEDURE `encdb_proc` ()
BEGIN
$select_stmt;
END |
delimiter ;|

--exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
--exec echo "call encdb_proc();" >> $MYSQL_TMP_DIR/aurora_exec.sql
--exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $MYSQL_TMP_DIR/encrypt_data.txt
--replace_result $MYSQL_TMP_DIR MYSQL_TMP_DIR
eval load data local infile "$MYSQL_TMP_DIR/encrypt_data.txt" into table mysql.cipher_text;
drop procedure encdb_proc;

if (!$decrypt_sql){
  --exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
  --exec echo "$default_decrypt_sql" >> $MYSQL_TMP_DIR/aurora_exec.sql
  --exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $diff_file_prev
}
if ($decrypt_sql){
  --exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
  --exec echo "$decrypt_sql" >> $MYSQL_TMP_DIR/aurora_exec.sql
  --exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $diff_file_prev
}
--exec $MYSQL --skip-column-names -uroot -e "$select_stmt_orgdata" test > $diff_file_after;
diff_files $diff_file_prev $diff_file_after;
delete from mysql.cipher_text;

delete from mysql.cipher_text;
remove_file $diff_file_prev;
remove_file $diff_file_after;
remove_file $MYSQL_TMP_DIR/encrypt_data.txt;
remove_file $MYSQL_TMP_DIR/aurora_prov.sql;
remove_file $MYSQL_TMP_DIR/aurora_exec.sql;

--enable_warnings
