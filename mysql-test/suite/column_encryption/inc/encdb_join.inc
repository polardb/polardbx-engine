--disable_warnings

# Write per-connection provision SQL to a reusable temp file.
# Each --exec $MYSQL spawns a new connection with no CipherContext;
# the connection must do MEK_PROVISION before touching encrypted columns.
--write_file $MYSQL_TMP_DIR/aurora_prov.sql
SELECT from_base64(encdb_process_message(encdb_generate_mek_request(encdb_process_message('{"request_type":0, "version":"1.2.15"}'), '00112233445566778899aabbccddeeff'))) INTO @mek_prov;
EOF
let $diff_file_prev=$MYSQL_TMP_DIR/encdb_unencrypted_data.txt;
let $diff_file_after=$MYSQL_TMP_DIR/encdb_original_data.txt;
let $default_decrypt_sql=select c0, encdb_decrypt(c1, 'aurora'),
                                c2, encdb_decrypt(c3, 'aurora'),
                                c4, encdb_decrypt(c5, 'aurora'),
                                c6, encdb_decrypt(c7, 'aurora'),
                                c8, encdb_decrypt(c9, 'aurora'),
                                c10, encdb_decrypt(c11, 'aurora'),
                                c12, encdb_decrypt(c13, 'aurora'),
                                c14, encdb_decrypt(c15, 'aurora'),
                                c16, encdb_decrypt(c17, 'aurora'),
                                c18, encdb_decrypt(c19, 'aurora'),
                                c20, encdb_decrypt(c21, 'aurora'),
                                c22, encdb_decrypt(c23, 'aurora'),
                                encdb_decrypt(c24, 'aurora'), encdb_decrypt(c25, 'aurora'),
                                encdb_decrypt(c26, 'aurora'), encdb_decrypt(c27, 'aurora'),
                                encdb_decrypt(c28, 'aurora'), encdb_decrypt(c29, 'aurora'),
                                encdb_decrypt(c30, 'aurora'), encdb_decrypt(c31, 'aurora'),
                                encdb_decrypt(c32, 'aurora'), encdb_decrypt(c33, 'aurora'),
                                encdb_decrypt(c34, 'aurora'), encdb_decrypt(c35, 'aurora'),
                                encdb_decrypt(c36, 'aurora'), encdb_decrypt(c37, 'aurora'),
                                encdb_decrypt(c38, 'aurora'), encdb_decrypt(c39, 'aurora'),
                                encdb_decrypt(c40, 'aurora'), encdb_decrypt(c41, 'aurora'),
                                encdb_decrypt(c42, 'aurora'), encdb_decrypt(c43, 'aurora'),
                                encdb_decrypt(c44, 'aurora'), encdb_decrypt(c45, 'aurora'),
                                encdb_decrypt(c46, 'aurora'), encdb_decrypt(c47, 'aurora')
                                from mysql.cipher_text_join;


create table mysql.cipher_text_join(
               c0 text, c1 text, c2 text, c3 text,
               c4 text, c5 text, c6 text, c7 text,
               c8 text, c9 text, c10 text, c11 text,
               c12 text, c13 text, c14 text, c15 text,
               c16 text, c17 text, c18 text, c19 text,
               c20 text, c21 text, c22 text, c23 text,
               c24 text, c25 text, c26 text, c27 text,
               c28 text, c29 text, c30 text, c31 text,
               c32 text, c33 text, c34 text, c35 text,
               c36 text, c37 text, c38 text, c39 text,
               c40 text, c41 text, c42 text, c43 text,
               c44 text, c45 text, c46 text, c47 text);

echo # Verify data correctness in plain SQL.;
--exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
--exec echo "$select_stmt" >> $MYSQL_TMP_DIR/aurora_exec.sql
--exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $MYSQL_TMP_DIR/encrypt_data.txt
--replace_result $MYSQL_TMP_DIR MYSQL_TMP_DIR
eval load data local infile "$MYSQL_TMP_DIR/encrypt_data.txt" into table mysql.cipher_text_join;

--exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
--exec echo "$default_decrypt_sql" >> $MYSQL_TMP_DIR/aurora_exec.sql
--exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $diff_file_prev
--exec $MYSQL --skip-column-names -uroot -e "$select_stmt" test > $diff_file_after;
diff_files $diff_file_prev $diff_file_after;
delete from mysql.cipher_text_join;


echo # Verify data correctness in prepared staments.;
--exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
--exec echo "PREPARE stmt_1 FROM '$select_stmt';EXECUTE stmt_1;" >> $MYSQL_TMP_DIR/aurora_exec.sql
--exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $MYSQL_TMP_DIR/encrypt_data.txt
--replace_result $MYSQL_TMP_DIR MYSQL_TMP_DIR
eval load data local infile "$MYSQL_TMP_DIR/encrypt_data.txt" into table mysql.cipher_text_join;

--exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
--exec echo "$default_decrypt_sql" >> $MYSQL_TMP_DIR/aurora_exec.sql
--exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $diff_file_prev
--exec $MYSQL --skip-column-names -uroot -e "$select_stmt" test > $diff_file_after;
diff_files $diff_file_prev $diff_file_after;
delete from mysql.cipher_text_join;


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
eval load data local infile "$MYSQL_TMP_DIR/encrypt_data.txt" into table mysql.cipher_text_join;
drop procedure encdb_proc;

--exec cp $MYSQL_TMP_DIR/aurora_prov.sql $MYSQL_TMP_DIR/aurora_exec.sql
--exec echo "$default_decrypt_sql" >> $MYSQL_TMP_DIR/aurora_exec.sql
--exec $MYSQL --skip-column-names -uaurora -p123456 test < $MYSQL_TMP_DIR/aurora_exec.sql > $diff_file_prev
--exec $MYSQL --skip-column-names -uroot -e "$select_stmt" test > $diff_file_after;
diff_files $diff_file_prev $diff_file_after;
delete from mysql.cipher_text_join;

drop table mysql.cipher_text_join;
remove_file $MYSQL_TMP_DIR/aurora_prov.sql;
remove_file $MYSQL_TMP_DIR/aurora_exec.sql;

--enable_warnings
