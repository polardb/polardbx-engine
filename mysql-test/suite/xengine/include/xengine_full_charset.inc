# --let $test_file = ...
# --source suite/xengine/include/xengine_full_charset.inc

--let $cs = latin1
--let $coll = latin1_bin
--echo # Test $test_file with $cs and $coll
--source $test_file


--let $cs = gbk
--let $coll = gbk_bin
--echo # Test $test_file with $cs and $coll
--source $test_file

--let $coll= gbk_chinese_ci
--echo # Test $test_file with $cs and $coll
--source $test_file


--let $cs = utf8
--let $coll = utf8_bin
--echo # Test $test_file with $cs and $coll
--source $test_file

--let $coll= utf8_general_ci
--echo # Test $test_file with $cs and $coll
--source $test_file


--let $cs = utf8mb4
--let $coll = utf8mb4_bin
--echo # Test $test_file with $cs and $coll
--source $test_file

--let $coll = utf8mb4_general_ci
--echo # Test $test_file with $cs and $coll
--source $test_file

--let $coll = utf8mb4_0900_ai_ci
--echo # Test $test_file with $cs and $coll
--source $test_file

