1. start mysqld
2. create db test
3. run sysbench
```
  sysbench scan_check_mixed.lua --mysql-host=${mysql_ip} --mysql-port=${mysql_port} --mysql-db=test --mysql-user=${user} --threads=40 --insert_thread_num=10 --update_thread_num=10 --events=1000000000 --time=1000000000 --sst_file_limit=2000 --padding_size=128 --report-interval=10 prepare

  sysbench scan_check_mixed.lua --mysql-host=${mysql_ip} --mysql-port=${mysql_port} --mysql-db=test --mysql-user=${user} --threads=40 --insert_thread_num=10 --update_thread_num=10 --events=1000000000 --time=1000000000 --sst_file_limit=2000 --padding_size=128 --report-interval=10 run
```
  scan\_thread\_num is thread\_num - insert\_thread\_num - update\_thread\_num.
  For blob, set padding\_size to a larger value, e.g. 600000.
