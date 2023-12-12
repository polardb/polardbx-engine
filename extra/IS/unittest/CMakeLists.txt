set(UT_LIST
  consensus-t
  consensus1-t
  consensus2-t
  consensus-add-t
  consensus-async-t
  consensus-del-t
  consensus-learner-t
  consensus-bug-t
  consensus-priv-t
  consensus-crash-t
  consensus-crash1-t
  consensus-replicate-with-cache-log-t
  consensus-leader-transfer-t
  consensus-purge-t
  client_service-t
  easyNet-t
  misc-t
  paxos_log-t
  paxos_log_cache-t
  service-t
  witness-t
  consensus-trx-t
  tmp-t
  pxserror-t
  #msg_compress-t
  log_meta_cache-t
  consensus-leader-degrade-t
  )
foreach (ut ${UT_LIST})
  list(APPEND UT_SRC ${ut}.cc)
endforeach ()

# build ut
set(UT_LIBS
  gtest
  gtest_main
  mysqlclient
  rocksdb
  aliconsensus_no_easy
  myeasy_internal
  )
add_executable(merge_tests-t ${UT_SRC})
target_link_libraries(merge_tests-t ${UT_LIBS})
foreach (ut ${UT_LIST})
  add_executable(${ut} ${ut}.cc)
  target_link_libraries(${ut} ${UT_LIBS})
endforeach ()
