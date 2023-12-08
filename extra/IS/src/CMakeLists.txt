set(CONSENSUS_SRC
  algorithm/paxos.cc
  algorithm/single_leader.cc
  algorithm/witness.cc
  client/client_service.cc
  client/learner_client.cc
  common/crc.cc
  common/msg_compress.cc
  common/paxos_error.cc
  common/thread_timer.cc
  config/configuration.cc
  log/file_paxos_log.cc
  log/log_meta_cache.cc
  log/mem_paxos_log.cc
  log/paxos_log_cache.cc
  net/easyNet.cc
  protocol/paxos.pb.cc
  server/learner_server.cc
  server/paxos_server.cc
  service/service.cc
  )

if (WITH_TESTS)
  list(APPEND CONSENSUS_SRC log/rd_paxos_log.cc)
endif ()

# build aliconsensus
add_library(aliconsensus STATIC ${CONSENSUS_SRC})
add_library(aliconsensus_no_easy STATIC ${CONSENSUS_SRC})
list(APPEND ALICONSENSUS_LIBS ${ROCKSDB_OUTPUT} snappy z bz2 lz4 rt)
list(APPEND ALICONSENSUS_LIBS libprotobuf-lite)
if (WITH_TESTS)
  list(APPEND ALICONSENSUS_LIBS rocksdb)
endif ()
target_link_libraries(aliconsensus PRIVATE ${ALICONSENSUS_LIBS})
target_link_libraries(aliconsensus_no_easy PRIVATE ${ALICONSENSUS_LIBS})
target_link_libraries(aliconsensus_no_easy PRIVATE mysqlclient)

target_include_directories(aliconsensus INTERFACE ${EASY_INCLUDE})
target_include_directories(aliconsensus INTERFACE ${CONSENSUS_INCLUDE_DIR})
target_include_directories(aliconsensus_no_easy INTERFACE ${EASY_INCLUDE})
target_include_directories(aliconsensus_no_easy INTERFACE ${CONSENSUS_INCLUDE_DIR})
target_link_libraries(aliconsensus PRIVATE myeasy)
