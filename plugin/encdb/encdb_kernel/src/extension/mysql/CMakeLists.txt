# Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.

################################################
# EncDB for MySQL
################################################
set(ENCDB_MYSQL_INC_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/../../crypto/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../common/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../type/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../core/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../rule/include
    )
if(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)
  set(ENCDB_MYSQL_INC_DIR
      ${ENCDB_MYSQL_INC_DIR}
      ${CMAKE_CURRENT_SOURCE_DIR}/../../masking/include
      ${CMAKE_CURRENT_SOURCE_DIR}/utils/include)
  add_subdirectory(utils)
endif()

set(ENCDB_MYSQL_FILES
    include/encdb.h
    src/encdb.cpp
    )
if(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)
  set(ENCDB_MYSQL_FILES
      ${ENCDB_MYSQL_FILES}
      include/encdb_encode.h
      include/encdb_encode_impl.h
      include/auto_encode.h
      src/encdb_encode.cpp
      src/encdb_encode_impl.cpp
      src/auto_encode.cpp
      )
endif()

# build shared library
if(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)
  add_library(encdb-mysql SHARED ${ENCDB_MYSQL_FILES}
                                 $<TARGET_OBJECTS:encdb-core>
                                 $<TARGET_OBJECTS:encdb-rule>)
else(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)
  add_library(encdb-mysql STATIC ${ENCDB_MYSQL_FILES}
                                $<TARGET_OBJECTS:encdb-core>
                                $<TARGET_OBJECTS:encdb-rule>)
endif(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)

target_include_directories(encdb-mysql PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(encdb-mysql PRIVATE ${ENCDB_MYSQL_INC_DIR})
set_target_properties(encdb-mysql PROPERTIES COMPILE_FLAGS "${GLOBAL_APP_CXX_FLAGS}")

# link against db-mock
if(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)
  add_dependencies(encdb-mysql encdb-db-mock)
  target_link_libraries(encdb-mysql PRIVATE encdb-common encdb-crypto encdb-key encdb-type encdb-db-mock encdb-mask encdb-mysql-utils)
else(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)
  target_link_libraries(encdb-mysql PRIVATE encdb-common encdb-crypto encdb-key encdb-type)
endif(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)

target_link_libraries(encdb-mysql PRIVATE ${ENCDB_APP_LINK_FLAGS})

add_dependencies(encdb-mysql encdb-core)
