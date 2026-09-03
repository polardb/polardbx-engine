# Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.

set(ENCDB_COMMON_BASE_FILES
    include/memory.hpp
    include/bytes.h
    include/encdb_common_exception.h
    include/helper.hpp
    include/json.hpp
    include/concurrent_limited_cache.hpp
    include/storage.h
    include/encdb_constants.h
    include/shared_lock.hpp
    include/logger.h
    include/timestamp.h
    include/global_timer.h
    src/encdb_common_exception.cpp
    src/bytes.cpp
    src/logger.cpp
    src/timestamp.cpp
    src/global_timer.cpp
    )

###############################
# ENCDB_COMMON_${LIBRARY}_FILES
###############################

set(ENCDB_COMMON_HOST_FILES
    ${ENCDB_COMMON_BASE_FILES}
    include/encdb_version.h
    src/encdb_version.cpp
    include/db_operation.h
    src/db_operation.cpp
    )

##############################
# common module static library
##############################

# add library
set(TARGET_NAME encdb-common)
add_library(${TARGET_NAME} STATIC ${ENCDB_COMMON_HOST_FILES})

# set library properties
target_include_directories(${TARGET_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(${TARGET_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../core/include ${CMAKE_CURRENT_SOURCE_DIR}/../..)

set_target_properties(${TARGET_NAME} PROPERTIES COMPILE_FLAGS "${GLOBAL_APP_CXX_FLAGS} ${XXHASH_INCLUDE_FLAGS}")
target_link_libraries(${TARGET_NAME} PRIVATE ${ENCDB_APP_LINK_FLAGS})

target_link_libraries(${TARGET_NAME} PRIVATE encdb-type)
