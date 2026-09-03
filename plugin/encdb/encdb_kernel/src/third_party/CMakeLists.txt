# Copyright (c) 2019-2021 Alibaba DAMO DBLab.

# xxHash, gcc-dependent

execute_process(
    COMMAND sh -c "${CMAKE_CXX_COMPILER} -xc++ /dev/null -E -Wp,-v 2>&1 | grep -E '/[.0-9]+/include$' | sed -n 's/^ //p'"
    OUTPUT_VARIABLE INTRINSICS_INCLUDE_PATH
)

string(REPLACE "\n" " " INTRINSICS_INCLUDE_PATH "${INTRINSICS_INCLUDE_PATH}")
set(XXHASH_INCLUDE_FLAGS "-idirafter ${CMAKE_CURRENT_SOURCE_DIR}/xxHash-0.8.1 -idirafter ${INTRINSICS_INCLUDE_PATH}" PARENT_SCOPE)

# TODO: unused

#1.1.1.c shall not be needed
#add_subdirectory(openssl-1.1.1c)
#use sgxssl with openssl-1.1.1k instead
#add_subdirectory(openssl_1_1_1k)
#add_subdirectory(sgxssl_2.14_1.1.1k)
