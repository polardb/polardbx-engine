cmake_minimum_required(VERSION 2.8)

MESSAGE(STATUS "Building consensus library")

# Build consensus library and its dependency
execute_process(
COMMAND bash -c "cd ${CMAKE_CURRENT_SOURCE_DIR}/consensus && ./build.sh -m 80"
)

SET (TARGET_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../cluster)

# Set up directory to save header files
execute_process(
  COMMAND ${CMAKE_COMMAND} -E remove_directory ${TARGET_DIR}
  COMMAND ${CMAKE_COMMAND} -E make_directory ${TARGET_DIR}
  )

SET(LIBEASY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/consensus/output)

# Copy libmyeasy header files
execute_process(
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${LIBEASY_DIR}/include/easy ${TARGET_DIR}/libmyeasy
  )

SET(LIBCONSENSUS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/consensus)

# Copy libaliconsensus header files
execute_process(
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${LIBCONSENSUS_DIR}/include ${TARGET_DIR}/consensus
  )
