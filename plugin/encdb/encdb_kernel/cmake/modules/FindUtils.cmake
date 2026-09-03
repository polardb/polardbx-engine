# Some util functions for cmake

cmake_minimum_required(VERSION 3.8)
include(CMakeParseArguments)


# make symbol link from each file in files to dest_dir with the same file basename
function(createOutputFiles files dest_dir)
  file(MAKE_DIRECTORY ${dest_dir})
  file(GLOB FILE_LIST ${files})
  foreach(src_file ${FILE_LIST})
    get_filename_component(src_file_basename ${src_file} NAME)
    configure_file(${src_file} ${dest_dir}/${src_file_basename} COPYONLY)
  endforeach()
endfunction(createOutputFiles files dest_dir)


# add double quote to the first argument as it is list
# cmake script version
FILE(WRITE ${CMAKE_BINARY_DIR}/merge_file.cmake "
string(TOLOWER \"\${SRC}\" src_lower )
string(TOLOWER \"\${DST}\" dst_lower )
if (NOT \"\${src_lower}\" MATCHES \${dst_lower})
  file(WRITE \${DST} \"\")
endif (NOT \"\${src_lower}\" MATCHES \${dst_lower})
# convert input string to a list
separate_arguments(SRC)
foreach(src_file \${SRC})
  string(TOLOWER \"\${src_file}\" src_file_lower)
  if (NOT \${src_file_lower} STREQUAL \${dst_lower})
    FILE(READ \${src_file} content)
    file(APPEND \${DST} \"\${content}\")
  endif(NOT \${src_file_lower} STREQUAL \${dst_lower})
endforeach(src_file \${SRC})

")
message(STATUS "[ENCDB] ${CMAKE_BINARY_DIR}/merge_file.cmake is generated successfully.")
