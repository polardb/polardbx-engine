# Copyright (c) 2019-2022 Alibaba DAMO DBLab

##############################################
# configure encdb core environmental variables
##############################################

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wundef -fpermissive -Wno-narrowing")

#####################################
# include modules in dependency order
#####################################

# imported libraries
if(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)
  add_subdirectory(mock_db)   # no denpendency, only to pass compilation.
  add_subdirectory(test)      # depends on all
  add_subdirectory(masking)   # no dependency
endif(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)
add_subdirectory(third_party)
add_subdirectory(type)        # no dependency
add_subdirectory(common)      # depends on: type
add_subdirectory(crypto)      # depends on: type, common
add_subdirectory(extension)   # depends on: core
add_subdirectory(key)         # depends on: type, common, crypto
add_subdirectory(rule)        # depends on: type, common, crypto, key
add_subdirectory(core)        # depends on all
