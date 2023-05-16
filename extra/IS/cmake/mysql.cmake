# import system mysql
MACRO(IMPORT_MYSQL)
  link_directories(
    /usr/lib64/mysql
    /usr/lib/mysql
  )
ENDMACRO(IMPORT_MYSQL)