#!/bin/bash

# Script for Dev's daily work.  It is a good idea to use the exact same
# build options as the released version.

function version_ge()
{
  if test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"
  then
      return 0
  else
      return 1
  fi
}

get_mach_type()
{
  # ARM64: aarch64
  # SW64 : sw_64
  # X86  : x86_64
  mach_type=`uname -m`;
}

get_os_type()
{
  # Windows: MINGW32_NT
  # Mac OSX: Darwin
  # Linux  : Linux
  os_type=`uname`;
  if [ "$(expr substr $(uname -s) 1 10)" == "MINGW32_NT" ]; then
      # Windows NT
      os_type="WIN"
  fi
}

get_linux_version()
{
  if uname -r | grep -q -o "el6\|alios6"
  then
    linux_version="alios6"
  elif uname -r | grep -q -o "el7\|alios7"
  then
    linux_version="alios7"
  else
    linux_version="not_alios"
  fi
}

get_key_value()
{
  echo "$1" | sed 's/^--[a-zA-Z_-]*=//'
}

usage()
{
cat <<EOF
Usage: $0 [-t debug|release] [-d <dest_dir>] [-s <server_suffix>] [-g asan|tsan] [-i] [-r]
       Or
       $0 [-h | --help]
  -t                      Select the build type.
  -d                      Set the destination directory.
  -l                      Enable lizard debug mode.
  -s                      Set the server suffix.
  -g                      Enable the sanitizer of compiler, asan for AddressSanitizer, tsan for ThreadSanitizer
  -c                      Enable GCC coverage compiler option
  -i                      initialize mysql server
  -r                      rebuild without make chean
  -h, --help              Show this help message.

Note: this script is intended for internal use by MySQL developers.
EOF
}

parse_options()
{
  while test $# -gt 0
  do
    case "$1" in
    -t=*)
      build_type=`get_key_value "$1"`;;
    -t)
      shift
      build_type=`get_key_value "$1"`;;
    -d=*)
      dest_dir=`get_key_value "$1"`;;
    -d)
      shift
      dest_dir=`get_key_value "$1"`;;
    -s=*)
      server_suffix=`get_key_value "$1"`;;
    -s)
      shift
      server_suffix=`get_key_value "$1"`;;
    -g=*)
      san_type=`get_key_value "$1"`;;
    -g)
      shift
      san_type=`get_key_value "$1"`;;
    -c=*)
      enable_gcov=`get_key_value "$1"`;;
    -c)
      shift
      enable_gcov=`get_key_value "$1"`;;
    -l)
      enable_lizard_dbg=1;;
    -i)
      with_initialize=1;;
    -r)
      with_rebuild=1;;
    -h | --help)
      usage
      exit 0;;
    *)
      echo "Unknown option '$1'"
      exit 1;;
    esac
    shift
  done
}

dump_options()
{
  echo "Dumping the options used by $0 ..."
  echo "build_type=$build_type"
  echo "enable_lizard_dbg=$enable_lizard_dbg"
  echo "dest_dir=$dest_dir"
  echo "server_suffix=$server_suffix"
  echo "Sanitizer=$san_type"
  echo "GCOV=$enable_gcov"
  echo "mach_tpye=$mach_type"
  echo "os_type=$os_type"
  echo "cmake_version=$cmake_version"
  echo "cmake_path=$CMAKE"
  echo "gcc_version=$gcc_version"
  echo "gcc_path=$CC"
  echo "cxx_path=$CXX"
  echo "CFLAGS=$CFLAGS"
  echo "CXXFLAGS=$CXXFLAGS"
}

initialize()
{
  echo "
  [mysqld]
  port = 3306
  basedir = $dest_dir
  datadir = $dest_dir/data
  sql_mode=STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
  default_authentication_plugin = 'mysql_native_password'  #更改加密方式

  #gtid:
  gtid_mode = on                  #开启gtid模式
  enforce_gtid_consistency = on   #强制gtid一致性，开启后对于特定create table不被支持

  #binlog
  log_bin = mysql-binlog
  log_slave_updates = on
  binlog_format = row             #强烈建议，其他格式可能造成数据不一致
  binlog-ignore-db=sys
  binlog-ignore-db=mysql
  binlog-ignore-db=information_schema
  binlog-ignore-db=performance_schema
  binlog-do-db=test

  server_id = 1                   #服务器id
  " > $HOME/my.cnf

  rm -rf $dest_dir/data
  mkdir -p $dest_dir/data
  ./runtime_output_directory/mysqld --defaults-file=$HOME/my.cnf --initialize --cluster-id=1 --cluster-start-index=1 --cluster-info='127.0.0.1:23456@1'
  nohup ./runtime_output_directory/mysqld --defaults-file=$HOME/my.cnf &
}

if test ! -f sql/mysqld.cc
then
  echo "You must run this script from the MySQL top-level directory"
  exit 1
fi

build_type="release"
dest_dir=$HOME/tmp_run
server_suffix="rds-dev"
san_type=""
asan=0
tsan=0
enable_gcov=0
enable_lizard_dbg=0
with_initialize=0
with_rebuild=0

parse_options "$@"
get_mach_type
get_os_type
get_linux_version

if [ x"$build_type" = x"debug" ]; then
  build_type="Debug"
  debug=1
  if [ $enable_gcov -eq 1 ]; then
    gcov=1
  else
    gcov=0
  fi
elif [ x"$build_type" = x"release" ]; then
  # Release CMAKE_BUILD_TYPE is not compatible with mysql 8.0
  # build_type="Release"
  build_type="RelWithDebInfo"
  debug=0
  gcov=0
else
  echo "Invalid build type, it must be \"debug\" or \"release\"."
  exit 1
fi

server_suffix="-""$server_suffix"

if [ x"$build_type" = x"RelWithDebInfo" ]; then
  COMMON_FLAGS="-O3 -g -fexceptions -fno-strict-aliasing"
elif [ x"$build_type" = x"Debug" ]; then
  COMMON_FLAGS="-O0 -g3 -gdwarf-2 -fexceptions -fno-strict-aliasing"
fi

if [ x"$mach_type" = x"x86_64" ]; then # X86
  COMMON_FLAGS="$COMMON_FLAGS -fno-omit-frame-pointer -D_GLIBCXX_USE_CXX11_ABI=0"
elif [ x"$mach_type" = x"aarch64" ]; then # ARM64
  # ARM64 needn't more flags
  COMMON_FLAGS="$COMMON_FLAGS" #"-static-libstdc++ -static-libgcc"
fi

COMMON_FLAGS="$COMMON_FLAGS -fdiagnostics-color=always"
export GCC_COLORS='error=01;31:warning=01;35:note=01;36:caret=01;32:locus=01:quote=01'

CFLAGS="$COMMON_FLAGS"
CXXFLAGS="$COMMON_FLAGS"

if [ x"$san_type" = x"" ]; then
    asan=0
    tsan=0
elif [ x"$san_type" = x"asan" ]; then
    asan=1
    tsan=0
    ## gcov is conflicting with gcc sanitizer (at least for devtoolset-7),
    ## disable gcov if sanitizer is requested
    gcov=0
elif [ x"$san_type" = x"tsan" ]; then
    asan=0
    tsan=1
    ## gcov is conflicting with gcc sanitizer (at least for devtoolset-7),
    ## disable gcov if sanitizer is requested
    gcov=0
else
  echo "Invalid sanitizer type, it must be \"asan\" or \"tsan\"."
  exit 1
fi

if [ x"$mach_type" = x"aarch64" ]; then # ARM64
    CC=gcc
    CXX=g++
else # X86
    CC=/opt/rh/devtoolset-7/root/usr/bin/gcc
    CXX=/opt/rh/devtoolset-7/root/usr/bin/g++
    source /opt/rh/devtoolset-7/enable
fi

# Update choosed version
gcc_version=`$CC --version | awk 'NR==1 {print $3}'`
cmake_version=`cmake --version | awk 'NR==1 {print $3}'`

# Dumpl options
dump_options

export CC CFLAGS CXX CXXFLAGS

if [ x"$with_rebuild" = x"1" ]; then
  echo "need rebuild without clean".
else
  echo "need rebuild with clean".
  # Avoid unexpected cmake rerunning
  rm -rf packaging/deb-in/CMakeFiles/progress.marks
  rm -rf CMakeCache.txt
  make clean
  cat extra/boost/boost_1_70_0.tar.gz.*  > extra/boost/boost_1_70_0.tar.gz
  cmake .                               \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
      -DFORCE_INSOURCE_BUILD=ON          \
      -DCMAKE_BUILD_TYPE="$build_type"   \
      -DWITH_NORMANDY_CLUSTER=ON         \
      -DWITH_NORMANDY_TEST=$debug        \
      -DWITH_PROTOBUF:STRING=bundled     \
      -DSYSCONFDIR="$dest_dir"           \
      -DCMAKE_INSTALL_PREFIX="$dest_dir" \
      -DMYSQL_DATADIR="$dest_dir/data"   \
      -DWITH_DEBUG=$debug                \
      -DWITH_LIZARD_DEBUG=$enable_lizard_dbg \
      -DENABLE_GCOV=$gcov                \
      -DINSTALL_LAYOUT=STANDALONE        \
      -DMYSQL_MAINTAINER_MODE=0          \
      -DWITH_EMBEDDED_SERVER=0           \
      -DWITH_SSL=openssl                 \
      -DWITH_ZLIB=bundled                \
      -DWITH_ZSTD=bundled                \
      -DWITH_MYISAM_STORAGE_ENGINE=1     \
      -DWITH_INNOBASE_STORAGE_ENGINE=1   \
      -DWITH_CSV_STORAGE_ENGINE=1        \
      -DWITH_ARCHIVE_STORAGE_ENGINE=1    \
      -DWITH_BLACKHOLE_STORAGE_ENGINE=1  \
      -DWITH_FEDERATED_STORAGE_ENGINE=1  \
      -DWITH_PERFSCHEMA_STORAGE_ENGINE=1 \
      -DWITH_EXAMPLE_STORAGE_ENGINE=0    \
      -DWITH_TEMPTABLE_STORAGE_ENGINE=1  \
      -DWITH_XENGINE_STORAGE_ENGINE=0    \
      -DWITH_QUERY_TRACE=1               \
      -DWITH_EXTRA_CHARSETS=all          \
      -DDEFAULT_CHARSET=utf8mb4          \
      -DDEFAULT_COLLATION=utf8mb4_0900_ai_ci \
      -DENABLED_PROFILING=1              \
      -DENABLED_LOCAL_INFILE=1           \
      -DWITH_ASAN=$asan                  \
      -DWITH_TSAN=$tsan                  \
      -DWITH_BOOST="./extra/boost/boost_1_70_0.tar.gz" \
      -DDOWNLOAD_BOOST=0                \
      -DMYSQL_SERVER_SUFFIX="$server_suffix"         \
      -DENABLE_DOWNLOADS=0              \
      -DWITH_UNIT_TESTS=0
fi

make -j `getconf _NPROCESSORS_ONLN`


if [ $with_initialize -eq  1 ]; then
  initialize
  echo "use follow cmd to login mysql:"
  echo "./runtime_output_directory/mysql -uroot -hlocalhost -P3306 -p"
  echo ""
  echo "use follow sql to modify password:"
  echo "alter user 'root'@'localhost' identified WITH mysql_native_password by '';"
fi

# end of file