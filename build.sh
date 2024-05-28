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
Usage: $0 [-t debug|release] [-d <dest_dir>] [-s <server_suffix>] [-g asan|tsan|ubsan|valg] [-i none|master|xpaxos] [-r]
       Or
       $0 [-h | --help]
  -t                      Select the build type.
  -d                      Set the destination directory.
  -l                      Enable lizard debug mode.
  -s                      Set the server suffix.
  -g                      Enable the sanitizer of compiler, asan for AddressSanitizer, tsan for ThreadSanitizer, ubsan for UndefinedBehaviorSanitizer, valg for valgrind
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
    -i=*)
      initialize_type=`get_key_value "$1"`;;
    -i)
      shift
      initialize_type=`get_key_value "$1"`;;
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
  mysqlx_port = 3406
  basedir = $dest_dir
  datadir = $dest_dir/node1/data
  tmpdir = $dest_dir/node1/temp
  socket = $dest_dir/node1/temp/mysql.sock
  log_error_verbosity=3
  log_error=$dest_dir/mysql-err1.log

  default_authentication_plugin = 'mysql_native_password'
  #debug=+d,query,info,error,enter,exit:t:i:A,$dest_dir/mysqld.trace

  gtid_mode = on
  enforce_gtid_consistency = on
  log_bin = mysql-binlog
  binlog_format = row
  binlog_row_image = FULL
  master_info_repository = TABLE
  relay_log_info_repository = TABLE

  consensus_log_level=1

  server_id = 1
  cluster_id=1
  " > $dest_dir/my1.cnf

  cat $dest_dir/my1.cnf > $dest_dir/my2.cnf
  echo "
  port = 3307
  mysqlx_port = 3407
  datadir = $dest_dir/node2/data
  tmpdir = $dest_dir/node2/temp
  socket = $dest_dir/node2/temp/mysql.sock
  log_error=$dest_dir/mysql-err2.log
  " >> $dest_dir/my2.cnf

  cat $dest_dir/my1.cnf > $dest_dir/my3.cnf
  echo "
  port = 3308
  mysqlx_port = 3408
  datadir = $dest_dir/node3/data
  tmpdir = $dest_dir/node3/temp
  socket = $dest_dir/node3/temp/mysql.sock
  log_error=$dest_dir/mysql-err3.log
  " >> $dest_dir/my3.cnf

  if [ x$initialize_type == x"xpaxos" ]; then
    rm -rf $dest_dir/node1 $dest_dir/node2 $dest_dir/node3
    mkdir -p $dest_dir/node1/temp  $dest_dir/node2/temp  $dest_dir/node3/temp
    $dest_dir/bin/mysqld --defaults-file=$dest_dir/my1.cnf --initialize-insecure  --cluster-info='127.0.0.1:23451;127.0.0.1:23452;127.0.0.1:23453@1'
    $dest_dir/bin/mysqld --defaults-file=$dest_dir/my2.cnf --initialize-insecure  --cluster-info='127.0.0.1:23451;127.0.0.1:23452;127.0.0.1:23453@2'
    $dest_dir/bin/mysqld --defaults-file=$dest_dir/my3.cnf --initialize-insecure  --cluster-info='127.0.0.1:23451;127.0.0.1:23452;127.0.0.1:23453@3'
    nohup $dest_dir/bin/mysqld --defaults-file=$dest_dir/my1.cnf &
    nohup $dest_dir/bin/mysqld --defaults-file=$dest_dir/my2.cnf &
    nohup $dest_dir/bin/mysqld --defaults-file=$dest_dir/my3.cnf &
  else
    rm -rf $dest_dir/node1
    mkdir -p $dest_dir/node1/temp
    ./runtime_output_directory/mysqld --defaults-file=$dest_dir/my1.cnf --initialize-insecure  --cluster-info='127.0.0.1:23451@1'
    nohup ./runtime_output_directory/mysqld --defaults-file=$dest_dir/my1.cnf & #--debug='+d,query,info,error,enter,exit:t:i:A,/home/mysql/tmp_run/mysqld.trace'
  fi
}

if test ! -f sql/mysqld.cc
then
  echo "You must run this script from the MySQL top-level directory"
  exit 1
fi

build_type="release"
dest_dir=$HOME/tmp_run
server_suffix="dev"
san_type=""
asan=0
tsan=0
ubsan=0
valg=0
gcov=0
enable_gcov=0
enable_lizard_dbg=0
initialize_type="none"
with_rebuild=0

parse_options "$@"

get_mach_type
get_os_type
get_linux_version

if [[ x"$build_type" = x"debug" ]]; then
  build_type="Debug"
  debug=1
  if [ $enable_gcov -eq 1 ]; then
    gcov=1
  else
    gcov=0
  fi
elif [[ x"$build_type" = x"release" ]]; then
  build_type="Release"
  debug=0
  gcov=0
elif [[ x"$build_type" = x"release_with_debinfo" ]]; then
  build_type="RelWithDebInfo"
  debug=0
  gcov=0
else
  echo "Invalid build type, it must be \"debug\" or \"release\" or \"release_with_debinfo\"."
  exit 1
fi

server_suffix="-""$server_suffix"

if [[ x"$build_type" = x"RelWithDebInfo" ]] || [[ x"$build_type" = x"Release" ]]; then
  COMMON_FLAGS="-O3 -g -D_FORTIFY_SOURCE=2  "
elif [[ x"$build_type" = x"Debug" ]]; then
  COMMON_FLAGS="-O0 -g3 -fstack-protector-strong"
fi

COMMON_FLAGS="$COMMON_FLAGS -fdiagnostics-color=always -fexceptions -fno-omit-frame-pointer"

if [ x"$mach_type" = x"x86_64" ]; then # X86
  COMMON_FLAGS="$COMMON_FLAGS"
elif [ x"$mach_type" = x"aarch64" ]; then # ARM64
  # ARM64 needn't more flags
  COMMON_FLAGS="$COMMON_FLAGS -Wl,-Bsymbolic"
fi

export GCC_COLORS='error=01;31:warning=01;35:note=01;36:caret=01;32:locus=01:quote=01'

CFLAGS="$COMMON_FLAGS"
CXXFLAGS="$COMMON_FLAGS"

if [ x"$san_type" = x"" ]; then
  asan=0
  tsan=0
  ubsan=0
  valg=0
elif [ x"$san_type" = x"asan" ]; then
    asan=1
    ## gcov is conflicting with gcc sanitizer (at least for devtoolset-7),
    ## disable gcov if sanitizer is requested
    gcov=0
elif [ x"$san_type" = x"tsan" ]; then
    tsan=1
    ## gcov is conflicting with gcc sanitizer (at least for devtoolset-7),
    ## disable gcov if sanitizer is requested
    gcov=0
elif [ x"$san_type" = x"ubsan" ]; then
    ubsan=1
    ## gcov is conflicting with gcc sanitizer (at least for devtoolset-7),
    ## disable gcov if sanitizer is requested
    gcov=0
elif [ x"$san_type" = x"valg" ]; then
    valg=1
    ## gcov is conflicting with gcc sanitizer (at least for devtoolset-7),
    ## disable gcov if sanitizer is requested
    gcov=0
else
  echo "Invalid sanitizer type, it must be \"asan\" or \"tsan\" or \"ubsan\" or \"valg\"."
  exit 1
fi

CC=gcc
CXX=g++

# Update choosed version
gcc_version=`$CC --version | awk 'NR==1 {print $3}'`
cmake_version=`cmake --version | awk 'NR==1 {print $3}'`

# Dumpl options
dump_options

export CC CFLAGS CXX CXXFLAGS

if [ x"$with_rebuild" = x"1" ]; then
  echo "need rebuild without clean"
else
  echo "need rebuild with clean"
  # Avoid unexpected cmake rerunning
  rm -rf packaging/deb-in/CMakeFiles/progress.marks
  rm -rf CMakeCache.txt
  make clean
  cat extra/boost/boost_1_77_0.tar.bz2.*  > extra/boost/boost_1_77_0.tar.bz2
  cmake .                               \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
      -DFORCE_INSOURCE_BUILD=1           \
      -DCMAKE_BUILD_TYPE="$build_type"   \
      -DWITH_PROTOBUF:STRING=bundled     \
      -DSYSCONFDIR="$dest_dir"           \
      -DCMAKE_INSTALL_PREFIX="$dest_dir" \
      -DMYSQL_DATADIR="$dest_dir/data"   \
      -DMYSQL_UNIX_ADDR="$dest_dir/mysql.sock"   \
      -DWITH_DEBUG=$debug                \
      -DENABLE_GCOV=$gcov                \
      -DINSTALL_LAYOUT=STANDALONE        \
      -DMYSQL_MAINTAINER_MODE=1          \
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
      -DWITH_EXTRA_CHARSETS=all          \
      -DDEFAULT_CHARSET=utf8mb4          \
      -DDEFAULT_COLLATION=utf8mb4_0900_ai_ci \
      -DENABLED_PROFILING=1              \
      -DENABLED_LOCAL_INFILE=1           \
      -DWITH_ASAN=$asan                  \
      -DWITH_TSAN=$tsan                  \
      -DWITH_UBSAN=$ubsan                \
      -DWITH_VALGRIND=$valg              \
      -DWITH_BOOST="./extra/boost/boost_1_77_0.tar.bz2" \
      -DDOWNLOAD_BOOST=0                \
      -DWITH_TESTS=0
fi

make -j 40 install

if [ x$initialize_type != x"none" ]; then
  initialize
  echo "use follow cmd to login mysql:"
  echo -e "./runtime_output_directory/mysql -uroot -S $dest_dir/node1/temp/mysql.sock \n"
fi

# end of file
