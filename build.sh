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
Usage: $0 [-t debug|release] [-d <dest_dir>] [-s <server_suffix>] [-g asan|tsan]
       Or
       $0 [-h | --help]
  -t                      Select the build type.
  -d                      Set the destination directory.
  -l                      Enable lizard debug mode.
  -s                      Set the server suffix.
  -g                      Enable the sanitizer of compiler, asan for AddressSanitizer, tsan for ThreadSanitizer
  -c                      Enable GCC coverage compiler option
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
    -l)
      shift
      enable_lizard_dbg=1;;
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

if test ! -f sql/mysqld.cc
then
  echo "You must run this script from the MySQL top-level directory"
  exit 1
fi

build_type="release"
dest_dir="/u01/mysql"
server_suffix="rds-dev"
san_type=""
asan=0
tsan=0
enable_gcov=0
enable_lizard_dbg=0

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
fi

# Update choosed version
gcc_version=`$CC --version | awk 'NR==1 {print $3}'`
cmake_version=`cmake --version | awk 'NR==1 {print $3}'`

# Dumpl options
dump_options

export CC CFLAGS CXX CXXFLAGS

# Avoid unexpected cmake rerunning
rm -rf packaging/deb-in/CMakeFiles/progress.marks

rm -rf CMakeCache.txt
make clean

cmake .                               \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DFORCE_INSOURCE_BUILD=ON          \
    -DCMAKE_BUILD_TYPE="$build_type"   \
    -DWITH_NORMANDY_CLUSTER=ON         \
    -DWITH_NORMANDY_TEST=$debug        \
    -DWITH_7U=ON                       \
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
    -DWITH_EXTRA_CHARSETS=all          \
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
    -DWITH_BOOST="../extra/boost/boost_1_70_0.tar.gz" \
    -DDOWNLOAD_BOOST=1 \
    -DWITH_BOOST=extra/boost \
    -DMYSQL_SERVER_SUFFIX="$server_suffix"         \
    -DWITHOUT_IS_UT=1

make -j `cat /proc/cpuinfo | grep processor| wc -l`

# end of file
