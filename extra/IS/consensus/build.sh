#!/bin/bash
#
# Script for Dev's daily work.  It is a good idea to use the exact same
# build options as the released version.

get_key_value()
{
  echo "$1" | sed 's/^--[a-zA-Z_-]*=//'
}

usage()
{
cat <<EOF
Usage: $0 [-t debug|release] [-m 57|80]
       Or
       $0 [-h | --help]
  -t                      Select the build type [debug|release].
  -m                      mysql mode [57|80].
  -p                      platforms [x86|arm]
  -h, --help              Show this help message.

Note: this script is intended for internal use by X-Paxos developers.
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
    -m=*)
      mysql_mode=`get_key_value "$1"`;;
    -m)
      shift
      mysql_mode=`get_key_value "$1"`;;
    -p=*)
      platforms=`get_key_value "$1"`;;
    -p)
      shift
      platforms=`get_key_value "$1"`;;
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
  echo "mysql_mode=$mysql_mode"
  echo "platforms=$platforms"
}

build_type="release"
mysql_mode="57"
platforms="x86"

parse_options "$@"
dump_options

if [ x"$build_type" = x"debug" ]; then
  debug="ON"
elif [ x"$build_type" = x"release" ]; then
  debug="OFF"
else
  echo "Invalid build type, it must be \"debug\" or \"release\"."
  exit 1
fi

if [ x"$mysql_mode" = x"57" ]; then
  use_proto3="OFF"
  CC=gcc
  CXX=g++
elif [ x"$mysql_mode" = x"80" ]; then
  use_proto3="ON"
  if [ x"$platforms" = x"x86" ]; then
    if [ -e /opt/rh/devtoolset-7/root/usr/bin/gcc ]; then
      CC=/opt/rh/devtoolset-7/root/usr/bin/gcc
      CXX=/opt/rh/devtoolset-7/root/usr/bin/g++
    else
      CC=gcc
      CXX=g++
    fi
  else
  #  CC=/opt/gcc-9.2.0/bin/gcc
  #  CXX=/opt/gcc-9.2.0/bin/g++
  CC=gcc
  CXX=g++
  fi
else
  echo "Invalid mysql mode, it must be \"57\" or \"80\"."
  exit 1
fi
export CC CXX

rm -rf bu output
mkdir bu && cd bu

# modify this cmake script for you own needs
cmake -D CMAKE_INSTALL_PREFIX=../output -D WITH_DEBUG=$debug -D WITH_TSAN=OFF -D WITH_ASAN=OFF -D WITH_PROTOBUF3=$use_proto3 ..
make libmyeasy -j

# this library is only needed in the test, it's not necessary to make it
#make librocksdb -j

if [ x"$mysql_mode" = x"80" ]; then
  make libprotobuf -j
fi
cd ../protocol
if [ x"$mysql_mode" = x"80" ]; then
  ../../dependency/protobuf-3.6.1/bu/bin/protoc -I. --cpp_out=. paxos.proto
else
  protoc -I. --cpp_out=. paxos.proto
fi
cd ../bu
make -j
