#!/bin/bash
cd ../dependency
tar -xvzf libmemcached-1.0.18.tar.gz
tar -xvzf snappy-1.1.1.tar.gz

cd ../cluster
cmake .
echo "----------building libeasy---------"
make libmyeasy
echo "----------building snappy---------"                                                                                                                       
make libsnappy
echo "----------building libmemcached---------"
make libmemcached
echo "----------building librocksdb---------"
make librocksdb
echo "----------building libgflags---------"
make libgflags
echo "----------building cluster---------"
make -j
