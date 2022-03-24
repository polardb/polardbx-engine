#!/bin/bash
sudo yum install devtoolset-2-runtime.noarch -b current -y
sudo yum install devtoolset-2-binutils.x86_64 -b current -y
sudo yum install devtoolset-2-gcc.x86_64 -b current -y
sudo yum install devtoolset-2-gcc-c++.x86_64 -b current -y
sudo yum install cmake.x86_64 -b current -y
sudo yum install protobuf.x86_64 -b current -y
sudo yum install protobuf-devel.x86_64 -b current -y
sudo yum install snappy.x86_64 -b current -y
sudo yum install t-ais-db-lz4.x86_64 -b current -y
sudo yum install gtest-devel.x86_64 -b current -y
