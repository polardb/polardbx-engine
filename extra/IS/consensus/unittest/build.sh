CC=/opt/rh/devtoolset-2/root/usr/bin/gcc
CXX=/opt/rh/devtoolset-2/root/usr/bin/g++

CLUSTER_SRC="../algorithm/configuration.cc ../protocol/paxos.pb.cc ../net/easyNet.cc ../service/service.cc ../algorithm/paxos.cc ../algorithm/paxos_log.cc ../algorithm/paxos_server.cc"

$CXX -std=c++11 -I/usr/include/easy/ -I../include gunit_test_main.cc consensus-t.cc easyNet-t.cc service-t.cc $CLUSTER_SRC -g -O0 -lgtest -lprotobuf -leasy -o gtest_run
