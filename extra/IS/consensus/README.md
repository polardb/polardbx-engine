# 编译步骤
1. 获取源码
2. 安装依赖程序
  - protobuf snappy-devel t-ais-db-lz4
  - 因为依赖c++11，6u机器需要安装devtoolset-2系列包
3. cd consensus && sh build.sh
4. 输出目录为 consensus/output

# SDK
1. SDK输出目录consensus/output/sdk
2. 第三方程序编译方法：${CXX} -std=c++11 -o echo_learner_client echo_learner_client.cc liblearnerSDK.a -I ./include -L /usr/lib/mysql/ -lmysqlclient -lprotobuf -lpthread -lssl -lcrypto
