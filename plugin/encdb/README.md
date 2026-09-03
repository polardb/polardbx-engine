# Secure Gateway
Secure Gateway is a project that brings powerful end-to-end data protection to databases. We now support PolarDB MySQL and RDS MySQL.
## Compile
### RDS MySQL
To Compile with RDS MySQL, Secure Gateway has to be placed inside a RDS MySQL source tree.
Your first step would be to clone RDS MySQL code repo:
```shell
git clone git@gitlab.alibaba-inc.com:RDS-Server/RDS-MySQL-5.7.git
```
Then place `SecureGateway` directory inside RDS MySQL's source tree:
```shell
cp -r SecureGateway RDS_5715/plugin/encdb
```
The last step would be to compile RDS MySQL altogether. Secure Gateway would be compiled with RDS kernel automatically.
```shell
cd RDS_5715 &&
sh build.sh -t release
```
### PolarDB MySQL
For now, we support PolarDB MySQL by exporting a shared(.so) library. We can compile this library without PolarDB MySQL's source.

It should be noticed that we compile this version of Secure Gateway inside `SecureGateway/encdb_kernel` subdirectory, not `SecureGateway`.
```shell
cd SecureGateway/encdb_kernel &&
mkdir build &&
cd build &&
cmake .. -DENCDB_DB_TYPE=POLARDB_MYSQL &&
make -j package
```
After the steps we generate a tar package, which includes library files and a sample program.
