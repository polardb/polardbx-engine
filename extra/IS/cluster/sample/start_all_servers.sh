#!/bin/bash

host_name=`hostname`
echo "--server_members=$host_name:10000,$host_name:10001,$host_name:10002" >server_members.flag
echo "--rpc_members=$host_name:20000,$host_name:20001,$host_name:20002" >rpc_members.flag


nohup ./rd_server --flagfile=server_members.flag --flagfile=rpc_members.flag --server_id=1 &>1.log &
nohup ./rd_server --flagfile=server_members.flag --flagfile=rpc_members.flag --server_id=2 &>2.log &
nohup ./rd_server --flagfile=server_members.flag --flagfile=rpc_members.flag --server_id=3 &>3.log &




