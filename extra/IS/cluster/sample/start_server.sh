#!/bin/bash

n_th=$1
nohup ./rd_server --flagfile=cluster.conf --server_id=$n_th &>$n_th.log &
