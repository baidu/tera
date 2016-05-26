#!/bin/bash

set -e -u -E # this script will exit if any sub-command fails

########################################
# download mysql source
########################################

if [ ! -d "mysql-cluster-gpl-7.4.10" ]; then
    wget -O mysql-cluster-gpl-7.4.10.tar.gz http://cdn.mysql.com//Downloads/MySQL-Cluster-7.4/mysql-cluster-gpl-7.4.10.tar.gz
    rm -rf mysql-cluster-gpl-7.4.10
    tar zxf mysql-cluster-gpl-7.4.10.tar.gz
fi

########################################
# build
########################################

make -j4
