#!/bin/bash

set -e -u -E # this script will exit if any sub-command fails

########################################
# download mysql source
########################################

if [ ! -d "mysql-cluster-gpl-7.4.8" ]; then
    wget -O mysql-cluster-gpl-7.4.8.tar.gz \
    http://dev.mysql.com/get/Downloads/MySQL-Cluster-7.4/mysql-cluster-gpl-7.4.8-linux-glibc2.5-x86_64.tar.gz
    rm -rf mysql-cluster-gpl-7.4.8
    tar zxf mysql-cluster-gpl-7.4.8.tar.gz
fi

########################################
# build
########################################

make -j4
