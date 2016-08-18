#!/bin/bash

set -e -u -E # this script will exit if any sub-command fails

########################################
# download mysql source
########################################

if [ ! -d "mysql-5.6.31" ]; then
    wget -O mysql-5.6.31.tar.gz http://dev.mysql.com/get/Downloads/MySQL-5.6/mysql-5.6.31.tar.gz
    rm -rf mysql-5.6.31
    tar zxf mysql-5.6.31.tar.gz
fi

########################################
# build
########################################

make -j4
