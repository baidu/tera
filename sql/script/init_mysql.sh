#!/bin/bash

BIN_DIR=`pwd`
ROOT_DIR=$BIN_DIR/..

mkdir -p $ROOT_DIR/log
mkdir -p $ROOT_DIR/run
mkdir -p $ROOT_DIR/var
mkdir -p $ROOT_DIR/tmp
mkdir -p $ROOT_DIR/mysql-files

../scripts/mysql_install_db --user=$USER --basedir=$ROOT_DIR --datadir=$ROOT_DIR/data

