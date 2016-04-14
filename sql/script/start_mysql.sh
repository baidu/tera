#!/bin/bash

BIN_DIR=`pwd`
ROOT_DIR=$BIN_DIR/..

export LD_LIBRARY_PATH=$ROOT_DIR/lib/plugin:$LD_LIBRARY_PATH
nohup \
./mysqld --port=8806 --user=$USER \
    --datadir=$ROOT_DIR/data \
    --tmpdir=$ROOT_DIR/tmp \
    --socket=$ROOT_DIR/var/mysql.sock \
    --secure-file-priv=$ROOT_DIR/mysql-files \
    --pid-file=$ROOT_DIR/run/mysqld.pid \
    --log-error=$ROOT_DIR/log/mysqld.log \
    --lc-messages-dir=$ROOT_DIR/share \
    --plugin-dir=$ROOT_DIR/lib/plugin \
    --plugin-load="ha_tera.so;libtera.so" \
    --old_passwords=2 \
    --gdb --core-file --debug \
    &> $ROOT_DIR/log/stderr &

