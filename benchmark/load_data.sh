#!/bin/bash

if [ $# != 4 ]; then
    echo "$0 ROW_NUM VALUE_SIZE COLUMN_NUM TABLE_NAME"
    exit 0
fi

ROW_NUM=$1
VALUE_SIZE=$2
COLUMN_NUM=$3
TABLE_NAME=$4

cd YCSB-0.12.0
ln -s ../tera_mark tera_mark
ln -s ../tera.flag tera.flag

bin/ycsb load tera -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
		-p recordcount=$ROW_NUM \
		-p fieldlength=$VALUE_SIZE \
		-p fieldcount=$COLUMN_NUM \
		-p exportfile=ycsb.out \
		| ./tera_mark --mode=m --tablename=$TABLE_NAME --type=async --verify=false
