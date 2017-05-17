#!/bin/bash

if [[ $# != 7 || $6 -lt 0 || $6 -gt 100 ]]; then
    echo "$0 DIST[zipfian, uniform, latest] ROW_NUM OP_NUM VALUE_SIZE COLUMN_NUM UPDATE_PROPORTION[0~100] TABLE_NAME"
    exit 0
fi

DIST=$1
ROW_NUM=$2
OP_NUM=$3
VALUE_SIZE=$4
COLUMN_NUM=$5
UPDATE_PROPORTION=$6
TABLE_NAME=$7

UPDATE_PROPORTION=`printf "%02d" $6`
READ_PROPORTION=`expr 100 - $UPDATE_PROPORTION`
READ_PROPORTION=`printf "%02d" $READ_PROPORTION`

echo "$UPDATE_PROPORTION"
echo "$READ_PROPORTION"

cd YCSB-0.12.0
ln -s ../tera_mark tera_mark
ln -s ../tera.flag tera.flag

bin/ycsb run tera -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
		-p requestdistribution=$DIST \
		-p recordcount=$ROW_NUM \
		-p operationcount=$OP_NUM \
		-p fieldlength=$VALUE_SIZE \
		-p fieldcount=$COLUMN_NUM \
		-p updateproportion=0.$UPDATE_PROPORTION \
		-p readproportion=0.$READ_PROPORTION \
		-p exportfile=ycsb.out \
		| ./tera_mark --mode=m --tablename=$TABLE_NAME --type=async --verify=false

