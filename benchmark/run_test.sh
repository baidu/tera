#!/bin/bash

if [[ $# != 9 || $6 -lt 0 || $6 -gt 100 ]]; then
    echo "$0 DIST[zipfian, uniform, latest] ROW_NUM OP_NUM VALUE_SIZE COLUMN_NUM UPDATE_PROPORTION[0~100] OP_SPEED THREAD_NUM TABLE_NAME"
    exit 1
fi

DIST=$1
ROW_NUM=$2
OP_NUM=$3
VALUE_SIZE=$4
COLUMN_NUM=$5
UPDATE_PROPORTION=$6
OP_SPEED=$7
THREAD_NUM=$8
TABLE_NAME=$9

UPDATE_PROPORTION=`echo $6 | awk '{printf("%.2f",$1/100)}'`
READ_PROPORTION=`echo $6 | awk '{printf("%.2f",(100-$1)/100)}'`

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
                -p updateproportion=$UPDATE_PROPORTION \
                -p readproportion=$READ_PROPORTION \
                -p target=$OP_SPEED \
                -p thread=$THREAD_NUM \
		-p exportfile=ycsb.out \
		| ./tera_mark --mode=m --tablename=$TABLE_NAME --type=async --verify=false

exit $?

