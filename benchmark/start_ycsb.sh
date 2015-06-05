#!/bin/bash

if [ $# != 7 ]; then
    echo "./start_ycsb.sh MODE[load, run] DIST[zipfian, uniform, latest] NUM VALUE_SIZE COLUMN_NUM TABLE_NAME LOG"
    exit 0
fi

MODE=$1
DIST=$2
NUM=$3
VALUE_SIZE=$4
COLUMN_NUM=$5
TABLE_NAME=$6
LOG=$7

# 
nohup bin/ycsb $MODE tera -p workload=com.yahoo.ycsb.workloads.CoreWorkload -p requestdistribution=$DIST -p recordcount=$NUM -p operationcount=$NUM -p fieldlength=$VALUE_SIZE -p fieldcount=$COLUMN_NUM -p updateproportion=0 -p readproportion=1 | ./tera_mark --mode=m --tablename=$TABLE_NAME --type=async --verify=false > $LOG 2> $LOG.err &

# use pre-define workload
#nohup bin/ycsb $MODE tera -P workloads/$WORK_LOAD -p recordcount=$NUM -p operationcount=$NUM | ./tera_mark --mode=m --tablename=$TABLE_NAME --type=async --verify=false > $LOG 2> $LOG.err &
