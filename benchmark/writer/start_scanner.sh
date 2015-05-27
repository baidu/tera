#!/bin/bash

source ~/.bash_profile

if [ $# != 6 ]; then
    echo "./start_scanner.sh TABLE_NAME START END CF_LIST IF_PRINT LOG"
    exit 0
fi

TABLE_NAME=$1
START=$2
END=$3
CF_LIST=$4
IF_PRINT=$5
LOG=$6

nohup ./sample --mode=s --tablename=$TABLE_NAME --start_key=$START --end_key=$END --cf_list=$CF_LIST --print=$IF_PRINT --verify=false &> $LOG &
