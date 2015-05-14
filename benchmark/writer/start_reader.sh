#!/bin/bash

source ~/.bash_profile

if [ $# != 9 ]; then
    echo "./start_reader.sh MODE TABLE_NAME NUM KEY_SIZE VALUE_SIZE CF_SIZE COMPRESS_RETIO TABLET_NUM LOG"
    exit 0
fi

MODE=$1
TABLE_NAME=$2
NUM=$3
KEY_SIZE=$4
VALUE_SIZE=$5
CF_SIZE=$6
COMPRESS_RATIO=$7
TABLET_NUM=$8
LOG=$9

nohup ./tera_bench --benchmarks=$MODE --num=$NUM --key_size=$KEY_SIZE --value_size=$VALUE_SIZE --compression_ratio=$COMPRESS_RATIO --tablet_num=$TABLET_NUM | awk -F '\t' -v OFS='\t' -v CF=$CF_SIZE '{ts=substr($1,15,9); for(i=0;i<CF;i++) {col=sprintf("cf%d:", i); print $1,col,ts} }' | ./sample --mode=r --tablename=$TABLE_NAME --type=async --verify=false &> $LOG &
