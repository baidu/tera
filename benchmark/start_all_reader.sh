#!/bin/bash

source client_conf.sh

if [ $# != 8 ]; then
    echo "./start_all_reader.sh MODE NUM KEY_SIZE VALUE_SIZE CF_SIZE COMPRESS_RETIO TABLET_NUM LOG"
    exit 0
fi

MODE=$1
NUM=$2
KEY_SIZE=$3
VALUE_SIZE=$4
CF_SIZE=$5
COMPRESS_RATIO=$6
TABLET_NUM=$7
LOG=$8

#for((i=1; i<HOST_NUM; i++)); do
for((i=2; i<HOST_NUM; i++)); do
    echo "########################### ${HOST[$i]} ################################"
    ssh ${HOST[$i]} "cd ~/writer; sh start_reader.sh $MODE t$i $NUM $KEY_SIZE $VALUE_SIZE $CF_SIZE $COMPRESS_RATIO $TABLET_NUM $LOG </dev/null &>/dev/null"
    #ssh ${HOST[$i]} "cd ~/writer; sh start_reader.sh $MODE t0 $NUM $((KEY_SIZE++)) $VALUE_SIZE $CF_SIZE $COMPRESS_RATIO $TABLET_NUM $LOG </dev/null &>/dev/null"
done
