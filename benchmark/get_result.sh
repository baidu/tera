#!/bin/bash

source client_conf.sh

if [ $# != 1 ]; then
    echo "./get_result LOG_NAME"
    exit 0
fi

LOG_NAME=$1

for((i=1; i<HOST_NUM; i++)); do
    echo "########################### ${HOST[$i]} ################################"
    ssh ${HOST[$i]} "cd ~/writer; tail -n 3 $LOG_NAME"
done
