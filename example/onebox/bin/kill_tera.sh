#!/bin/bash
source ./config

for ((i=0; i<=${TABLETNODE_NUM}; i++)); do
    PID=`ps x | grep tera_main | grep $((PORT+i)) | awk '{print $1}'`;
    if [ ${PID}"x" != "x" ]; then
        kill -9 $PID;
    fi
done
