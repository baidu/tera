#!/bin/bash
source ./config

PID=`ps x | grep tera_master | grep $PORT | awk '{print $1}'`;
if [ ${PID}"x" != "x" ]; then
    kill -9 $PID;
fi

for ((i=1; i<=${TABLETNODE_NUM}; i++)); do
    PID=`ps x | grep tabletserver | grep $((PORT+i)) | awk '{print $1}'`;
    if [ ${PID}"x" != "x" ]; then
        kill -9 $PID;
    fi
done
