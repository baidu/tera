#!/bin/bash

source client_conf.sh

for((i=1; i<HOST_NUM; i++)); do
    echo "########################### ${HOST[$i]} ################################"
    rsync -r writer/ ${HOST[$i]}:~/writer/ &
done

wait
