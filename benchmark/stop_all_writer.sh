#!/bin/bash

source client_conf.sh

for((i=1; i<HOST_NUM; i++)); do
    echo "########################### ${HOST[$i]} ################################"
    #ssh ${HOST[$i]} "killall -9 sample; killall -9 sample; killall -9 sample;find /home/tera/writer -type f -name '*log*' -exec rm {} \;"
    ssh ${HOST[$i]} "killall -9 sample; killall -9 sample; killall -9 sample"
done
