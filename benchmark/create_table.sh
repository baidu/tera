#!/bin/bash

source ts_conf.sh

if [ $# -ne 3 ]; then
    echo "./create_table.sh TABLET_NUM STORE[0=disk,1=flash,2=memory] COMPRESS[0=non,1=snappy]"
    exit 1
fi

n=$1

STORE="disk"
if [ $2 -eq 0 ]; then
    STORE="disk"
elif [ $2 -eq 1 ]; then
    STORE="flash"
elif [ $2 -eq 2 ]; then
    STORE="memory"
else
    exit
fi

COMPRESS="none"
if [ $3 -eq 0 ]; then
    COMPRESS="none"
elif [ $3 -eq 1 ]; then
    COMPRESS="snappy"
else
    exit 1
fi

rm -rf txt
for((i=1;i<n;i++)); do m=`printf "%06d" $i`; echo $m >> txt; done

for((i=1; i<HOST_NUM; i++)); do
    ./teracli create "t$i <splitsize=1000000000> {lg0 <storage=$STORE,compress=$COMPRESS> {cf0}, lg1 <storage=$STORE,compress=$COMPRESS> {cf1}}" txt
done

sleep 1

for((i=1; i<HOST_NUM; i++)); do
    for((k=0;k<n;k++)); do
        m=`printf "%02d" $k`;
        ./teracli tablet move t$j/tablet$m ${HOST[$i]}:2200;
    done
done

