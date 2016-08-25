#!/bin/bash

max_tablet_num=10
teracli="./teracli"
table="hello"

if [ $# -ne 1 ]; then
    echo "error: ./$0 <prefix-set-file>"
    exit 1
fi

if [ ! -f $1 ]; then
    echo "error: $1 not a file"
    exit 2
fi

$teracli showx >/dev/null
if [ $? != 0 ]; then
    echo "error: teracli not work."
    exit 3
fi

# calc range set
start_set=()
end_set=()
set_len=0
while read line; do
    s=($line)
    if [ ${#s[@]} != 2 ]; then
        echo "error: prefix-set-file format error[${#s[@]}]"
        exit 4
    fi
    start_set=(${start_set[*]} ${s[0]})
    end_set=(${end_set[*]} ${s[1]})
done < $1
set_len=${#start_set[@]}
echo "prefix_set: len[$set_len], content[${start_set[*]} ${end_set[*]}]"

# calc tablet set waiting moved
tablet_set=()
tablet_num=0
for (( i=0; i<$set_len; i++)); do
    tablets=`$teracli findtablet $table ${start_set[$i]} ${end_set[$i]} | awk '{print $1}'`
    echo tablets: ${tablets[*]}
    tablet_set=(${tablet_set[*]} ${tablets[*]})
    tablet_num=${#tablet_set[@]}
    if [ $tablet_num -gt $max_tablet_num ]; then
        echo "error: too many tablets[$tablet_num] than $max_tablet_num"
        exit 5
    fi
done
echo "tablet_set: len[$tablet_num], content[${tablet_set[*]}]"

# calc idle ts set 
ts_set=()
tss=`$teracli showtsx --stdout_is_tty=false | sort -k6n | head -n $((tablet_num * 2)) | tail -n $((tablet_num * 2 - 1)) | sort -k20n | head -n $tablet_num | awk '{print $2}'`
ts_set=($tss)
echo "ts_set: len[${#ts_set[@]}], content[${ts_set[*]}]"

if [ ${#ts_set[@]} -ne $tablet_num ]; then
    echo "error: tablet_num[$tablet_num] ts_num[${#ts_set[@]}] not equal"
    exit 6
fi

# try move tablet
for ((i=0; i<$tablet_num; i++)); do 
#    echo "try move ${tablet_set[$i]} to ${ts_set[$i]}"
    $teracli tablet move ${tablet_set[$i]} ${ts_set[$i]}
done
