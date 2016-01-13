#!/bin/bash

test_dir=tera_ft_test_data

function usage() {
    echo "usage: $0 [case]"
    echo "example: $0                          # all test cases"
    echo "         $0 testcase/test_put_get.py # specify a case"
}

if [[ $# -gt 1 ]]; then
    echo "0 or 1 command line argument"
    usage
    exit 1
fi

set -x -e

rm -rf $test_dir
mkdir $test_dir
cp -r example/onebox/* $test_dir
cp build/bin/teracli $test_dir/bin
cp build/bin/tera_main $test_dir/bin
cp build/benchmark/tera_bench $test_dir/bin
cp build/benchmark/tera_mark $test_dir/bin

mkdir -p $test_dir/log
mkdir -p $test_dir/data
cp -r test/testcase $test_dir/bin
cp -r test/testcase/shell_script/* $test_dir/bin
cp src/sdk/python/TeraSdk.py $test_dir/bin/testcase
cp build/lib/libtera_c.so $test_dir/bin

cd $test_dir/bin/
sh kill_tera.sh
sh launch_tera.sh
sleep 2

export PYTHONPATH=$PYTHONPATH:../../thirdparty/include/; export PATH=$PATH:../../thirdparty/bin/

if [[ $# == 0 ]]; then
    nosetests -s -v -x > ../log/test.log
else
    nosetests -s -v -x $1 > ../log/test.log
fi

sh kill_tera.sh
