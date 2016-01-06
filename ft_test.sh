#!/bin/bash

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

rm -rf tmp
mkdir tmp
cp -r example/onebox/* tmp
cp build/bin/teracli tmp/bin
cp build/bin/tera_main tmp/bin
cp build/benchmark/tera_bench tmp/bin
cp build/benchmark/tera_mark tmp/bin

mkdir -p tmp/log
mkdir -p tmp/data
cp -r test/testcase tmp/bin
cp -r test/testcase/shell_script/* tmp/bin
cp src/sdk/python/TeraSdk.py tmp/bin/testcase
cp build/lib/libtera_c.so tmp/bin

cd tmp/bin/
sh kill_tera.sh
sh launch_tera.sh
sleep 2

export PYTHONPATH=$PYTHONPATH:../../thirdparty/include/; export PATH=$PATH:../../thirdparty/bin/

if [[ $# == 0 ]]; then
    nosetests -s -v -x -w testcase > ../log/test.log
else
    nosetests -s -v -x $1 > ../log/test.log
fi

sh kill_tera.sh
