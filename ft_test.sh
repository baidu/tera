#!/bin/bash

function usage() {
    echo "usage: 
    $0 [opts] 
    -d path:         test temp file path
    -c casename:     run one case
    -f               just perform fetches for all tests
    -r               just perform runs for all tests

    e.g. $0                             # all test cases
         $0 -c testcase/test_put_get.py # specify a case"
}

test_dir="test_output/functional_test"
case_name=""
fetch_without_run=false
run_without_fetch=false

while getopts c:d:h:fr arg
do
    case $arg in
        c)
            case_name=$OPTARG
            echo "case_name: $case_name";; 
        d)
            test_dir=$OPTARG
            echo "test_dir: $test_dir";; 
        f)
            fetch_without_run=true;;
        r)
            run_without_fetch=true;;
        h)
            usage
            exit 0;;
        ?) 
            echo "unkonw argument: $arg"
            exit 1;; 
    esac
done

set -x -e

if ! $run_without_fetch; then
    if [ -d "$test_dir" ]; then
        echo "path already exists, please delete it manually before run this script: "$test_dir
        exit 65
    fi

    mkdir -p $test_dir

    cp -r example/onebox/* $test_dir
    cp build/bin/teracli $test_dir/bin
    cp build/bin/tera_main $test_dir/bin
    cp build/bin/tera_test $test_dir/bin
    cp build/benchmark/tera_bench $test_dir/bin
    cp build/benchmark/tera_mark $test_dir/bin

    mkdir -p $test_dir/log
    mkdir -p $test_dir/data
    cp -r test/testcase $test_dir/bin
    cp -r test/testcase/shell_script/* $test_dir/bin
    cp src/sdk/python/TeraSdk.py $test_dir/bin/testcase
    cp build/lib/libtera_c.so $test_dir/bin
fi

if $fetch_without_run; then
    exit 0
fi

cd $test_dir/bin/
sh kill_tera.sh
sh launch_tera.sh
sleep 2

export PYTHONPATH=$PYTHONPATH:../../../thirdparty/include/; export PATH=$PATH:../../../thirdparty/bin/

nosetests -s -v -x $case_name > ../log/test.log

sh kill_tera.sh
cd ../../..
rm -rf $test_dir
