#!/bin/bash

function usage() {
    echo "usage: 
    $0 [opts] 
    -d path:         test temp file path
    -c casename:     run one case

    e.g. $0                             # all test cases
         $0 -c testcase/test_put_get.py # specify a case"
}

test_dir="tera_ft_test_data"
case_name=""

while getopts c:d:h arg
do
    case $arg in
        c)
            case_name=$OPTARG
            echo "case_name: $case_name";; 
        d)
            test_dir=$OPTARG
            echo "test_dir: $test_dir";; 
        h)
            usage
            exit 0;;
        ?) 
            echo "unkonw argument: $arg"
            exit 1;; 
    esac
done

set -x -e

rm -rf $test_dir
mkdir $test_dir
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

cd $test_dir/bin/
sh kill_tera.sh
sh launch_tera.sh
sleep 2

export PYTHONPATH=$PYTHONPATH:../../thirdparty/include/; export PATH=$PATH:../../thirdparty/bin/

nosetests -s -v -x $case_name > ../log/test.log

sh kill_tera.sh
