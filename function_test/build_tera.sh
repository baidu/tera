#!/bin/sh
#/***************************************************************************
# * 
# * Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
# * 
# **************************************************************************/




#/**
# * @file build_tera.sh
# * @author zhangmiao07@baidu.com
# * @date 2015/08/24/ 2:32:37
# * @version $Revision: 1.0 $ 
# * @brief run ft
# *  
# **/

CURRENT_DIR=`dirname $0`
WORKSPACE=`cd ${CURRENT_DIR}; pwd`

function func_test()
{
    echo "start running fuction_test cases..."

    cd ${WORKSPACE}

    rm -rf ${WORKSAPCE}/data/*
    rm -rf ${WORKSAPCE}/cache/tabletnode.1..
    rm -rf ${WORKSAPCE}/cache/tabletnode.2..
    rm -rf ${WORKSAPCE}/cache/tabletnode.3..

    cp ../build/bin/teracli bin/
    cp ../build/bin/tera_main bin/

    cd ${WORKSPACE}/bin
    sh -x kill_tera.sh
    sh -x launch_tera.sh
    sleep 8

    cd ${WORKSPACE}/bin/testcase
    nosetests -s -v  

    if [ $? -ne 0 ] 
    then
        echo "function test fail ... please check."
    else
        echo "function test success!"
    fi
}

print_help()
{
    echo "samples:"
    echo "--------------------------------------------------------------"
    echo "build_tera.sh ft          :         quick test"
    echo "--------------------------------------------------------------"
}

Main()
{
    if [ "$1" == "-h" ]; then
        print_help
    elif [ "$1" == "ft" ]; then
        func_test
    fi

    ret=$?
    return $ret
}

Main "$@"
