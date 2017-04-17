################################################################################
#
# Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
This module provide common function for function_test.

Authors: zhangmiao07(zhangmiao07@baidu.com)
Date:    2015/09/18 17:23:06
"""

import subprocess
import nose.tools

def print_debug_msg(sid=0, msg=""):
    """
    provide general print interface
    """
    print "@%d======================%s"%(sid, msg)


def construct_env():
    """
    provide start env
    """

    print_debug_msg(1, "start master, ts1, ts2, ts3, and status is ok")


def table_prepare():
    """
    create two tables for other process
    """

    print_debug_msg(1, "create test_table001 and test_table002(kv)")

    cmd = "cd ../; ./teracli createbyfile testcase/data/create_table_schema; \
           cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])

    cmd = "cd ../; ./teracli create 'table_test002 <storage=flash, splitsize=2048, mergesize=128>'; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])


def clear_env():
    """
    clear env
    """

    print_debug_msg(4, "delete table_test001 and table_test002, clear env")

    cmd = "cd ../; ./teracli disable table_test001; cd testcase/"    
    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])

    cmd = "cd ../; ./teracli drop table_test001; cd testcase/"                                                  
    print cmd 
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])

    cmd = "cd ../; ./teracli disable table_test002; cd testcase/"                                               
    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])

    cmd = "cd ../; ./teracli drop table_test002; cd testcase/"                                                  
    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])
