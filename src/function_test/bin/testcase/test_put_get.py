################################################################################
#
# Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
This module provide function test for put and get method.

Authors: zhangmiao07(zhangmiao07@baidu.com)
Date:    2015/09/18 17:23:06
"""

import nose.tools
import subprocess
import time
import os
import common

def setUp():
    """
    set env
    """
    
    common.construct_env()
    common.table_prepare()


def test_put_table():
    """
    put method
    """

    common.print_debug_msg(2, "put one data to table_test001 and table_test002")
    
    cmd = "cd ../; ./teracli put table_test001 test001key update_flag:test001q test001v; \
           cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])

    cmd = "cd ../; ./teracli put table_test002 test002key test002v; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])

def test_get_table():
    """
    get method
    """

    common.print_debug_msg(3, "read data form table_test001 and table_test002")

    cmd = "cd ../; ./teracli get table_test001 test001key update_flag:test001q; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])

    cmd = "cd ../; ./teracli get table_test002 test002key; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])

def tearDown():
    """
    clear env
    """

    common.clear_env()
