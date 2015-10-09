"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
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
    
    common.print_debug_msg(0, "start master, ts1, ts2, ts3, and status is ok")
    common.print_debug_msg(1, "create test_table001 and test_table002(kv)")
    cmd = "cd ../; ./teracli createbyfile testcase/data/create_table_schema; \
           cd testcase/"
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli create 'table_test002 <storage=flash, splitsize=2048, mergesize=128>'; \
           cd testcase/"
    common.exe_and_check_res(cmd)


def test_put_table():
    """
    put method
    """

    common.print_debug_msg(2, "put one data to table_test001 and table_test002")
    
    cmd = "cd ../; ./teracli put table_test001 test001key update_flag:test001q test001v; \
           cd testcase/"
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli put table_test002 test002key test002v; cd testcase/"
    common.exe_and_check_res(cmd)


def test_get_table():
    """
    get method
    """

    common.print_debug_msg(3, "read data form table_test001 and table_test002")

    cmd = "cd ../; ./teracli get table_test001 test001key update_flag:test001q; cd testcase/"
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli get table_test002 test002key; cd testcase/"
    common.exe_and_check_res(cmd)


def tearDown():
    """
    clear env
    """

    common.clear_env()
