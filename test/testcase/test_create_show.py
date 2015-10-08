################################################################################
#
# Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
This module provide function test for create and show method.

Authors: zhangmiao07(zhangmiao07@baidu.com)
Date:    2015/09/18 17:23:06
"""

import nose.tools
import subprocess
import common 
    
def setUp():
    """
    set env
    """

    common.print_debug_msg(1, "start master, ts1, ts2, ts3 and status is ok")
    
    
def test_create_table():
    """
    create method
    """

    common.print_debug_msg(2, "create table_test001 and create table_test002(kv), write and check")

    cmd = "cd ../; ./teracli createbyfile testcase/data/create_table_schema; cd testcase/"
    common.exe_and_check_res(cmd)

    cmd = 'cd ../; ./teracli create "table_test002 <storage=flash, splitsize=2048, mergesize=128>"; \
           cd testcase/'
    common.exe_and_check_res(cmd)


def test_show_table():
    """
    show method
    """

    common.print_debug_msg(3, "show and show(x) table")    

    cmd = "cd ../; ./teracli show; cd testcase/"
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli showx; cd testcase/"
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli show table_test001; cd testcase/"
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli show table_test002; cd testcase/"
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli showx table_test001; cd testcase/"                                                 
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli showx table_test002; cd testcase/"                                                 
    common.exe_and_check_res(cmd)
    
    cmd = "cd ../; ./teracli showschema table_test001; cd testcase/"                                            
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli showschema table_test002; cd testcase/"                                            
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli showschemax table_test001; cd testcase/"                                           
    common.exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli showschemax table_test002; cd testcase/"                                           
    common.exe_and_check_res(cmd)


def tearDown():
    """
    clear env
    """

    common.print_debug_msg(4, "delete table_test001 and table_test002, clear env")

    common.clear_env()
