"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

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

    cmd = "./teracli createbyfile testcase/data/create_table_schema"
    common.exe_and_check_res(cmd)

    cmd = './teracli create "table_test002 <storage=flash, splitsize=2048, mergesize=128>"'
    common.exe_and_check_res(cmd)


def test_show_table():
    """
    show method
    """

    common.print_debug_msg(3, "show and show(x) table")    

    cmd = "./teracli show"
    common.exe_and_check_res(cmd)

    cmd = "./teracli showx"
    common.exe_and_check_res(cmd)

    cmd = "./teracli show table_test001"
    common.exe_and_check_res(cmd)

    cmd = "./teracli show table_test002"
    common.exe_and_check_res(cmd)

    cmd = "./teracli showx table_test001"
    common.exe_and_check_res(cmd)

    cmd = "./teracli showx table_test002"
    common.exe_and_check_res(cmd)
    
    cmd = "./teracli showschema table_test001"
    common.exe_and_check_res(cmd)

    cmd = "./teracli showschema table_test002"
    common.exe_and_check_res(cmd)

    cmd = "./teracli showschemax table_test001"
    common.exe_and_check_res(cmd)

    cmd = "./teracli showschemax table_test002"
    common.exe_and_check_res(cmd)


def tearDown():
    """
    clear env
    """

    common.clear_env()
