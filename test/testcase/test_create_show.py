'''
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
'''

import common 
    
def setUp():
    common.clear_env()
    common.print_debug_msg(1, "start master, ts1, ts2, ts3 and status is ok")

def tearDown():
    pass
    
    '''
    create method
    '''
def test_create_table():

    common.print_debug_msg(2, "create table_test001 and create table_test002(kv), write and check")

    cmd = "./teracli createbyfile testcase/data/create_table_schema"
    common.runcmd(cmd)

    cmd = './teracli create "table_test002 <storage=flash, splitsize=2048, mergesize=128>"'
    common.runcmd(cmd)

    '''
    show method
    '''
def test_show_table():

    common.print_debug_msg(3, "show and show(x) table")    

    cmd = "./teracli show"
    common.runcmd(cmd)

    cmd = "./teracli showx"
    common.runcmd(cmd)

    cmd = "./teracli show table_test001"
    common.runcmd(cmd)

    cmd = "./teracli show table_test002"
    common.runcmd(cmd)

    cmd = "./teracli showx table_test001"
    common.runcmd(cmd)

    cmd = "./teracli showx table_test002"
    common.runcmd(cmd)
    
    cmd = "./teracli showschema table_test001"
    common.runcmd(cmd)

    cmd = "./teracli showschema table_test002"
    common.runcmd(cmd)

    cmd = "./teracli showschemax table_test001"
    common.runcmd(cmd)

    cmd = "./teracli showschemax table_test002"
    common.runcmd(cmd)

