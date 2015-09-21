################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
This module provide configure file management service in i18n environment.

Authors: zhangmiao07(zhangmiao07@baidu.com)
Date:    2015/09/18 17:23:06
"""

import nose.tools
import subprocess
import time
import os

def print_debug_msg(sid=0, msg=""):
    """
    provide general print interface
    """
    print "@%d======================%s"%(sid, msg)


def setUp():
    print_debug_msg(1, "start master, ts1, ts2, ts3, create test_table001 and test_table002(kv)")

    cmd = "rm -rf ../log_test/*"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)
    if ret.poll() is not None:
        time.sleep(1)

    cmd = "cd ../; ./teracli createbyfile testcase/data/create_table_schema; \
           cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)
    if ret.poll() is not None:
        time.sleep(1)

    cmd = "cd ../; ./teracli create 'table_test002 <storage=flash, splitsize=2048, mergesize=128>'; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd,  shell=True)
    if ret.poll() is not None:
        time.sleep(1)
    time.sleep(2)


def test_put_table():
    print_debug_msg(2, "put one data to table_test001 and table_test002")
    
    cmd = "cd ../; ./teracli put table_test001 test001key update_flag:test001q test001v; \
           cd testcase/"
    print cmd
    fout = open('../log_test/put_table_test001_out', 'w')
    ferr = open('../log_test/put_table_test001_err', 'w')
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()
    ferr.close()
    time.sleep(1)
    nose.tools.assert_true(os.path.getsize("../log_test/put_table_test001_err") == 0)

    cmd = "cd ../; ./teracli put table_test002 test002key test002v; cd testcase/"
    print cmd
    fout = open('../log_test/put_table_test002_out', 'w')
    ferr = open('../log_test/put_table_test002_err', 'w')
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)                                           
    if ret.poll() is not None:                                                                                  
        time.sleep(1)                                                                                           
    fout.close()                                                                                                
    ferr.close()                                                                                                
    time.sleep(1)                                                                                               
    nose.tools.assert_true(os.path.getsize("../log_test/put_table_test002_err") == 0) 


def test_get_table():
    print_debug_msg(3, "read data form table_test001 and table_test002")

    cmd = "cd ../; ./teracli get table_test001 test001key update_flag:test001q; cd testcase/"
    print cmd
    fout = open('../log_test/get_table_test001_out', 'w')
    ferr = open('../log_test/get_table_test001_err', 'w')
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()                                                                                                
    ferr.close()
    time.sleep(1)                                                                                               
    nose.tools.assert_true(os.path.getsize("../log_test/get_table_test001_err") == 0)

    cmd = "cd ../; ./teracli get table_test002 test002key; cd testcase/"
    print cmd
    fout = open('../log_test/get_table_test002_out', 'w')
    ferr = open('../log_test/get_table_test002_err', 'w')
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()                                                                                                
    ferr.close()
    time.sleep(1)                                                                                               
    nose.tools.assert_true(os.path.getsize("../log_test/get_table_test002_err") == 0)


def tearDown():
    print_debug_msg(4, "delete table_test001 and table_test002, clear env")

    cmd = "cd ../; ./teracli disable table_test001; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)
    time.sleep(2)

    cmd = "cd ../; ./teracli drop table_test001; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)
    time.sleep(2)

    cmd = "cd ../; ./teracli disable table_test002; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)
    time.sleep(2)

    cmd = "cd ../; ./teracli drop table_test002; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)    
    time.sleep(2)

