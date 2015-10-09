"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import subprocess
import nose.tools

def print_debug_msg(sid=0, msg=""):
    """
    provide general print interface
    """
    print "@%d======================%s"%(sid, msg)


def exe_and_check_res(cmd):
    """
    execute cmd and check result
    """

    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])


def clear_env():
    """
    clear env
    """

    print_debug_msg(4, "delete table_test001 and table_test002, clear env")

    cmd = "cd ../; ./teracli disable table_test001; cd testcase/"    
    exe_and_check_res(cmd)   

    cmd = "cd ../; ./teracli drop table_test001; cd testcase/"                                                  
    exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli disable table_test002; cd testcase/"                                               
    exe_and_check_res(cmd)

    cmd = "cd ../; ./teracli drop table_test002; cd testcase/"                                                  
    exe_and_check_res(cmd)
