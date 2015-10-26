"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import common

from conf import const

def setUp():
    """
    set env
    """

    common.print_debug_msg(1, "setup()")

def test_create_user():
    cmd = "./teracli user create z1 z1pwd --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 0)

    cmd = "./teracli user show z1"
    common.check_show_user_result(cmd, True, "z1")

    # user already exists
    cmd = "./teracli user create z1 z1pwd --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 255)

def test_change_pwd():
    cmd = "./teracli user changepwd root rootpassword --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 0)
    #now, using old password can not get root permission
    cmd = "./teracli user create dummy dummypwd --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 255)

    #update flag file
    cmd = ("sed -i 's/^--tera_user_passcode=.*/--tera_user_passcode=rootpassword/' "
           + const.user_root_flag_path)
    common.execute_and_check_returncode(cmd, 0)
    #now, using new password should work
    cmd = "./teracli user changepwd root helloroot --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 0)
    #restore the original root password in flag file
    cmd = ("sed -i 's/^--tera_user_passcode=.*/--tera_user_passcode=helloroot/' "
           + const.user_root_flag_path)
    common.execute_and_check_returncode(cmd, 0)

    # user not found
    cmd = "./teracli user changepwd oops z1pw2 --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 255)

def test_addtogroup():
    cmd = "./teracli user addtogroup z1 z1g --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 0)

    cmd = "./teracli user show z1"
    common.check_show_user_result(cmd, True, "z1g")
    common.execute_and_check_returncode(cmd, 0)

    # user not found
    cmd = "./teracli user addtogroup z2 z1g --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 255)

    # user already in group
    cmd = "./teracli user addtogroup z1 z1g --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 255)

def test_deletefromgroup():
    cmd = "./teracli user deletefromgroup z1 z1g --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 0)
    cmd = "./teracli user show z1"
    common.check_show_user_result(cmd, False, "z1g")

    # user not found
    cmd = "./teracli user deletefromgroup z2 z1g --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 255)

    # user not in group
    cmd = "./teracli user deletefromgroup z1 z1g --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 255)

def test_delete_user():
    cmd = "./teracli user delete z1 --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 0)
    cmd = "./teracli user show z1"
    common.check_show_user_result(cmd, False, "z1")

    # can not delete root
    cmd = "./teracli user delete root --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 255)
    cmd = "./teracli user show root"
    common.check_show_user_result(cmd, True, "root")

    # user not found
    cmd = "./teracli user delete z1 --flagfile=" + const.user_root_flag_path
    common.execute_and_check_returncode(cmd, 255)

def tearDown():
    """
    tear down
    """
    common.print_debug_msg(1, "teardown()")
