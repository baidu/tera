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
    cmd = "./teracli user create z1 z1pwd" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 0)

    # user already exists
    cmd = "./teracli user create z1 z1pwd" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 255)

def test_change_pwd():
    cmd = "./teracli  user changepwd z1 z1pw2" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 0)

    # user not found
    cmd = "./teracli  user changepwd oops z1pw2" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 255)

def test_addtogroup():
    cmd = "./teracli  user addtogroup z1 z1g" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 0)

    # user not found
    cmd = "./teracli  user addtogroup z2 z1g" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 255)

    # user already in group
    cmd = "./teracli  user addtogroup z1 z1g" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 255)

def test_deletefromgroup():
    cmd = "./teracli  user deletefromgroup z1 z1g" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 0)

    # user not found
    cmd = "./teracli  user deletefromgroup z2 z1g" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 255)

    # user not in group
    cmd = "./teracli  user deletefromgroup z1 z1g" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 255)

def test_delete_user():
    cmd = "./teracli  user delete z1" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 0)

    # user not found
    cmd = "./teracli  user delete z1" + const.user_root_flag_suffix
    common.execute_and_check_returncode(cmd, 255)

def tearDown():
    """
    tear down
    """
    common.print_debug_msg(1, "teardown()")
