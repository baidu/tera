"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import common
import nose.tools
from TeraSdk import Client, TeraSdkException


def setUp():
    cmd = "./teracli create 'shared_table{lg0{cf0,cf1}}'"
    common.exe_and_check_res(cmd)

def test_shared_table_impl():
    cmd = "./tera_test shared-tableimpl-test --table=shared_table"
    common.execute_and_check_returncode(cmd, 0)

def tearDown():
    cmd = "./teracli disable shared_table"
    common.exe_and_check_res(cmd)
    cmd = "./teracli drop shared_table"
    common.exe_and_check_res(cmd)
