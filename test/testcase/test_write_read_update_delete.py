'''
Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
'''

import common
import nose.tools
from TeraSdk import Client, TeraSdkException


table = None

def setUp():
    #clear env
    common.drop_table("crud_table")

    #set env
    cmd = "./teracli create 'crud_table{lg0{cf0}}'"
    common.exe_and_check_res(cmd)
    global table
    try:
        client = Client("", "pysdk")
        table = client.OpenTable("crud_table")
    except TeraSdkException as e:
        print(e.reason)
        nose.tools.assert_true(False)

def tearDown():
    pass

'''
0. put
1. read
2. delete row
3. read
4. put
5. read
'''
def test_case_0():
    table.Put("row", "cf0", "qu0", "value")
    nose.tools.assert_equal(table.Get("row", "cf0", "qu0", 0), "value")

    mu = table.NewRowMutation("row")
    mu.DeleteRow()
    table.ApplyMutation(mu)

    try:
        table.Get("row", "cf0", "qu0", 0)
    except TeraSdkException as e:
        nose.tools.assert_true("not found" in e.reason)

    table.Put("row", "cf0", "qu0", "value")
    nose.tools.assert_equal(table.Get("row", "cf0", "qu0", 0), "value")
