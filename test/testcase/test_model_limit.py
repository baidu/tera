"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import common
import nose.tools
from TeraSdk import Client, TeraSdkException


table = None


def setUp():
    """
    set env
    """
    cmd = "./teracli create 'model_table{lg0{cf0}}'"
    common.exe_and_check_res(cmd)
    global table
    try:
        client = Client("", "pysdk")
        table = client.OpenTable("model_table")
    except TeraSdkException as e:
        print(e.reason)
        nose.tools.assert_true(False)


def test_rowkey_size_0():
    """
    64KB rowkey
    """
    try:
        table.Put("a" * (64 * 1024), "cf0", "qu0", "value_0")
    except TeraSdkException as e:
        nose.tools.assert_true("Bad parameters" in e.reason)
        return
    nose.tools.assert_true(False)


def test_rowkey_size_1():
    """
    64KB - 1 rowkey
    """
    table.Put("a" * (64 * 1024 - 1), "cf0", "qu0", "value_0")
    nose.tools.assert_equal(table.Get("a" * (64 * 1024 - 1), "cf0", "qu0", 0),
                            "value_0")


def test_qualifier_size_0():
    """
    64KB qualifier
    """
    try:
        table.Put("row_qu_size", "cf0", "a" * (64 * 1024), "value_0")
    except TeraSdkException as e:
        nose.tools.assert_true("Bad parameters" in e.reason)
        return
    nose.tools.assert_true(False)


def test_qualifier_size_1():
    """
    64KB - 1 qualifier
    """
    table.Put("row_qu_size", "cf0", "b" * (64 * 1024 - 1), "value_0")
    nose.tools.assert_equal(table.Get("row_qu_size", "cf0", "b" * (64 * 1024 - 1), 0),
                            "value_0")


def test_value_size_0():
    """
    32MB value
    """
    try:
        table.Put("row_qu_size", "cf0", "qu0", "v" * (32 * 1024 * 1024))
    except TeraSdkException as e:
        nose.tools.assert_true("Bad parameters" in e.reason)
        return
    nose.tools.assert_true(False)


def test_value_size_1():
    """
    32MB - 1 value
    """
    table.Put("row_qu_size", "cf0", "qu0", "v" * (32 * 1024 * 1024 - 1))
    nose.tools.assert_equal(table.Get("row_qu_size", "cf0", "qu0", 0),
                            "v" * (32 * 1024 * 1024 - 1))


def tearDown():
    """
    clear env
    """
    cmd = "./teracli disable model_table"
    common.exe_and_check_res(cmd)
    cmd = "./teracli drop model_table"
    common.exe_and_check_res(cmd)
