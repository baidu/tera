"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import common
import nose.tools
from TeraSdk import Client, TeraSdkException


def setUp():
    cmd = "./teracli create 'filter_table{lg0{cf0,cf1}}'"
    common.exe_and_check_res(cmd)


def test_filter():
    try:
        client = Client("", "pysdk")
        table = client.OpenTable("filter_table")
    except TeraSdkException as e:
        print(e.reason)
        return
    put_int64(table, "row0", "cf0", "", -1)
    put_int64(table, "row0", "cf1", "", 5)

    put_int64(table, "row1", "cf0", "", 0)
    put_int64(table, "row1", "cf1", "", 10)

    put_int64(table, "row2", "cf0", "", 10)
    put_int64(table, "row2", "cf1", "", 100)

    filter_str = "SELECT * WHERE int64 cf0 >= 0"
    expect = ["row1:cf0::0", "row1:cf1::10",
              "row2:cf0::10", "row2:cf1::100"]
    check_filter_and_expect(table, filter_str, expect)

    filter_str = "SELECT * WHERE int64 cf0 < 0"
    expect = ["row0:cf0::-1", "row0:cf1::5"]
    check_filter_and_expect(table, filter_str, expect)

    filter_str = "SELECT cf1 WHERE int64 cf0 == -1"
    expect = ["row0:cf1::5"]
    check_filter_and_expect(table, filter_str, expect)

    filter_str = "SELECT cf0,cf1 WHERE int64 cf0 > 99 AND int64 cf1 <= 100"
    expect = []
    check_filter_and_expect(table, filter_str, expect)


def tearDown():
    cmd = "./teracli disable filter_table"
    common.exe_and_check_res(cmd)
    cmd = "./teracli drop filter_table"
    common.exe_and_check_res(cmd)


def check_filter_and_expect(table, filter_str, expect):
    nose.tools.assert_equal(
        scan_with_filter(table, filter_str),
        expect
    )


def put_int64(table, rowkey, cf, qu, value):
    try:
        table.PutInt64(rowkey, cf, qu, value)
    except TeraSdkException as e:
        print(e.reason)


def scan_with_filter(table, filter_str):
    from TeraSdk import ScanDescriptor
    scan_desc = ScanDescriptor("")
    scan_desc.SetBufferSize(1024 * 1024)  # 1MB
    if not scan_desc.SetFilter(filter_str):
        print("invalid filter")
        return list()
    try:
        stream = table.Scan(scan_desc)
    except TeraSdkException as e:
        print(e.reason)
        return list()

    res = list()
    while not stream.Done():
        row = stream.RowName()
        col = stream.ColumnName()
        val = stream.ValueInt64()
        res.append(row + ":" + col + ":" + str(val))
        stream.Next()
    return res
