"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import nose
import time
import common

@nose.tools.with_setup(common.create_kv_table, common.cleanup)
def test_showsize_kv_table():
    table_name = "test"
    scan_file = 'scan.out'
    common.run_tera_mark([], op='w', table_name='test', random='random',
                         key_seed=1, value_seed=10, value_size=100, num=5000, key_size=20)
    time.sleep(3)
    show_ret = common.parse_showinfo()
    size = float(show_ret[table_name]["size"][:-1])
    if size >= 774 * 0.9 and size <= 774 * 1.1:
        nose.tools.assert_true(True)
    else:
        nose.tools.assert_true(False)

@nose.tools.with_setup(common.create_singleversion_table, common.cleanup)
def test_showsize_table():
    table_name = "test"
    scan_file = 'scan.out'
    common.run_tera_mark([], op='w', table_name='test', cf='cf0:q,cf1:q', random='random',
                         key_seed=1, value_seed=10, value_size=100, num=5000, key_size=20)
    show_ret = common.parse_showinfo()
    time.sleep(3)
    show_ret = common.parse_showinfo()
    size = float(show_ret[table_name]["size"][:-1])
    if size >= 1.65 * 0.95 and size <= 1.65 * 1.05:
        nose.tools.assert_true(True)
    else:
        nose.tools.assert_true(False)
