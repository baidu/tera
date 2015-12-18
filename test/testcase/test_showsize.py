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

    time.sleep(10)
    show_ret = common.parse_showinfo()
    table_name = "test"
    nose.tools.assert_equal(show_ret[table_name]["size"], "4K")
    dump_file = 'dump.out'
    scan_file = 'scan.out'
    common.run_tera_mark([(dump_file, False)], op='w', table_name='test', random='random',
                         key_seed=1, value_seed=10, value_size=100, num=5000, key_size=20)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=True)
    nose.tools.assert_true(common.compare_files(dump_file, scan_file, need_sort=True))
    nose.tools.assert_not_equal(show_ret[table_name]["size"], "4K")


@nose.tools.with_setup(common.create_singleversion_table, common.cleanup)
def test_showsize_table():

    time.sleep(10)
    show_ret = common.parse_showinfo()
    table_name = "test"
    nose.tools.assert_equal(show_ret[table_name]["size"], "4K")
    dump_file = 'dump.out'
    scan_file = 'scan.out'
    common.run_tera_mark([(dump_file, False)], op='w', table_name='test', cf='cf0:q,cf1:q', random='random',
                         key_seed=1, value_seed=10, value_size=100, num=500000, key_size=20)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=True)
    nose.tools.assert_true(common.compare_files(dump_file, scan_file, need_sort=True))
    nose.tools.assert_not_equal(show_ret[table_name]["size"], "4K")
