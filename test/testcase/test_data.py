"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import nose

import common


@nose.tools.with_setup(common.create_kv_table, common.cleanup)
def test_kv_random_write():
    """
    kv table write
    1. write data set 1
    2. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    scan_file = 'scan.out'
    common.run_tera_mark([(dump_file, False)], op='w', table_name='test', random='random',
                         value_size=100, num=5000, key_size=20)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=True)
    nose.tools.assert_true(common.compare_files(dump_file, scan_file, need_sort=True))


@nose.tools.with_setup(common.create_singleversion_table, common.cleanup)
def test_table_random_write():
    """
    table write simple
    1. write data set 1
    2. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    scan_file = 'scan.out'
    common.run_tera_mark([(dump_file, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                         key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False)
    nose.tools.assert_true(common.compare_files(dump_file, scan_file, need_sort=True))


@nose.tools.with_setup(common.create_multiversion_table, common.cleanup)
def test_table_random_write_versions():
    """
    table write w/versions
    1. write data set 1
    2. write data set 2
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file1 = 'dump1.out'
    dump_file2 = 'dump2.out'
    scan_file = 'scan.out'
    common.run_tera_mark([(dump_file1, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q',
                         random='random', key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    common.run_tera_mark([(dump_file1, True), (dump_file2, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                         key_seed=1, value_seed=11, value_size=100, num=10000, key_size=20)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=True)
    nose.tools.assert_true(common.compare_files(dump_file1, scan_file, need_sort=True))
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False)
    nose.tools.assert_true(common.compare_files(dump_file2, scan_file, need_sort=True))


@nose.tools.with_setup(common.create_singleversion_table, common.cleanup)
def test_table_write_delete():
    """
    table write and deletion
    1. write data set 1
    2. delete data set 1
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    scan_file = 'scan.out'
    common.run_tera_mark([], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                         key_seed=1, value_seed=1, value_size=100, num=10000, key_size=20)
    common.run_tera_mark([], op='d', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                         key_seed=1, value_seed=1, value_size=100, num=10000, key_size=20)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=True)
    nose.tools.assert_true(common.file_is_empty(scan_file))


@nose.tools.with_setup(common.create_multiversion_table, common.cleanup)
def test_table_write_delete_version():
    """
    table write and deletion w/versions
    1. write data set 1, 2, 3, 4
    2. scan
    3. delete data set 3
    4. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file1 = 'dump1.out'
    dump_file2 = 'dump2.out'
    scan_file1 = 'scan1.out'
    scan_file2 = 'scan2.out'
    common.run_tera_mark([(dump_file1, False), (dump_file2, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q',
                         random='random', key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    common.run_tera_mark([(dump_file1, True), (dump_file2, True)], op='w', table_name=table_name, cf='cf0:q,cf1:q',
                         random='random', key_seed=1, value_seed=11, value_size=100, num=10000, key_size=20)
    common.run_tera_mark([(dump_file1, True)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                         key_seed=1, value_seed=12, value_size=100, num=10000, key_size=20)
    common.run_tera_mark([(dump_file1, True), (dump_file2, True)], op='w', table_name=table_name, cf='cf0:q,cf1:q',
                         random='random', key_seed=1, value_seed=13, value_size=100, num=10000, key_size=20)
    common.compact_tablets(common.get_tablet_list(table_name))
    common.scan_table(table_name=table_name, file_path=scan_file1, allversion=True, snapshot=0)
    common.run_tera_mark([], op='d', table_name=table_name, cf='cf0:q,cf1:q', random='random', key_seed=1,
                         value_seed=12, value_size=100, num=10000, key_size=20)
    common.compact_tablets(common.get_tablet_list(table_name))
    common.scan_table(table_name=table_name, file_path=scan_file2, allversion=True, snapshot=0)
    nose.tools.assert_true(common.compare_files(dump_file1, scan_file1, need_sort=True))
    nose.tools.assert_true(common.compare_files(dump_file2, scan_file2, need_sort=True))
