"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import nose
import time

import common
from conf import const


@nose.tools.with_setup(common.create_kv_table, common.cleanup)
def test_rollback_kv():
    """
    test kv rollback
    1. write data set 1
    2. create snapshot
    3. write data set 2
    4. scan & compare with set 2
    5. rollback to snapshot
    6. scan & compare snapshot 1
    7. compact then compare
    :return:
    """
    table_name = 'test'
    dump_file1 = 'dump1.out'
    dump_file2 = 'dump2.out'
    scan_file = 'scan.out'
    common.run_tera_mark([(dump_file1, False)], op='w', table_name=table_name, random='random',
                         key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    snapshot = common.snapshot_op(table_name)
    common.run_tera_mark([(dump_file2, False)], op='w', table_name=table_name, random='random',
                         key_seed=1, value_seed=11, value_size=100, num=10000, key_size=20)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False, snapshot=0)
    nose.tools.assert_true(common.compare_files(dump_file2, scan_file, need_sort=True))

    common.rollback_op(table_name, snapshot, 'roll')
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False, snapshot=0)
    nose.tools.assert_true(common.compare_files(dump_file1, scan_file, need_sort=True))

    common.compact_tablets(common.get_tablet_list(table_name))
    nose.tools.assert_true(common.compare_files(dump_file1, scan_file, need_sort=False))


@nose.tools.with_setup(common.create_singleversion_table, common.cleanup)
def test_rollback_table():
    """
    test table rollback
    1. write data set 1
    2. create snapshot
    3. write data set 2
    4. scan & compare with set 2
    5. rollback to snapshot
    6. scan & compare snapshot 1
    7. compact then compare
    :return:
    """
    table_name = 'test'
    dump_file1 = 'dump1.out'
    dump_file2 = 'dump2.out'
    scan_file = 'scan.out'
    common.run_tera_mark([(dump_file1, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                         key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    snapshot = common.snapshot_op(table_name)
    common.run_tera_mark([(dump_file2, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                         key_seed=1, value_seed=11, value_size=100, num=10000, key_size=20)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False, snapshot=0)
    nose.tools.assert_true(common.compare_files(dump_file2, scan_file, need_sort=True))

    common.rollback_op(table_name, snapshot, 'roll')
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False, snapshot=0)
    nose.tools.assert_true(common.compare_files(dump_file1, scan_file, need_sort=True))

    common.compact_tablets(common.get_tablet_list(table_name))
    nose.tools.assert_true(common.compare_files(dump_file1, scan_file, need_sort=True))


@nose.tools.with_setup(common.create_kv_table)
def test_rollback_kv_relaunch():
    """
    test kv rollback w/relaunch
    1. test_rollback_kv()
    2. relaunch
    3. compare
    :return:
    """
    table_name = 'test'
    dump_file1 = 'dump1.out'
    scan_file = 'scan.out'
    test_rollback_kv()

    common.cluster_op('kill')
    common.cluster_op('launch')
    time.sleep(2)

    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False, snapshot=0)
    nose.tools.assert_true(common.compare_files(dump_file1, scan_file, need_sort=True))

    common.compact_tablets(common.get_tablet_list(table_name))
    nose.tools.assert_true(common.compare_files(dump_file1, scan_file, need_sort=False))


@nose.tools.with_setup(common.create_singleversion_table, common.cleanup)
def test_rollback_table_relaunch():
    """
    test table rollback w/relaunch
    1. test_rollback_table()
    2. relaunch
    3. compare
    :return:
    """
    table_name = 'test'
    dump_file1 = 'dump1.out'
    scan_file = 'scan.out'
    test_rollback_table()

    common.cluster_op('kill')
    common.cluster_op('launch')
    time.sleep(2)

    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False, snapshot=0)
    nose.tools.assert_true(common.compare_files(dump_file1, scan_file, need_sort=True))

    common.compact_tablets(common.get_tablet_list(table_name))
    nose.tools.assert_true(common.compare_files(dump_file1, scan_file, need_sort=False))


@nose.tools.with_setup(None, common.cleanup)
def test_rollback_kv_multitablets():
    """
    test kv rollback w/multi tablets
    1. test_rollback_kv_relaunch()
    :return:
    """

    common.createbyfile(schema=const.data_path + 'kv.schema', deli=const.data_path + 'deli.10')
    test_rollback_kv_relaunch()


@nose.tools.with_setup(None, common.cleanup)
def test_rollback_table_multitablets():
    """
    test table rollback w/multi tablets
    1. test_rollback_table_relaunch()
    :return:
    """

    common.createbyfile(schema=const.data_path + 'table.schema', deli=const.data_path + 'deli.10')
    test_rollback_table_relaunch()
