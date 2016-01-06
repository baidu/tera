"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import nose
import common

@nose.tools.with_setup(common.createbyfile('testcase/data/table_schema'), common.cleanup)
def test_table_write_get_scan():
    """
    table write
    1. write data
    2. get & compare
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    scan_file = 'scan.out'
    cf_list = 'cf1:q11,cf1:q12,cf1:q13,cf1:q14,cf1:q15,cf1:q16,cf1:q17,cf1:q18,cf1:q19,cf1:q10,cf2:q21,cf2:q22,cf2:q23,cf2:q24,cf2:q25,cf2:q26,cf2:q27,cf2:q28,cf2:q29,cf2:q20,cf3:q31,cf3:q32,cf3:q33,cf3:q34,cf3:q35,cf3:q36,cf3:q37,cf3:q38,cf3:q39,cf3:q30,cf4:q41,cf4:q42,cf4:q43,cf4:q44,cf4:q45,cf4:q46,cf4:q47,cf4:q48,cf4:q49,cf4:q40,cf5:q51,cf5:q52,cf5:q53,cf5:q54,cf5:q55,cf5:q56,cf5:q57,cf5:q58,cf5:q59,cf5:q50,cf6:q61,cf6:q62,cf6:q63,cf6:q64,cf6:q65,cf6:q66,cf6:q67,cf6:q68,cf6:q69,cf6:q60,cf7:q71,cf7:q72,cf7:q73,cf7:q74,cf7:q75,cf7:q76,cf7:q77,cf7:q78,cf7:q79,cf7:q70,cf8:q81,cf8:q82,cf8:q83,cf8:q84,cf8:q85,cf8:q86,cf8:q87,cf8:q88,cf8:q89,cf8:q80,cf9:q91,cf9:q92,cf9:q93,cf9:q94,cf9:q95,cf9:q96,cf9:q97,cf9:q98,cf9:q99,cf9:q90,cf10:q101,cf10:q102,cf10:q103,cf10:q104,cf10:q105,cf10:q106,cf10:q107,cf10:q108,cf10:q109,cf10:q100'
    common.run_tera_mark([(dump_file, False)], op='w', table_name='test', random='seq',
                         value_size=1000, num=100000, key_size=24, cf=cf_list, value_seed=20, entry_limit=10000)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False)
    nose.tools.assert_true(common.compare_files(dump_file, scan_file, need_sort=True))


@nose.tools.with_setup(common.createbyfile('testcase/data/table_schema'), common.cleanup)
def test_table_write_all_update_get_scan():
    """
    table write
    1. write data & update
    2. get & compare
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    update_file = 'update.out'
    merge_f = 'merge.out'
    scan_file = 'scan.out'
    cf_list = 'cf1:q11,cf1:q12,cf1:q13,cf1:q14,cf1:q15,cf1:q16,cf1:q17,cf1:q18,cf1:q19,cf1:q10,cf2:q21,cf2:q22,cf2:q23,cf2:q24,cf2:q25,cf2:q26,cf2:q27,cf2:q28,cf2:q29,cf2:q20,cf3:q31,cf3:q32,cf3:q33,cf3:q34,cf3:q35,cf3:q36,cf3:q37,cf3:q38,cf3:q39,cf3:q30,cf4:q41,cf4:q42,cf4:q43,cf4:q44,cf4:q45,cf4:q46,cf4:q47,cf4:q48,cf4:q49,cf4:q40,cf5:q51,cf5:q52,cf5:q53,cf5:q54,cf5:q55,cf5:q56,cf5:q57,cf5:q58,cf5:q59,cf5:q50,cf6:q61,cf6:q62,cf6:q63,cf6:q64,cf6:q65,cf6:q66,cf6:q67,cf6:q68,cf6:q69,cf6:q60,cf7:q71,cf7:q72,cf7:q73,cf7:q74,cf7:q75,cf7:q76,cf7:q77,cf7:q78,cf7:q79,cf7:q70,cf8:q81,cf8:q82,cf8:q83,cf8:q84,cf8:q85,cf8:q86,cf8:q87,cf8:q88,cf8:q89,cf8:q80,cf9:q91,cf9:q92,cf9:q93,cf9:q94,cf9:q95,cf9:q96,cf9:q97,cf9:q98,cf9:q99,cf9:q90,cf10:q101,cf10:q102,cf10:q103,cf10:q104,cf10:q105,cf10:q106,cf10:q107,cf10:q108,cf10:q109,cf10:q100'
    common.run_tera_mark([(dump_file, False)], op='w', table_name='test', random='seq',
                         value_size=1000, num=10000, key_size=24, cf=cf_list, entry_limit=5000, value_seed=10)
    common.run_tera_mark([(update_file, False)], op='w', table_name='test', random='seq',
                         value_size=1000, num=1000, key_size=24, cf=cf_list, value_seed=20, 
                         entry_limit=5000, key_step=10)
    common.merge_file(dump_file, update_file, merge_f)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False)
    nose.tools.assert_true(common.compare_files(merge_f, scan_file, need_sort=True))


@nose.tools.with_setup(common.createbyfile('testcase/data/table_schema'), common.cleanup)
def test_table_write_part_update_get_scan():
    """
    table write
    1. write data & update part
    2. get & compare
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    part_update_file = 'part_update.out'
    merge_f = 'merge.out'
    scan_file = 'scan.out'
    cf_list = 'cf1:q11,cf1:q12,cf1:q13,cf1:q14,cf1:q15,cf1:q16,cf1:q17,cf1:q18,cf1:q19,cf1:q10,cf2:q21,cf2:q22,cf2:q23,cf2:q24,cf2:q25,cf2:q26,cf2:q27,cf2:q28,cf2:q29,cf2:q20,cf3:q31,cf3:q32,cf3:q33,cf3:q34,cf3:q35,cf3:q36,cf3:q37,cf3:q38,cf3:q39,cf3:q30,cf4:q41,cf4:q42,cf4:q43,cf4:q44,cf4:q45,cf4:q46,cf4:q47,cf4:q48,cf4:q49,cf4:q40,cf5:q51,cf5:q52,cf5:q53,cf5:q54,cf5:q55,cf5:q56,cf5:q57,cf5:q58,cf5:q59,cf5:q50,cf6:q61,cf6:q62,cf6:q63,cf6:q64,cf6:q65,cf6:q66,cf6:q67,cf6:q68,cf6:q69,cf6:q60,cf7:q71,cf7:q72,cf7:q73,cf7:q74,cf7:q75,cf7:q76,cf7:q77,cf7:q78,cf7:q79,cf7:q70,cf8:q81,cf8:q82,cf8:q83,cf8:q84,cf8:q85,cf8:q86,cf8:q87,cf8:q88,cf8:q89,cf8:q80,cf9:q91,cf9:q92,cf9:q93,cf9:q94,cf9:q95,cf9:q96,cf9:q97,cf9:q98,cf9:q99,cf9:q90,cf10:q101,cf10:q102,cf10:q103,cf10:q104,cf10:q105,cf10:q106,cf10:q107,cf10:q108,cf10:q109,cf10:q100'
    cf_list_part = 'cf1:q11,cf1:q12,cf1:q13,cf1:q14,cf1:q15,cf1:q16,cf1:q17,cf1:q18,cf1:q19,cf1:q10,cf2:q21,cf2:q22,cf2:q23,cf2:q24,cf2:q25,cf2:q26,cf2:q27,cf2:q28,cf2:q29,cf2:q20,cf3:q31,cf3:q32,cf3:q33,cf3:q34,cf3:q35,cf3:q36,cf3:q37,cf3:q38,cf3:q39,cf3:q30,cf4:q41,cf4:q42,cf4:q43,cf4:q44,cf4:q45,cf4:q46,cf4:q47,cf4:q48,cf4:q49,cf4:q40,cf5:q51,cf5:q52,cf5:q53,cf5:q54,cf5:q55,cf5:q56,cf5:q57,cf5:q58,cf5:q59,cf5:q50'
    common.run_tera_mark([(dump_file, False)], op='w', table_name='test', random='seq',
                         value_size=1000, num=10000, key_size=24, cf=cf_list, value_seed=10, entry_limit=5000)
    common.run_tera_mark([(part_update_file, False)], op='w', table_name='test', random='seq',
                         value_size=1000, num=2000, key_size=24, cf=cf_list_part, value_seed=20,
                         entry_limit=5000)
    common.merge_file(dump_file, part_update_file, merge_f)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False)
    nose.tools.assert_true(common.compare_files(merge_f, scan_file, need_sort=True))


@nose.tools.with_setup(common.createbyfile('testcase/data/table_schema'), common.cleanup)
def test_table_write_all_delete_get_scan():
    """
    table write
    1. write data & delete
    2. get & compare
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    delete_file = 'delete.out'
    rest_f = 'rest.out'
    scan_file = 'scan.out'
    cf_list = 'cf1:q11,cf1:q12,cf1:q13,cf1:q14,cf1:q15,cf1:q16,cf1:q17,cf1:q18,cf1:q19,cf1:q10,cf2:q21,cf2:q22,cf2:q23,cf2:q24,cf2:q25,cf2:q26,cf2:q27,cf2:q28,cf2:q29,cf2:q20,cf3:q31,cf3:q32,cf3:q33,cf3:q34,cf3:q35,cf3:q36,cf3:q37,cf3:q38,cf3:q39,cf3:q30,cf4:q41,cf4:q42,cf4:q43,cf4:q44,cf4:q45,cf4:q46,cf4:q47,cf4:q48,cf4:q49,cf4:q40,cf5:q51,cf5:q52,cf5:q53,cf5:q54,cf5:q55,cf5:q56,cf5:q57,cf5:q58,cf5:q59,cf5:q50,cf6:q61,cf6:q62,cf6:q63,cf6:q64,cf6:q65,cf6:q66,cf6:q67,cf6:q68,cf6:q69,cf6:q60,cf7:q71,cf7:q72,cf7:q73,cf7:q74,cf7:q75,cf7:q76,cf7:q77,cf7:q78,cf7:q79,cf7:q70,cf8:q81,cf8:q82,cf8:q83,cf8:q84,cf8:q85,cf8:q86,cf8:q87,cf8:q88,cf8:q89,cf8:q80,cf9:q91,cf9:q92,cf9:q93,cf9:q94,cf9:q95,cf9:q96,cf9:q97,cf9:q98,cf9:q99,cf9:q90,cf10:q101,cf10:q102,cf10:q103,cf10:q104,cf10:q105,cf10:q106,cf10:q107,cf10:q108,cf10:q109,cf10:q100'
    common.run_tera_mark([(dump_file, False)], op='w', table_name='test', random='seq',
                         value_size=1000, num=10000, key_size=24, cf=cf_list, entry_limit=5000, value_seed=10)
    common.run_tera_mark([(delete_file, False)], op='d', table_name='test', random='seq',
                         value_size=1000, num=1000, key_size=24, cf=cf_list, entry_limit=5000, value_seed=10)
    common.rest_file(dump_file, delete_file, rest_f)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False)
    nose.tools.assert_true(common.compare_files(rest_f, scan_file, need_sort=True))


@nose.tools.with_setup(common.createbyfile('testcase/data/table_schema'), common.cleanup)
def test_table_write_part_delete_get_scan():
    """
    table write
    1. write data & delete part
    2. get & compare
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    part_delete_file = 'part_delete.out'
    rest_f = 'rest.out'
    scan_file = 'scan.out'
    cf_list = 'cf1:q11,cf1:q12,cf1:q13,cf1:q14,cf1:q15,cf1:q16,cf1:q17,cf1:q18,cf1:q19,cf1:q10,cf2:q21,cf2:q22,cf2:q23,cf2:q24,cf2:q25,cf2:q26,cf2:q27,cf2:q28,cf2:q29,cf2:q20,cf3:q31,cf3:q32,cf3:q33,cf3:q34,cf3:q35,cf3:q36,cf3:q37,cf3:q38,cf3:q39,cf3:q30,cf4:q41,cf4:q42,cf4:q43,cf4:q44,cf4:q45,cf4:q46,cf4:q47,cf4:q48,cf4:q49,cf4:q40,cf5:q51,cf5:q52,cf5:q53,cf5:q54,cf5:q55,cf5:q56,cf5:q57,cf5:q58,cf5:q59,cf5:q50,cf6:q61,cf6:q62,cf6:q63,cf6:q64,cf6:q65,cf6:q66,cf6:q67,cf6:q68,cf6:q69,cf6:q60,cf7:q71,cf7:q72,cf7:q73,cf7:q74,cf7:q75,cf7:q76,cf7:q77,cf7:q78,cf7:q79,cf7:q70,cf8:q81,cf8:q82,cf8:q83,cf8:q84,cf8:q85,cf8:q86,cf8:q87,cf8:q88,cf8:q89,cf8:q80,cf9:q91,cf9:q92,cf9:q93,cf9:q94,cf9:q95,cf9:q96,cf9:q97,cf9:q98,cf9:q99,cf9:q90,cf10:q101,cf10:q102,cf10:q103,cf10:q104,cf10:q105,cf10:q106,cf10:q107,cf10:q108,cf10:q109,cf10:q100'
    cf_list_part = 'cf1:q11,cf1:q12,cf1:q13,cf1:q14,cf1:q15,cf1:q16,cf1:q17,cf1:q18,cf1:q19,cf1:q10,cf2:q21,cf2:q22,cf2:q23,cf2:q24,cf2:q25,cf2:q26,cf2:q27,cf2:q28,cf2:q29,cf2:q20,cf3:q31,cf3:q32,cf3:q33,cf3:q34,cf3:q35,cf3:q36,cf3:q37,cf3:q38,cf3:q39,cf3:q30,cf4:q41,cf4:q42,cf4:q43,cf4:q44,cf4:q45,cf4:q46,cf4:q47,cf4:q48,cf4:q49,cf4:q40,cf5:q51,cf5:q52,cf5:q53,cf5:q54,cf5:q55,cf5:q56,cf5:q57,cf5:q58,cf5:q59,cf5:q50'
    common.run_tera_mark([(dump_file, False)], op='w', table_name='test', random='seq',
                         value_size=1000, num=10000, key_size=24, cf=cf_list, entry_limit=5000, value_seed=10)
    common.run_tera_mark([(part_delete_file, False)], op='d', table_name='test', random='seq',
                         value_size=1000, num=2000, key_size=24, cf=cf_list_part, 
                         entry_limit=5000, value_seed=10)
    common.rest_file(dump_file, part_delete_file, rest_f)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False)
    nose.tools.assert_true(common.compare_files(rest_f, scan_file, need_sort=True))


@nose.tools.with_setup(common.createbyfile('testcase/data/table_schema_single_lgcf'), common.cleanup)
def test_table_write_get_scan_single():
    """
    table write
    1. write data
    2. get & compare
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    scan_file = 'scan.out'
    common.run_tera_mark([(dump_file, False)], op='w', table_name='test', random='seq',
                         value_size=1000, num=1000000, key_size=24, cf='cf1:q1', value_seed=2, entry_limit=1000)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False)
    nose.tools.assert_true(common.compare_files(dump_file, scan_file, need_sort=True))


@nose.tools.with_setup(common.createbyfile('testcase/data/table_schema_single_lgcf_multiver'), common.cleanup)
def test_table_write_get_scan_multiversion():
    """
    table write
    1. write data
    2. get & compare
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    scan_file = 'scan.out'
    common.run_tera_mark([(dump_file, False)], op='w', table_name='test', random='seq',
                         value_size=1000, num=5000, key_size=24, cf='cf1:q1', value_seed=2)
    common.scan_table(table_name=table_name, file_path=scan_file, allversion=False)
    nose.tools.assert_true(common.compare_files(dump_file, scan_file, need_sort=True))

