"""
Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import common
import nose
import re
import subprocess


def doSingleRowTxnBaseTest(value):
    '''
    there is no concurrency in this case,
    so if we insert a value with single-row-txn,
    the commit should success and should found the value when read
    '''
    input_str = 'txn start single_row_txn_test row\n' \
                + 'get single_row_txn_test row cf0:qu0\n' \
                + 'put single_row_txn_test row cf0:qu0 ' + value + '\n' \
                + 'txn commit\nquit\n'
    output_str = 'row:cf0:qu0:.*:' + value
    p = subprocess.Popen('./teracli',
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    p.stdin.write(input_str)
    print p.communicate('')

    p = subprocess.Popen('./teracli get single_row_txn_test row cf0:qu0',
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    ret = p.communicate('')[0]
    print ret
    res_re = re.compile(output_str)
    nose.tools.assert_true(res_re.match(ret) is not None)


def setUp():
    '''
    setup
    '''
    common.drop_table("single_row_txn_test")

    cmd = "./teracli create 'single_row_txn_test<txn=on>{lg0{cf0}}'"
    common.exe_and_check_res(cmd)


def testSingleRowTxnBase():
    # do test
    doSingleRowTxnBaseTest('v1')
    doSingleRowTxnBaseTest('v2')


def tearDown():
    # clear env
    common.drop_table("single_row_txn_test")
    pass
