"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""
import nose
import time
import common
import subprocess
from conf import const

def cleanup():
    """
    clean up
    """
    ret = subprocess.Popen(const.teracli_binary + ' disable test1',
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())
    ret = subprocess.Popen(const.teracli_binary + ' drop test1',
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())


@nose.tools.with_setup(common.create_kv_table, cleanup)
def test_kv_rename():
    """
    table type:kv
    """
    time.sleep(5)
    ret = subprocess.Popen(const.teracli_binary + ' rename test test1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), "rename OK: test -> test1")
    ret = subprocess.Popen(const.teracli_binary + ' show test1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stderr.readlines()).strip(), '')
    ret = subprocess.Popen(const.teracli_binary + ' show test', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), '')


@nose.tools.with_setup(common.create_singleversion_table, cleanup)
def test_singleversion_table_rename():
    """
    table type:singleversion
    """
    time.sleep(5)
    ret = subprocess.Popen(const.teracli_binary + ' rename test test1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), "rename OK: test -> test1")
    ret = subprocess.Popen(const.teracli_binary + ' show test1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stderr.readlines()).strip(), '')
    ret = subprocess.Popen(const.teracli_binary + ' show test', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), '')


@nose.tools.with_setup(common.create_multilg_table, cleanup)
def test_multilg_table_rename():
    """
    table type:multilg
    """
    time.sleep(5)
    ret = subprocess.Popen(const.teracli_binary + ' rename test test1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), "rename OK: test -> test1")
    ret = subprocess.Popen(const.teracli_binary + ' show test1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stderr.readlines()).strip(), '')
    ret = subprocess.Popen(const.teracli_binary + ' show test', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), '')


@nose.tools.with_setup(common.create_multiversion_table, cleanup)
def test_multiversion_table_rename():
    """
    table type:multilg
    """
    time.sleep(5)
    ret = subprocess.Popen(const.teracli_binary + ' rename test test1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), "rename OK: test -> test1")
    ret = subprocess.Popen(const.teracli_binary + ' show test1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stderr.readlines()).strip(), '')
    ret = subprocess.Popen(const.teracli_binary + ' show test', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), '')
