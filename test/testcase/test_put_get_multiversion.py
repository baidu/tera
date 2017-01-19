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

@nose.tools.with_setup(common.create_kv_table, common.cleanup)
def test_kv_get_multiverison():
    """
    put multiversion data and get
    """
    time.sleep(5)
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k 1v', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k 2v', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    time.sleep(5)
    ret = subprocess.Popen(const.teracli_binary + ' get test 1k', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), "2v")


@nose.tools.with_setup(common.create_multiversion_table, common.cleanup)
def test_multiversion_table_get():
    """
    put multiversion data and get
    """
    time.sleep(5)
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k cf0:q0 1v', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k cf0:q0 2v', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    time.sleep(10)
    ret = subprocess.Popen(const.teracli_binary + ' get test 1k cf0:q0', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), "2v")

