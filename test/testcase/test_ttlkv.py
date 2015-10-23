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

@nose.tools.with_setup(common.create_ttlkv_table, common.cleanup)
def test_ttlkv():

    time.sleep(5)
    ret = subprocess.Popen(const.teracli_binary + ' put-ttl test 1k 1v 5', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' get test 1k', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), "1v")
    time.sleep(2)
    ret = subprocess.Popen(const.teracli_binary + ' get test 1k', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_not_equal(''.join(ret.stdout.readlines()).strip(), "1v")
