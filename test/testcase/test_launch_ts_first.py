"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import nose
import common 
import time
import subprocess

@nose.tools.with_setup()
def test_launch_ts_first():
    """
    Test launch TS first
    """
    common.cluster_op('kill')
    common.cluster_op('launch_ts_first')
    ret = subprocess.Popen('./teracli show', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())


