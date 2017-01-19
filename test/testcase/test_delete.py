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

@nose.tools.with_setup(common.create_multilg_table, common.cleanup)
def test_delete_row_cf_q():
    """
    delete different level data
    """
    time.sleep(5)
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k cf0:q1 v1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k cf0:q2 v2', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k cf0:q3 v3', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k cf1:q1 v1', \
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k cf1:q2 v2', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()

    ret = subprocess.Popen(const.teracli_binary + ' delete test 1k cf0:q1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' get test 1k cf0:q1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_not_equal(''.join(ret.stdout.readlines()).strip(), "v1")

    ret = subprocess.Popen(const.teracli_binary + ' delete test 1k cf0', \
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' get test 1k cf0:q2', \
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)                     
    nose.tools.assert_not_equal(''.join(ret.stdout.readlines()).strip(), "v2")
    ret = subprocess.Popen(const.teracli_binary + ' get test 1k cf0:q3', \
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)              
    nose.tools.assert_not_equal(''.join(ret.stdout.readlines()).strip(), "v3")

    ret = subprocess.Popen(const.teracli_binary + ' delete test 1k', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)         
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' scan test "" ""', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)              
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), "")


@nose.tools.with_setup(common.create_multiversion_table, common.cleanup)
def test_delete_multiversion():
    """
    delete mutiversion data
    """
    time.sleep(5)
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k cf0:q1 v1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k cf0:q1 v2', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    ret = subprocess.Popen(const.teracli_binary + ' put test 1k cf0:q1 v3', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()

    ret = subprocess.Popen(const.teracli_binary + ' delete1v test 1k cf0:q1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)                     
    ret.communicate()                                                                                      
    ret = subprocess.Popen(const.teracli_binary + ' get test 1k cf0:q1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)                     
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), "v2")   

    ret = subprocess.Popen(const.teracli_binary + ' delete test 1k cf0:q1', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)                     
    ret.communicate()                                                                                      
    ret = subprocess.Popen(const.teracli_binary + ' scan test 1k "" ""', \
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)                     
    nose.tools.assert_equal(''.join(ret.stdout.readlines()).strip(), "")    
