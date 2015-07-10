# -*- coding:utf-8 -*-
################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
DOCUEMENT ME
Authors: wangtaize(wangtaize@baidu.com)
Date: 2014年11月19日
"""

import os
from monitor import service
from bootstrap import settings
import time
def test_get_node_list():


    node_service = service.NodeStatService(settings.TERA_MO_BIN)
    zk = 'nj01-build-en-s-bak369.nj01.baidu.com:3000'
    zk_path = '/tera_nj02_pl'

    response = node_service.get_node_list(zk,zk_path)

    assert response
    assert len(response.stat_list) >=0

def test_get_node_data():

    node_service = service.NodeStatService(settings.TERA_MO_BIN)
    t_end = time.time()
    t_start = t_end -(12*60*60)
    zk = 'nj01-build-en-s-bak369.nj01.baidu.com:3000'
    zk_path = '/tera_nj02_pl'
    response = node_service.get_stat(zk,zk_path,'nj02-stest-tera3.nj02.baidu.com:7700',t_start=t_start,t_end=t_end)
    assert response
    assert response.stat_list
    print  len(response.stat_list[0].stat)
    assert False
if __name__ == "__main__":
    test_get_node_list()
