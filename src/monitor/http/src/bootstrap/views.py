# -*- coding:utf-8 -*-
################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
index
Authors: wangtaize(wangtaize@baidu.com)
Date:  2014年8月14日
"""


from bootstrap import settings
from frame.net import http
from monitor import service

def index(request):
    """
    index
    """
    response_builder = http.ResponseBuilder(request=request)
    node_list = [zk_node for zk_node in settings.ZK_DICT]
    response_builder.set_content({"node_list":node_list})
    return response_builder.build_tmpl("index.html")
