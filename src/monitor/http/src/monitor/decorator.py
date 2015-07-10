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

from frame.net import http


def node_required(func):
    def node_required_wrapper(request, *args, **kwds):
        response_builder = http.ResponseBuilder()
        node = request.GET.get("node", None)
        if not node:
            response_builder.error("node is required")\
                            .build_json()
        request.node = node
        return func(request, *args, **kwds)
    return node_required_wrapper
