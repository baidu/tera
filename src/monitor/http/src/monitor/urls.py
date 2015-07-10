# -*- coding:utf-8 -*-
################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
monitor router
Authors: wangtaize(wangtaize@baidu.com)
Date: 2014年11月18日
"""
from django.conf import urls
urlpatterns = urls.patterns("",
    (r'^statLine$', 'monitor.views.render_node_stat_line'),
    (r'^getNodeList$', 'monitor.views.get_node_list'),
)