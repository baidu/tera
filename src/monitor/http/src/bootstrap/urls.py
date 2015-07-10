# -*- coding:utf-8 -*-
################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
code-se route 配置
Authors: wangtaize(wangtaize@baidu.com)
Date:  2014年8月11日
"""
# 主页入口

from django.conf import urls


urlpatterns = urls.patterns("",
    (r'^$', 'bootstrap.views.index'),
    #===========================================================================
    # (r'^v$', 'codesearch.views.viewcode'),
    # (r'^s$', 'codesearch.views.search'),
    # (r'^suggest', 'codesearch.views.suggest'),
    # (r'^feedback', 'codesearch.feedback.feedback'),
    # (r'^accounts/login/$', 'django_cas.views.login'),
    # (r'^accounts/logout/$', 'django_cas.views.login'),
    #===========================================================================
)

urlpatterns += urls.patterns('',
     (r'^monitor/', urls.include('monitor.urls')),
)
#===============================================================================

# 
# # 目录操作
# urlpatterns += urls.patterns('',
#     (r'^tree/', urls.include('codesearch.tree.urls')),
# )
# # 对外提供的接口
# urlpatterns += urls.patterns('',
#     (r'^api/', urls.include('api.urls')),
# )
#===============================================================================
