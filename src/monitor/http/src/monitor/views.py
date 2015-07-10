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

import datetime
import time
from protobuf_to_dict import protobuf_to_dict
from bootstrap import settings
from frame.net import http
from monitor import decorator
from monitor import service



def time_stamp_formatter(timestamp):
    return datetime.datetime.fromtimestamp(timestamp / 1000000).strftime('%H:%M:%S')


def mem_used_formatter(mem_used):
    return mem_used / (1024 * 1024 * 1024)


def net_formatter(net_p):
    return net_p / (1024 * 1024)


FORMATTER_TABLE = {"time_stamp":time_stamp_formatter,
                   "mem_used":mem_used_formatter,
                   "net_tx":net_formatter,
                   "net_rx": net_formatter,
                   "dfs_io_r":net_formatter,
                   "dfs_io_w":net_formatter,
                   "local_io_r":net_formatter,
                   "local_io_w":net_formatter}

def dicts_to_prop_stream(dict_list):
    result = {}
    for i_dict in dict_list:
        for key in i_dict:
            if key not in result:
                result[key] = []
            formatter = FORMATTER_TABLE.get(key, None)
            if formatter:
                result[key].append(formatter(i_dict[key]))
            else:
                result[key].append(i_dict[key])
    return result

@decorator.node_required
def render_node_stat_line(request):
    response_builder = http.ResponseBuilder()
    node = request.node
    start_time = None
    end_time = None
    end = request.GET.get('end',None)
    try:
        start_str = request.GET.get('start',None)
        if start_str:
            start_time = time.mktime(time.strptime(start_str,'%Y/%m/%d %H:%M'))
        if end :
            end_time = time.mktime(time.strptime(end,'%Y/%m/%d %H:%M'))
    except:
        return  response_builder.error("no data")\
                                 .build_json()
    zk = request.GET.get('zk',None)
    if not zk:
        return  response_builder.error("no data")\
                                .build_json()

    if zk not in settings.ZK_DICT:
        return response_builder.error("%s is invalidate" % zk)\
                               .build_json()
    zk_path = settings.ZK_DICT[zk]
    node_service = service.NodeStatService(settings.TERA_MO_BIN)
    response = node_service.get_stat(zk,zk_path, node, start_time,end_time)
    if not response or not response.stat_list:
        return response_builder.error("no data")\
                               .build_json()
    node_stat = response.stat_list[0]
    node_stat_dict = protobuf_to_dict(node_stat)
    node_stat_dict_stream = {}
    if 'stat' in  node_stat_dict:
        node_stat_dict_stream = dicts_to_prop_stream(node_stat_dict['stat'])
    return response_builder.ok(data=node_stat_dict_stream)\
                           .build_json()

def get_node_list(request):
    response_builder = http.ResponseBuilder()
    node_service = service.NodeStatService(settings.TERA_MO_BIN)
    zk_node = request.GET.get('zknode', None)
    if not zk_node :
        return response_builder.error("zknode is required")\
                               .build_json()
    if zk_node not in settings.ZK_DICT:
        return response_builder.error("%s is invalidate" % zk_node)\
                               .build_json()
    root_path = settings.ZK_DICT[zk_node]
    reponse = node_service.get_node_list(zk_node, root_path)
    if not reponse or not reponse.stat_list:
        return response_builder.error("no data")\
                               .build_json()
    node_list = [node.addr for node in reponse.stat_list]
    return response_builder.ok(data=node_list)\
                           .build_json()
