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

from datetime import datetime
from datetime import timedelta
import os
import tempfile
import time

from frame.shell import command
import monitor
from tera import monitor_pb2

LOGGER = monitor.logger
class NodeStatService(object):
    """
    节点监控服务
    """
    def __init__(self, tera_mo_bin):
        # 指定tera monitor 可执行文件位置
        self.tera_mo_bin = tera_mo_bin
        self.shell_helper = command.ShellHelper()

    def get_stat(self,zk,zk_path, node_name, t_start=None, t_end=None):
        """
        获取单个节点监控信息
        args:
            node_name 节点名称
            t_start 起始时间 单位为秒
            t_end 结束时间 单位为秒
        return:
            MonitorResponse 详情参照monitor.proto,返回值可能为空
        """
        if not node_name:
            return None

        if t_end:
            t_end = t_end * 1000000
        else:
             # seconds
            t_end = time.time() * 1000000
        if t_start:
            t_start = t_start * 1000000
        else:
            t_start = t_end + timedelta(hours=-12).total_seconds() * 1000000
        table_node_list = [node_name]
        req = monitor_pb2.MonitorRequest(tabletnodes=table_node_list)
        req.min_timestamp = long(t_start)
        req.max_timestamp = long(t_end)
        req.cmd = monitor_pb2.kGetPart
        req.tera_zk_addr = zk
        req.tera_zk_root = zk_path
        response = self._execute(req)
        return response

    def get_node_list(self, zk_node, root_path):
        """
        获取节点列表
        return:
            MonitorResponse 详情参照monitor.proto,返回值可能为空
        """
        req = monitor_pb2.MonitorRequest()
        req.tera_zk_addr = zk_node
        req.tera_zk_root = root_path
        req.cmd = monitor_pb2.kList
        response = self._execute(req)
        return response

    def _execute(self, req_proto):
        """
        执行命令入口
        """
        temp_req_path = tempfile.mktemp()
        with open(temp_req_path, "wb+") as fd:
            fd.write(req_proto.SerializeToString())
        temp_rep_path = tempfile.mktemp()
        try:
            command = [self.tera_mo_bin, temp_req_path, temp_rep_path]
            status, _ , error = self.shell_helper.run_with_retuncode(command)
            LOGGER.error(error)
            if status != 0 or not os.path.exists(temp_rep_path):
                LOGGER.error("fail to execute command %s ,error %s", command, error)
                return None
            with open(temp_rep_path, "rb") as rfd:
                response = monitor_pb2.MonitorResponse()
                response.ParseFromString(rfd.read())
                return response
        except:
            LOGGER.exception("execute command error the input %s",
                             req_proto.SerializeToString())
        finally:
            if os.path.exists(temp_req_path):
                #os.remove(temp_req_path)
                pass
            if os.path.exists(temp_rep_path):
                #os.remove(temp_rep_path)
                pass
        return None


