#! /bin/sh
#
# start.sh
# Copyright (C) 2014 wangtaize <wangtaize@baidu.com>
#
# Distributed under terms of the MIT license.
#
HOST_NAME=`hostname`
cd src
nohup gunicorn -w 4 tera_monitor_boot:app -b $HOST_NAME:8012 1>/dev/null 2>/dev/null &

