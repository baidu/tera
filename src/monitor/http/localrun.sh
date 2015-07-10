#! /bin/sh
#
# localrun.sh
# Copyright (C) 2014 wangtaize <wangtaize@baidu.com>
#
# Distributed under terms of the MIT license.
#
export PYTHONPATH=`pwd`/src
HOST_NAME=`hostname`
cd src
gunicorn -w 2 tera_monitor_boot:app -b $HOST_NAME:8081
