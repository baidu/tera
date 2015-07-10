#! /bin/sh
#
# localtest.sh
# Copyright (C) 2014 wangtaize <wangtaize@baidu.com>
#
# Distributed under terms of the MIT license.
#
export PYTHONPATH=`pwd`/src
cd src
nosetests -v 

