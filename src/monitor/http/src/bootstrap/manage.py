#!/usr/bin/env python
# ! -*- coding:utf-8 -*-
"""
本地开发使用
"""
import os
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "bootstrap.settings")
    from django.core.management import execute_from_command_line
    execute_from_command_line(sys.argv)

