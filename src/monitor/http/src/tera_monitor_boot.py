# -*- coding:utf-8 -*-
"""
monitor boot script
"""
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append("/home/users/wangtaize/workspace/ps/se/tera/tera/monitor/http/src")
os.environ['DJANGO_SETTINGS_MODULE'] = 'bootstrap.settings'
from django.core.wsgi import get_wsgi_application
from dj_static import Cling
app = Cling(get_wsgi_application())
