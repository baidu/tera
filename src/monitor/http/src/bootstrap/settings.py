# -*- coding:utf-8 -*-
################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
Authors: wangtaize(wangtaize@baidu.com)
Date:  2014年8月11日
"""

import os
DEBUG = True
TEMPLATE_DEBUG = DEBUG
# 项目路径
PROJECT_PATH = os.path.realpath(os.path.dirname(os.path.dirname(__file__)))
ADMINS = (
    ('wangtaize', 'wangtaize@baidu.com'),
)

MAIL_PROXY = "proxy-in.baidu.com"


MANAGERS = ADMINS
# 数据库配置
DATABASES = {
    #===========================================================================
    # 'default': {
    #     'ENGINE': 'django.db.backends.mysql',
    #     'NAME': 'eagleeye-ci',
    #     'USER': 'root',
    #     'PASSWORD': 'iitmysql_dev01',
    #     'HOST': '10.46.8.106',
    #     'PORT': '8888'
    # }
    #===========================================================================
}


LANGUAGE_CODE = 'zh_CN'
TIME_ZONE = 'Asia/Shanghai'


# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale
USE_L10N = True

# Absolute filesystem path to the directory that will hold user-uploaded files.
# Example: "/home/media/media.lawrence.com/media/"




# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/home/media/media.lawrence.com/static/"
STATICFILES_DIRS = [
    os.path.abspath(os.path.join(PROJECT_PATH, 'static')),
]
# URL prefix for static files.
# Example: "http://media.lawrence.com/static/"
STATIC_URL = '/static/'

# URL prefix for admin static files -- CSS, JavaScript and images.
# Make sure to use a trailing slash.
# Examples: "http://foo.com/static/admin/", "/static/admin/".
ADMIN_MEDIA_PREFIX = '/static/admin/'


# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    #    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'trl=d0*ah$rc_r7y5@hra$()9q$x^qt#otfvmo$j(5wy53s5l('

TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
    #     'django.template.loaders.eggs.Loader',
)


MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.locale.LocaleMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.middleware.doc.XViewMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware'

)

TEMPLATE_CONTEXT_PROCESSORS = (
    'django.contrib.auth.context_processors.auth',
    'django.core.context_processors.request'
)



# CAS_RETRY_LOGIN = True
# CAS_LOGOUT_COMPLETELY = True

ROOT_URLCONF = 'bootstrap.urls'

TEMPLATE_DIRS = (
    os.path.join(PROJECT_PATH, 'templates'),
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.admin',
    'bootstrap',
)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
        'access': {
            'format': '%(asctime)s %(message)s'
        },
    },
    'handlers': {
        'null': {
            'level': 'DEBUG',
            'class': 'django.utils.log.NullHandler',
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'verbose',
            'filename': os.path.join(os.path.dirname(__file__), 'log/monitor.log'),
            'when': 'midnight',
        },
        'cron': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'verbose',
            'filename': os.path.join(os.path.dirname(__file__), 'log/monitor_cron.log'),
            'when': 'midnight',
        },
        'mail_admins': {
            'level': 'ERROR',
            'class': 'django.utils.log.AdminEmailHandler',
        },
        'request': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'access',
            'filename': os.path.join(os.path.dirname(__file__), 'log/monitor_request.log'),
            'when': 'midnight',
        },
        'exception': {
            'level': 'ERROR',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'access',
            'filename': os.path.join(os.path.dirname(__file__), 'log/monitor_excpetion.log'),
            'when': 'midnight',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'propagate': True,
            'level': 'INFO',
        },
        'django.request': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'monitor': {
            'handlers': ['console'],
            'level': 'DEBUG',
        }
    }
}

TERA_MO_BIN = "/home/users/wangtaize/workspace/ps/se/tera/teramo"
ZK_DICT = {"nj01-build-en-s-bak369.nj01.baidu.com:3000":"/tera_nj02_pl", "db-stest-zk0.db01.baidu.com:3000":"/sandbox/tera/nmg-lb"}

