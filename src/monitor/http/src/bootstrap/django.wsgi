import os,sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append("/home/work/workdir/apps/fatcat/")
os.environ['DJANGO_SETTINGS_MODULE'] = 'bootstrap.settings'
from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()