# encoding:utf-8
"""
Created on 2014-1-26
@author: wangtaize@baidu.com
@copyright: www.baidu.com
"""

import cookielib
from io import BytesIO as BIO
import json
import logging
import mimetools
import mimetypes
import os
import socket
import urllib
import urllib2


class URLBuilder(object):
    @classmethod
    def build_tcp(cls, host, port):
        result = []
        result.append("tcp")
        result.append("://")
        result.append(host)
        result.append(":")
        result.append(port)
        return "".join(result)
    @classmethod
    def build_http(cls, host, port):
        result = []
        result.append("http")
        result.append("://")
        result.append(host)
        result.append(":")
        result.append(port)
        return "".join(result)

class StatefullHttpService(object):
    """
     httpservice keep session on cookies

    """
    def __init__(self, cookies_file_name=None,
                      use_old_cookies=True,
                      logger=None):
        self.logger = logger or logging.getLogger(__name__)
        if not cookies_file_name:
            cookies_file_name = "cookies.txt"
        cookies_folder = os.path.join(os.path.expanduser("~"), ".netscape")
        if not os.path.exists(cookies_folder):
            os.mkdir(cookies_folder)
        self.cookies_path = os.path.join(cookies_folder, cookies_file_name)
        self.authenticated = False
        self._build_opener(use_old_cookies)
        self.pending_request = None
    def post(self, url, params, timeout=10000):
        """
        the parama can be a dict like {"field1":"value1","field2":"value2"}
        or be a tuple like [("field1","value1"),("field2","value2"),("field2","value2")].
        the tuple can have duplicate keys
        """
        url = self._process_url(url)
        request = self._build_request(url, params)
        return self._service(request, timeout)
    def get(self, url, timeout=10000):
        url = self._process_url(url)
        request = self._build_request(url)
        return self._service(request, timeout)
    def _build_request(self, url, params=None):
        # POST
        if params:
            self.logger.debug('post %s params[%s]' % (url, params))
            request = urllib2.Request(url, urllib.urlencode(params))
        # GET
        else:
            self.logger.debug('get %s params[%s]' % (url, params))
            request = urllib2.Request(url)
        return request
    def _build_multipart_request(self, url, fields, files):
        content_type, body = self._encode_multipart_formdata(fields, files)
        headers = {'Content-Type': content_type,
                   'Content-Length': str(len(body))}
        r = urllib2.Request(url, body, headers)
        return r
    def _process_url(self, url):
        url = url.strip()
        if not url.startswith("http"):
            url = "http://%s" % url
        return url
    def _build_opener(self, use_old_cookies):
        opener = urllib2.OpenerDirector()
        opener.add_handler(urllib2.ProxyHandler())
        opener.add_handler(urllib2.UnknownHandler())
        opener.add_handler(urllib2.HTTPHandler())
        opener.add_handler(urllib2.HTTPDefaultErrorHandler())
        opener.add_handler(urllib2.HTTPSHandler())
        opener.add_handler(urllib2.HTTPErrorProcessor())
        self.cookie_jar = cookielib.MozillaCookieJar(self.cookies_path)
        if use_old_cookies and os.path.exists(self.cookies_path):
            try:
                self.cookie_jar.load()
                self.authenticated = True
            except (cookielib.LoadError, IOError):
                pass
        else:
            if os.path.exists(self.cookies_path):
                os.remove(self.cookies_path)
            fd = os.open(self.cookies_path, os.O_CREAT, 0600)
            os.close(fd)
            # Always chmod the cookie file
            os.chmod(self.cookies_path, 0600)
        opener.add_handler(urllib2.HTTPCookieProcessor(self.cookie_jar))
        self.opener = opener
    def _service(self, request, timeout):
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(timeout)
        try:
            response = self.opener.open(request)
            content = response.read()
            response.close()
            self.logger.debug('content->%s,code->%d' % (content, response.code))
            if response.code in [200, 201, 202] :
                return content, True
            return content, False
        except urllib2.HTTPError, e:
            if e.code in [302, 403]:
                self.pending_request = (request, timeout)
                try:
                    return self.login()
                except:
                    # 如果没有实现login方法直接返回
                    pass
            return e.read(), False
        except Exception, ex:
            return str(ex), False
        finally:
            socket.setdefaulttimeout(old_timeout)
    def post_multipart(self, url, fields, files, timeout=1000):
        url = self._process_url(url)
        request = self._build_multipart_request(url, fields, files)
        return self._service(request, timeout)
    def _encode_multipart_formdata(self, fields, files, encode="utf-8"):
        """
        fields is a sequence of (name, value) elements for regular form fields.
        files is a sequence of (name, filename, value) elements for data to be uploaded as files
        Return (content_type, body) ready for httplib.HTTP instance
        """
        BOUNDARY = mimetools.choose_boundary()
        CRLF = '\r\n'
        body = BIO()
        for (key, value) in fields:
            key = str(key)
            body.write('--' + BOUNDARY)
            body.write(CRLF)
            body.write('--' + BOUNDARY)
            body.write(CRLF)
            body.write('Content-Disposition: form-data; name="%s"' % key)
            body.write(CRLF)
            body.write('')
            body.write(CRLF)
            if isinstance(value, unicode):
                value = value.encode(encode)
            body.write(str(value))
            body.write(CRLF)
        for (key, filename, value) in files:
            if not value:
                continue
            key = str(key)
            filename = str(filename)
            body.write('--' + BOUNDARY)
            body.write(CRLF)
            body.write('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
            body.write(CRLF)
            body.write('Content-Type: %s' % self._get_content_type(filename))
            body.write(CRLF)
            body.write('')
            body.write(CRLF)
            body.write(value)
            body.write(CRLF)
        body.write('--' + BOUNDARY + '--')
        body.write(CRLF)
        body.write('')
        value = body.getvalue()
        content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
        return content_type, value
    def _get_content_type(self, filename):
        try:
            return str(mimetypes.guess_type(filename)[0]) or 'application/octet-stream'
        except:
            return 'application/octet-stream'
    def login(self):
        raise NotImplementedError(
            "abstract method -- subclass %s must override" % self.__class__)
try:
    from django.http import response
    from django import shortcuts
    from django.template import context
    class ResponseBuilder(object):
        def __init__(self, request=None):
            self.mimetype = None
            self.content_type = 'text/html'
            self.status = 200
            self.content = None
            self.request = request
        def set_status(self, status):
            self.status = status
            return self
        def set_content(self, content):
            self.content = content
            return self
        def set_content_type(self, content_type):
            self.content_type = content_type
            return self
        def set_minetype(self, mimetype):
            self.mimetype = mimetype
            return self
        def add_context_attr(self, key, value):
            if not self.content:
                self.content = {}
                self.content[key] = value
            elif  type(self.content) == dict:
                self.content[key] = value
            else:
                raise Exception('the type of content should  be dict ,but it is %s' % (type(self.content)))
            return self
        def add_params(self, params):
            """
            添加模板context
            args:
                params {dict}
            """
            if not self.content:
                self.content = {}
            self.content.update(params)
            return self

        def ok(self, data=None):
            self.add_context_attr("status", 1)\
                .add_context_attr('msg', "ok")\
                .add_context_attr('data', data)
            return self
        def error(self, msg):
            self.add_context_attr("status", 0)\
                .add_context_attr('msg', msg)
            return self
        def build_json(self):
            """
            this will ingore the content_type,and the type content should be dict
            """
            if not self.content:
                raise Exception('content should not be null')
            if type(self.content) != dict:
                raise Exception('the type of content should  be dict,but it is %s' % (type(self.content)))
            self.set_content(json.dumps(self.content))\
                .set_content_type('application/json')
            return self.build()

        def add_req(self, request):
            """
            生成模块函数需要这个对象
            args:
                request {django.http.request.HttpRequest}
            return:
                result {self}
            """
            self.request = request
            return self

        def build_html(self):
            """
            生成网页内容
            """
            self.set_content_type('text/html')
            return self.build()

        def build_tmpl(self, template):
            """
            指定模板生成网页代码
            args:
                template {string} 模版名称
            return:
                response {django.http.reponse.HttpResponse}
            """
            # TODO 改善content这个变量名称
            if not self.content:
                self.content = {}
            root_url = self.request.build_absolute_uri("/")
            self.content["root_url"] = root_url
            return shortcuts.render_to_response(template, self.content,
                    context_instance=context.RequestContext(self.request))

        def build(self):
            if not self.content:
                raise Exception('content should not be null')
            return response.HttpResponse(content=self.content,
                                         content_type=self.content_type,
                                         status=self.status,
                                         mimetype=self.mimetype)
except:
    pass
