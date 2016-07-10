#!/usr/bin/env python
# encoding: utf-8

"""
tera http sample

用grequests模块实现http POST读写tera

前置条件：
1. 已安装python-grequests模块
2. write_url/read_url指向自己的http地址
3. 已建表 "oops{lg0{cf0}}" && "oops2{lg0{cf0}}"

"""

import grequests

write_url = 'http://127.0.0.1:8657/tera.http.HttpProxy.Put'
read_url = 'http://127.0.0.1:8657/tera.http.HttpProxy.Get'

read_params = '''
{
    "tablename": "oops",
    "reader_list": [
        {
            "rowkey": "row404",
            "columnfamily": "cf0",
            "qualifier": "qu0"
        },
        {
            "rowkey": "row35",
            "columnfamily": "cf404",
            "qualifier": "qu0"
        },
        {
            "rowkey": "row35",
            "columnfamily": "cf0",
            "qualifier": "qu404"
        },
        {
            "rowkey": "row35",
            "columnfamily": "cf0",
            "qualifier": "qu0"
        }
    ]
}'''

write_params1 = '''
{
    "tablename": "oops",
    "mutation_list": [
        {
            "rowkey": "row35",
            "type": "put",
            "columns": [
                {
                    "columnfamily": "cf0",
                    "qualifier": "qu0",
                    "value": "value35"
                },
                {
                    "columnfamily": "cf0",
                    "qualifier": "qu1",
                    "value": "value35.2"
                }
            ]
        },
        {
            "rowkey": "row36",
            "type": "put",
            "columns": [
                {
                    "columnfamily": "cf0",
                    "qualifier": "qu0",
                    "value": "value36"
                },
                {
                    "columnfamily": "cf0",
                    "qualifier": "qu1",
                    "value": "value36.2"
                }
            ]
        }
    ]
}
'''

write_params2 = '''
{
    "tablename": "oops2",
    "mutation_list": [
        {
            "rowkey": "row35",
            "type": "put",
            "columns": [
                {
                    "columnfamily": "cf0",
                    "qualifier": "qu0",
                    "value": "value35"
                },
                {
                    "columnfamily": "cf0",
                    "qualifier": "qu1",
                    "value": "value35.2"
                }
            ]
        },
        {
            "rowkey": "row36",
            "type": "put",
            "columns": [
                {
                    "columnfamily": "cf0",
                    "qualifier": "qu0",
                    "value": "value36"
                },
                {
                    "columnfamily": "cf0",
                    "qualifier": "qu1",
                    "value": "value36.2"
                }
            ]
        }
    ]
}
'''

write_data_set = [write_params1, write_params2]

write_rs = (grequests.request('POST', write_url, data=write_params) for write_params in write_data_set)
write_re = grequests.map(write_rs)
for i in write_re:
    print(i.json())


read_rs = [grequests.request('POST', read_url, data=read_params)]
read_re = grequests.map(read_rs)
for i in read_re:
    print(i.json())
