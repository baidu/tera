Tera原生的SDK是C++实现，考虑到部分用户受语言所限，我们也提供了一套HTTP的接口。

# 生成tera的http代理

```
make terahttp
```

# 运行http代理

```
./terahttp --flagfile=/path/to/flag
```
http代理需要和普通tera客户端一样的配置文件。

# 通过HTTP访问tera

请求需以json格式组织，例如：(POST方法)

请求的具体形式可以参考[sofa-pbrpc的HTTP支持](https://github.com/baidu/sofa-pbrpc/wiki/%E9%AB%98%E7%BA%A7%E4%BD%BF%E7%94%A8)

## demo-0 curl

```
curl -d '

{
    "tablename": "oops",                        // 表名
    "mutation_list": [                          // 支持批量修改（写入）
        {                                       // 每条记录的修改（写入）作为"mutation_list"数组的一个item
            "rowkey": "row35",                  // 这条记录的rowkey
            "type": "put",                      // 操作类型：put表示更新（写入），del-col表删除一列，del-row表删除一行
            "columns": [                        // 待修改（写入）的列
                {
                    "columnfamily": "cf0",      // 列名中columnfamily字段
                    "qualifier": "qu0",         // 列名中qualifier字段
                    "value": "value35"          // 这一列对应的值
                },
                {                               // 待修改（写入）的另外一列
                    "columnfamily": "cf0",
                    "qualifier": "qu1",
                    "value": "value35.2"
                }
            ]

        },
        {                                       // 修改（写入）另外一行
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

' http://localhost:12321/tera.http.HttpProxy.Get
```

## demo-1 python

`http_sample.py`演示了用grequest模块异步POST读写。

安装grequests模块可能需要自行解决数个依赖模块。
