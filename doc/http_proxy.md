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

```
curl -d '{"tablename":"oops",
          "rowkey":"row1",
          "cf":"cf0",
          "qu":"qu0"}' http://localhost:12321/tera.http.HttpProxy.Get
```

请求的具体形式可以参考[sofa-pbrpc的HTTP支持](https://github.com/baidu/sofa-pbrpc/wiki/%E9%AB%98%E7%BA%A7%E4%BD%BF%E7%94%A8)
