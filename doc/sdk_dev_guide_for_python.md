# 使用tera的Python Sdk

1. 下载[TeraSdk.py](https://github.com/baidu/tera/blob/master/src/sdk/python/TeraSdk.py)
2. 编译（或从其它途径获取）得到libtera_c.so，与TeraSdk.py置于同一目录下
3. 编写应用程序
  1. 示例[sample](https://github.com/baidu/tera/blob/master/src/sdk/python/sample.py)

# API文档

[TeraSdk.py](https://github.com/baidu/tera/blob/master/src/sdk/python/TeraSdk.py)里对常用接口都有比较全面的注释

# 已支持功能

1. 同步读
1. 同步、异步写
1. scan


# 使用示例

Python SDK实现的是数据读、写、scan功能。

表格的创建、删除等管理任务需要teracli实现。

## Client & Table

一个Client对象对应一个tera集群；用一个Client打开需要读写的表。

```
try:
    client = Client("./tera.flag", "pysdk_log_prefix")
    '''
    oops表已由管理员创建
    '''
    table = client.OpenTable("oops")
except TeraSdkException as e:
    print(e.reason)
```

## write

同步写
```
try:
    table.Put("row_key", "column_family", "qualifier", "value")
except TeraSdkException as e:
    print(e.reason)
```

## read

同步读
```
try:
    print(table.Get("row_key", "column_family", "qualifier", 0))
except TeraSdkException as e:
    print(e.reason)
```

# 背景

1. Tera Python Sdk实现原理：Tera原生的C++ SDK接口导出为C接口，封装出libtera_c.so，Python通过ctypes库与libtera_c.so通信。
从v2.3起，Python标准库自带ctypes.

1. Python解释器和libtera_c.so的二进制兼容需要由用户自己保证。gcc3编译的Python解释器和gcc4编译的libtera_c.so可能存在ABI兼容问题。
