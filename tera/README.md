tera源码目录结构
----
client

    Tera的命令行工具，用于表格创建、schema更新和执行管理命令。
    
io

    Tabletserver的数据存储相关代码实现。

leveldb

    keyvalue存储代码实现，基于开源kv系统leveldb实现。

master

    Master模块的实现。

proto

    Master、Tabletserver和SDK三者之间通信协议。

sample

    Tera使用实例。

tabletnode

    Tabletserver的tablet管理部分代码实现。

sdk

    SDK代码

utils

    多个模块共用的工具代码。

zk

    Tera对zookeeper的封装，主要实现选主、存活性检测等逻辑。
