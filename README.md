[Tera - 分布式表格系统](http://github.com/BaiduPS/tera)
====

Copyright 2015 Baidu.com, Inc.

#Overview
Tera实现了按行key、列和时间戳全局排序的三维数据模型，并且针对万兆网卡和SSD进行了优化，适合海量记录（万亿量级）的存储和随机访问，能够很好的解决局部热点问题和扩展性问题。

#特性
    * 按列存储
    * 自动分片
    * 动态schema、稀疏表
    * 多版本
    * 支持快照
    * 强一致
    * 高效随机读写

#系统依赖
使用分布式文件系统（HDFS、NFS等）持久化数据与元信息

使用zookeeper选主与协调

使用Sofa-pbrpc实现跨进程通信

#API使用示例
参考[sample.cc](https://github.com/bluebore/tera/blob/master/tera/sample/tera_sample.cc)
