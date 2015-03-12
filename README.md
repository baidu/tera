[Tera - 分布式表格系统](http://github.com/BaiduPS/tera)
====
[![Build Status](https://travis-ci.org/BaiduPS/tera.svg)](https://travis-ci.org/BaiduPS/tera)

Copyright 2015, Baidu.com, Inc.

#Overview
Tera实现了按行key、列和时间戳全局排序的三维数据模型，并且针对万兆网卡和SSD进行了优化，适合海量记录（万亿量级）的持久存储和高效随机访问。

#特性
    * 全局有序
    * 热点自动分片
    * 数据强一致
    * 多版本,自动垃圾收集
    * 按列存储,支持内存表
    * 动态schema，支持稀疏表
    * 表格快照
    * 高效随机读写

#系统依赖
使用分布式文件系统（HDFS、NFS等）持久化数据与元信息

使用zookeeper选主与协调

使用Sofa-pbrpc实现跨进程通信

#API使用示例
参考[wiki](https://github.com/BaiduPS/tera/wiki/%E4%B8%BB%E8%A6%81API%E4%BD%BF%E7%94%A8%E6%96%B9%E6%B3%95)






