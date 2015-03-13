[Tera - 分布式表格系统](http://github.com/BaiduPS/tera)
====
[![Build Status](https://travis-ci.org/BaiduPS/tera.svg)](https://travis-ci.org/BaiduPS/tera)

Copyright 2015, Baidu.com, Inc.

#Overview
Tera是一个高性能、可伸缩的数据库系统，被设计用来管理搜索引擎万亿量级的超链与网页信息。为实现数据的实时分析与高效访问，我们使用按行键、列名和时间戳全局排序的三维数据模型组织数据，使用多级Cache系统，充分利用新一代服务器硬件大内存、SSD盘和万兆网卡的性能优势，做到模型灵活的同时，实现了高吞吐与水平扩展。

#特性
 * 全局有序
 * 热点自动分片
 * 数据强一致
 * 多版本,自动垃圾收集
 * 按列存储,支持内存表
 * 动态schema
 * 支持表格快照
 * 高效随机读写

#系统结构
系统主要由Tabletserver、Master和ClientSDK三部分构成。其中Tabletserver是核心服务器，承载着所有的数据管理与访问；Master是系统的仲裁者，负责表格的创建、schema更新与负载均衡；ClientSDK包含供管理员使用的命令行工具teracli和给用户使用的SDK。

#系统依赖
使用分布式文件系统（HDFS、NFS等）持久化数据与元信息

使用zookeeper选主与协调

使用Sofa-pbrpc实现跨进程通信

#使用示例
参考[wiki](https://github.com/BaiduPS/tera/wiki/%E4%B8%BB%E8%A6%81API%E4%BD%BF%E7%94%A8%E6%96%B9%E6%B3%95)
