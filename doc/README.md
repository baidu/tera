# Tera文档专区

## 简介

[系统设计](https://github.com/BaiduPS/tera/blob/master/doc/tera_design.md)

[master、ts、client的交互](https://github.com/BaiduPS/tera/blob/master/doc/master-ts-client-interactive.md)

## 使用帮助

[构建tera](https://github.com/baidu/tera/blob/master/BUILD)

[体验单机Tera](https://github.com/baidu/tera/blob/master/doc/Onebox.md)

[通过docker体验Tera](https://github.com/baidu/tera/blob/master/example/docker)

[命令行工具teracli使用方法](https://github.com/baidu/tera/blob/master/doc/teracli.md)

[主要api使用方法](https://github.com/baidu/tera/blob/master/doc/sdk_dev_guide.md)

[搭建tera集群](https://github.com/baidu/tera/blob/master/doc/cluster_setup.md)

[各flag配置项含义介绍](https://github.com/baidu/tera/blob/master/doc/tera_flag.md)

## 性能优化

[针对表格不同读写特性的性能优化建议](https://github.com/baidu/tera/blob/master/doc/perf_opz.md)

## 功能实现

[动态负载均衡](https://github.com/baidu/tera/blob/master/doc/load-balance.md)

[数据删除的实现](https://github.com/baidu/tera/blob/master/doc/data-deletion-in-tera.md)

[tablet的可用性统计实现](https://github.com/baidu/tera/blob/master/doc/tablet-availability.md)

## 版本发布

[版本发布及管理](https://github.com/baidu/tera/blob/master/doc/release_management.md)

## 源码相关

[源码目录结构](https://github.com/baidu/tera/blob/master/src/README.md)

[LevelDB](https://github.com/baidu/tera/blob/master/src/leveldb/README.md)

## TodoList
1. Master的设计与实现（职责、功能、每个功能怎么实现的、为什么这么实现、还有哪些遗留问题）
1. Tabletserver设计与实现
1. SDK的结构化文档（可折叠，可搜索，类是windows的帮助）
1. 性能数据，不同场景下的测试数，瓶颈分析
1. 性能方面设计折衷（怎样实现的高性能随机读写）
1. 数据模型的详细介绍
  * cf的使用建议
  * 多版本保留机制
1. 监控系统的安装和使用
1. 快速、低成本Split&Merge的实现原理
