# Tera文档专区
文档还在集中整理中，现在想学习tera还只能看代码。

后续整理的文档可以放在这个目录下，也可以放在对应代码目录下，但都在这留个链接方便索引。

[系统设计](https://github.com/BaiduPS/tera/blob/master/doc/tera_design.md)

[系统构建](https://github.com/BaiduPS/tera/blob/master/BUILD)

[通过OneBox体验Tera](https://github.com/BaiduPS/tera/blob/master/doc/Onebox.md)

[使用示例](https://github.com/BaiduPS/tera/wiki/%E4%B8%BB%E8%A6%81API%E4%BD%BF%E7%94%A8%E6%96%B9%E6%B3%95)

[源码目录结构](https://github.com/BaiduPS/tera/blob/master/src/README.md)

[LevelDB](https://github.com/BaiduPS/tera/blob/master/src/leveldb/README.md)

[命令行工具使用手册](https://github.com/BaiduPS/tera/blob/master/doc/teracli.md)

[tera实现之master、ts、client的交互](https://github.com/BaiduPS/tera/blob/master/doc/master-ts-client-interactive.md)

## TodoList
1. Master的设计与实现（职责、功能、每个功能怎么实现的、为什么这么实现、还有哪些遗留问题）
1. Tabletserver设计与实现
1. SDK的结构化文档（可折叠，可搜索，类是windows的帮助）
1. 性能数据，不同场景下的测试数，瓶颈分析
1. 性能方面设计折衷（怎样实现的高性能随机读写）
1. 数据模型的详细介绍
  * cf的使用建议
  * 各种删除的语义
  * 多版本保留机制
1. 监控系统的安装和使用
1. 快速、低成本Split&Merge的实现原理
