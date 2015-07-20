#Tera新人学习指导

怎么学习tera？  
我这里给出个建议的范围，欢迎大家补充、修改。

1. 明确Tera产生的背景与设计目标:  
	高性能的结构化数据存储，用来存储搜索引擎万亿量级的超链和网页数据。
1. 理解Tera的数据模型:  
	Tera数据模型主要参考bigtable设计，可以通过阅读[bigtable论文](http://static.googleusercontent.com/media/research.google.com/zh-CN//archive/bigtable-osdi06.pdf), 重点是理解其中的数据模型和Localitygroup的设计。
1. 了解tera架构和各模块的功能定位  
	这个通过阅读tera设计文档实现，只需要大致了解包含哪些模块，每个模块的职责是什么。
1. tera都实现了哪些特性，这些特性都是怎么实现的  
	自动分片、自动负载均衡、多版本、稀疏表、强一致、行级事务等等
1. tera的表格是怎么实现的  
	怎样将列族、qualifier、时间戳这些映射到底层kv存储中的
1. 学习底层kv存储Leveldb的实现  
	阅读leveldb相关资料，了解leveldb本身的特点，怎样实现读和写的均衡、怎样将随机写转化为顺序写的，同时了解tera中leveldb的分裂和合并是怎么实现。
1. tera都依赖哪些系统，借助这些系统实现了什么功能  
	可以查看Makefile里的编译依赖，看tera都依赖了哪些东西
1. 表格管理流程  
	创建、删除、更新流程，这部分需要自己学习代码
1. 数据读写流程  
	数据读写依赖哪些模块，SDK的meta表是怎么更新和缓存的
1. tablet分裂流程  
	tablet分裂过程是怎么产生垃圾的，垃圾收集是怎么做的，分裂失败是怎么回滚的
1. tablet合并流程  
  * Master在什么情况下会调度tablet的分裂、合并及迁移
  * Localitygroup怎么实现一致性的
1. Tera的cache
	有哪些cache类型，内存表和flash表是怎么实现的
1. Tera的性能
	Tera的读写性能大概是多少，瓶颈点在哪

强烈建议将了解到的内容形成成文档，放在这供后来人学习。

学习完后可以尝试回答以下问题：  
1. Tera和HBase的异同，有哪些优势和不足  
2. 除了链接和网页存储，tera还适用于哪些场景  
