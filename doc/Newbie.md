#Tera新人学习指导

学习Tera遵循以下顺序

1. 阅读[bigtable论文](http://static.googleusercontent.com/media/research.google.com/zh-CN//archive/bigtable-osdi06.pdf), 了解bigtable数据模型，参考
2. 阅读tera设计文档, 了解tera设计的初衷和各模块功能定位
3. 阅读leveldb相关资料，了解leveldb本身的特点
4. 阅读源码, 了解
  * 表格创建流程
  * 数据读写流程
  * tablet分裂过程
  * tablet合并过程
  * Master在什么情况下会调度tablet的分裂和合并
  * Localitygroup怎么实现一致性的
  * tera都依赖哪些系统，借助这些系统实现了什么功能。
  * Tera和HBase的异同，有哪些优势和不足

Tera当前文档奇缺，所以很多学习都要通过代码实现，新人学习要产出学习文档，Review后会放在这供大家学习。
