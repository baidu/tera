#单行事务设计
Copyright 2016, Baidu, Inc.

##背景
* Tera的RowMutation接口提供了单行写入的原子语义，但对于read-modify-write操作没有原子性保证
* Tera的原子操作接口（特别是对整型数据）覆盖了一些简单的read-modify-write需求，如AtomicAdd、AtomicAppend、PutIfAbsent等，但用户更复杂的、个性化的modify逻辑仍然需要将数据读到客户端有用户程序进行处理
* 没有单行事务，通用的事务功能可能无从谈起，例如Google的触发式计算框架Percolator中实现的多行事务功能就是在Bigtable单行事务的基础上构建的

##功能
* 提供单行的read-modify-write原子语义
* 两个事务修改同一行，但读操作所涉及到的CF或者Colum无任何交集时，两个事务不会冲突
* 能够避免“幻影读”现象  
  *详见事务用户手册*

##约束
* 不支持多版本语义  
  *详见事务用户手册*

##设计和实现
* read-modify-write原子性的保证  
  * 在事务过程中，SDK将记录所有读操作所涉及到的CF以及Column的范围（简称ReadRange），以及该范围内**所有数据及删除标记（PUT&DEL）**的最大时间戳（简称ReadMaxTS）
  * 在事务过程中，SDK将所有写操作的更新数据保存到本地内存中（简称WriteBuffer）
  * 在用户发起事务提交时，SDK将WriteBuffer、ReadRange和ReadMaxTS一并发送至Tabletserver
  * Tabletserver收到SDK的事务提交请求后，校验ReadRange所覆盖的**当前所有数据及删除标记**的最大时间戳（简称CurrentMaxTS）是否等于ReadMaxTS，如果等于，则校验成功，将更新数据写入表格，并返回SDK提交成功，否则返回提交失败
* 时间戳校验  
  假设某个事务提交时，ReadRange = {CF1, CF2:Column1~CF2:Column5, CF3:Column2}，表示本次事务读取了CF1的全部数据，CF2的[Column1, Column5)的数据，CF3的Column2的数据，Tabletserver将分别采用三种校验方式：
  * 对于整个CF的时间戳校验  
  扫描整个CF内的所有Column，得到最大时间戳，与ReadMaxTS进行比较。  
  因为CF下的Column数量可以非常多，如果每个column分别进行校验，可能产生大量的扫描操作（要依次读取这些column），为了避免这种情况，可以为每个CF维护一个特殊的Cell：CFMaxTS，代表CF里所有Cell（包含Delete）的最大时间戳。校验时将ReadMaxTS与此CFMaxTS进行比较。（这种方式作为未来优化的备选方案，暂不实现。）
  * 对于一段Column范围的时间戳校验  
  通过扫描这个Column段内的数据，得到最大时间戳，与ReadMaxTS进行比较。
  * 对于单个Column的时间戳校验  
  读此Column的最新版本，用其时间戳与ReadMaxTS进行比较。
* CFMaxTS的存储（暂不实现）  
  CFMaxTS有两种存储方式，一种是作为一个单独的、用户不可见的LG存储，与用户的LG隔离，另一种是放在每个CF的开头，与用户数据混合存储：
  * 单独存储  
  好处是用户的读操作不会受CFMaxTS标记的影响，读吞吐不会下降，缺点是进行时间戳校验时，CFMaxTS标记有可能在磁盘上，需要发起IO操作才能读到，并且是一次磁盘seek，影响写吞吐
  ```
             -------------------------------------------------------------------------
             |  ROW   |         LG1         |     LG2     |   MAX_TS_LG (insivible)  |
             |        |     CF1     |  CF2  |     CF3     |  CF1   |   CF2  |   CF3  |
             |        |  C1  |  C2  |  C1   |  C1  |  C2  | MAX_TS | MAX_TS | MAX_TS |
             -------------------------------------------------------------------------
  ```
  * 混合存储
  与单独存储方案刚好相反，读操作会在每个Column内多读出一个cell，最坏情况下会使cell数量增加一倍（当每个CF内只有一个Column，并且每个Column内只有一个Cell时），但好处是事务提交时，它有可能仍在BlockCache或文件系统CacheBuffer中，能够减少一次磁盘Seek
  ```
             -------------------------------------------------------------------------------
             |  ROW   |                       LG1                 |           LG2          |
             |        |             CF1        |       CF2        |           CF3          |
             |        |  MAX_TS  |  C1  |  C2  |  MAX_TS   |  C1  |  MAX_TS  |  C1  |  C2  |
             -------------------------------------------------------------------------------
  ```
* CFMaxTS的更新和清除（暂不实现）  
  当CF被修改时，不论是Put还是Delete，都需要一并更新CFMaxTS，使其记录本次修改的时间戳。  
  当CF的全部数据都被删除时，CFMaxTS也应该被清除，实现方法是在Compact的Drop判断逻辑中遇到CFMaxTS时，检查后面一个**不被Drop**的Cell是否属于同一个CF，如果是，则保留CFMaxTS，否则清除CFMaxTS。
* 事务内多次读操作的一致性视图保证  
  * 事务内第一次读操作，Tabletserver将读leveldb时所使用的LastSequence（简称ReadSeqNum）一并返回给SDK
  * 之后的读操作，SDK要求Tabletserver使用此ReadSeqNum作为leveldb的快照进行读取
  * 在事务最大超时时间(简称TxnTimeout）内，Tabletserver保证在此期间写入的所有Cell不会被Compact清除
* Compact垃圾清理算法的修改  
  在原有GC算法的基础上，只需要在某个Cell需要被Drop时，检查其时间戳距当前时间是否在TxnTimeout之内，是的话予以保留
* 读操作逻辑的修改  
  在原有GC算法的基础上，维护每个tablet的最大参与Compact的sequence_number（简称MaxCompactSeqNum），在读用户数据之前，检查ReadSeqNum是否大于等于MaxCompactSeqNum，是的话，进行读操作，否则，认为事务超时。
* WriteBuffer的实现
  * WriteBuffer由两个数据结构组成，一个是修改队列，即vector<Mutation>，用于向Tabletserver提交修改，另一个是可索引的两级map，即map<CF, map<Column, Value/Delete>>。  
  * 对于Put操作，插入修改队列的末尾，同时修改两级map
  * 对于Get操作，如果要读的数据已经读过（被ReadRange覆盖），直接读取两级map，或者从Tabletserver读取，将结果添加到两级map中，新读取的数据要被已有数据覆盖
  
