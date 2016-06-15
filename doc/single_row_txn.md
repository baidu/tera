#单行事务设计
Copyright 2016, Baidu, Inc.

##背景
* Tera的RowMutation接口提供了单行写入的原子语义，但对于read-modify-write操作没有原子性保证
* Tera的原子操作接口（特别是对整型数据）覆盖了一些简单的read-modify-write需求，如AtomicAdd、AtomicAppend、PutIfAbsent等，但用户更复杂的、个性化的modify逻辑仍然需要将数据读到客户端有用户程序进行处理
* 没有单行事务，通用的事务功能可能无从谈起，例如Google的触发式计算框架Percolator中实现的多行事务功能就是在Bigtable单行事务的基础上构建的

##功能
* 提供单行的read-modify-write原子语义  
  下面的例子中，Session B在事务进行过程中，value被Session A所修改，因此B的提交将失败，否则将出现A的更新被覆盖的问题：
```
           -------------------
           |  Wang  |  100   |
           -------------------
             Session A              Session B
time
|                                   Begin
|          Begin
|          value=Get(Wang);
|          100
|                                   value=Get(Wang);
|                                   100
|
v          value+=20;
           120
           Put(Wang, value);
                                    value-=20;
                                    80
                                    Put(Wang, value);
           Commit
           success
                                    Commit
                                    fail
```

* 两个事务修改同一行，但读操作所涉及到的CF或者Colum无任何交集时，两个事务不会冲突  
  下面的例子中，Session A和Session B分别对同一行的Cf1和Cf2进行read-modify-write操作，后提交的事务不会因先提交的事务而失败：
```
           ---------------------------
           |  ROW   |  CF1   |  CF2  |
           ---------------------------
           |  Wang  |  100   |  100  |
           ---------------------------
             Session A              Session B
time
|                                   Begin
|          Begin
|          value=Get(Wang, CF1);
|          100
|                                   value=Get(Wang, CF2);
|                                   100
|
v          value+=20;
           120
           Put(Wang, CF1, value);
                                    value-=20;
                                    80
                                    Put(Wang, CF2, value);
           Commit
           success
                                    Commit
                                    success
```
* 能够避免“幻影读”现象  
  假设一个事务对多个CF或者CF内的一个Column范围进行了read-modify-write操作，在提交前，另一个事务在这些CF或者Column区间内新增或删除了一个Column，前者在提交时将会失败。

##约束
* 多版本语义的约束
  * 写操作的时间戳不能由用户指定，而是由Tera分配，Tera保证一行内的修改时间戳单调递增，也就是说，只支持以下四种操作：
    * 增加最新版本
    * 删除整行
    * 删除整个CF
    * 删除整个Column
  * 读操作的时间戳不能由用户指定，只能读到第一次Get时快照视图上每个Column的最新版本
  * 数据的历史版本只能由非事务操作修改，历史版本不能参与到事务过程中

##设计和实现
* read-modify-write原子性的保证  
  * 在事务过程中，SDK将记录所有读操作所涉及到的CF以及Column的范围（简称ReadRange），以及所读到的所有数据的最大时间戳（简称ReadMaxTS）
  * 在事务过程中，SDK将所有写操作的更新数据保存到本地内存中（简称WriteBuffer）
  * 在用户发起事务提交时，SDK将WriteBuffer、ReadRange和ReadMaxTS一并发送至Tabletserver
  * Tabletserver收到SDK的事务提交请求后，校验ReadRange所覆盖的**当前数据**的最大时间戳（简称CurrentMaxTS）是否等于ReadMaxTS，如果等于，则校验成功，将更新数据写入表格，并返回SDK提交成功，否则返回提交失败
* 时间戳校验  
  假设某个事务提交时，ReadRange = {CF1, CF2:Column1~CF2:Column5, CF3:Column2}，表示本次事务读取了CF1的全部数据，CF2的[Column1, Column5)的数据，CF3的Column2的数据，Tabletserver将分别采用三种校验方式：
  * 对于整个CF的时间戳校验  
  因为CF下的Column数量可以非常多，如果每个column分别进行校验，可能产生大量的扫描操作（要依次读取这些column），为了避免这种情况，为每个CF维护一个特殊的Cell：CFMaxTS，代表CF里所有Cell（包含Delete）的最大时间戳。校验时将ReadMaxTS与此CFMaxTS进行比较。
  * 对于一段Column范围的时间戳校验  
  通过扫描这个Column段内的数据，得到最大时间戳，ReadMaxTS进行比较。
  * 对于单个Column的时间戳校验  
  读此Column的最新版本，用其时间戳与ReadMaxTS进行比较。
* CFMaxTS的存储  
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
  两种方式都不会影响LG的稀疏程度，which is better？
* CFMaxTS的更新和清除  
  当CF被修改时，不论是Put还是Delete，都需要一并更新CFMaxTS，使其记录本次修改的时间戳。  
  当CF的全部数据都被删除时，CFMaxTS也应该被清除，实现方法是在Compact的Drop判断逻辑中遇到CFMaxTS时，检查后面一个**不被Drop**的Cell是否属于同一个CF，如果是，则保留CFMaxTS，否则清除CFMaxTS。
* 事务内多次读操作的一致性视图保证  
  * 事务内第一次读操作，Tabletserver将读leveldb时所使用的LastSequence（简称ReadSeqNum）一并返回给SDK
  * 之后的读操作，SDK要求Tabletserver使用此ReadSeqNum作为leveldb的快照进行读取
  * 在事务最大超时时间(简称TxnTimeout）内，Tabletserver保证在此期间写入的所有Cell不会被Compact清除
* Compact垃圾清理算法的修改  
  在原有GC算法的基础上，只需要在某个Cell需要被Drop时，检查其时间戳距当前时间是否在TxnTimeout之内，是的话予以保留
* 读操作逻辑的修改  
  在原有GC算法的基础上，维护一个最大被Drop的sequence_number（简称MaxDropSeq），在读用户数据之前，检查ReadSeqNum是否大于DropSeq，是的话，进行读操作，否则，认为事务超时。
* WriteBuffer的实现
  * WriteBuffer由两个数据结构组成，一个是修改队列，即vector<Mutation>，用于向Tabletserver提交修改，另一个是可索引的两级map，即map<CF, map<Column, Value/Delete>>。  
  * 对于Put操作，插入修改队列的末尾，同时修改两级map
  * 对于Get操作，如果要读的数据已经读过（被ReadRange覆盖），直接读取两级map，或者从Tabletserver读取，将结果添加到两级map中，新读取的数据要被已有数据覆盖
  
