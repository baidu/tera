##Tera分布式存储系统介绍

###1 Tera及其特性
Tera是一个实时的，自动负载均衡的，可伸缩的高性能分布式存储系统，用来管理搜索引擎万亿量级的超链与网页信息。
####Tera的主要特性
**全局有序：** 整个表格按主键有序，可以高效访问一个小的区间（如获取某个站点的全部链接，只需要和少数节点通信）。
**自动负载均衡：**系统自动处理局部数据分布不均，对数据量大的热点区域自动分割，将负载到转移到更多机器。（数据分布不均、数据热点区域）
**按列存储：**表格按照行排列，但可以设置不同的列分别存储（可以在不同的物理介质上），比如调度相关属性列全内存存储（linkcache），链接短属性flash存储（linkbase相关），而长属性可以存储在硬盘上（网页库），提高数据访问效率。

**多版本存储： **对于部分属性，可以指定存储一定时间内、一定数量的历史版本，用于更新对比和历史问题追查。（snapshot）
其他的特性还有：*数据的强一致性*，*动态schema*，*自动垃圾回收*等.
###2 Tera的数据模型和架构
####2.1 数据模型
首先来看一下传统的关系型数据库的模型，也就是**_ER_** 模型。 
 ![webtable_er](https://github.com/yoyzhou/tera/blob/master/resources/images/webtable_er.png)
体现在数据库中就是如下的数据表以及建立在列上的索引。

|...    |   m_url   |   sign    |   anchor  |   weight      |   density|
|:------:|:------:|:------:|-------------|:------:|:------:|
|Row1|  com.sina.www|   4677879969| 新浪首页|   30| 19|
|Row2|  com.jd.www| 8736463483| 中国最大的…| 31| 98|

Tera的数据模型和传统的关系型数据库有可比性，但是又有很大的不同；由于Tera底层采用leveldb作为数据操纵层，所以数据模型是一个KV（key-value）模型，Tera里面表是由KV对组成的。Tera中的KV数据模型如下所示：
> (row:string, column:string,time:uint64)->string

这里：
*row*是用户的key，相当于关系型数据库中的主键，如上表1中的m_url，com.sina.www和com.jd.www，可以是任意的字符串；
*column*是列名称，在tera中由column family和qualifier构成（column family:qualifier）。qualifier就是具体的列，如表1中的content和anchor。column family是列簇，简称CF，通常一个列簇是由一组业务相关、数据类型相近的列组成，一个列簇可包含任意多的列（qualifier），并且列是可以动态增加的；
*time*是时间戳，tara可以存储数据的多版本信息，可以设定数据的存活时间。
数据模型里面还有一个概念是locality group，局部性群组（LG）。不同的列簇可以属于不同的局部性群组，局部性群组是数据物理存储介质类型的最小逻辑单位，不同的LG可以存储在不同的物理介质上。 LG对table进行纵向的分割，使列能够按照不同的用途存储在不同介质上。
下面是一个具体的数据模型的例子。
 
 ![webtable_datamodel](https://github.com/yoyzhou/tera/blob/master/resources/images/webtable_datamodel.png)

如上图中，如果sign和density属于列簇cf1，anchor和weight属于列簇cf2，那么图中com.sina.www的density列t7时刻的kv模型实现就是:（com.sina.www cf1: density t7）-> “0:0 1:0”。
可以相比较前面的关系数据库，Tera里面的key相当于主键，但不是单纯的一列作为主键，而是一个复合主键，包含了关系数据库里面讲的主键row之外，还有column列和时间戳也包含了，这就导致了进行查询的时候要进行key的解析的过程。

####2.2 Tera的总体架构
 
 ![tara_arch](https://github.com/yoyzhou/tera/blob/master/resources/images/tara_arch.png)

Tera包含三个功能模块和一个监控子系统：
* Tabletserver 是核心服务器，负责Tablets管理和提供数据读写服务，是系统的数据节点，承载几乎所有客户端访问压力；
* Master，是系统的仲裁者，负责表格的管理、schema更新与负载均衡，用户只有在创建表格和修改表格属性时才访问Master，所以负载较低，Master还会监控所有TabletServer的状态，发起Tablet拆分合并操作和负载均衡操作；
* Client，封装系统的各项操作，以SDK和命令行工具的形式提供给用户使用，管理员也可以通过client对集群行为进行人工干预，如强制负载均衡、垃圾收集和创建快照等；
* 监控子系统，负责整个表格系统的状态监控、状态展示，同时提供Web形式的管理接口，可以实现机器上下线、数据冷备等操作，方便运维。

###3 Tera功能模块
上面一节简要介绍了Tera包含的三个功能模块和监控子系统，这一节详细介绍三个功能模块的具体实现。
####3.1 Master
Master最重要的功能就是表格管理、tabletnode管理和负载均衡管理这三个方面。
 
 ![tera_master_imp](https://github.com/yoyzhou/tera/blob/master/resources/images/tera_master_impl.png)

#####3.1.1 表格管理
上面我们已经介绍了Tera的数据模型，也就是tera中Table的逻辑模型，也讲到tera保证数据按照主键（key）全局有序。下面介绍tera中table是如何管理的。
Tera将数据表横向划分为若干个有序的区间，每一个区间就是一个tablet，是数据分布和负载均衡的最小单位。Master根据每个数据节点的负载状况，将Tablet安排到各个TabletServer上，每个TabletServer管理着若干个Tablet，提供这些Tablet的读写服务，负责将数据持久化到DFS上。区间内的有序性由tablet自身保证，tablet之间的有序性通过master来维护。
 
 一个表格包含多少个tablet，每个tablet被安排到哪个数据节点上，这些属于系统的meta信息，meta信息存储在一个叫meta\_table的表里，meta表和普通表一样，也可以包含多个tablet，所有数据的写入也都会落地到hdfs上。meta\_table地址保存在zookeeper上，在系统启动时master先从zookeeper上找到meta\_table，并调度加载，meta\_table内记录了tablet的地址等相关信息，master读取这些信息后就可以完成整个meta表的加载，从而获得了系统中所有表格及其tablets的分布信息。
 
#####3.1.2 Master的具体实现
Master实现的代码在master\_impl模块中，master\_impl通过tablet\_manager、tabletnode\_manager和workload\_scheduler进行表格管理、tablet节点管理和负载均衡管理等工作。
**_tablet\_manager_**实现表格信息的管理，拥有一个all\_table\_list成员，是一个table name到Table指针的hash表；每一个Table拥有一个m\_tablets\_list数据成员，是一个start\_key到Tablet类指针的hash表，而一个Tablet类包含了Tablet的meta信息，包括tablet的起始key，结束key，TS地址，tablet的状态、大小、是否压缩等详细信息。
通过这样的层次结构，tablet\_manager就拥有了从table名称到该table的所有tablet信息的映射关系。简单的说all\_table\_list就是meta表在tablet\_manager内存中的数据结构，所有对内存中all\_table\_list的操作都会同步更新到meta表中。
tablet\_manager模块实现了如下功能：装载meta table信息，dump meta table到本地，添加、删除、查找table和tablet，合并tablet，分裂tablet等。
**_tabletnode\_manager_**实现tablet节点的管理，拥有一个m\_tabletnode\_list的数据成员，m\_tabletnode\_list是一个server address到tabletnode的hash表，TabletNode类管理tabletnode的信息和相应的操作，TabletNode包含一个tabletnode的状态，数据大小，地址，uuid等。
tabletnode\_manager类实现了如下功能：添加、删除、更新tabletnode，获得所有tabletnode的信息等。
**_master\_impl_**通过LoadBalanceTimer定期（10s）对tablet信息进行监控，对满足条件（tablet的data_size大于512M）的tablet进行split;
tablet\_manager通过MergeTabletTimer定期（180s）对teblet信息进行扫描，对满足条件的tablets进行合并。

####3.2 Tablet Server
Tablet server是tera的核心服务器，负责tablets管理和提供数据读写服务。Tablet Server最重要的功能就是通过TabletIO提供数据读写服务。

 
 ![tera_ts_impl](https://github.com/yoyzhou/tera/blob/master/resources/images/tera_ts_impl.png)
tablet server在架构实现上和master是差不多的，他们都是TeraEntry，但是角色不同，master负责表格信息管理和负载均衡的角色，而tabletnode充当的是提供数据读写服务的功能，几乎所有的数据读写服务都是通过tabletnode提供的。
#####3.2.1 tablet_manager实现
Tablet Server通过tablet\_manager管理TabletIO，tablet\_manager拥有m\_tablet\_list成员变量，m\_tablet\_list是TabletRange（tablename，startkey，endkey的三元组）对象到TabletIO的hash表，每一个TabletIO就是一个leveldb实例。 
tablet_manager提供如下的方法：
* AddTablet 增加一个tablet
* RemoveTablet 删除一个tablet
* GetTable 通过table name和key获得一个TabletIO指针
* GetAllTabletMeta 获得所有Tablets的元信息
* GetAllTablets 获得TS中所有TabletIO指针
* RemoveAllTablets 移除所有的Tablets
* Size 获得TS中Tablet的数量

#####3.2.2 tabletnode_impl实现的主要方法
* Init() 初始化tablenode实例，向ZK注册TS节点和端口
* InitCacheSystem() 初始化TS的leveldb cache机制
* Exit() 退出实例，关闭所有的tablet
* Register 向master 注册tabletnode信息（好像没有找到实现）
* Report 向master汇报tabletnode统计信息
* LoadTablet 接收master的loadtablet request，进行请求有效性的检查，通过tablet\_manager添加该tablet，添加成功之后，通过tabletIO load 指定的tablet，tabletIO load失败，tablet_manager删除该tablet，返回装载tablet成功。
* UnloadTablet 卸载tablet
* CompactTablet 紧缩tablet
* ReadTablet 读tablet数据
* WriteTablet 写入数据
* GetSnapshot 获取快照
* ReleaseSnapshot 释放快照
* Query 响应master的query请求，返回tabletnode的相关信息（监控、meta信息）
* ScanTablet对Tablet进行数据扫描，返回扫描结果
* SplitTablet 对Tablet进行快速分裂
* MergeTablet 合并两个Tablet

Tabletnode除了提供数据读写服务外，还定期向master汇报tabletnode本身的系统信息，包括ts的硬件信息（内存占用，网卡信息，cpu等）和tablet的统计信息（所有tablet的数据大小，tablet读写次数、读和scan任务pending的数量等等）
#####3.2.3 TabletIO
TabletIO是tera中最接近leveldb的模块，是tera对leveldb的上层封装，提供tablet装载、卸载、split、compact、merge等接口和数据读写scan服务，同时通过StatCounter记录tablet操作的统计信息。
* Load 装载指定的tablet
TabletIO拥有一个m\_db的Leveldb::DB指针，在load tablet的时候设置相关leveldb参数后打开指定路径下的leveldb，leveldb::DB::Open(m\_ldb\_options, m\_tablet\_path, &m_db)，如果打开失败，会尝试进行repair修复打开
* Unload 卸载tablet
等待所有的写结束后，关闭leveldb实例，关闭tablet的写进程
* Split 分裂tablet
Tera采用快速分类的方式，在tabletIO层面split只是通过leveldb找到一个分裂的key，将状态设置为kTableonSplit
* Compact 执行major compact
* CompactMinor 执行minor compact
* Read 根据key读取数据
* LowLevelScan 根据start\_key、end\_key和filter进行扫描
* ReadCells 调用LowLevelScan获得多行数据

####3.3 SDK/Client
Tera SDK/Client封装系统的各项操作，以SDK和命令行工具的形式提供给用户使用，管理员也可以通过client对集群行为进行人工干预，如强制负载均衡、垃圾收集和创建快照等。
一般来说数据库系统有DDL、DML和DCL三种操作，目前Tera支持的有数据定义语言（DDL），包括创建表、删除表、修改表schema、创建快照等和数据操作语言（DML），包括了读GET、写（PUT、ADD、PutIfAbsent）、扫描（SCAN）等。

###4 附录

####4.1 Tera的分裂过程
***master\_impl**通过LoadBalanceTimer定期（默认10s）对tablet信息进行监控，对满足条件（tablet的data_size大于512M）的tablet进行split;
tablet_manager通过MergeTabletTimer定期（默认180s）对teblet信息进行扫描，对满足条件的tablets进行合并。

Tera在两种情况下进行Tablet分裂，一种是Master定期执行负载均衡过程中对符合条件的Tablet进行分裂，另外一种情况是管理员通过Tera Client强制执行分裂。下面从Master负载均衡的过程介绍Tablet的分裂过程。
* 1 通过LoadBalanceTimer定期（每10s）执行LoadBalance
* 2 Master通过tablet_manager获得所有table和tablet的信息和通过tablenode\_manager获得所有tabletnode（ts）的信息
* 3 如果只是对特定table进行loadbalance（默认是），如果对所有table进行均衡，调转到7
* 4 获得table下面的所有tablet，创建tablet server到tablet列表的hash表
* 5 根据table对每一个tabletnode按照负载降序排列，这样负载大的优先进行负载均衡
* 6 对每一个tabletnode和table对应的tablet调用TabletNodeLoadBalance进行负载均衡
* 7 对所有tablet不分table调用TabletNodeLoadBalance进行负载均衡

**_TabletNodeLoadBalance_**
* 1 对于tabletlist中的每一个tablet判断tablet的大小是否比设定的splitsize要大，如果要大并且tablet不在进行compact，执行TrySplitTablet
* 2 如果存在split的tablet或者设置master不能move tablet返回，否则
* 3 将size最小的tablet移动到负载最小的tabletnode上面

**_TabletNodeImpl::TrySplitTablet_**
* 1 获得tablet所在的server address
* 2 通过tablenode_manager查看server是否在服务
* 3 tabletnode_manager TrySplit,将tabletnode的datasize减小 tablet的size，查看等待split的tablet队列大小是否小于设定的最大同步split限制，如果超过了放到等待split队列里面，
* 4 设置tablet的状态为kTableOnSplit
* 5 执行SplitTabletAsync(tablet)

**_MasterImpl::SplitTabletAsync_**
* 1 向tabletnode server发送SplitTabletRequest请求SplitTablet
* 2 在回调函数中根据tablet的startkey和endkey查询meta表信息
* 3 如果返回记录数大于2或者等于0个表明split不成功，修复meta表信息，
* 4 如果返回是1,表示split还没有完成，重新load tablet
* 5 返回的结果是2个记录，说明split成功了，tablet_manager添加第二个tablet信息，删除第原来的tablet，添加第一个tablet信息，tabletnode装载第一个分裂后的tablet，装载第二个分裂后的tablet。

**_TabletNodeImpl::SplitTablet_**
* 1 根据request中的tablename和startkey、endkey，获得TabletIO指针
* 2 tablet_io找到分裂的splitkey
* 3 tablet_io unload
* 4 tablet_manager remove 相应的tablet
* 5 更新meta表信息UpdateMetaTableAsync
* 6 返回MasterImpl::SplitTabletAsync


####4.2 Leveldb的读写和Compaction
#####Leveldb的写
Commitlog -> memtable(skiplist) 根据用户提供的key comparator保持有序，将key插入到相应的位置
当memtable的大小超过阀值，memtable变成immemtable，同时生成新的memtable和log文件，因为leveldb的写只是写log和一次memtable的内存写，所以写入非常快。
#####Leveldb的读
按照文件的新鲜度逐层查找
Memtable->immemtable->level0（有重叠）-> 其他level
Leveldb的compaction
Minor compact
Immemtable dump成sst，形成level0的一个文件
Major compaction
当各个level的文件数目（level0）或者文件大小（大于level0）超过一定阀值时，触发major compact。对level0 来说，比较特殊，选文件的时候需要把重叠的文件都选上，和level1的文件进行多路合并。

###4 参考资料
[Bigtable: A Distributed Storage System for Structured Data](http://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf)

[LevelDb日知录 - LevelDb实现原理](http://www.cnblogs.com/haippy/archive/2011/12/04/2276064.html)
