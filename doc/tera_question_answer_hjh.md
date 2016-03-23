tera串讲时被问到的问题，及我整理的答案
======
by 黄俊辉

1. master的内存和meta不一致怎么办
 - master对meta的操作都是先更新自己本地内存，然后meta上的数据；
 - 如果master更新了自己本地内存的数据，但是写meta表失败，则master会重试；
 - 超过重试次数后，则设置失败，并返回给客户端对应的错误码；
 - 对于分裂，TS是直接把两个新tablet的信息更新到meta表上，而没有经过master；
 - 如果TS返回分裂成功，则master扫描该tablet在meta的信息，确认分裂是否真的成功；

2. merge之后master宕机怎么保证meta状态正确
 - merge主要分为3个步骤:(1) unload两个旧的tablet；(2) 向meta表的ts发送删除两个旧tablet、新增1个新tablet的信息请求；(3) load新tablet；
 - master在步骤1或步骤2宕机，不会是meta状态不一致；对于步骤2，master向ts发送WriteTablet请求，ts内部是一个原子操作，保证都成功或都失败；

3. merge后leveldb load时路径参数是什么
 - 两个父tablet的id，根据这两个id可以生成父tablet的路径，进而知道父tablet下面的所有lg，即leveldb的数据，根据这些信息生成merge后tablet的每个lg的manifest文件；
 - 所以，两个tablet合并成一个新tablet，不是移动原db数据到新目录，而是在新db目录上修改manifest文件，指向真实的db数据的位置；

4. master在gc时怎么知道哪些数据可以删除
 - 遍历Table类的m\_tablets\_list成员，如果tablet的状态不是ready，则不对这个table作gc，否则把tablet放到存活的tablet\_list；
 - 扫描table的存储目录，获取table里所有占用磁盘的tablet；
 - 如果某tablet占用磁盘，但不在存活tablet\_list里，则认为该tablet已死状态；
 - 遍历已死的tablet，扫描数据目录，除sst外的其它文件都删除，m\_gc\_live\_files保存需要进一步确认是否可以删除的文件；
  - m\_gc\_live\_files是表名到GcFileSet的映射，GcFileSet是一个vector，成员的类型是set；
  - set的内容是uint64\_t的数字，最高位固定为1，接着31位是tablet的num，低32位是sst的num；
 - master通过query请求的响应，获得哪些仍然在用的tablet；
 - 根据前面的m\_gc\_live\_files-ts返回的live\_files，剩余的就是需要删除的sst文件；

5. client读写时如何寻找对应ts
 - client访问zk拿到加载meta表的ts地址；
 - client访问meta ts，得到待读写key所属的ts；
 - client访问ts，进行读写操作；

6. tera的读操作在leveldb中的流程
 - 设置rowkey=用户指定行key, cf="", qualifier="", type=TKT\_FORSEEK, ts=INT64\_MAX, 把这四元组序列化为leveldb的user\_key，seek到leveldb的行首开始读；其中TKT\_FORSEEK是所有操作类型中值最小的key，值为0；
 - 如果seek成功，则根据leveldb的迭代器遍历读出需要的数据，否则结束。

7. leveldb的compact流程
 - 见tera\_understanding.md文档

8. leveldb写吞吐瓶颈在哪里（影响写吞吐的因素）
 - 写日志文件，immutable memtable写sst文件，compact操作

9. ts挂掉一台后，上面的tablet多久能够恢复服务（ts挂掉的，tera内部的流程）
 - ts挂掉后，zk最长10（tera\_zk\_timeout）秒能检测到ts挂掉，然后通知master；
 - master等待60（tera\_master\_tabletnode\_timeout）秒，如果ts在60s内没有起来，则上面所有tablet将重新分配；
 - 当正在load的tablet大于或等于5（tera\_master\_max\_load\_concurrency）个时，剩下的tablet就要排队了，完成一个load，再开始load一个新的；
 
10. 热备的ts间同步的是什么数据
 - 目前tera没有做ts的热备；
 - 如果需要做热备的话，ts之间最好同步所有数据；

11. 表格操作的过程
 - master在自己本地内存检查表名是否存在；
 - master检查参数是否正确；
 - maste检查数据目录下是否存在同样表名的数据，如果存在则把老数据移到trash目录；
 - master把新表的表名，表的模式，tablet信息更新到本地内存；
 - master向加载meta表的ts发送更新meta的请求；
 - master等待响应，成功就返回给客户端，失败就重试，超过次数返回客户建表失败；
 - master选择ts加载该表的tablet；

12. 建表时写完meta就返回成功，是否有问题
 - tera目前的做法是：把表格的相关信息写到本地内存后，就更新meta表，写完meta表后就返回客户端；
 - 对于该表格的第一个tablet是由master异步调用ts加载的，这就有可能导致问题：建表成功后，用户立马写数据，有可能因为tablet还没成功加载导致写数据失败；
 - 为什么不是写完meta，并且load表格的第一个tablet后，才返回客户端，而是采用上面这种做法？建表是定义表格的行为，往表格读写数据是表格的可用性问题，表格可用性问题是tera比较常见的问题，如某ts挂掉后，上面的tablet需要重新分配给新ts，在成功加载前，tablet的读写就是不可用的；

13. load流程，特别是与dfs的交互
 - 遍历tablet下所有的lg；
 - 如果lg目录下不存在CURRENT和MANIFEST文件，则认为是新db，
 - 对于旧db，读取lg的CURRENT文件，获取当前的MANIFEST文件名；
  - 如果parent\_size为0，则获取当然目录的CURRENT和MANIFEST文件；
  - 如果parent\_size为1（合并），则获取父tablet的CURRENT和MANIFESST文件；
  - 如果parent\_size为2（分裂），则分别获取双亲tablet的CURRENT和MANIFEST文件；
 - 扫描MANIFEST文件（CURRENT指向的那个），检查sst文件的key范围是否属于自己的tablet，如果不是，则跳过；如果sst文件的内容只有部分是属于自己tablet的，则修改smallest和或largest的值，更新FileMetaData的信息；
 - 获取所有lg最小的log\_sequence，从log\_sequence+1开始从log文件恢复数据；
 - 更新MANIFEST文件；
  
14. bloomfilter加载到内存是在什么时刻进行的，数据特别多时内存不够用怎么办
 - 打开sst文件时加载对应的bloomfilter；
 - bloomfilter是table cache是一个成员，table cache是通过LRU管理内存；

15. tera在存储介质上的优化有哪些
 - tera基于lg实现类似多级cache的功能，对于数据量比较小，且实时性要求比较高的列，可以放到内存中；
 - tera基于lg实现按列存储，提高访问的效率；
 - tera是批量写hdfs，然后sync的，提高写的吞吐量；

16. 写入的数据什么时候落盘，每次写都sync有什么问题
 - 数据先写到本地消息队列，由一个线程批量写数据到磁盘上；
 - 数据写到日志文件后就执行sync；
 - 返回客户端写数据成功与否；
 - 每次执行sync操作，对磁盘压力比较大，会导致写性能低，为保证数据的可靠性只能这样；

17. sdk如何更新meta，对zk的压力如何解决
 - sdk从zk拿到meta的地址后，就在本地缓存，后面直接使用缓存的meta地址；
 - 当读写数据返回kKeyNotInRange时，sdk重新从zk获取meta地址；

18. 负载均衡是如何实现的
 - 当某ts的负载是数据量最小ts的1.2倍时，就会触发负载均衡策略；
 - 通过迁移数据量比较大的tablet到另一个ts实现负载均衡的上的；
  - 计算数据量比较大的tablet的方法：分别计算源ts和目的ts上该table的数据量，ideal\_move\_size = (src\_node\_size - dst\_node\_size) / 2，找出tablet集合中小于该值的最大tablet，作为迁移的目标；

19. 一致性如何保证，即如何保证一个区间只由一个ts提供服务（例如load超时）
 - master逻辑保证一个tablet只能由一个ts提供服务；
 - master选定一个ts后，要求其load某个tablet，如果load超时，master就是会重试让ts load这个tablet，超过重试次数后，就unload该tablet，unload失败，也会重试unload，超过unload次数后，就把这个ts踢出服务；
 - ts发现自己被踢出服务后，进程主动退出；
 - ts退出后，zk最长10秒就发现ts退出，因master监听了ts节点变化的事件，zk将调用master的RefreshTabletNodeList函数；
 - master延时60秒（tera\_master\_tabletnode\_timeout）把已经退出的ts上所有tablet重新分配，如果ts在60秒内重新注册上来，原来的在这台ts加载的tablet，master还会要求其加载；
 - 通过上面机制，master保证了只有tablet没有被ts服务后，才会重新把这个tablet分配给新ts；

20. 如何踢掉ts
 - ts监听自己在zk节点的变化事件；
 - master把ts在zk的节点移动到kick目录；
 - ts发现自己被移到kick了，就主动退出服务；
 - ts先打印一行日志，然后以FATAL错误的方式退出，所以不存在因某种原因卡住而没有及时退出的问题。

21. locality group的作用
 - 首先，不同的lg可以指定不同的存储介质，所以可以根据实时性要求，把一个表的某些cf放到内存，某些cf放到ssd，某些cf放到磁盘；
 - 第二，lg实现了tera按列存储，所以当查询只要表格某几列里，不需要扫描表格的所有列，只要访问这几列所在的lg就行，提高读取性能；
 - 第三，以lg为单位，设置不同的压缩算法，有利于提高存储效率；

22. ts的内存主要消耗在哪里
 - table cache，当sst里包含的记录很多时，占内存比较多；
 - block cache，缓存了已经读过的数据；
 - memtable和immutable memtable

23. ts一般有多少memtable
 - 一个lg对应一个leveldb的对象，一个leveldb就有一个memtable；

24. master启动后会做什么
 - 分配还没分配的tablet；
 - 启动loadbalance、gc、query的定时任务；

25. bigtable模型到leveldb的映射
 - 见tera\_understanding.md文档

26. tera对leveldb的改造（split、merge、LG、切log、GC）
 - split，merge是原leveldb不支持的功能，tera新加的；
 - LG对应一个leveldb的对象，一个tablet有一个或多个LG；
 - 不是一个leveldb对应一个log，而一个tablet里的多个leveldb对应一个log文件；
 - compact后，原生leveldb是删除旧数据的，但是tera里不能删除的，因为存在split和merge的操作；

27. 一致性保证（master的mem与meta；ts与tablet）
 - master更新完mem后，就要要求保证meta的更新成功；
 - 一个tablet只能由一个ts服务，所以写是只有唯一的入口；

28. 性能相关（读写瓶颈、io/cpu/mem等资源细节、zk和hdfs压力、各类cache等等）
 - 见前面的8和22问；
 - sdk通过缓存meta地址，缓解zk的压力；
 - 批量写，然后sync，缓存hdfs的压力；

29. Snapshot、行级事务
 - snapshot是tera系统在某一时刻的数据状态，用于数据恢复；
 - sdk向master发送创建某个表格的快照的请求，master向所有加载该表格的ts发送创建快照请求；
 - 目前tera只支持行内的事务，不支持跨行事务；
 - db\_table保存了last sequence，读只能读到该sequence前的数据，所以只有当某行所有列都写成功了，才更新last sequence；
 - 这样就避免读数据读到有些列是新数据，有些列是旧数据；
 
