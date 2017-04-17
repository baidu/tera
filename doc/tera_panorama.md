#Tera串讲#
##1.Tera的背景，概念和特性##
当前公司采用的链接存储系统，可以实现数据快速读，但无法实现数据的随机写，而且链接属性的合并需要采用hadoop mapreduce任务进行处理，当前dlb-saver和dlb-select采用的MAPRED批量处理模式链接合并入库到链接选取需要天级处理时间，不能保证价值资源的快速发现和收录。
Tera的实现目标就是提供一个实时的，自动负载均衡的，可伸缩的高性能分布式存储系统，用来管理搜索引擎万亿量级的超链与网页信息。Tera突出的特点是实时读写和自动负载均衡。
 
前缀信息统计 -- 全局有序
热点频繁爆发 -- 自动分片，动态负载均衡
实时入库选取 -- 随机读写，强一致，列存储
记录数10万亿 -- 分布式，可扩展
记录历史信息 -- 多版本，自动垃圾回收
频繁增删字段 -- 动态schema ，稀疏表

##2.tera的数据模型和架构##
###2.1tera的数据模型###
首先来看一下传统的关系型数据库的模型，也就是ER 模型。

![webtable实体模型](https://github.com/baidu/tera/blob/master/resources/images/webtable_er.png)

 图1：Webtable实体模型
体现在数据库中就是如下的数据表以及建立在列上的索引：

表1：Webtable表

![webtable表]()

Tera的数据模型和传统的关系型数据库有可比性，但是又有很大的不同；由于Tera采用leveldb作为底层的数据操纵层，所以数据模型是一个KV（key-value）模型，Tera里面表是由KV对组成的。Tera中的KV数据模型如下所示：
 (row:string, column:string,time:uint64)->string
row是用户的key，相当于关系型数据库中的主键，如表1中的m_url，com.sina.www和com.jd.www，可以是任意的字符串；
column是列名称，在tera中由column family和qualifier构成（column family:qualifier）。qualifier就是具体的列，如表1中的content和anchor。column family是列簇，简称CF，通常一个列簇是由一组业务相关、数据类型相近的列组成，一个列簇可包含任意多的列（qualifier），并且列是可以动态增加的；
time是时间戳，tara可以存储数据的多版本信息，可以设定数据的存活时间。
Tera将数据表横向划分为若干个有序的区间，每一个区间就是一个tablet，是数据分布和负载均衡的最小单位。
数据模型里面还有一个概念是locality group，局部性群组（LG）。不同的列簇可以属于不同的局部性群组，局部性群组是数据物理存储介质类型的最小逻辑单位，不同的LG可以存储在不同的物理介质上。 LG对table进行纵向的分割，使列能够按照不同的用途存储在不同介质上。
下面是一个具体的数据模型的例子。

![webtable的tera的数据模型](https://github.com/baidu/tera/blob/master/resources/images/webtable_datamodel.png)

图2：Webtable的Tera数据模型
如上图中，如果sign和density属于列簇cf1，anchor和weight属于列簇cf2，那么图中com.sina.www的density列t7时刻的kv模型实现就是:（com.sina.www cf1: density t7）-> “0:0 1:0”。
可以相比较前面的关系数据库，Tera里面的key相当于主键，但不是单纯的一列作为主键，而是一个复合主键，包含了关系数据库里面讲的主键row之外，还有column列和时间戳也包含了，这就导致了进行查询的时候要进行key的解析的过程。

###2.2tera架构###

![tera架构](https://github.com/baidu/tera/blob/master/resources/images/tara_arch.png)

图3：tera架构

Tera包含三个功能模块和一个监控子系统：
Tabletserver，是核心服务器，负责Tablets管理和提供数据读写服务，是系统的数据节点，承载几乎所有客户端访问压力；
Master，是系统的仲裁者，负责表格的管理、schema更新与负载均衡，用户只有在创建表格和修改表格属性时才访问Master，所以负载较低，Master还会监控所有TabletServer的状态，发起Tablet拆分合并操作和负载均衡操作；
Client，封装系统的各项操作，以SDK和命令行工具的形式提供给用户使用，管理员也可以通过client对集群行为进行人工干预，如强制负载均衡、垃圾收集和创建快照等；
监控子系统，负责整个表格系统的状态监控、状态展示，同时提供Web形式的管理接口，可以实现机器上下线、数据冷备等操作，方便运维。

##3.tera功能模块##
上面介绍了tera包含的三个功能模块和监控子系统，这一节详细介绍三个功能模块的具体实现。
###3.1master###
Master最重要的功能就是表格管理（创建，删除，更新流程）、tabletnode管理和负载均衡管理这三个方面。

![tera master](https://github.com/baidu/tera/blob/master/resources/images/tera_master_impl.png)

图4：Tera Master实现模型

Tera将数据表横向划分为若干个有序的区间，每一个区间就是一个tablet，是数据分布和负载均衡的最小单位。Master根据每个数据节点的负载状况，将Tablet安排到各个TabletServer上，每个TabletServer管理着若干个Tablet，提供这些Tablet的读写服务，负责将数据持久化到DFS上。区间内的有序性由tablet自身保证，tablet之间的有序性通过master来维护。

![tera全局有序]()

图5：Tera数据全局有序性
创建，删除和更新都是load和unload

####Create table####
#####Client#####
1.在src/teracli_main.cc
if (cmd == "create") ret = CreateOp(client, argc, argv, &error_code);

2.在CreateOp里，进入到client->CreateTable(table_desc, delimiters, err)

3. src/sdk/client_impl.cc中的tera::ClientImpl::CreateTable (const TableDescriptor& desc,
                             const std::vector<string>& tablet_delim,
                             ErrorCode* err)
首先和master建立连接，创建request， master_client.CreateTable(&request, &response)

4.再返回到2中ShowTableDescriptor(table_desc)，到了src/sdk/sdk_utils.cc中的void ShowTableDescriptor(TableDescriptor& table_desc, bool is_x)中，然后里面会用到函数void TableDescToSchema(const TableDescriptor& desc, TableSchema* schema)将table_desc转化成schema的形式，另一个是void ShowTableSchema(const TableSchema& schema, bool is_x)显示在终端。
#####Master#####
1.在Master_impl.cc中void MasterImpl::CreateTable(const CreateTableRequest* request,
                             CreateTableResponse* response,
                             google::protobuf::Closure* done)
先做检查m_tablet_manager->FindTable(request->table_name(), &table)，确认table不存在。
再获取到tablet_num，在for循环，为每个tablet找到它要存放的路径
std::string path = leveldb::GetTabletPathFromNum(request->table_name(), i)

2.在filename.cc中std::string GetTabletPathFromNum(const std::string& tablename, uint64_t tablet)，返回路径名字tablename+ /tablet%08llu

3.在1中，继续m_tablet_manager->AddTablet(table_name, start_key, end_key, path,
                                         "", request->schema(), kTableNotInit,
                                         FLAGS_tera_tablet_write_block_size * 1024,
                                         &tablets[i-1], &status)
                                         
4.到tabletnode中的tablet_manager.cc中
bool TabletManager::AddTablet(const std::string& table_name,
                              const std::string& table_path,
                              const std::string& key_start,
                              const std::string& key_end,
                              io::TabletIO** tablet_io,
                              StatusCode* status)
填好了table和tablet

5.回到1中继续BatchWriteMetaTableAsync(table, tablets, false, closure)，这函数实现也在当前文件中void MasterImpl::BatchWriteMetaTableAsync(TablePtr table,
                                          const std::vector<TabletPtr>& tablets,
                                          bool is_delete, WriteClosure* done)
tabletnode::TabletNodeClient meta_node_client(meta_addr)
和meta_node_client.WriteTablet(request, response, done) 将tablet load到tabletnode上

####Put####
#####Client#####
1.在src/teracli_main.cc中PutOp(client, argc, argv, &error_code)

2. src/teracli_main.cc中的int32_t PutOp(Client* client, int32_t argc, char** argv, ErrorCode* err)中先判断client->OpenTable(tablename, err)是否可以正常打开，然后调用函数table->Put(rowkey, columnfamily, qualifier, value, err)
                                                                                          * 
3. src/sdk/table_impl.cc中的bool TableImpl::Put(const std::string& row_key, const std::string& family,
                                    const std::string& qualifier, const std::string& value,
                                    ErrorCode* err)
中的RowMutationImpl* row_mu = new RowMutationImpl(this, row_key)分配一个空间，然后row_mu->Put(family, qualifier, value)
                 * 
4. src/sdk/mutate_impl.cc中的void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                                                 const std::string& value)
中的Put(family, qualifier, kLatestTimestamp, value)
写完返回到4

5.返回到3，然后执行ApplyMutation(row_mu)的操作

6. 仍在当前文件src/sdk/table_impl.cc中void TableImpl::ApplyMutation(RowMutation* row_mu)
将这个改动加到了vector mu_list中，ApplyMutation(mu_list, true)

7.仍在当前文件中，src/sdk/table_impl.cc中ApplyMutation() 通过GetTabletAddrOrScheduleUpdateMeta(row_mutation->RowKey(),
                                 row_mutation, &server_addr)找到ts的addr。然后sync。
                                 
#####Tabletserver#####
1.tabletnode_impl.cc中，void TabletNodeImpl::WriteTablet(const WriteTabletRequest* request,
                                 WriteTabletResponse* response,
                                 google::protobuf::Closure* done,
                                 WriteRpcTimer* timer)中的
tablet_io = m_tablet_manager->GetTablet(
                request->tablet_name(), request->row_list(i).row_key(), &status)
                
2.tablet_manager.cc中的io::TabletIO* TabletManager::GetTablet(const std::string& table_name,
                                       const std::string& key,
                                       StatusCode* status)获取到tablet arrange
                                       
3.回到1中tablet_io->Write(request, response, done, index_list,
                                     done_counter, timer, &status)
                                     
4.tablet_io.cc中m_async_writer->Write(request, response, done, index_list,
                          done_counter, timer)
                          
5.tablet_writter.cc中的void TabletWriter::Write(const WriteTabletRequest* request,
                         WriteTabletResponse* response,
                         google::protobuf::Closure* done,
                         const std::vector<int32_t>* index_list,
                         Counter* done_counter, WriteRpcTimer* timer)
里的m_write_event，会后台调用TabletWriter::FlushToDiskBatch()写
####Get####
#####Client#####
1.在src/teracli_main.cc中GetOp(client, argc, argv, &error_code)中的
table->Get(rowkey, columnfamily, qualifier, &value, err)

2.在src/sdk/table_impl.cc中，bool TableImpl::Get(const std::string& row_key, const std::string& family,
                                 const std::string& qualifier, std::string* value,
                                 ErrorCode* err)
中void TableImpl::ReadRows(const std::vector<RowReaderImpl*>& row_reader_list,
                        bool called_by_user)
的CommitReaders(server_addr, commit_reader_list);

3.在上一个函数中tabletnode_client_async.ReadTablet(request, response, done);
#####Tablet server#####
1.在tabletnode_impl.cc中void TabletNodeImpl::ReadTablet(int64_t start_micros,
                                const ReadTabletRequest* request,
                                ReadTabletResponse* response,
                                google::protobuf::Closure* done,
                                ReadRpcTimer* timer)
设置status = kTabletNodeOk，tablet_io = m_tablet_manager->GetTablet(request->table_name(),
                                            request->start(), &status)
                                            
2.在tablet_manager.cc中io::TabletIO* TabletManager::GetTablet(const std::string& table_name,
                                       const std::string& key,
                                       StatusCode* status)
中的io::TabletIO* tablet_io = m_tablet_manager->GetTablet将tablet的信息获取到，接下来
tablet_io->ReadCells(request->row_info_list(i),
                 response->mutable_detail()->add_row_result(),
                 snapshot_id, &row_status))
                 
3.tablet_io.cc中的bool TabletIO::ReadCells(const RowReaderInfo& row_reader, RowResult* value_list,
                                    uint64_t snapshot_id, StatusCode* status)
中的LowLevelScan(start_tera_key, end_row_key, scan_options,
                      value_list, &read_row_count, &read_bytes,
                      &is_complete, status)
####Split####
#####Client：#####
1.在src/teracli_main.cc中ret = TabletOp(client, argc, argv, &error_code)

2.仍在src/teracli_main.cc文件中，int32_t TabletOp(Client* client, int32_t argc, char** argv, ErrorCode* err)中的
client->CmdCtrl("tablet", arg_list, NULL, NULL, err)，传入参数就是table split hello_table4,
server_addr

3.在src/sdk/client_impl.cc中的bool ClientImpl::CmdCtrl(const string& command,
                                             const std::vector<string>& arg_list,
                                             bool* bool_result,
                                             string* str_result,
                                             ErrorCode* err)
master::MasterClient master_client(_cluster->MasterAddr())得到master的addr，然后
request发往到master去。
#####Master#####
1.Master_impl.cc中的void MasterImpl::CmdCtrl(const CmdCtrlRequest* request,
                                       CmdCtrlResponse* response)
TabletCmdCtrl(request, response)和MetaCmdCtrl(request, response)

2.在第一个void MasterImpl::TabletCmdCtrl(const CmdCtrlRequest* request,
                               CmdCtrlResponse* response) 中的TrySplitTablet(tablet)
                               
3.在当前文件Master_impl.cc中bool MasterImpl::TrySplitTablet(TabletPtr tablet)
获取到tablet的server addr，然后找到tabletnode，用函数m_tabletnode_manager->FindTabletNode(server_addr, &node)如果server down，则报错；如果node正在split tablet，返回delay。如果都正确，则SplitTabletAsync(tablet);

4.在当前文件void MasterImpl::SplitTabletAsync(TabletPtr tablet)，构建SplitTabletRequest，master将请求发往tabletnode，node_client.SplitTablet(request, response, done);
#####Tabletnode#####
1.在Tabletnode_impl.cc中
void TabletNodeImpl::SplitTablet(const SplitTabletRequest* request,
                                 SplitTabletResponse* response,
                                 google::protobuf::Closure* done)
tablet_io得到了tablet信息（tabletname，keystart，keyend，status（kTabletNodeOk），然后tablet_io->Split(&split_key, &status)

2.在Tablet_io.cc中bool TabletIO::Split(std::string* split_key, StatusCode* status)中，status置成kOnSplit，然后
m_db->FindSplitKey(m_raw_start_key, m_raw_end_key, 0.5,&raw_split_key))，最后status置为kSplited

3.在db_impl.cc中的bool DBImpl::FindSplitKey(const std::string& start_key,
                                        const std::string& end_key,
                                        double ratio,
                                        std::string* split_key)
中, versions_->current()->FindSplitKey(start_key.empty()?NULL:&start_slice,
                                              end_key.empty()?NULL:&end_slice,
                                              ratio, split_key);
                                              
4.在version_set.cc的bool Version::FindSplitKey(const Slice* smallest_user_key,
                           const Slice* largest_user_key,
                           double ratio,
                           std::string* split_key)
splitkey = 最大文件的large key，修改fileMetaData
####Merge####
#####Master#####
1. 在master_impl.cc中的bool MasterImpl::TabletNodeLoadBalance(TabletNodePtr tabletnode, Scheduler* scheduler,
                                       const std::vector<TabletPtr>& tablet_list,
                                       const std::string& table_name)
tablet->GetDataSize() < (merge_size << 20那么发起TryMergeTablet(tablet)

2.在当前文件中bool MasterImpl::TryMergeTablet(TabletPtr tablet)中的
MergeTabletAsync(tablet, tablet2);

3.在当前文件中void MasterImpl::MergeTabletAsync(TabletPtr tablet_p1, TabletPtr tablet_p2)
检查tablet的status，load到同一个ts上
#####Tabletserver#####
1.在Tabletnode_impl.cc中
void TabletNodeImpl::MergeTablet(const MergeTabletRequest* request,
                                 MergeTabletResponse* response,
                                 google::protobuf::Closure* done)
得到tb1，tb2和tbmerge的tablet path，然后
io::MergeTablesWithLG(tb1_path, tb2_path, tb_merge)

2.在Tablet_io.cc中bool MergeTablesWithLG(const std::string& table_1,
                                     const std::string& table_2,
                                     const std::string& merged_table,
                                     uint32_t lg_num)中的
MergeTables(table_1_lg, table_2_lg, "", env)

3.在同一文件中bool MergeTables(const std::string& mf, const std::string& mf1,
                             const std::string& mf2,
                               std::map<uint64_t, uint64_t>* mf2_file_maps,
                               leveldb::Env* db_env)
的leveldb::MergeBoth(env, cmp, mf, mf1, mf2, mf2_file_maps)

4. bool MergeBoth(Env* env, Comparator* cmp,
               const std::string& merged_fn,
               const std::string& fn1, const std::string& fn2,
               std::map<uint64_t, uint64_t>* file_num_map)中的
env->NewWritableFile(merged_fn, &final_file)根据env看是哪个flashwritableFile

####3.1.2合并分裂####
合并是指两个Key Range相邻的tablet合并成一个可被调度的tablet；分裂是指一个tablet的Key Range分解成两部分，从而形成两个可被独立调度的tablet。通过合并，降低了负载管理的复杂度和开销，通过分裂，为未来的访问热点迁移做准备。
为了实现合并和分裂，TabletServer必须提供Merge和Split两个接口。通过这两个接口，Master命令TabletServer将其负责的两个Key Range相邻的tablet合并、将其负责的一个tablet分裂成两个。合并或分裂后，tablet仍然由该TabletServer负责，不发生迁移。
 
分裂

当Master判定某个Tablet符合分裂条件（体积大于阈值或者访问频度高于均值特定倍数），切当前集群负载允许Tablet分裂，向加载这个Tablet的TS发起split tablet。Tablet分裂工作主要由TS完成。

合并

Tablet合并工作也是由TS完成。但是TS对tablet进行合并之前要求两个Tablet是在RowKey上连续的，且加载在同一个TS上，所以在发起合并操作之前，master检查两个连续的tablet，是否在同一台TS上，如果不在，则要unload一台，load到另一台上。

###3.2 Tablet server###

![tablet server](https://github.com/baidu/tera/blob/master/resources/images/tera_ts_impl.png)


图6：Tablet Server实现模型

tablet server在架构上和master是差不多的，他们都是TeraEntry，但是角色不同，master负责表格信息管理和负载均衡的角色，而tabletnode充当的是提供数据读写服务的功能，几乎所有的数据读写服务都是通过tabletnode提供的。
####3.2.1数据读写####
数据读写过程，client只需要和zk、TS交互，不需要Master参与：

client端流程：

1.访问ZK获得Root_table地址，如果失败，结束；

2.扫描Root_table，获取到指定的Table、RowKey所在Meta_table各Tablet分布，如果失败，跳回1；

3.扫描Meta_table获取RowKey所在Tablet地址，如果失败，跳回2；

4.通过Rpc发送读写请求给对应TabletServer，如果Tablet不在服务中，跳回3，其他错误报错退出。

TS端流程

1. 接收到读写命令，进行权限检查，如果失败，结束；
2. 查找Tablet，调用Tablet读写接口，返回读写结果。
3. 更新Tablet访问信息（访问并发，频度等）；

####3.2.2Tablet分裂####
TabletServer收到Master的Split命令后：

1. 查找Tablet，如果找不到返回不再服务，返回失败，结束；
2. 检查是否有这个Tablet的管理权且在服务中，否则返回失败，结束；
3. 对Tablet执行minor compaction；
4. 将Tablet状态切换为Splitting，后续写操作阻塞；
5. 查找Tablet的中间key，作为划分边界；
6. 在MetaTable中新增以中间key为起始key的tablet，如果修改不成功（失败或超时），强行卸载这个Tablet，返回split失败，结束；
7. 卸载旧的Tablet，并加载两个新的Tablet，如果失败，强行卸载新旧Tablet，返回失败，结束；
8. 分裂结束，向master返回成功，Master更新自己状态。

####3.2.3tablet合并####
前置流程
TS对tablet进行合并要求两个Tablet是在RowKey上连续的，且加载在同一个TS上，所以前置流程是：

1. Master发现两个连续的Tablet，T1和T2体积都小于阈值v1（如300MB），体积之和小于阈值v2（如500MB）。
2. Master检查两个Tablet是否在同一个TS上，如果是，转6；
3. 假设两个Tablet分别在TS1和TS2上，Master向发起TS2发起Unload T2，如果失败，停掉TS2。
4. 向TS1发起Load T2命令，如果失败，停掉TS1，调度T1和T2在一个新的TS上加载。
5. Master可以向装载T1和T2的TS发起Merge（T1，T2）命令，如果失败，处理同4；
合并流程
TS收到Master的merge命令后，处理流程是：
1. 确认两个要Merge的Tablet状态，如果不是服务中，返回失败，结束；
2. 对两个Tablet执行minor compaction；
3. 修改两个Tablet状态为停止服务，阻塞读写；
4. 关闭两个Tablet对应的Leveldb，并依次对两个Tablet的多个LocalStore进行Merge，如果不成功，回滚，恢复可服务状态，恢复master失败，结束；
5. 修改MetaTable信息，修改T1范围，删除T2对应Tablet条目，如果失败，强行卸载两个Tablet，返回失败，结束。
6. 向Master报告合并成功。

###3.3SDK/Client###
Tera SDK/Client封装系统的各项操作，以SDK和命令行工具的形式提供给用户使用，管理员也可以通过client对集群行为进行人工干预，如强制负载均衡、垃圾收集和创建快照等。
一般来说数据库系统有DDL、DML和DCL三种操作，目前Tera支持的有数据定义语言（DDL），包括创建表、删除表、修改表schema、创建快照等和数据操作语言（DML），包括了读GET、写（PUT、ADD、PutIfAbsent）、扫描（SCAN）等。
##4.底层levelDB##
###4.1levelDB的特点###
持久化的KV存储系统，和Redis这种内存型的KV系统不同，LevelDb不会像Redis一样狂吃内存，而是将大部分数据存储到磁盘上。

Key值有序

操作接口简单，读写以及删除记录
###4.2levelDB架构###

从图中可以看出，构成LevelDb静态结构的包括六个主要部分：内存中的MemTable和Immutable MemTable以及磁盘上的几种主要文件：Current文件，Manifest文件，log文件以及SSTable文件。
Log文件在系统中的作用主要是用于系统故障恢复而不丢失数据，假如没有Log文件，因为写入的记录刚开始是保存在内存中的，如果此时统崩溃，内存中的数据还没有来得及Dump到磁盘，所以会丢失数据（Redis就存在这个问题）。为了避免这种情况，LevelDb在写入内存前先将操作记录写到Log文件中，然后再记入内存中，这样即使系统崩溃，也可以从Log文件中恢复内存中的Memtable，不会造成数据的丢失。

当Memtable插入的数据占用内存到了一个界限后，需要将内存的记录导出到外存文件中，LevleDb会生成新的Log文件和Memtable，原先的Memtable就成为Immutable Memtable，顾名思义，就是说这个Memtable的内容是不可更改的，只能读不能写入或者删除。新到来的数据被记入新的Log文件和Memtable，LevelDb后台调度会将Immutable Memtable的数据导出到磁盘，形成一个新的SSTable文件。SSTable就是由内存中的数据不断导出并进行Compaction操作后形成的，而且SSTable的所有文件是一种层级结构，第一层为Level 0，第二层为Level 1，依次类推，层级逐渐增高，这也是为何称之为LevelDb的原因。
SSTable中的文件是Key有序的，就是说在文件中小key记录排在大Key记录之前，各个Level的SSTable都是如此，但是这里需要注意的一点是：Level 0的SSTable文件（后缀为.sst）和其它Level的文件相比有特殊性：这个层级内的.sst文件，两个文件可能存在key重叠。对于其它Level的SSTable文件来说，则不会出现同一层级内.sst文件的key重叠现象，就是说Level L中任意两个.sst文件，那么可以保证它们的key值是不会重叠的。

SSTable中的某个文件属于特定层级，而且其存储的记录是key有序的，那么必然有文件中的最小key和最大key，这是非常重要的信息，LevelDb应该记下这些信息。Manifest就是干这个的，它记载了SSTable各个文件的管理信息，比如属于哪个Level，文件名称叫啥，最小key和最大key各自是多少。

Current文件是干什么的呢？这个文件的内容只有一个信息，就是记载当前的manifest文件名。因为在LevleDb的运行过程中，随着Compaction的进行，SSTable文件会发生变化，会有新的文件产生，老的文件被废弃，Manifest也会跟着反映这种变化，此时往往会新生成Manifest文件来记载这种变化，而Current则用来指出哪个Manifest文件才是我们关心的那个Manifest文件。
###4.3Leveldb的写###
先写入logCommitlog，如果写入成功，则将这条KV记录插入memtable(skiplist) 根据用户提供的key comparator保持有序，将key插入到相应的位置。当memtable的大小超过阀值，memtable变成immemtable，同时生成新的memtable和log文件，因为leveldb的写只是写log和一次memtable的内存写，所以写入非常快。
###4.4Leveldb的读###
按照文件的新鲜度逐层查找

Memtable->immemtable->level0（有重叠）-> 其他level

LevelDb首先会去查看内存中的Memtable，如果Memtable中包含key及其对应的value，则返回value值即可；如果在Memtable没有读到key，则接下来到同样处于内存中的Immutable Memtable中去读取，类似地，如果读到就返回，若是没有读到,那么只能万般无奈下从磁盘中的大量SSTable文件中查找。因为SSTable数量较多，而且分成多个Level，所以在SSTable中读数据是相当蜿蜒曲折的一段旅程。
总的读取原则是这样的：首先从属于level 0的文件中查找，如果找到则返回对应的value值，如果没有找到那么到level 1中的文件中去找，如此循环往复，直到在某层SSTable文件中找到这个key对应的value为止（或者查到最高level，查找失败，说明整个系统中不存在这个Key)。
###4.5Compaction###
上面一层层的新鲜度是如何实现的呢？

Minor compact

Immemtable dump成sst，形成level0的一个文件

Major compaction

当各个level的文件数目（level0）或者文件大小（大于level0）超过一定阀值时，触发major compact。对level0 来说，比较特殊，选文件的时候需要把重叠的文件都选上，和level1的文件进行多路合并。
