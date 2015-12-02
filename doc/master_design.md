#master design
Copyright 2015, Baidu, Inc.

##功能
* 表格管理
  * 表格创建、删除
  * 表格禁用、启用
  * 表格属性更新
* 权限管理
  * 用户创建、删除
  * 用户权限更改
* 节点管理
  * 节点存活检测
  * 故障节点剔除
* 负载管理
  * 子表分裂、合并
  * 子表迁移
  * 节点间数据量均衡
  * 节点间压力均衡


##特性
* 高可用
  * 多点备份
* 轻量、高性能
  * 胜任千台以上规模集群的管理
* 松耦合
  * 不参与读写逻辑

##实现
###数据结构
  * tablet_manager
    * tablet_manager是master的内存数据结构，维护了所有表格及子表的信息
    * tablet_manager是table_name到table结构体的映射
    * table结构体包括schema、status、tablet list等属性
    * tablet list是start key到tablet结构体的映射
    * tablet结构体包括key range、node addr、status、size等属性
  * user_manager
    * user_manager是master的内存数据结构，维护了用户信息
  * tabletnode_manager
    * tabletnode_manager是master的内存数据结构，维护了所有活动的tabletnode信息
    * tabletnode_manager是host addr到tabletnode结构体的映射
    * tabletnode包括节点status、节点总体统计信息
  * meta_table
    * meta_table是一个特殊的表格，用于table、tablet、user等元数据的持久化
    * meta_table是key-value的，每一项代表一个table，或一个tablet，或一个user
    * key的开头是magic num，用于区分这一项的类型，其余部分是该项的唯一ID
    * table的ID是table name，tablet的ID是start key，user的ID是user name

###流程
####启动(startup)
  1. 内存status设为secondary
  1. 在名字服务(name service)的master目录下创建实节点，key是自增整数，value是本机地址
  1. 获取master目录下的所有实节点
  1. 判断key最小的节点value是否为本机地址
    * 若为本机地址，将内存status设为running，启动路程完成
    * 若不是本机地址，监视(watch)master目录的改变，进入等待，当zk通知master目录发送改变后，重新执行最后两步

####初始化(initial)
  1. 确认内存status为running
  1. 获取名字服务的ts目录下的实节点列表，并监视目录的改变
  1. 命令各节点汇报各级状态，等待汇报全部完成
  1. 若汇报结果中没有meta tablet， 选择一个ts，命令其加载meta tablet，并等待加载完成
  1. 在名字服务的根目录下创建虚节点，key是"root_table"，value是ts地址
  1. 向meta tablet所在的ts发送扫描命令，扫描meta tablet的全部内容
  1. 将扫描结果中的tablet全部添加到内存tablet_manager结构体中
  1. 将扫描结果中的tablet与汇报结果中的tablet做对比，不一致的tablet以扫描结果为准
    * 对于多于的tablet，命令所在ts将其卸载
    * 对于缺少的tablet，选择一个ts，命令其加载
    * 对于所在ts有误的tablet，将其迁移到正确的ts上
  1. 启动多项定时任务
    * 每隔一段时间获取ts最新状态
    * 每隔一段时间清理垃圾数据文件
    * 每隔一段时间进行ts间的tablet负载均衡

####加载子表(load tablet)
  1. 确定内存tablet结构的status是offline
  1. 修改内存tablet结构，status改为onload，addr改为新tabletnode
  1. 更新meta table中的addr
  1. 命令ts执行load操作
  1. 修改内存tablet结构，status改为ready

####卸载子表(unload tablet)
  1. 确定内存tablet结构的status是ready
  1. 修改内存tablet结构体，status改为unloading
  1. 更新meta table中的status
  1. 命令ts执行unload操作
  1. 修改内存tablet结构体，status改为offline

####分裂子表(split tablet)
  1. 确定内存tablet结构的status是ready
  1. 修改内存tablet结构体，status改为onsplit
  1. 更新meta table
  1. 命令ts执行split操作
  1. 删除内存tablet结构，增加两个新tablet结构
  1. 删除meta table中的tablet项，增加两个新的tablet项

####合并子表(merge tablets)
  TODO

####创建表格(create table)
  1. 在内存tablet_manager中增加对应的table和tablet结构体，status设为offline
  1. 在meta table中插入table和tablet项
  1. 命令ts执行load tablet

####启用表格(enable table)
  1. 将内存table结构体的status改为enabled
  1. 持久化到meta table
  1. 对table的所有tablet，命令ts执行load

####停用表格(disable table)
  1. 将内存table结构体的status改为disabled
  1. 持久化到meta table
  1. 对table的所有tablet，命令ts执行unload

####删除表格(drop table)
  1. 将内存table结构体的status改为deleted
  1. 删除meta table中的对应的table和tablet项
  1. 从内存tablet_manager中将对应的table和tablet结构体删除

####更新表格属性(update table schema)
  1. 修改内存table结构体的schema
  1. 持久化到meta table
  1. 命令ts更新schema
    * 对于支持热更新的schema，命令ts更新schema
    * 对于其它schema，先命令ts执行unload，再命令ts以新schema执行load

####节点存活检测
  1. 收到zk通知，获取节点列表
  1. 与内存tabletnode_manager对比，得到新节点和死节点地址
    * 对于新节点，在内存tabletnode_manager中增加对应的tabletnode结构体，status设为ready
    * 对于死节点，从tablet_manager中获取该节点管理的tablet列表
      将这些tablet的status改为offline，从tabletnode_manager中删除对应的tabletnode结构体

####故障节点剔除
  1. 识别故障节点，确认其对应的tabletnode结构体的status为ready
  1. 将tabletnode结构体的status改为waitkick
  1. 判断当前运行模式
    * 如果是安全模式，则退出流程
    * 如果是普通模式，则继续将status改为onkick
  1. 在名字服务的kick目录下创建虚节点，key和value分别是故障节点的session ID和地址

####节点状态更新
  1. 每隔一定时间命令全部存活节点汇报各自状态
  1. 将获取到的状态保存到tabletnode结构体中

####垃圾清理
  TODO
  
####负载均衡
  TODO
