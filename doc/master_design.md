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

####新节点加入
  1. 收到zk通知，获取新节点地址
  1. 在内存tabletnode_manager中增加对应的tabletnode结构体，status设为ready

####死节点退出
  1. 收到zk通知，获取死节点地址
  1. 从tablet_manager中获取该节点管理的tablet列表
  1. 将这些tablet的status改为offline
  1. 从内存tabletnode_manager中删除对应的tabletnode结构体

####慢节点剔除
####表格操作
  1. 修改内存元数据
    * create: 增加table、tablet结构
    * drop: 删除table、tablet结构
    * disable/enable: 修改table status
    * update: 修改table schema
  2. 更新持久化meta
    * create: 增加table、tablet项
    * drop: 删除table、tablet项
    * disable/enable: 修改table status
    * update: 修改table schema
  3. 命令tabletserver执行相应操作
    * create/enable: load
    * disable: unload
  4. 发送response
