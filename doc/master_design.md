#master design
Copyright 2015, Baidu, Inc.

##功能
* 表格管理
  * 表格创建、删除
  * 表格禁用、启用
  * 表格schema更新
* 权限管理
  * 用户创建、删除
  * 用户组创建、删除
  * 用户和用户组权限更改
* 节点管理
  * 新节点、死节点发现
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
  * 元数据全内存存储
  * 胜任千台以上规模集群的管理
* 松耦合
  * 不参与读写逻辑

##实现
###数据结构
  1. tablet_manager
    1. tablet_manager是master的内存数据结构，维护了所有表格及子表的信息
    1. tablet_manager是table_name到table结构体的映射
    1. table结构体包括schema、status、tablet list等属性
    1. tablet list是start key到tablet结构体的映射
    1. tablet结构体包括key range、node addr、status、size等属性
  1. user_manager
    1. user_manager是master的内存数据结构，维护了用户信息
  1. tabletnode_manager
    1. tabletnode_manager是master的内存数据结构，维护了所有活动的tabletnode信息
    1. tabletnode_manager是host addr到tabletnode结构体的映射
    1. tabletnode包括节点status、节点总体统计信息
  1. meta_table
    1. meta_table是一个特殊的表格，用于table、user等元数据的持久化
    1. meta_table是key-value的，每一项代表一个table，或一个user，或一个tablet
    1. key的开头是magic num，用于区分这一项的类型，其余部分是该项的唯一ID
    1. table的ID是table name，tablet的ID是start key，user的ID是user name

###流程
####tablet load
  1. 修改内存tablet结构，status改为onload，addr改为新tabletnode
  1. 更新meta table中的addr
  1. 命令ts执行load操作
  1. 修改内存tablet结构，status改为ready

####tablet unload
  1. 修改内存tablet结构体，status改为unloading
  1. 更新meta table
  1. 命令ts执行unload操作
  1. 修改内存tablet结构体，status改为offline

####tablet split
  1. 修改内存tablet结构体，status改为onsplit
  1. 更新meta table
  1. 命令ts执行split操作
  1. 删除内存tablet结构，增加两个新tablet结构
  1. 删除meta table中的tablet项，增加两个新的tablet项

####tablet merge

####表格创建、删除、停用、启用、更新
  1. 修改内存meta
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
####权限
####节点管理
  
####负载均衡

 * 
