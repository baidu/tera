# Tera表格数据模型及实现

>**目录**
>  1. [概述](#overview)
>  2. [操作](#operation)
>   * 随机写
>   * 随机读
>   * 扫描
>  3. [存储原理](#storage)
>  4. [实现](#implementation)
>   * 主要数据结构
>   * 随机写
>   * 随机读、扫描
>   * 读放大优化

<a name="overview"></a>

## 一、概述
Tera使用了bigtable的数据模型，可以将一张表格理解为这样一种多级map数据结构：
```
map<RowKey, map<ColummnFamily:Qualifier, map<Timestamp, Value> > > 
```
其中：
 * RowKey是二进制字符串，为每一行的主键，也是全序的依据；
 * ColumnFamily是可读字符串，为每一列的前缀，需要建表时由schema确定；
 * Qualifier是任意二进制字符串，为每一列独立的标识符，与ColumnFamily组成列名，可为空；
 * Timestamp是一个64位整型，是访问控制、版本保留等策略的基本单位；
 * 在一个表格中由RowKey、ColumnFamily、Qualifier、Timestamp唯一确定一个Value。
	
如果忽略Qualifier、TimeStamp，则可被视为传统行列模型的表格。

通过按RowKey范围进行分片切分，tera实现了将一个大表分散存储在集群中，提供集群级别的读写能力。

本文主要描述一个分片内数据模型的原理及实现
	
<a name="operation"></a>

## 二、操作

Tera提供多种数据操作：随机写（包含删除）、随机读（seek）、扫描（scan）。

### 随机写

 * 支持按行、列、单元等多种方式插入、更新及删除。
 * 支持单元内的多版本，新数据更新后，旧版本数据依旧可读。
 * 支持某个特定历史版本的删除。
 * 支持PutIfAbsent语义
 * 支持高性能Counter
 * 除主动删除外，tera还支持按ttl、最大版本数等条件进行数据淘汰。 
		
### 随机读
		
 * 支持按行、列、版本等多个维度进行读取。
		
### 扫描
	
 * 返回一个迭代器进行数据访问
 * 支持传入多种过滤条件，包括：
  * RowKey区间
  * ColumnFamily/Qualifier集合
  * 数据更新时间区间
  * 版本数
  * 部分类型数据的条件过滤

<a name="storage"></a>

## 三、存储原理

tera的底层存储引擎采用了基于leveldb优化后的key-value存储。
通过将表格内容平展为key-value结构进行存储。
表格中的rowkey/columnfamily/qualifier/timestamp统一编码为一个rawkey，结合value进行存储。

例如：

```
          age     weight     country    language:en    language:cn
John              54KG       USA        yes          
Lilei     17                 China                     yes
Toshi     19      60KG       Japan      no                 
```

在底层存储引擎中的存储格式为：

```
John:country:USA
John:language:en:yes
John:weight:54KG
Lilei:age:17
Lilei:country:China
Lilei:language:cn:yes
Toshi:age:19
Toshi:country:Japan
Toshi:language:en:no
Toshi:weight:60KG
```

其中：

 * 空字段不占用实际存储
 * 平展化后，同一行的数据存储在一起，方便进行前缀压缩
 * 一行数据不会被分配至不同的表格分片中

Tera中的数据删除采用标记删除方式，
不同的删除操作通过插入不同的删除标记进行数据屏蔽，
通过后台compact完成数据的物理删除，细节请[参见](https://github.com/baidu/tera/blob/master/doc/data-deletion-in-tera.md)。

从存储中看，删除标记与数据没有任何区别，可以统一存储。

<a name="implementation"></a>

## 四、实现

表格与key-value之间的映射关系通过TabletIO模块完成。

### 1. 主要数据结构

#### TabletIO

主结构，所有分片操作的入口，主要功能：

 * 表格分片（tablet）的载入、卸载
 * 获取分片状态（大小、区间等）
 * 获取分片分裂row_key
 * 读、写、扫描等数据操作

#### TeraKey

内部key结构

 * 完成编解码、比较、删除标记判定等操作

#### TabletWriter

写请求处理结构

 * 每个分片有一个TabletWriter，负责完成并发写请求的打包、异步调度等操作

#### CompactStratgy

数据读取及compact时的策略模块。
对应到表格存储中：

 * 删除标记及被删除数据的判定
 * ttl、多版本等数据淘汰策略
 * 待合并数据（Counter/PutIfAbsent）的合并

#### TabletIterator

表格迭代器

 * 随机读、扫描等操作的基础结构
 * 为用户过滤条件（版本、时间等）提供数据
 * 内部完成key-value至表格结构的转换

### 2. 随机写

 1. TabletIO::Write接受写请求
 1. 将请求传递至TabletWriter
 1. TabletWriter判定请求为同步/异步
 1. 如同步，直接进行写入（7）
 1. 如异步，将请求放入缓存池
 1. 待刷新条件（时间、缓存池大小）达到时，进行写入（7）
 1. 通过TabletIO::WriteBatch进行平展化、写入下层存储
 1. 按写入结果，返回用户（确保持久化后才返回用户）

### 3. 随机读、扫描

 1. TabletIO::ReadCells/ScanRow接受请求（附带过滤条件）
 1. 随机读、扫描共用同一个下层访问接口TabletIO::LowLevelScan
 1. TabletIO::LowLevelScan中按条件创建TabletIterator
 1. TabletIterator中创建kv存储访问迭代器，对用户不可见数据进行过滤（已删除、需要合并等）
 1. TabletIO::LowLevelScan接受TabletIterator返回的用户数据，按用户过滤要求进行再次过滤
 1.  数据传回TabletIO::ReadCells/ScanRow，返回用户
 
### 4. 读放大优化
		
数据读取（随机、扫描）是一个顺序读取的过程。
迭代器的操作贯穿始终，如果处理不当，会产生巨量的读放大（用户一次读请求对应底层的多次key-value访问）。
tera围绕读放大进行了大量优化，保证资源的合理利用及更高的性能。
		
#### 迭代器缓存
			
每一个迭代器的创建过程可能产生大量底层IO及计算操作，
Tera在扫描过程中会将可能用到的迭代器进行缓存，待下次使用时，直接使用而不用重新创建。

#### 后台compact

随着数据的增删、过期，存储中的垃圾会越来越多，
读放大会变得越来越严重（会读出大量删除标记及已淘汰数据）。按一定条件（写入量、时间等）触发，
将可以合并、删除的数据进行后台处理，为读取做好准备。
此操作不影响正在进行的读写行为。

#### 内存compact

有别于普通compact，内存compact不产生实际IO，将数据在内存中进行直接淘汰。
对于淘汰数据频率很高的场景（比如反复更新同一数据）会有非常明显的性能提升。
