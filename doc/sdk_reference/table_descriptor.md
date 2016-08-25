# 表格描述

tera中的表格由TableDescriptor、LocalityGroupDescriptor、ColumnFamilyDescriptor三个数据结构进行描述，C++接口。

同时也支持更简单的字符串描述，参见本文最后。

## TableDescriptor

表格描述符主体，LocalityGroupDescriptor、ColumnFamilyDescriptor由其管理。

描述表格全局属性，如key拼装方式、分片分裂合并阈值、ACL等信息。

### 创建与析构

此结构由用户自己创建并析构。

### 使用场景

 * 表格创建，通过`tera::Client::CreateTable`
 * 表格Schema更新，通过`tera::Client::UpdateTable`
 * 获取表格属性，通过`tera::Client::GetTableDescriptor`
 
### API

#### TableDescriptor

```
TableDescriptor(const std::string& name);
```

构造表格名为“name”的表格描述符。

其中表格名长度需要小于256字节，字符只支持{[a-z],[A-Z],[0-9],'_','-'}。

#### TableName

```
void SetTableName(const std::string& name);
std::string TableName() const;
```

设置、返回表格名。

#### LocalityGroup

```
LocalityGroupDescriptor* AddLocalityGroup(const std::string& lg_name);
```

新增一个名为‘lg_name’的LG。

其中的LocalityGroup名长度需要小于256字节，字符只支持{[a-z],[A-Z],[0-9],'_','-'}。

```
bool RemoveLocalityGroup(const std::string& lg_name);
```

删除名为‘lg_name’的LG。

如果此LG中还有列族存在，删除失败。

```
const LocalityGroupDescriptor* LocalityGroup(int32_t id) const;
const LocalityGroupDescriptor* LocalityGroup(const std::string& lg_name) const;
```

通过id/名称访问对应LG。

LG在表格内部以vector形式保存，id为其对应的下标。

```
int32_t LocalityGroupNum() const;
```

返回当前表格中LG数量。

#### ColumnFamily

```
ColumnFamilyDescriptor* AddColumnFamily(const std::string& cf_name,const std::string& lg_name);
```

在‘lg_name’下新增一个名为‘cf_name’的列族。

若‘lg_name’不存在，返回NULL。

其中列族名长度需要小于256字节，字符只支持{[a-z],[A-Z],[0-9],'_','-'}。

```
void RemoveColumnFamily(const std::string& cf_name);
```

删除名为‘cf_name’的列族。

```
const ColumnFamilyDescriptor* ColumnFamily(int32_t id) const;
const ColumnFamilyDescriptor* ColumnFamily(const std::string& cf_name) const;
```

通过id/名称访问对应列族。

列族在表格内部以vector形式保存，id为其对应的下标。

```
int32_t ColumnFamilyNum() const;
```

返回当前表格中列族数量。

#### RawKey

```
enum RawKeyType {
    kReadable = 0,
    kBinary = 1, 
    kTTLKv = 2,
    kGeneralKv = 3,
};                 
void SetRawKey(RawKeyType type);
RawKeyType RawKey() const;
```

表格内部key的拼装格式。

决定了表格的存储及访问格式，推荐kBinary。

#### SplitSize

```
void SetSplitSize(int64_t size);
int64_t SplitSize() const;
```

分片分裂阈值。

当分片数据量（物理存储）超过此阈值时，会被一分为二，并可能被两个不同服务器加载。

此分裂阈值是一个基础参考值，系统会根据实际动态负载在此值基础上进行调整。

#### MergeSize

```
void SetMergeSize(int64_t size);
int64_t MergeSize() const;
```

分片合并阈值。

当分片数据量（物理存储）低于此阈值时，会被合并至相临分片中。

此值是一个基础参考值，系统会根据实际动态负载在此值基础上进行调整。

需要小于分裂阈值的1/3，防止出现合并、分裂的循环出现。

#### Write Ahead Log

```
void DisableWal();         
bool IsWalDisabled() const;
```

配置日志开关，默认打开。

当此表格数据没有强特久化需求时，可以选择关闭日志。

会大幅提升写性能、降低系统IO消耗。

当有服务器宕机时，内存中数据将丢失，谨慎关闭。

#### Admin

```
void SetAdmin(const std::string& name);
std::string Admin() const;
void SetAdminGroup(const std::string& name);
std::string AdminGroup() const;
```

设置表格ACL信息。

## LocalityGroupDescriptor

描述一个locality group的属性。

### 创建与析构

通过`TableDescriptor::AddLocalityGroup`进行创建。

无须用户析构。
 
### API

#### Name

```
const std::string& Name() const;
```

获取此LG名字。

#### Store

```
enum StoreType {                                                                    
    kInDisk = 0,                                                                    
    kInFlash = 1,                                                                   
    kInMemory = 2,                                                                  
};                                                                                  
void SetStore(StoreType type);
StoreType Store() const;
```

设定、获取存储介质，默认kInDisk。

#### BlockSize、SstSize、BloomFilter

```
void SetBlockSize(int block_size);                                  
int BlockSize() const;                                              
```

设定、获取物理文件内部block大小，单位KB，默认值：4。

物理存储基于leveldb开发，此概念与leveldb中的block相似。

```
void SetSstSize(int sst_size);                                  
int SstSize() const;                                              
```

设定、获取物理文件基础大小，单位MB，默认值：8。

物理存储基于leveldb开发，此概念与leveldb中的level1文件大小相同。

```
void SetUseBloomfilter(bool use_bloomfilter);
bool UseBloomfilter() const;
```

设定、获取是否使用bloom filter，默认不使用。

物理存储基于leveldb开发，此概念与leveldb中的bloom filter。

#### 内存内compact

```
bool UseMemtableOnLeveldb() const;
void SetUseMemtableOnLeveldb(bool use_mem_ldb);
```

是否使用内存内compact。

```
int32_t MemtableLdbWriteBufferSize() const;
void SetMemtableLdbWriteBufferSize(int32_t buffer_size);
```

设定、获取内存compact中写缓存大小，单位KB。

```
int32_t MemtableLdbBlockSize() const;
void SetMemtableLdbBlockSize(int32_t block_size);
```

设定、获取内存compact中对应block大小，单位KB。

## ColumnFamilyDescriptor

描述一个列族的属性。

属性支持动态更新。更新状态为最终一致，过程中存在分片之前属性不一致情况，使用时需要注意。

### 创建与析构

通过`TableDescriptor::AddColumnFamily`进行创建。

无须用户析构。
 
### API

#### TTL

```
void SetTimeToLive(int32_t ttl);
int32_t TimeToLive() const; 
```

设定列族内cell的TTL（time-to-live)，单位秒，默认无穷大。

当列族内某cell的更新时间超过此值后，读取时被屏蔽，并在垃圾回收时物理删除。

#### MaxVersion

```
void SetMaxVersions(int32_t max_versions);
int32_t MaxVersions() const; 
```

设定列族内cell的最大版本数，默认为1。

当某cell的版本数超过此限制后，会将最旧的版本进行屏蔽，并在垃圾回收时物理删除。

此值不做最大值限制，但随着版本数大量增加，相应的随机读、扫描性能会下降，存储使用上升，用户可按实际情况调整。

## 字符串描述

描述表格的字符串是一个支持描述节点属性的树结构，语法详见[PropTree](https://github.com/BaiduPS/tera/blob/master/doc/prop_tree.md)

### 描述表格存储

表格结构中包含表名、locality groups定义、column families定义，一个典型的表格定义如下（可写入文件）：

    # tablet分裂阈值为4096M，合并阈值为512M
    # 三个lg，分别配置为flash、flash、磁盘存储
    table_hello <splitsize=4096, mergesize=512> {
        lg_index <storage=flash, blocksize=4> {
            update_flag <maxversions=1>
        },
        lg_props <storage=flash, blocksize=32> {
            level<ttl=1000000>,
            weight
        },
        lg_raw <storage=disk, blocksize=128> {
            data <maxversions=10>
        }
    }

如果无需配置LG，指定表名和所需列名即可（所有的属性可配）：

    table_hello {cf0<ttl=10000>, cf1, cf2}

### 描述key-value存储

只需指定表名即可，若需要指定存储介质等属性，可选择性添加：

    kv_hello                                                # 简单key-value
    kv_hello <storage=flash, splitsize=2048, mergesize=128> # 配置若干属性

### 属性及含义

span | 属性名 | 意义 | 有效取值 | 单位 | 默认值 | 其它说明
---  | ---    | ---  | ---      | ---  | ---    | ---
table | splitsize | 某个tablet增大到此阈值时分裂为2个子tablets| >=0，等于0时关闭split | MB | 512 |
table | mergesize | 某个tablet减小到此阈值时和相邻的1个tablet合并 | >=0，等于0时关闭merge | MB | 0 | splitsize至少要为mergesize的5倍
lg    | storage   | 存储类型 | "disk" / "flash" / "memory" | - | "disk" |
lg    | blocksize | LevelDB中block的大小       | >0 | KB | 4 |
lg    | use_memtable_on_leveldb | 是否启用内存compact | "true" / "false" | - | false |
lg    | sst_size  | 第一层sst文件大小 | >0 | MB | 8 |
cf    | maxversions | 保存的最大版本数  | >0 | - | 1 |
cf    | ttl | 数据有效时间 | >=0，等于0时此数据永远有效 | second | 0 |
