
# table_descriptor接口说明
tera中的表格由ColumnFamilyDescriptor、LocalityGroupDescriptor、TableDescriptor三个数据结构进行描述。
### 1. ColumnFamilyDescriptor
描述一个列族的属性。
属性支持动态更新。更新状态为最终一致，过程中存在分片之前属性不一致情况，使用时需要注意。
##### (1) TTL 
设定列族内cell的TTL（time-to-live)，单位秒，默认无穷大。
当列族内某cell的更新时间超过此值后，读取时被屏蔽，并在垃圾回收时物理删除。
```
void SetTimeToLive(int32_t ttl) = 0;
int32_t TimeToLive() const = 0;
```

##### (2) 最大版本数MaxVersions
设定列族内cell的最大版本数，默认为1。
当某cell的版本数超过此限制后，会将最旧的版本进行屏蔽，并在垃圾回收时物理删除。
此值不做最大值限制，但随着版本数大量增加，相应的随机读、扫描性能会下降，存储使用上升，用户可按实际情况调整。
```
void SetMaxVersions(int32_t max_versions) = 0;
int32_t MaxVersions() const = 0;
```

##### (3) 获取LG的名字
```
const std::string& LocalityGroup() const = 0;
```
##### (4) 获取Id
```
int32_t Id() const = 0;
```

### 2. LocalityGroupDescriptor
描述一个locality group的属性。

##### (1) 获取此LG名字
```
const std::string& Name() const;
```

##### (2) 设定、获取存储介质，默认kInDisk
```
void SetStore(StoreType type) = 0;
StoreType Store() const = 0;
enum StoreType {
    kInDisk = 0,
    kInFlash = 1,
    kInMemory = 2,
};
```

##### (3) 设定、获取物理文件内部block大小
```
void SetBlockSize(int block_size) = 0;//设定、获取物理文件内部block大小，单位KB，默认值：4。
int BlockSize() const = 0; 
```
##### (4) 设定、获取物理文件基础大小
```
int32_t SstSize() const = 0;//设定、获取物理文件内部block大小，单位KB，默认值：4。
void SetSstSize(int32_t sst_size) = 0;
```
##### (5) 获取／得到compress type
```
 void SetCompress(CompressType type) = 0;
 CompressType Compress() const = 0;
```
##### (6) 设定、获取是否使用bloom filter
设定、获取是否使用bloom filter，默认不使用。
``` 
void SetUseBloomfilter(bool use_bloomfilter) = 0;
bool UseBloomfilter() const = 0;
```
##### (7) 内存内compact
是否使用内存内compact。
``` 
bool UseMemtableOnLeveldb() const = 0;
void SetUseMemtableOnLeveldb(bool use_mem_ldb) = 0;
```
##### (8) 设定、获取内存compact中写缓存大小
设定、获取内存compact中写缓存大小，单位KB。
```
int32_t MemtableLdbWriteBufferSize() const = 0;
void SetMemtableLdbWriteBufferSize(int32_t buffer_size) = 0;
```
##### (9) 设定、获取内存compact中对应block大小
设定、获取内存compact中对应block大小，单位KB。
``` 
int32_t MemtableLdbBlockSize() const = 0;
void SetMemtableLdbBlockSize(int32_t block_size) = 0;
```
 
### 3. TableDescriptor
表格描述符主体，LocalityGroupDescriptor、ColumnFamilyDescriptor由其管理。
描述表格全局属性，如key拼装方式、分片分裂合并阈值、ACL等信息。
使用场景
<ul>
<li>表格创建，通过tera::Client::CreateTable</li>
<li>表格Schema更新，通过tera::Client::UpdateTable</li>
<li>获取表格属性，通过tera::Client::GetTableDescriptor</li>
</ul>

#### 3.1 TableDescriptor

##### (1) 获取表名
设置、返回表格名。
```
void SetTableName(const std::string& name);
std::string TableName() const;
```

##### (2) 新增一个名为‘lg_name’的LG
其中，LocalityGroup名长度需要小于256字节，字符只支持{[a-z],[A-Z],[0-9],'_','-'}
```
LocalityGroupDescriptor* AddLocalityGroup(const std::string& lg_name);
```

##### (3) 删除名为‘lg_name’的LG
```
bool RemoveLocalityGroup(const std::string& lg_name);//如果此LG中还有列族存在，删除失败。
```
##### (4) 通过id/名称访问对应LG
LG在表格内部以vector形式保存，id为其对应的下标。
```
const LocalityGroupDescriptor* LocalityGroup(int32_t id) const;
const LocalityGroupDescriptor* LocalityGroup(const std::string& lg_name) const;
```
##### (5) 获取／得到compress type
```
 void SetCompress(CompressType type) = 0;
 CompressType Compress() const = 0;
```
##### (6) 返回当前表格中LG数量
```
int32_t LocalityGroupNum() const;
```
 
#### 3.2 ColumnFamily

##### (1) 在‘lg_name’下新增一个名为‘cf_name’的列族
若‘lg_name’不存在，返回NULL。其中列族名长度需要小于256字节，字符只支持{[a-z],[A-Z],[0-9],'_','-'}。
``` 
ColumnFamilyDescriptor* AddColumnFamily(const std::string& cf_name, const std::string& lg_name = "lg0");
```
##### (2) 删除名为‘cf_name’的列族
``` 
void RemoveColumnFamily(const std::string& cf_name);
```
##### (3) 通过id/名称访问对应列族
列族在表格内部以vector形式保存，id为其对应的下标。
```
const ColumnFamilyDescriptor* ColumnFamily(int32_t id) const;
const ColumnFamilyDescriptor* ColumnFamily(const std::string& cf_name) const;
```
##### (4) 返回当前表格中列族数量
```
int32_t ColumnFamilyNum() const;
```

#### 3.3 RawKey

##### (1) 表格内部key的拼装格式
决定了表格的存储及访问格式，推荐kBinary。
``` 
void SetRawKey(RawKeyType type);
RawKeyType RawKey() const;
enum RawKeyType {
    kReadable = 0,
    kBinary = 1,
    kTTLKv = 2,
    kGeneralKv = 3,
};
```
#### 3.4 SplitSize
##### (1) 分片分裂阈值
当分片数据量（物理存储）超过此阈值时，会被一分为二，并可能被两个不同服务器加载。
此分裂阈值是一个基础参考值，系统会根据实际动态负载在此值基础上进行调整。
```
void SetSplitSize(int64_t size);
int64_t SplitSize() const;
```

#### 3.5 MergeSize
##### (1) 分片合并阈值
当分片数据量（物理存储）低于此阈值时，会被合并至相临分片中。
此值是一个基础参考值，系统会根据实际动态负载在此值基础上进行调整。
需要小于分裂阈值的1/3，防止出现合并、分裂的循环出现。

``` 
void SetMergeSize(int64_t size);
int64_t MergeSize() const;
```
#### 3.6 Write Ahead Log
##### (1) 配置日志开关，默认打开
当此表格数据没有强特久化需求时，可以选择关闭日志。
会大幅提升写性能、降低系统IO消耗。
当有服务器宕机时，内存中数据将丢失，谨慎关闭。

``` 
void DisableWal();
bool IsWalDisabled() const;
```
#### 3.7 事务
##### (1) 事务处理

```
void EnableTxn();
bool IsTxnEnabled() const;
```
#### 3.8 Admin
##### (1) 设置表格的admin

```
void SetAdmin(const std::string& name);
std::string Admin() const;
void SetAdminGroup(const std::string& name);
std::string AdminGroup() const;
```
