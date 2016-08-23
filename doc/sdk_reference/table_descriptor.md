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

## ColumnFamilyDescriptor

## 字符串描述
