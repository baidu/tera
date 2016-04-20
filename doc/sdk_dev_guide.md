# Tera SDK 主要api使用方法

>**目录**
>  1. [主要数据结构](#main-data-structure)
>   * tera::Client、tera::Table
>   * RowMutation、RowReader、ScanDescriptor/ResultStream
>   * TableDescriptor、LocalityGroupDescriptor、ColumnFamilyDescriptor
>  2. [主要功能及代码示例](#sample-code)
>   * [表格管理](#table-management)
>     * 表格描述符
>     * 表格操作
>   * [数据操作（同步、异步、批量）](#data-management)
>     * 读
>     * 写
>     * 扫描
>  3. [Java SDK](https://github.com/baidu/tera/blob/master/doc/sdk_dev_guide_for_java.md)
>  4. [Python SDK](https://github.com/baidu/tera/blob/master/doc/sdk_dev_guide_for_python.md)

<a name="main-data-structure"></a>

# 1. 主要数据结构

#### (1) tera::Client 访问tera服务主结构

所有对tera的访问或操作全部由此发起。

功能包括：

1. 表格操作：建、删、加载、卸载、打开、关闭、更新表结构、获取表格信息、快照等
2. 用户管理：建、删、修改密码、组管理等
3. 集群信息获取：获取全部表格列表、状态等

使用建议：

* 一个集群对应一个Client即可，如需访问多个Client，需要创建多个

#### (2) tera::Table 表格主结构

对表格的所有增删查改操作由此发起。

由`tera::Client::OpenTable`产生，`tera::Client::CloseTable`关闭，不可析构。

#### (3) tera::RowReader 随机行读取

由`tera::Table NewRowReader()`产生，通过`tera::Table Get()`生效。

支持同步、异步、批量读取，需要用户析构。

可同时读多列，支持按时间戳、版本等过滤。

迭代器方式使用，或返回一个columnfamily/qualifier索引的多级Map。

#### (4) tera::RowMutation 行插入/更新/删除

由`tera::Table NewRowMutation()`产生，通过`tera::Table ApplyMutation()`生效。

支持同步、异步、批量操作，需要用户析构。

可同时插入/更新/删除多列，原子操作，操作服务端生效时序与客户端相同。

支持Counter、Append、PutIfAbsent等特殊操作。

#### (5) tera::ScanDescriptor / tera::ResultStream 遍历

`tera::ScanDescriptor`描述遍历需求，包括行区间、列集合、时间区间、版本、特殊过滤器等。

由`tera::Table Scan()`生效，返回迭代器`tera::ResultStream`。

#### (6) 表格描述

包含`tera::TableDescriptor / tera::LocalityGroupDescriptor / tera::ColumnFamilyDescriptor`

建表及更新表格结构时使用，通过这些描述符定义表格的各种结构及其属性。

#### (7) tera::ErrorCode

很多操作会返回，注意检查。

<a name="sample-code"></a>
# 2. 主要功能及代码示例

```
// tera的统一操作，可以传入指定配置，默认会依次从./tera.flag、../conf/tera.flag、TERA_CONF环境变量查找
tera::ErrorCode error_code;
tera::Client* client = tera::Client::NewClient("./tera.flag", &error_code);
if (client == NULL) {
}
...
delete client;                        // 注意回收内存
```

<a name="table-management"></a>
## 表格管理

#### 表格描述符

```
// create 名为 hello 的表格，包含一个LocalityGroup：lg_1，lg_1 中有一个ColumnFamily：cf_11
tera::TableDescriptor table_desc("hello");

// lg_1 设置为flash存储、block大小为 8K
tera::LocalityGroupDescriptor* lg_desc = table_desc->AddLocalityGroup(“lg_1”);
lg_desc->SetStore(tera::kInFlash);
lg_desc->SetBlockSize(8);

// cf_11 设置为最大版本数5，ttl为10000s
tera::ColumnFamilyDescriptor* cf_t = table_desc->AddColumnFamily("cf_11", "lg_1");
cf_t->SetMaxVersions(5);
cf_t->SetTimeToLive(10000);
```

#### 表格操作
```
// 建、删、加载、卸载
client->CreateTable(table_desc, &error_code)
client->DisableTable("hello", &error_code);
client->EnableTable("hello", &error_code);
client->DeleteTable("hello", &error_code);

// 获取表格描述符
tera::TableDescriptor* hello_desc = client->GetTableDescriptor("hello", &error_code);
...
delete hello_desc;    // 注意清理内存

// 获取表格状态
if (client->IsTableExist("hello", &error_code)) {
        ...
}
if (client->IsTableEnable("hello", &error_code)) {
        ...
}
if (client->IsTableEmpty("hello", &error_code)) {
        ...
}

// 获取表格 schema、tablet 信息
tera::TableInfo table_info = {NULL, ""};
std::vector<tera::TabletInfo> tablet_list;
client->List("hello", &table_info, &tablet_list, &error_code);
```

<a name="data-management"></a>
## 数据操作

```
// 打开表格，不需析构
tera::Table* table =  client->OpenTable("hello", &error_code);
```

#### 读
```
// 同步单条读出数据（简单，性能较低）
std::string value;
table->Get(“rowkey1”, “columnfamily1”, “qualifier1”, &value, &error_code);

// RowReader（多列读取，支持同步、异步、批量）
tera::RowReader* reader = table->NewRowReader(“rowkey2”);
reader->AddColumn("family21", "qualifier21");
reader->AddColumnFamily("family22");
...
reader->SetCallBack(BatchGetCallBack);                   //若不设定回调，则为同步读取
table->Get(reader);
...
while (!table->IsGetFinished());                         // 如使用异步读取，等待后台读取完成后退出
delete reader;                                           // 注意回收内存，若异步，可在回调中回收

// 批量读取
std::vector<tera::RowReader*> readers;
tera::RowReader* reader = table->NewRowReader(“rowkey2”);
...
readers.push_back(reader);
...
table->Get(readers);
...
for (size_t i = 0; i < readers.size(); ++i) {
    delete readers[i];
}
```

#### 写、删

```
// 同步单条写入数据（简单，性能较低）
table->Put("rowkey1", "columnfamily1", "qualifier1", "value11", &error_code);

// RowMutation 增删（保证多列原子操作，支持同步、异步、批量）
tera::RowMutation* mutation = table->NewRowMutation("rowkey2");
mutation->Put("family21", "qualifier21", "value21");
mutation->Put("family22", "qualifier22", "value22");
mutation->DeleteFamily("family11");
mutation->DeleteColumns("family22", "qualifier22");
...
mutation->SetCallBack(CallBack);                         // 若不设定回调，则为同步写入
table->ApplyMutation(mutation);                          // 同步写入会阻塞
...
while (!table->IsPutFinished());                         // 如使用异步，等待后台写入完成后退出
delete mutation;                                         // 注意回收内存，若异步，可在回调中回收

// 批量
std::vector<tera::RowMutation*> mutations;
tera::RowMutation* mutation = table->NewRowMutation(“rowkey2”);
...
mutations.push_back(mutation);
...
table->ApplyMutation(mutations);
...
for (size_t i = 0; i < mutations.size(); ++i) {
    delete mutations[i];
}
```

#### 扫描

```
// 闭开区间
tera::ScanDescriptor desc(“rowkey1”);                    // scan 开始KEY，为空，则从表头开始scan
desc.SetEnd("rowkey2");                                  // 结束行KEY，不包含rowkey2，为""，则至表尾
desc->AddColumnFamily("family21");                       // 返回此cf中数据，若不设，返回所有列
desc->AddColumn("family22", "qualifier22");
...
tera::ResultStream* result_stream = table->Scan(desc, &error_code);
while (!result_stream->Done()) {
        ...
        result_stream->Next();
}
delete result_stream;                                    // 注意回收内存
```
