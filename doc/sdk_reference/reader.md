
# Reader接口说明
tera sdk中通过RowReader结构描述一次行读取操作，并获取返回数据。

## 1. 主要接口与用法
#### 1.1 描述过滤条件
通过相关的API可以对列名、更新时间、版本数目等信息描述，从而对返回数据集合进行过滤。如果不进行任何描述，默认返回此行所有数据。
##### (1) 可以增加多个列族  RowReader::AddColumnFamily
```
void AddColumnFamily(const std::string& family) = 0;//如此“family”不存在于表格的schema中，则不进行过滤
```
 
##### (2) 可以增加多个列 RowReader::AddColumn
```
void AddColumn(const std::string& family, const std::string& qualifier); //除限定返回数据列族为“family”外，其列名必须为“qualifier”。
```
 
##### (3) 设定最大版本数  RowReader::SetMaxVersions
```
void SetMaxVersions(uint32_t max_version) = 0; //从最新版本开始计数，若实际数据版本数小于此值，全部返回。在最大版本数基础上再进行时间过滤。
```
 
##### (4) 设定返回数据的更新时间范围  RowReader::SetTimeRange
```
void SetTimeRange(int64_t ts_start, int64_t ts_end) = 0;//只返回更新时间在[ts_start, ts_end]范围内的数据。其中ts_start、ts_end均为Unix时间戳，单位为微秒（us）。
```
 
#### 1.2 获取数据
在RowReader被提交至服务端并返回后，可以从此结构中获取返回的数据。
支持两种获取方式：
<ul>
<li>迭代器方式。依次遍历所有列、所有版本。</li>
<li>全量输出。返回一个特定结构的std::Map，可按列名等信息进行访问。</li>
</ul>

##### (1) 访问数据前通过Done进行确认  RowReader::Done
```
bool Done() = 0;;//若返回false，则数据已遍历完毕。

```
 
##### (2) 访问数据前通过Next进行确认  RowReader::Next
```
void Next() = 0;
```
 
##### (3) 当数据存在时，可以通过以下接口访问此单元格的各字段值
当通过RowReader访问key-value模式的表时，除RowKey和Value外，其它字段值无效。
```
const std::string& RowKey();
std::string Value();
std::string Family() = 0;
std::string Qualifier() = 0;
int64_t Timestamp() = 0;
```
 
##### (4) 全量输出
通过多级std::map的形式进行访问。
```
typedef std::map<int64_t, std::string> TColumn;
typedef std::map<std::string, TColumn> TColumnFamily;
typedef std::map<std::string, TColumnFamily> TRow;
virtual void ToMap(TRow* rowmap);
```

#### 1.3 错误码
##### (1) 获取错误码  RowReader::ErrorCode
```
const ErrorCode& GetError() = 0; //成功返回KOK
```
#### 1.4 异步
若设定回调，则异步提交；否则同步提交。
##### (1) 设置回调  RowReader::SetCallBack
```
void SetCallBack(Callback callback) = 0;
```

##### (2) 设置回调  RowReader::GetCallBack
```
void (*Callback)(RowReader* param);
```

#### 1.5 上下文设定
用于回调中获取用户自定义上下文信息。 内存由用户自己管理。

##### (1) 设置上下文  RowReader::SetContext
```
void SetContext(void* context) = 0;
```
 
##### (2) 获取上下文  RowReader::GetContext
```
void* GetContext() = 0;
```
#### 1.6 超时设定 
设定单个reader的超时时间。如没有特殊需要，不必要单独设定，使用sdk的统一超时即可。
##### (1) 设置超时时间  RowReader::SetTimeOut
```
void SetTimeOut(int64_t timeout_ms) = 0;
```
 
#### 1.7 其他
##### (1) 获取表格  RowReader::GetTable
```
Table* GetTable() = 0;
```
 
##### (2) 获取按列过滤的map
```
typedef std::map<std::string, std::set<std::string> >ReadColumnList;
const ReadColumnList& GetReadColumnList() = 0;
```
