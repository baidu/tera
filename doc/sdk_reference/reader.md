# RowReader

tera sdk中通过RowReader结构描述一次行读取操作，并获取返回数据。

## 创建与析构

由tera::Table::NewRowReader创建，不能由用户创建。

用户需要自行析构：
 * 同步模式下Get返回后即可析构
 * 异步模式下需要等待回调返回，并处理完成后析构，建议在回调函数末尾进行析构
 
## API

### 描述过滤条件

通过相关的API可以对列名、更新时间、版本数目等信息描述，从而对返回数据集合进行过滤。

如果不进行任何描述，默认返回此行所有数据。

#### AddColumnFamily

```
void AddColumnFamily(const std::string& family);
```

限定返回数据的列族为“family”。

可以增加多个列族。

如此“family”不存在于表格的schema中，则不进行过滤。

#### AddColumn

```
void AddColumn(const std::string& family, const std::string& qualifier);
```

与AddColumnFamily类似，除限定返回数据列族为“family”外，其列名必须为“qualifier”。

此操作与AddColumnFamily共同生效，返回数据为二者并集。

#### SetTimeRange

```
void SetTimeRange(int64_t ts_start, int64_t ts_end);
```

设定返回数据的更新时间范围。

只返回更新时间在[ts_start, ts_end]范围内的数据。

其中ts_start、ts_end均为Unix时间戳，单位为微秒（us）。

#### SetMaxVersions

```
void SetMaxVersions(uint32_t max_version);
```

设定最大版本数。

从最新版本开始计数，若实际数据版本数小于此值，全部返回。

过滤优先级高于TimeRange，即在最大版本数基础上再进行时间过滤。

### 获取数据

在RowReader被提交至服务端并返回后，可以从此结构中获取返回的数据。

支持两种获取方式：

 * 迭代器方式。依次遍历所有列、所有版本。
 * 全量输出。返回一个特定结构的std::Map，可按列名等信息进行访问。

#### 迭代器方式

```
bool Done();
void Next();
```

访问数据前通过Done()进行确认。

若返回false，则数据已遍历完毕。

```
const std::string& RowKey();
std::string Value();
std::string Family();
std::string Qualifier();
int64_t Timestamp();
```

当数据存在时，可以通过这些接口访问此单元格的各字段值。

当通过RowReader访问key-value模式的表时，除RowKey和Value外，其它字段值无效。

#### 全量输出

```
typedef std::map<int64_t, std::string> TColumn;
typedef std::map<std::string, TColumn> TColumnFamily;
typedef std::map<std::string, TColumnFamily> TRow;
virtual void ToMap(TRow* rowmap);
```

通过多级std::map的形式进行访问。

### 异步与上下文设定

若设定回调，则异步提交；否则同步提交。
```
typedef void (*Callback)(RowMutation* param);
void SetCallBack(Callback callback);
Callback GetCallBack();
```

用于回调中获取用户自定义上下文信息。
内存由用户自己管理。

```
void SetContext(void* context);
void* GetContext();
```

### 超时设定

设定单个reader的超时时间。
如没有特殊需要，不必要单独设定，使用sdk的统一超时即可。
```
void SetTimeOut(int64_t timeout_ms);
int64_t TimeOut() = 0;
```

### 预发布

获取所属事务
```
Transaction* GetTransaction();
```
