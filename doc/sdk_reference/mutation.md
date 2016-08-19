# RowMutation

tera sdk中通过RowMutation结构描述一次行更新操作，包含删除操作。
一个RowMutaion中可以同时对多列进行操作，保证：
 * 服务端生效时序与RowMutation的执行时序相同。比如对某列的删除+更新，服务端生效时不会乱序，导致先更新再删除的情况发生。
 * 同一个RowMutation中的操作保证同时成功或失败。
 * 操作不存在的列族会返回成功，但无法读取。

## 创建与析构

由tera::Table::NewRowMutation创建，不能由用户创建。

用户需要自行析构：
 * 同步模式下Put返回后即可析构
 * 异步模式下需要等待回调返回，并处理完成后析构，建议在回调函数末尾进行析构
 
## API

### 更新

Key-value模式更新。若设定ttl，数据会在ttl时间超时后被淘汰。
```
void Put(const std::string& value, int32_t ttl = -1);
```
表格模式更新。若设定timestamp，数据会被更新至指定时间，危险，不建议使用。
```
void Put(const std::string& family, const std::string& qualifier, const std::string& value, int64_t timestamp = -1);
```
表格模式更新。Counter场景下使用，设定初始值。
```
void Put(const std::string& family, const std::string& qualifier, int64_t value, int64_t timestamp = -1);
```
表格模式更新。Counter场景下使用，累加。若无初始值，会从0开始累加。
```
void Add(const std::string& family, const std::string& qualifier, const int64_t delta);
```
表格模式更新。若不存在，更新生效；否则更新数据不生效。
```
void PutIfAbsent(const std::string& family, const std::string& qualifier, const std::string& value);
```
表格模式更新。将value追加至此列原数据末尾；若原数据不存在，则与Put等效。
```
void Append(const std::string& family, const std::string& qualifier, const std::string& value);
```

### 删除

删除整行。若设定timestamp，则删除此时间之前的所有更新。
Key-value模式下timestamp不生效。
```
void DeleteRow(int64_t timestamp = -1);
```
删除某列族。若设定timestamp，则删除此时间之前的所有更新。
```
void DeleteFamily(const std::string& family, int64_t timestamp = -1);
```
删除某列所有版本。若设定timestamp，则删除此时间之前的所有更新。
```
void DeleteColumns(const std::string& family, const std::string& qualifier, int64_t timestamp = -1);
```
删除某列指定时间更新。若不存在，则不生效。
```
void DeleteColumn(const std::string& family, const std::string& qualifier, int64_t timestamp);
```

### 异步

若设定回调，则异步提交；否则同步提交。
```
typedef void (*Callback)(RowMutation* param);
void SetCallBack(Callback callback);
Callback GetCallBack();
bool IsAsync(); 
```

### 超时设定

设定单个mutation的超时时间。
如没有特殊需要，不必要单独设定，使用sdk的统一超时即可。
```
void SetTimeOut(int64_t timeout_ms);
int64_t TimeOut() = 0;
```

### 上下文设定

用于回调中获取用户自定义上下文信息。
内存由用户自己管理。

```
void SetContext(void* context);
void* GetContext();
```

### 其它

```
uint32_t MutationNum();
uint32_t Size();
const RowMutation::Mutation& GetMutation(uint32_t index);
```

### 预发布

获取所属事务
```
Transaction* GetTransaction();
```
