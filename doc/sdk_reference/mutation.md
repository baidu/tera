
# RowMutation接口说明
tera sdk中通过RowMutation结构描述一次行更新操作，包含删除操作。
 
## 1. 数据结构
```
    enum Type {  
        kPut,
        kDeleteColumn,
        kDeleteColumns,
        kDeleteFamily,
        kDeleteRow,
        kAdd,
        kPutIfAbsent,
        kAppend,
        kAddInt64
    };
    struct Mutation {
        Type type;
        std::string family;
        std::string qualifier;
        std::string value;
        int64_t timestamp;
        int32_t ttl;
    };
```

## 2. 主要接口与用法
#### 2.1 更新
<style type="text/css">
table th:first-of-type {
    width: 10%;
}
table th:nth-of-type(2) {
    width: 10%;
}
table th:nth-of-type(3) {
    width: 5%;
}
table th:nth-of-type(4) {
    width: 50%;
}
table th:nth-of-type(5) {
    width: 10%;
}
table th:nth-of-type(6) {
    width: 5%;
}
</style>

表格类型 | 接口功能 | 接口 | 参数 | 可省参数 | 返回值类型 | 其它说明
---  | ---    | ---  | --- | ---  | --- | ---
表格模式 | 修改一个列 | Put | const std::string& family, const std::string& qualifier, const int64_t value, int64_t timestamp | timestamp可省，省略时为－1 | void | Counter场景下使用，设定初始值。
表格模式 | 修改一个列的特定版本 | Put | const std::string& family, const std::string& qualifier, const std::string& value, int64_t timestamp| timestamp可省，省略时为－1  | void | 若设定timestamp，数据会被更新至指定时间，危险，不建议使用
表格模式 | 修改一个带TTL列的特定版本 | Put | const std::string& family, const std::string& qualifier, int64_t timestamp, const std::string& value, int32_t ttl | | void |
表格模式 | 修改一个列的特定版本 | Put | const std::string& family, const std::string& qualifier, int64_t timestamp, const std::string& value | | void |
表格模式 | 原子操作：如果不存在才能Put成功 | PutIfAbsent | const std::string& family, const std::string& qualifier, const int64_t delta | | void |若不存在，更新生效；否则更新数据不生效。delta可为负数。
表格模式 | 原子加一个Cell | Add | const std::string& family, const std::string& qualifier, const int64_t delta | | void  | Counter场景下使用，累加。若无初始值，会从0开始累加
表格模式 | 原子加一个Cell | Append | const std::string& family, const std::string& qualifier, const std::string& value | | void | 将value追加至此列原数据末尾；若原数据不存在，则与Put等效。
k-v模式 |修改带TTL的默认列 | Put | const std::string& value, int32_t ttl | ttl 可省，默认为－1 | void |若设定ttl，数据会在ttl时间超时后被淘汰。
 
#### 2.2 删除
##### (1) 删除整行  RowMutation::DeleteRow
删除整行的指定范围版本。
```
void DeleteRow(int64_t timestamp = -1) = 0;//若设定timestamp，则删除此时间之前的所有更新。 Key-value模式下timestamp不生效。
```
 
##### (2) 删除某列族  RowMutation::DeleteFamily
删除一个列族的所有列的指定范围版本。
```
void DeleteFamily(const std::string& family, int64_t timestamp = -1) = 0;//若设定timestamp，则删除此时间之前的所有更新。
```
 
##### (3) 删除某列所有版本  RowMutation::DeleteColumns
删除一个列的指定范围版本。
```
void DeleteColumns(const std::string& family, const std::string& qualifier, int64_t timestamp = -1) = 0;//若设定timestamp，则删除此时间之前的所有更新。
```
 
##### (4) 删除一个列的指定版本  RowMutation::DeleteColumn
```
void DeleteColumn(const std::string& family, const std::string& qualifier, int64_t timestamp) = 0;//若不存在，则不生效。
```


#### 2.3 错误码
##### (1) 行更新错误码  RowMutation::ErrorCode
```
const ErrorCode& GetError() = 0; //成功返回KOK
```
##### (2) 设置错误码 RowMutation::SetError
```
void SetError(ErrorCode::ErrorCodeType err, const std::string& reason) = 0;
```
#### 2.4 异步
若设定回调，则异步提交；否则同步提交。
##### (1) 设置回调  RowMutation::SetCallBack
 
设置异步回调, 操作会异步返回。
```
void SetCallBack(Callback callback) = 0;
```

##### (2) 获得回调函数  RowMutation::GetCallBack
```
Callback GetCallBack() = 0;
```
 
#### 2.5 上下文设定
##### (1) 设置上下文  RowMutation::SetContext
设置用户上下文，可在回调函数中获取。
```
void SetContext(void* context) = 0;
```
 
##### (2) 获取用户上下文  RowMutation::GetContext
```
void* GetContext() = 0;
```
#### 2.6 超时设定 
设定单个mutation的超时时间。 如没有特殊需要，不必单独设定，使用sdk的统一超时即可。
##### (1) 设置超时时间  RowMutation::SetTimeOut
 
设置超时时间(只影响当前操作,不影响Table::SetWriteTimeout设置的默认写超时)
```
void SetTimeOut(int64_t timeout_ms) = 0;
```
 
##### (2) 超时  RowMutation::TimeOut
```
int64_t TimeOut() = 0
```
 #### 2.7 其他操作 
##### (1) 获取行更新的操作数  RowMutation::MutationNum
```
uint32_t MutationNum() = 0;
```
 
##### (2) 获取mutation总大小  RowMutation::Size
```
uint32_t Size() = 0;
```
 
##### (3) 返回row_key  RowMutation::RowKey
```
const std::string& RowKey() = 0;
```
 
##### (4) 返回mutation  RowMutation::GetMutation
```
const RowMutation::Mutation& GetMutation(uint32_t index) = 0;
```

