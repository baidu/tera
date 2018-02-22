
# 单行事务transaction接口说明

## 主要功能
 

##### (1) 提交一个修改操作  Transaction::ApplyMutation
```
void ApplyMutation(RowMutation* row_mu) = 0
```
 
##### (2) 读取操作 Transaction::Get
```
ErrorCode Get(RowReader* row_reader) = 0
```
##### (3) 回调函数原型 Transaction::Callback
```
typedef void (*Callback)(Transaction* transaction)
```
 
##### (4) 设置提交回调, 提交操作会异步返回 Transaction::SetCommitCallback
 
```
void SetCommitCallback(Callback callback) = 0;
```

##### (5) 获取提交回调 Transaction::GetCommit
 
```
Callback GetCommitCallback() = 0;
```
 
##### (6) 设置用户上下文，可在回调函数中获取 Transaction::SetContext
 
```
void SetContext(void* context) = 0;
```
 
##### (7) 获取用户上下文 Transaction::GetContext
 
```
void* GetContext() = 0
```
 
##### (8) 获得结果错误码 Transaction::GetError
 
```
const ErrorCode& GetError() = 0; // 异步模式下，通过GetError()获取提交结果
```
 
##### (9) 同步模式下，获得提交的结果 Transaction::Commit
```
ErrorCode Commit() = 0 // 同步模式下，Commit()的返回值代表了提交操作的结果(成功 或者 失败及其原因)
```
 
##### (10) 获取事务开始时间戳 Transaction::GetStartTimestamp
```
int64_t GetStartTimestamp() = 0 //仅在全局事务场景下有效
```

