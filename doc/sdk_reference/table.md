
# Table接口说明

## 1. 主要数据结构
#### 1. 表格信息
```
struct TableInfo {
    TableDescriptor* table_desc; //表的描述符
    std::string status; //表格状态信息
};
```
#### 2. tablet信息
```
struct TabletInfo {
    std::string table_name; //表名
    std::string path; //路径
    std::string server_addr; //服务器地址
    std::string start_key; //起始key
    std::string end_key;  //结束key
    int64_t data_size; //数据大小
    std::string status; //状态
};
```
 
## 2. 主要接口
##### (1) 获取表名  Table::GetName
```
const std::string GetName() = 0
```
 
##### (2) 行mutation操作 Table::NewRowMutation
```
RowMutation* NewRowMutation(const std::string& row_key) = 0
```
##### (3) 写数据 Table::Put
```
1) void Put(RowMutation* row_mutation) = 0
2) void Put(const std::vector<RowMutation*>& row_mutations) = 0
3) bool Put(const std::string& row_key, const std::string& family, const std::string& qualifier, const std::string& value, ErrorCode* err) = 0
4) bool Put(const std::string& row_key, const std::string& family, const std::string& qualifier, const int64_t value, ErrorCode* err) = 0;
5) bool PutIfAbsent(const std::string& row_key, const std::string& family, const std::string& qualifier, const std::string& value, ErrorCode* err) = 0;
```
 
##### (4) 检查写数据是否结束 Table::IsPutFinished
 
```
bool IsPutFinished() = 0
```

##### (5) 添加数据 Table::Add
 
```
bool Add(const std::string& row_key, const std::string& family, const std::string& qualifier, int64_t delta, ErrorCode* err) = 0;
```
 
##### (6) 追加数据 Table::Append
 
```
bool Append(const std::string& row_key, const std::string& family, const std::string& qualifier, const std::string& value, ErrorCode* err) = 0;
```
 
##### (7) 按行读数据 Table::NewRowReader
 
```
RowReader* NewRowReader(const std::string& row_key) = 0
```
 
##### (8) 读数据 Table::Get
 
```
1) void Get(RowReader* row_reader) = 0
2) void Get(const std::vector<RowReader*>& row_readers) = 0;
3) bool Get(const std::string& row_key, const std::string& family, const std::string& qualifier, std::string* value, ErrorCode* err) = 0;
4) bool Get(const std::string& row_key, const std::string& family, const std::string& qualifier, int64_t* value, ErrorCode* err) = 0;
```
 
##### (9) 检查get是否结束 Table::IsGetFinished
```
bool IsGetFinished() = 0;
```
 
##### (10) 扫描 Table::Scan
```
ResultStream* Scan(const ScanDescriptor& desc, ErrorCode* err) = 0
```
##### (11)  按行事务处理 Table::StartRowTransaction
```
Transaction* StartRowTransaction(const std::string& row_key) = 0
``` 
 
##### (12) 提交行事务 Table::CommitRowTransaction
```
void CommitRowTransaction(Transaction* transaction) = 0
```

##### (13)  执行mutation Table::ApplyMutation
```c
void ApplyMutation(RowMutation* row_mu) = 0;
void ApplyMutation(const std::vector<RowMutation*>& row_mu_list) = 0;
```
