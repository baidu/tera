
# scan接口说明
tera中scan操作由ResultStream和ScanDescriptor两个数据结构进行描述。
### 1. ResultStream

##### (1) 检查迭代是否结束
```
bool Done(ErrorCode* err = NULL) = 0; //如果检查失败则返回error code。
```

##### (2) 移到下一个cell

```
void Next() = 0;
```

##### (3) 获取当前cell的rowkey名字
```
std::string RowName() const = 0;
```
##### (4) 获取当前cell的簇
```
std::string Family() const = 0;
```
 
##### (5) 获取当前cell的列
```
std::string Qualifier() const = 0;
```
 
##### (6) 返回时间戳
```
int64_t Timestamp() const = 0;
```
 
##### (7) 返回当前cell的值
```
std::string Value() const = 0;
int64_t ValueInt64() const = 0;
```

### 2. ScanDescriptor
 
##### (1) 设置扫描的结束key
```
void SetEnd(const std::string& rowkey);
```

##### (2) 设置扫描的目标cf

```
void AddColumnFamily(const std::string& cf);
```

##### (3) 设置扫描的目标列
```
 void AddColumn(const std::string& cf, const std::string& qualifier);
```  
##### (4) 设置每列的maxversion
```
void SetMaxVersions(int32_t versions);
```
 
##### (5) 设置每个扫描结果的时间范围
```
void SetTimeRange(int64_t ts_end, int64_t ts_start);
```
 
##### (6) 设置批量扫描模式
```
void SetAsync(bool async);
```
 
##### (7) 检查扫描是否为批量扫描模式
```
bool IsAsync() const;
```
 
##### (8) 设置扫描的超时时间
```
void SetPackInterval(int64_t timeout);
```
 
##### (9) 设置扫描的buffersize
```
void SetBufferSize(int64_t buf_size);//默认为64K
```
 
##### (10) 设置每次扫描的cell数
```
void SetNumberLimit(int64_t number_limit);
```
 
##### (11) 获取每次扫描的cell数
```
int64_t GetNumberLimit();
```

