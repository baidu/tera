# YCSB for Tera

### YCSB参数
重要参数：

```
    数据行数
    recordcount: number of records to load into the database initially (default: 0)
    
    操作数
    operationcount: number of operations to perform.
    
    每行的qualifier个数
    fieldcount: the number of fields in a record (default: 10)
    
    value长度
    fieldlength: the size of each field (default: 100)
    
    随机读的数据分布
    requestdistribution: what distribution should be used to select the records to operate on – uniform, zipfian or latest (default: uniform)
    
    写入顺序，ordered是顺序写，hashed是随机写
    insertorder: should records be inserted in order by key (“ordered”), or in hashed order (“hashed”) (default: hashed)
    
    读取所有qualifier还是只读一个qualifier
    readallfields: should reads read all fields (true) or just one (false) (default: true)
    
    随机读占所有操作的比例
    readproportion: what proportion of operations should be reads (default: 0.95)
    
    更新（写入）占所有操作的比例
    updateproportion: what proportion of operations should be updates (default: 0.05)
```
以下参数对于tera的测试意义不大，使用默认值即可：

```
    插入（写入）占所有操作的比例
    insertproportion: what proportion of operations should be inserts (default: 0)
    
    scan占所有操作的比例，tera_mark不支持
    scanproportion: what proportion of operations should be scans (default: 0)
    
    readmodifywrite占所有操作的比例，tera不支持该操作
    readmodifywriteproportion: what proportion of operations should be read a record, modify it, write it back (default: 0)
    
    每次scan需要读取的行数，tera不支持指定行数的scan
    maxscanlength: for scans, what is the maximum number of records to scan (default: 1000)
    
    scan的行数选择策略
    scanlengthdistribution: for scans, what distribution should be used to choose the number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
    
    最大执行时间，超过此时间会强行结束测试
    maxexecutiontime: maximum execution time in seconds. The benchmark runs until either the operation count has exhausted or the maximum specified time has elapsed, whichever is earlier.
    
    表名，tera_mark不支持
    table: the name of the table (default: usertable)
  ```
### YCSB4tera使用方法
1. 建表

  ycsb的生成的row都是“user”+19位数字的格式，如 user9105318085603802964。
  因此，如果需要预分表，必须以“user”+N个数字作为分隔，建议选择2个数字。
  例如要预分4个tablet，分隔字符串为：user25、user50、user75
2. 向tera加载测试数据
  
  ```
  bin/ycsb load tera -p workload=com.yahoo.ycsb.workloads.CoreWorkload -p recordcount=$(ROW_NUM) \
                     -p fieldlength=$(QUALIFIER_NUM) -p fieldcount=$(VALUE_SIZE)
  ```
3. 执行测试

  ```
  bin/ycsb run tera -p workload=com.yahoo.ycsb.workloads.CoreWorkload -p recordcount=$(ROW_NUM) \
                    -p operationcount=$(ROW_NUM) -p requestdistribution=$(DIST) \
                    -p fieldlength=$(QUALIFIER_NUM) -p fieldcount=$(VALUE_SIZE) \
                    -p updateproportion=$(WRITE_PROP) -p readproportion=$(READ_PROP)
  ```

    
