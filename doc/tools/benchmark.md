
## 1. tera_bench 
造数据的工具
### (1) 用法
```
./tera_bench --compression_ratio=1 --key_seed=1 --value_seed=20  --value_size=1000 --num=200000 --benchmarks=random  --key_size=24 --key_step=1
```
 
## 2. tera_mark   
读写数据,支持异步读写scan

### (1) 用法
```
#示例：
./tera_mark --mode=w --tablename=test --type=async  --verify=false --entry_limit=1000
```

### (2) 参数列表

参数名 | 意义 | 有效取值 | 单位 | 默认值 | 其它说明
---    | ---  | ---      | ---  | ---    | ---
table | 表名 | - | - | "" |
mode | 模式 | "w"/"r"/"s"/"m" | - | "w" | -
type | 类型 | "sync"/"async" | - | "async" | -
pend_size | 最大pending大小 | - | - | 100 | -
pend_count | 最大pending数 | - | - | 100000 | -
start_key | scan的开始key | - | - | "" | -
end_key | scan的结束key | - | - | "" | -
cf_list | scan的列簇 | -  | - | "" | -
print | scan的结果是否需要打印 | true/false | - | false | -
buf_size | scan的buffer_size | >0  | - | 65536 | -
verify | md5 verify(writer&read) | true/false  | - | true | -
max_outflow | max_outflow | -  | - | -1 | -
max_rate | max_rate | - | - | -1 | -
scan_streaming | enable streaming scan | true/false  | - | false | -
batch_count | batch_count(sync) | - | - | 1 | -
entry_limit | writing/reading speed limit | - | - | 0 | -

