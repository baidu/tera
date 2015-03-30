teracli命令行工具使用手册
---

## 表格创建 (create)

`create   <tablename> <tableschema>`

1. tablename不要超过256个字符，合法字符包括：数字、大小写字母、下划线`_`、连字符`-`，
首字符不能为数字。

1. tableschema字符串请用引号括起来，避免`#`等特殊符号被shell解释。

1. tableschema详解
    
`schema syntax:
        {tableprops}#lg1{lgprops}:cf1{cfprops},cf2..|lg2...
        (all properties are optional)`

span | 属性名 | 意义 | 有效取值 | 其它说明
---  | ---    | ---  | ---      | ---
table | rawkey | rawkey的拼装模式 | readable：性能较高，但不允许包含`\0`。binary：性能稍差，允许所有字符 | 
table | splitsize | 某个tablet增大到此阈值时分裂为2个子tablets| >=0，等于0时关闭split | 
table | mergesize | 某个tablet减小到此阈值时和相邻的1个tablet合并 | >=0，等于0时关闭merge | splitsize至少要为mergesize的5倍
lg    | storage   | 存储类型 | "disk" / "flash" / "memory" | 
lg    | compress  | 压缩算法 | "snappy" / "none"
lg    | blocksize | LevelDB中block的大小       | >0 | 
lg    | use_memtable_on_leveldb | ? | "true" / "false" | 
lg    | memtable_ldb_write_buffer_size | ? | >0 |
lg    | memtable_ldb_block_size | ? | >0 |
lg    | sst_size  | 第一层sst文件大小 | >0 | 
cf    | maxversions | 保存的最大版本数  | >0 | 
cf    | minversions | 保存的最小版本数 | >0 |
cf    | diskquota   | 存储限额  | >0 |
cf    | ttl | 数据有效时间 | >=0，等于0时此数据永远有效 |


例如：

创建一个名为`books`的表格，设置2个lg，一个名为`lg0`，包含1个cf；另一个lg名为`lg1`，包含2个cf:

`% ./teracli create books "#lg0:cf|lg1:cf0,cf1"`

如果需要，可以给table、lg或者cf添加需要的属性：

`% ./teracli create books "{splitsize=1024,mergesize=128}#lg0{storage=disk,compress=snappy}:cf{maxversions=3}|lg1:cf0{minversions=5},cf1"`


## 表格schema更新-全量更新 (update)

`update  <tablename> <tableschema>`

1. schema的语法必须和create表格时一致。
tera会用新的schema覆盖原有schema.
更新schema前需要先disable表格。

1. tablename和tableschema的约束和`ctreate`一致。

## 表格schema更新-增量更新 (update-part)

适用于只需更新少数几个属性的场景，
建表时未指定的属性值，也可以用`update-part`在建表完成后设定。

`update-part <tablename> <part-schema>`

part-schema 语法:

    "prefix1:property1=value1, prefix2:property2=value2, ..."

prefix: 如果更新的是表格的属性，则取值"table"；如果更新的是lg属性，则取值为lg名字，cf与lg同理。

property: 待更新属性名，例如： splitsize | compress | ttl | ...

value:    待更新属性的新值。

每次可以更新一个或者多个属性，它们之间用逗号分隔。

例1： "table:splitsize=9, lg0:compress=none, cf3:ttl=0"

例2： "lg0:use_memtable_on_leveldb=true"

