teracli命令行工具使用手册
---

## 表格创建 (create)

### 命令格式

    ./teracli  create        <table-schema>  [<tablet-delimiter-file>]
    ./teracli  createbyfile  <schema-file>   [<tablet-delimiter-file>]

其中，table-schema是一个描述表格结构的字符串,语法详见[PropTree](https://github.com/BaiduPS/tera/blob/master/doc/prop_tree.md)

Tera支持在建立表格时预分配若干tablet，tablet分隔的key写在tablet-delimiter-file中，按“\n”分隔。

如果表格schema比较复杂，可以将其写入文件中，通过createbyfile命令进行创建。

### 创建表格模式存储

表格结构中包含表名、locality groups定义、column families定义，一个典型的表格定义如下（可写入文件）：
    
    # 二进制编码的key, tablet分裂阈值为4096M，合并阈值为512M
    # 三个lg，分别配置为内存、flash、磁盘存储
    table_hello <rawkey=binary, splitsize=4096, mergesize=512> {
        lg_index <storage=memory, compress=snappy, blocksize=4> {
            update_flag <maxversions=1>
        },
        lg_props <storage=flash, blocksize=32> {
            level,
            weight
        },
        lg_raw <storage=disk, blocksize=128> {
            data <maxversions=10>
        }
    }
    
如果只希望简单的使用tera，对性能没有很高要求，那么schema只需指定表名和所需列名即可（如需要，所有的属性也是可配的）：

    table_hello {cf0, cf1, cf2}
    
### 创建key-value模式存储

tera支持高性能的key-value存储，其schema只需指定表名即可，若需要指定存储介质等属性，可选择性添加：

    kv_hello                                                # 简单key-value
    kv_hello <storage=flash, splitsize=2048, mergesize=128> # 配置若干属性

### 各级属性及含义

span | 属性名 | 意义 | 有效取值 | 单位 | 默认值 | 其它说明
---  | ---    | ---  | ---      | ---  | ---    | ---
table | rawkey | rawkey的拼装模式 | "readable"：性能较高，但不允许包含`\0`。"binary"：性能差一些，允许所有字符。 | - | "readable" | 
table | splitsize | 某个tablet增大到此阈值时分裂为2个子tablets| >=0，等于0时关闭split | MB | 512 | 
table | mergesize | 某个tablet减小到此阈值时和相邻的1个tablet合并 | >=0，等于0时关闭merge | MB | 0 | splitsize至少要为mergesize的5倍
lg    | storage   | 存储类型 | "disk" / "flash" / "memory" | - | "disk" | 
lg    | compress  | 压缩算法 | "snappy" / "none" | - | "snappy" | 
lg    | blocksize | LevelDB中block的大小       | >0 | KB | 4 | 
lg    | use_memtable_on_leveldb | 是否启用内存compact | "true" / "false" | - | false | 
lg    | sst_size  | 第一层sst文件大小 | >0 | Bytes | 8,000,000 | 
cf    | maxversions | 保存的最大版本数  | >0 | - | 1 | 
cf    | minversions | 保存的最小版本数 | >0 | - | 1 |
cf    | ttl | 数据有效时间 | >=0，等于0时此数据永远有效 | second | 0 | 小于0表示提前过期；和minversions冲突时以minversions为准

<!--
lg    | memtable_ldb_write_buffer_size | 内存compact开启后，写buffer的大小 | >0 | MB | 1 | 一般不用暴露给用户
lg    | memtable_ldb_block_size |  内存compact开启后，压缩块的大小 | >0 | KB | 4 | 一般不用暴露给用户
cf    | diskquota   | 存储限额  | >0 | MB | 0 | 暂未使用
-->

## 表格schema更新 (update)

`update <tableschema>`

更新时使用schema语法和建表时的语法基本一致，
不同主要在于更新时只需指定要更新的属性，不需要改动的属性无需列出。

### 更新table模式schema

1. 更新lg或者cf属性时，需要disable表格

1. table的rawkey属性不能被修改

#### 示例

更新table级别的属性（不更新lg、cf属性）：
    
```bash
./teracli update "oops<mergesize=512>" 
./teracli update "oops<splitsize=1024,mergesize=128>" 
```

更新lg属性（不更新cf属性）：
    
```bash
./teracli update "oops{lg0<sst_size=9>}"

#也可以同时修改table属性
./teracli update "oops<splitsize=512>{lg0<sst_size=9>}"
```

更新cf属性：

```bash
./teracli update "oops{lg0{cf0<ttl=999>}}"

#也可以同时修改table或者lg属性
./teracli update "oops<splitsize=512>{lg0<sst_size=9>{cf0<ttl=999>}}"
```

增加、删除cf：

```bash
# 在lg0下增加cf1，并设置属性ttl值为123. 
# op意为操作，op=add需要放在cf属性的最前面
./teracli update "oops{lg0{cf1<op=add,ttl=123>}}"

# 从lg0中删除cf1
./teracli update "oops{lg0{cf1<op=del>}}"
```

### 更新kv模式schema

```bash
# 更新部分属性时需要disable表格，程序会在运行时给出提示
./teracli update "kvtable<splitsize=1024>"
```
