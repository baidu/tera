teracli命令行工具使用手册
---

## create 创建表格

    ./teracli  create        <table-schema>  [<tablet-delimiter-file>]
    ./teracli  createbyfile  <schema-file>   [<tablet-delimiter-file>]

其中，table-schema是一个描述表格结构的字符串,语法详见[PropTree](https://github.com/BaiduPS/tera/blob/master/doc/prop_tree.md)

表名规范：首字符为字母（大小写均可），
有效字符包括大小写的英文字母(a-zA-Z)、数字(0-9)、下划线(`_`)、连字符(`-`)、点(`.`)。
1 <= 有效长度 <= 512.

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
table | rawkey | rawkey的拼装模式 | "binary" / "kv"/ "ttlkv" | - | key的长度必须小于64KB |
table | splitsize | 某个tablet增大到此阈值时分裂为2个子tablets| >=0，等于0时关闭split | MB | 512 |
table | mergesize | 某个tablet减小到此阈值时和相邻的1个tablet合并 | >=0，等于0时关闭merge | MB | 0 | splitsize至少要为mergesize的5倍
lg    | storage   | 存储类型 | "disk" / "flash" / "memory" | - | "disk" |
lg    | compress  | 压缩算法 | "snappy" / "none" | - | "snappy" |
lg    | blocksize | LevelDB中block的大小       | >0 | KB | 4 |
lg    | use_memtable_on_leveldb | 是否启用内存compact | "true" / "false" | - | false |
lg    | sst_size  | 第一层sst文件大小 | >0 | MB | 8 |
cf    | maxversions | 保存的最大版本数  | >0 | - | 1 |
cf    | minversions | 保存的最小版本数 | >0 | - | 1 |
cf    | ttl | 数据有效时间 | >=0，等于0时此数据永远有效 | second | 0 | 和minversions冲突时以minversions为准

<!--
lg    | memtable_ldb_write_buffer_size | 内存compact开启后，写buffer的大小 | >0 | MB | 1 | 一般不用暴露给用户
lg    | memtable_ldb_block_size |  内存compact开启后，压缩块的大小 | >0 | KB | 4 | 一般不用暴露给用户
cf    | diskquota   | 存储限额  | >0 | MB | 0 | 暂未使用
-->

## update 更新表格schema

    ./teracli update <tableschema>

更新时使用schema语法和建表时的语法基本一致，
不同主要在于更新时只需指定要更新的属性，不需要改动的属性无需列出。

### 更新table模式schema

1. 更新lg或者cf属性时，需要disable表格

1. table的rawkey属性不能被修改

#### 示例

更新table级别的属性（不更新lg、cf属性）：

    ./teracli update "oops<mergesize=512>"
    ./teracli update "oops<splitsize=1024,mergesize=128>"

更新lg属性（不更新cf属性）：

    ./teracli update "oops{lg0<sst_size=9>}"

    #也可以同时修改table属性
    ./teracli update "oops<splitsize=512>{lg0<sst_size=9>}"


更新cf属性：

    ./teracli update "oops{lg0{cf0<ttl=999>}}"

    #也可以同时修改table或者lg属性
    ./teracli update "oops<splitsize=512>{lg0<sst_size=9>{cf0<ttl=999>}}"

增加、删除cf：

    # 在lg0下增加cf1，并设置属性ttl值为123.
    # op意为操作，op=add需要放在cf属性的最前面
    ./teracli update "oops{lg0{cf1<op=add,ttl=123>}}"

    # 从lg0中删除cf1
    ./teracli update "oops{lg0{cf1<op=del>}}"

### 更新kv模式schema

    # 更新部分属性时需要disable表格，程序会在运行时给出提示
    ./teracli update "kvtable<splitsize=1024>"

## putint64 写入一个int64类型counter（计数器）

例如写入一个初始值为67的计数器：

    ./teracli putint64 mytable row1 cf0:qu0 67

## getint64 读取一个int64类型的counter

    ./teracli getint64 mytable row1 cf0:qu0

## addint64 对int64类型的counter执行原子加操作

例如对之前写入的counter执行-3的操作：

    # addint64操作执行完以后，该counter的值为 64
    ./teracli addint64 mytable row1 cf0:qu0 -3

## disable

    ./teracli disable <tablename>

将表格置于disable状态，不能再提供读、写服务。

## enable

    ./teracli enable <tablename>

将处于disable状态的表格重新enable，恢复读、写服务。

## drop

    ./teracli drop <tablename>

删除处于disable状态的表格，此操作不可回滚。

## put 向表中写入一个value

    ./teracli put <tablename> <rowkey> [<columnfamily:qualifier>] <value>

向表中写入以rowkey为key,列为columnfamily:qualifier的值value.

对于kv模式的表来说，无需columnfamily:qualifier.

例如：

    ./teracli put mytable rowkey cf0:qu0 value

##  put-ttl

类似put，新增的ttl字段表示这个value的有效时间。

    ./teracli put-ttl <tablename> <rowkey> [<columnfamily:qualifier>] <value> <ttl(second)>

例如

    ./teracli put-ttl mytable rowkey cf0:qu0 value 20

这个value在20秒内有效，超时就读不到了。

## putif 原子操作：如果不存在才能Put成功

    ./teracli putif <tablename> <rowkey> [<columnfamily:qualifier>] <value>

## get 读取一个value

    ./teracli get <tablename> <rowkey> [<columnfamily:qualifier>]

例如：

    ./teracli get mytable rowkey cf0:qu0

## scan 扫描一个表

	./teracli scan[allv] <tablename> <startkey> <endkey>

将表中key从[startkey, endkey)范围的所有数据扫描出来。

每个value可以有多个版本(versions)，scan命令默认只输出每个value的最新版本，
想要获取全部版本可以使用`scanallv`命令。

## delete 删除一个value

	./teracli delete[1v] <tablename> <rowkey> [<columnfamily:qualifier>]

如果只想删除某列最新的一个版本可以用`delete1v`命令。

## put_counter 写入一个counter（计数器）

    ./teracli put_counter <tablename> <rowkey> [<columnfamily:qualifier>] <integer(int64_t)>

例如写入一个初始值为3的计数器：

    ./teracli put_counter mytable rowkey cf0:qu0 3

*注意*：(put_counter, get_counter, add三者对应的的counter) 与
(putint64, getint64, addint64三者对应的counter)在底层实现不一样，不可以混用。
用`put_counter`写入的counter要用`get_counter`读取，用`add`进行原子加。

## get_counter 读取一个counter

    ./teracli get_counter <tablename> <rowkey> [<columnfamily:qualifier>]

例如读取之前写入的那个counter：

    ./teracli get_counter mytable rowkey cf0:qu0

## add 给某个counter加上一个delta值

    ./teracli add <tablename> <rowkey> <columnfamily:qualifier> delta

例如给之前写入的那个初始值为3的counter加上2使这个counter现在的值为5：

    ./teracli add mytable rowkey cf0:qu0 2

## append 原子操作：追加内容到一个Cell

	./teracli append <tablename> <rowkey> [<columnfamily:qualifier>] <value>

例如：

    ./teracli put    mytalbe rowkey cf0:qu0 hello
    ./teracli append mytable rowkey cf0:qu0 world
    #此时再去get会得到helloworld
    ./teracli get mytable rowkey cf0:qu0

## show 显示表格信息

    ./teracli show[x]  [<tablename>]

例如，查看某个table的信息：

    ./teracli show mytable

查看集群内所有table的信息：

    ./teracli show

当需要更详细的信息时，可以带上'x'后缀。

## showschema 显示表格schema

	./teracli showschema[x] <tablename>

表格schema里含有很多属性（例如某个cf保留的最小版本数），创建表格时，没有显示指定的属性都取默认值，
这些属性在showschema时不会显示出来；想要显示全部属性，可以使用showschemax命令。

## showts 显示tabletnode的信息

显示某个tabletnode的信息，例如：

    ./teracli showts "example.company.com:7770"

显示集群内所有tabletnode的信息，例如：

    ./teracli showts

带上后缀'x'得到的信息会更详细（`showtsx`）。
