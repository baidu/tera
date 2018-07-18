
# teracli使用说明
./teracli help即可看到相关的命令和使用方法
 
### 1. create 创建表格
#### 1.1 基本命令

```c
./teracli  create        <table-schema>  [<tablet-delimiter-file>]
./teracli  createbyfile  <schema-file>   [<tablet-delimiter-file>]
```
说明：
* table-schema是一个描述表格结构的字符串。
* 表名规范：首字符为字母（大小写均可），
* 有效字符包括大小写的英文字母(a-zA-Z)、数字(0-9)、下划线(_)、连字符(-)、点(.)。 1 <= 有效长度 <=
* 512
* Tera支持在建立表格时预分配若干tablet，tablet分隔的key写在tablet-delimiter-file中，按“\n”分隔。
* 如果表格schema比较复杂，可以将其写入文件中，通过createbyfile命令进行创建。
 
#### 1.2 创建table模式存储
表格结构中包含表名、locality groups定义、column families定义，一个典型的表格定义如下（可写入文件）
```c
# tablet分裂阈值为4096M，合并阈值为512M
# 三个lg，分别配置为flash、flash、磁盘存储
table_hello <splitsize=4096, mergesize=512> {
    lg_index <storage=flash, blocksize=4> {
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
```
如果只希望简单的使用tera，对性能没有很高要求，那么schema只需指定表名和所需列名即可（如需要，所有的属性也是可配的）：
```c
table_hello {cf0, cf1, cf2}
```
 
#### 1.3 创建key-value表
tera支持高性能的key-value存储，其schema只需指定表名即可，若需要指定存储介质等属性，可选择性添加：
```c
 # 表名为key-value，默认storage为disk, splitsize为512M, mergesize为0
./teracli  create kv_hello   
 # 配置若干属性                                            
./teracli  create "kv_hello <storage=flash, splitsize=2048, mergesize=128>"
```
#### 1.4 表格各级属性
 
span | 属性名 | 意义 | 有效取值 | 单位 | 默认值 | 其它说明
---  | ---    | ---  | ---      | ---  | ---    | ---
table | splitsize | 某个tablet增大到此阈值时分裂为2个子tablets| >=0，等于0时关闭split | MB | 512 |
table | mergesize | 某个tablet减小到此阈值时和相邻的1个tablet合并 | >=0，等于0时关闭merge | MB | 0 |
splitsize至少要为mergesize的3倍,建议为mergesize的10倍，避免merge后又分裂
lg    | storage   | 存储类型 | "disk" / "flash" / "memory" | - | "disk" |
lg    | blocksize | LevelDB中block的大小       | >0 | KB | 4 |
lg    | use_memtable_on_leveldb | 是否启用内存compact | "true" / "false" | - | false |
lg    | sst_size  | 第一层sst文件大小 | >0 | MB | 8 |
cf    | maxversions | 保存的最大版本数  | >0 | - | 1 |
cf    | ttl | 数据有效时间 | >=0，等于0时此数据永远有效 | second | 0 |
和minversions冲突时以minversions为准
<!--
table | rawkey | rawkey的拼装模式 | "binary" / "kv"/ "ttlkv" | - | key的长度必须小于64KB |
lg    | compress  | 压缩算法 | "snappy" / "none" | - | "snappy" |
lg    | memtable_ldb_write_buffer_size | 内存compact开启后，写buffer的大小 | >0 | MB | 1 |
一般不用暴露给用户
lg    | memtable_ldb_block_size |  内存compact开启后，压缩块的大小 | >0 | KB | 4 | 一般不用暴露给用户
cf    | diskquota   | 存储限额  | >0 | MB | 0 | 暂未使用
cf    | minversions | 保存的最小版本数 | >0 | - | 1 |
-->

### 2 update 更新表格schema
更新时使用schema语法和建表时的语法基本一致，
不同主要在于更新时只需指定要更新的属性，不需要改动的属性无需列出。
#### 2.1 基本语法
```c
./teracli update <tableschema>
```
#### 2.2 分类
主要分为两大类更新：
* 更新table模式schema
* 更新kv模式schema
 
#### 2.3 更新table模式schema
 
支持表格、cf属性热更新
##### 2.3.1 更新table的属性（不更新lg、cf属性）
```c
./teracli update "table_hello<mergesize=512>" //更新mergesize
./teracli update "table_hello<splitsize=1024,mergesize=128>" //更新mergesize和splitsize
```
##### 2.3.2 更新lg属性时，***需要disable表格***
```c
./teracli disable table_hello
./teracli update "table_hello{lg0<sst_size=9>}"
./teracli update "table_hello<splitsize=1536>{lg0<sst_size=9>}" //也可以同时修改table属性
```
##### 2.3.3 更新cf属性
```c
./teracli update "table_hello{lg0{cf0<ttl=999>}}"
#也可以同时修改table或者lg属性
./teracli update "table_hello<splitsize=512>{lg0<sst_size=9>{cf0<ttl=999>}}"
```
##### 2.3.4 增加、删除cf

```c
# 在lg0下增加cf1，并设置属性ttl值为123.
# op意为操作，op=add需要放在cf属性的最前面
./teracli update "table_hello{lg0{cf1<op=add,ttl=123>}}"

# 从lg0中删除cf1
./teracli update "table_hello{lg0{cf1<op=del>}}"
```
 
#### 2.4 更新kv模式schema
```c
# 更新部分属性时需要disable表格，程序会在运行时给出提示
./teracli update "kv_hello<splitsize=1024>"
```
 
### 3. update-check
 
### 4. enable
将处于disable状态的表格重新enable，恢复读、写服务。
```c
./teracli enable <tablename>
```
 
### 5. disable
将处于表格置于disable状态，不再提供读、写服务。
```c
./teracli enable <tablename>
```
 
### 6. drop
删除处于disable状态的表格，此操作不可回滚。
```c
./teracli drop <tablename>
```
### 7. rename 重命名表格
```c
#语法：
./teracli rename <old table_name> <new table_name>
```
示例：
```c
./teracli rename tb1 tb2
```
 
### 8. put 向表中写入一个value
向表中写入以rowkey为key,列为columnfamily:qualifier的值value.对于kv模式的表来说，无需columnfamily:qualifier.
```c
#语法：
./teracli put <tablename> <rowkey> [<columnfamily:qualifier>] <value>
```
示例：
```c
./teracli put mytable rowkey cf0:qu0 value
```

### 9. put-ttl 新增的ttl字段表示这个value的有效时间
```c
#语法：
./teracli put-ttl <tablename> <rowkey> [<columnfamily:qualifier>] <value> <ttl(second)>
```
示例：
```c
#这个value在20秒内有效，超时就读不到了。
./teracli put-ttl mytable rowkey cf0:qu0 value 20
```

### 10. putif 原子操作，如果不存在才能put成功

```c
#语法：
./teracli putif <tablename> <rowkey> [<columnfamily:qualifier>] <value>
```
 
### 11. get 读取一个value
```c
#语法：
./teracli get <tablename> <rowkey> [<columnfamily:qualifier>]
```
示例：
```c
#这个value在20秒内有效，超时就读不到了。
./teracli get mytable rowkey cf0:qu0
```
 
### 12. scan 扫描一个表
将表中key从[startkey, endkey)范围的所有数据扫描出来。
每个value可以有多个版本(versions)，scan命令默认只输出每个value的最新版本，
想要获取全部版本可以使用scanallv命令。
```c
#语法：
./teracli scan[allv] <tablename> <startkey> <endkey>
```
示例：
```c
#扫描整个表
./teracli scan mytable "" ""
```

 
### 13. delete 删除一个value
如果只想删除某列最新的一个版本可以用delete1v命令。
```c
#语法：
./teracli delete[1v] <tablename> <rowkey> [<columnfamily:qualifier>]
```

### 14. put_counter 写入一个counter（计数器）
```c
#语法：
./teracli put_counter <tablename> <rowkey> [<columnfamily:qualifier>] <integer(int64_t)>
```
示例：
```c
#写入一个初始值为3的计数器：
./teracli put_counter mytable rowkey cf0:qu0 3
```
### 15. get_counter 读取一个counter
```
#语法：
./teracli get_counter <tablename> <rowkey> [<columnfamily:qualifier>]
```
示例：
```c
#读取之前写入的那个counter：
./teracli get_counter mytable rowkey cf0:qu0
```
 
### 16. add 给某个counter加上一个delta值
```
#语法：
./teracli add <tablename> <rowkey> <columnfamily:qualifier> delta
```
示例：
```c
#读取之前写入的那个counter：
./teracli get_counter mytable rowkey cf0:qu0
```
 
### 17. putint64 写入一个int64类型counter（计数器）

```
#语法：
./teracli putint64 <tablename> <rowkey> [<columnfamily:qualifier>] <integer(int64_t)>
```
示例：
```c
#写入一个初始值为67的计数器：
./teracli putint64 mytable row1 cf0:qu0 67
```
 
### 18. getint64 读取一个int64类型的counter

```
#语法：
./teracli getint64 <tablename> <rowkey> [<columnfamily:qualifier>]
```
示例：
```c
./teracli getint64 mytable row1 cf0:qu0
```
 
### 19. addint64 对int64类型的counter执行原子加操作
```
#语法：
./teracli addint64 <tablename> <rowkey> <columnfamily:qualifier>  delta
```
示例：
```c
#对之前写入的counter执行-3的操作：
# addint64操作执行完以后，该counter的值为 64
./teracli addint64 mytable row1 cf0:qu0 -3
```
### 20. append 原子操作：追加内容到一个Cell
```
#语法：
./teracli append <tablename> <rowkey> [<columnfamily:qualifier>] <value>
```
示例：
```c
./teracli put mytalbe rowkey cf0:qu0 hello
./teracli append mytable rowkey cf0:qu0 world
#此时再去get会得到helloworld
./teracli get mytable rowkey cf0:qu0
```
### 20. batchput 批量写数据
```
#语法：
./teracli batchput <tablename> <input file>
```
### 21. batchget 批量读数据
```
#语法：
./teracli batchget <tablename> <input file>
```
### 22. show 显示表格信息
```
#语法：
./teracli show[x]  [<tablename>]
```
示例：
```c
#查看某个table的信息：
./teracli show mytable
#查看集群内所有table的信息：
./teracli show
```
 
### 23. showx 显示表格详细信息
```
#语法：
./teracli show[x]  [<tablename>]
```
示例：
```c
#查看某个table的信息：
./teracli showx mytable
```
 
### 24. showschema 显示表格schema
表格schema里含有很多属性（例如某个cf保留的最小版本数），创建表格时，没有显示指定的属性都取默认值，
这些属性在showschema时不会显示出来；想要显示全部属性，可以使用showschemax命令。
```
#语法：
./teracli showschema[x] <tablename>
```


### 25. showts 显示tabletnode的信息
带上后缀'x'得到的信息会更详细（showtsx）。
```
#语法：
./teracli showts [<tabletnode_addr>]
```
示例：
```c
#显示某个tabletnode的信息：
./teracli showts "example.company.com:7770"
#显示集群内所有tabletnode的信息:
./teracli showts
```
 
### 26. range 显示表的范围
```
#语法：
./teracli range <tablename>
```
### 27. txn 事务（仅支持单事务行操作）
```
#语法：
./teracli txn <operation> <params>
operation包括start和commit
./teracli txn start <tablename> <row_key>
./teracli txn commit
```
 
### 28. user用户管理
```
#语法：
./teracli user <operation> <params>
operation包括create、changepwd、show、delete、addtogroup和deletefromgroup
user <operation> <params>
          create          <username> <password>
          changepwd       <username> <new-password>
          show            <username>
          delete          <username>
          addtogroup      <username> <groupname>
          deletefromgroup <username> <groupname>
```
### 29. tablet
```
#语法：
./teracli tablet <operation> <params>
operation包括move、reload、compact、split、merge和scan
tablet <operation> <params>
            move    <tablet_path> <target_addr>
            reload  <tablet_path>
                    force to unload and load on the same ts
            compact <tablet_path>
            split   <tablet_path>
            merge   <tablet_path>
            scan    <tablet_path>
```
 
### 30. compact
```
#语法：
./teracli compact <tablename>
```
 
### 31. safemode
```
#语法：
./teracli safemode [get|enter|leave]
```
 
### 32. meta
meta for master memory, meta2 for meta table.
```
#语法：
./teracli meta[2] [backup|check|repair|show]
```
### 33. findmaster master的位置
```
#语法：
./teracli findmaster
```
### 34. reload
```
#语法：
./teracli reload config hostname:port
```

### 35. kick
```
#语法：
./teracli kick <tablename>
```
 
### 36. findtablet
```
#语法：
./teracli findtablet <tablename> <rowkey-prefix>
./teracli findtablet <tablename> <start-key> <end-key>
```
 
### 37. cookie
```
#语法：
./teracli  cookie <command> <args>
cookie <command> <args>
            dump     cookie-file     -- dump contents of specified files
            findkey  cookie-file key -- find the info of a key
```
 
### 38. version版本
```
#语法：
./teracli version
```

