# admincli meta表相关操作使用说明
* `./admincli help`可以看到相关帮助说明
* 所有操作默认在master节点执行
* teracli、tera_master_control 在bin目录下，admincli 在tools目录下，tera.flag 在conf目录下
* 该工具操作对象为：1.分布式文件系统持久化的meta，2.master内存中的meta，3.本地备份的meta表文件
* 1.对于持久化的meta，支持查询、健康检查、备份、修改 4类操作：get、show、healthcheck、backup、put、delete、modify
* 2.对于内存中的meta，支持查询、健康检查、备份 3类操作：get、show、healthcheck、backup，不支持修复操作
* 3.对于备份的meta，可从持久化meta或者内存meta备份，作为diff和恢复的依据。

### 1. 功能说明
#### 1.1 get
获取一条指定表start_key对应分片的tablet meta信息
可选参数inmem，代表内存meta，缺省代表持久化meta表

```c
get [inmem] <table_name> <start_key>

例子：
./admincli get inmem table1 '\x01abc'

```

#### 1.2 show
查询meta表中等meta信息
可选参数inmem，代表内存meta，缺省代表持久化meta
可选参数start_key、end_key(须一起指定)，show指定KeyRange的meta信息, 缺省则show整个meta表

```c
show [inmem] [start_key] [end_key]

例子：
./admincli show inmem "table1#\\x00'n\\x842" "table1#\\x00K\\x85"

```

#### 1.3 healthcheck
对meta表进行健康检查，
可选参数inmem，代表内存meta，缺省代表持久化meta
可选参数start_key、end_key（须一起指定）代表KeyRange范围内的健康检查，缺省代表整个meta表健康检查

```c
healthcheck [inmem] [start_key] [end_key]

例子：
./admincli healthcheck inmem "table1#\\x00'n\\x842" "table1#\\x00K\\x85"

```

#### 1.4 backup
备份meta表中的信息
可选参数`inmem`表示内存meta，缺省表示文件系统持久化meta
可选参数`filename`代表生成名为`filename`+时间戳的meta表备份文件
内存meta表备份文件缺省命名为：inmem_meta.bak+时间戳
持久化meta表备份文件缺省命名为：meta.bak+时间戳
为避免备份文件冲突，时间戳后缀自动添加

```c
backup [inmem] [filename]

例子：
./admincli backup inmem inmem_meta.bak

```

#### 1.5 modify
修改持久化meta表中该table_name和start_key对应的tablet meta信息
选择参数endkey，value输入欲修改的end_key值，之后输入Y确认
选择参数hostname, vaule输入欲修改的主机名，之后输入Y确认
如需修改start_key或其他信息，可以先调用delete，再调用put完成

```c
modify <table_name> <start_key> <endkey|dest_ts> <value>

例子：
./admincli modify table1 '\x01abc' endkey '\x01add'
./admincli modify table1 '\x01abc' dest_ts yq01.baidu.com:2002

```

#### 1.6 delete
从持久化meta表中删除table_name和start_key对应的tablet meta信息, 按照提示输入Y确认

```c
delete <table_name> <start_key>

例子：
./admincli delete table1 '\x01abc'

```

#### 1.7 put
向持久化meta表中插入一条tablet meta信息

```c
put <tablet_path> <start_key> <end_key> <server_addr>

例子：
./admincli put table1/tablet00000019 '\x01abc' '\x4Fzzz' hostname:2002

```

#### 1.8 diff
扫描文件系统获取meta信息，对meta表备份文件`filename`进行diff检查
可选参数table_name，代表对名为`table_name`的表扫描与`filename`进行校验
可选参数tablet_path，代表对path为`tablet_path`的分片扫描与`filename`进行校验
可选参数缺省，代表对全部表扫描与`filename`进行校验
健康检查的异常会记入tools目录下meta.diff文件中, 只保留最新的一份

```c
diff [table_name|tablet_path] <filename>

全部表diff 例子：
./admincli diff ./meta.bak_20180926-20:55:32

指定表diff 例子：
./admincli diff test_table ./meta.bak_20180926-20:55:32

指定分片 path diff 例子：
./admincli diff test_table/tablet00000001 ./meta.bak_20180926-20:55:32

```

### 2. 场景选择
* tera集群meta表存有2份：master内存里的meta，文件系统持久化的meta
* 当持久化的meta出现异常，内存里的meta正常，此时使用`方法一`修复
* 当2份meta都发生异常，此时使用`方法二`修复

### 3. 使用步骤
* 设置tera为safemode模式:`./teracli safemode enter`
* 为`tera.flag`添加配置项:`--meta_cli_token=2862933555777941757`
* 校验两份meta表确认故障场景和修复方法:`./admincli healthcheck`和`./admincli healthcheck inmem`

#### 方法一：利用内存备份文件恢复持久化meta表
* 备份内存中的meta表:`./admincli backup inmem`
* 在master的tera.flag中添加如下配置项：
 `--tera_master_meta_recovery_enabled=true`
 `--tera_master_meta_recovery_file=${filename}`
  其中${filename}为meta表备份文件的全路径
* 重启master服务：`./tera_master_control restart`
* 执行`./admincli healthcheck`和`./admincli healthcheck inmem`确认meta表修复正常
* 特别注意：从tera.flag中删除这两行配置项

#### 方法二：通过admincli校验并修复异常的meta表项
* 备份内存中的meta表：`./admincli backup inmem`
* 备份持久化的meta表：`./admincli backup`
* 停止master服务：`./tera_master_control stop`
* 【可选项】：扫描并diff备份的meta文件：`./admincli diff`
* 根据已知信息，调用put、delete、modify修复持久化的meta表
* 执行`./admincli healthcheck`确认持久化meta表修复正常
* 启动master服务：`./tera_master_control start`


# admincli ugi&role相关操作使用说明
* 为`tera.flag`添加配置项:`--meta_cli_token=2862933555777941757`

### 1. 功能说明
#### 1.1 ugi update
创建或更新用户信息

```c
ugi update <user_name> <passwd>

例子：
./admincli ugi update user1 123456

```

#### 1.2 ugi del
删除用户

```c
ugi del <user_name>

例子：
./admincli ugi del user1

```

#### 1.3 ugi show
查询所有的用户和密码

```c
ugi show

例子：
./admincli ugi show

```

#### 1.4 role add
添加role

```c
role add <role_name>

例子：
./admincli role add role1

```


#### 1.5 role del
删除role

```c
role del <role_name>

例子：
./admincli role del role1

```

#### 1.6 role grant
将role授权给用户

```c
role grant <role_name> <user_name>

例子：
./admincli role grant role_name1 user_name1

```

#### 1.7 role revoke
取消role对用户的授权

```c
role revoke <role_name> <user_name>

例子：
./admincli role revoke role_name1 user_name1

```

#### 1.8 role show
展示所有的role信息

```c
role show

例子：
./admincli role show

```

# admincli设置表格访问方式的操作说明
#### 1.1 auth set
设置表格访问方式。

```c
auth set <table_name> <auth_policy>
auth_policy可以是none/ugi/giano的任一种，所有的表格如果没有设置默认为none。
如果table设置为none，这些表属于未设置，全部都不需要身份认证和鉴权，所有用户可以访问这些表；
如果table设置了ugi/giano，则按照对应的方式进行鉴权，并且要求用户按照对应方式来访问；未设置鉴权的sdk无法访问这些指定鉴权的表格
*备注：giano是百度公司内部的身份认证和鉴权方式，外部开源版本无此代码，外部用户无需关心*

例子：
./admincli auth test ugi
```

#### 1.2 auth show
展示所有表格设置的访问方式。

```c
auth show

例子：
./admincli auth show
  TableName         AuthType
    test              ugi
    test2             none
    test3             giano
```

# admincli dfs-throughput-limit 使用说明
线上发生过Case，集群因为compact失败并不停重试，导致dfs雪崩，全集群写吞吐非常高，但最终都写失败了，只能通过手动下galaxy节点恢复，成本很高。
因此，增加dfs 读写吞吐硬限功能，强行卡死全集群读写dfs吞吐，该功能仅用于集群从有可能发生的雪崩状态中恢复。

使用方法

```bash
./admincli dfs-throughput-limit get                    # 获取当前集群硬限配置
./admincli dfs-throughput-limit write $write_limit     # write_limit 为全集群具体的写吞吐数值，单位Byte
./admincli dfs-throughput-limit read $read_limit       # 与write_limit等价
```

注：该功能仅用于tera op/rd 恢复雪崩状态集群，因此限制不会持久化，而是维护在master内存中，master重启后失效。

# admincli设置procedure并发限制操作说明
#### 1.1 procedure-limit get
获取当前各procedure并发限制

```c
示例：
./admincli procedure-limit get
[kMerge, limit:20, in_use:0]
[kSplit, limit:10, in_use:0]
[kMove, limit:100, in_use:0]
[kLoad, limit:300, in_use:0]
[kUnload, limit:100, in_use:0]
```

#### 1.2 procedure-limit set
设置某一procedure并发限制
成功会返回设置后各procedure并发限制
命令格式：
procedure-limit set <procedure> <limit>
    procedure = [kMerge, kSplit, kMove, kLoad, kUnload]
    limit shoud be a non-negative number

```
示例：
./admincli procedure-limit set kMerge 30
[kMerge, limit:30, in_use:0]
[kSplit, limit:10, in_use:0]
[kMove, limit:100, in_use:0]
[kLoad, limit:300, in_use:0]
[kUnload, limit:100, in_use:0]
```
