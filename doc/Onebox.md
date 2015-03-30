为了最快速度体验Tera, Tera提供一个OneBox方式布署，利用本地存储代替DFS，单机多进程运行master、tabletnode。此模式下可正常使用tera的全部功能，包括（不限于）：
* 表格新建、载入、卸载、schema修改、删除等
* 数据写入、删除、随机读、顺序读等、多版本控制等
* 数据分片分裂、合并、负载均衡等

## 体验之前：
要先完成如下两项工作：
* **Tera编译通过**（以生成tera_main, teracli两个二进制文件为准）
* **拥有一个zookeeper环境**（单机即可）

## 准备工作
1. 在zk上创建一个Tera根节点，其下增加4个子节点：**root_table，master-lock，ts，kick**，成功后如下图所示：
2. 将编译生成的tera_main, teracli两个二进制文件放入example/onebox/bin下，此目录下应有如下文件：
3. 修改配置文件example/onebox/conf/tera.flag，将上文建立的zk节点信息填入对应项，其余项可暂时不变。

## 启动、停止Tera
* 执行example/onebox/bin/launch_tera.sh即可启动Tera，可通过config中选项配置tabletnode个数。
* 执行example/onebox/bin/kill_tera.sh即可停止Tera

## 体验开始！
### 尝试通过teracli来初步体验Tera
如果启动正常，尝试执行:

`./teracli show`

如出现下图，即表示启动成功

### 列举些常用命令
查看更详细的表格信息：

`./teracli showx`

查看当前有哪些存活的tabletnode:

`./teracli showts`

查看更详细的tabletnode信息：

`./teracli showtsx`

新建一个表格hello:

`./teracli create hello`

写入一条数据：

`./teracli put hello row_first ":" value`

读出一条数据：

`./teracli get hello row_first ":"`

卸载表格：

`./teracli disable hello`

删除表格：

`./teracli drop hello`

## 写在最后
Tera onebox模式可以体验几乎所有的功能特性，希望通过上面的介绍可以让大家对Tera有一个初步的认识。
