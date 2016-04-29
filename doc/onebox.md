通过OneBox体验Tera
=====

为了最快速度体验Tera, Tera提供一个OneBox方式布署，利用本地存储代替DFS，单机多进程运行master、tabletnode。此模式下可正常使用tera的全部功能，包括（不限于）：
* 表格新建、载入、卸载、schema修改、删除等
* 数据写入、删除、随机读、顺序读等、多版本控制等
* 数据分片分裂、合并、负载均衡等

## 体验之前：
* **Tera编译通过**（以生成tera_main, teracli两个二进制文件为准）
* onebox模式下，默认使用单机模拟zk，只能在单机访问Tera，如果需要通过网络访问，需要将zk配置修改为真实zk址

## 准备工作
1. 将编译生成的tera_main, teracli两个二进制文件放入example/onebox/bin.
1. 其余配置可暂时不变。

## 启动、停止Tera
* 执行example/onebox/bin/launch_tera.sh即可启动Tera，可通过config中选项配置tabletnode个数。
  
`[tera@example/onebox/bin]$ sh launch_tera.sh`

`launching tabletnode 1...`

`launching tabletnode 2...`

`launching tabletnode 3...`

`launching master...`

* 执行example/onebox/bin/kill_tera.sh即可停止Tera

## 体验开始！
### 尝试通过teracli来初步体验Tera
如果启动正常，尝试在example/onebox/bin下执行:

`./teracli show`

![onebox_show](https://github.com/BaiduPS/tera/blob/master/resources/images/onebox_show.png)

### 列举些常用命令
查看更详细的表格信息：

`./teracli showx`

![onebox_showx](https://github.com/BaiduPS/tera/blob/master/resources/images/onebox_showx.png)

查看当前有哪些存活的tabletnode:

`./teracli showts`

![onebox_showts](https://github.com/BaiduPS/tera/blob/master/resources/images/onebox_showts.png)

查看更详细的tabletnode信息：

`./teracli showtsx`

![onebox_showtsx](https://github.com/BaiduPS/tera/blob/master/resources/images/onebox_showtsx.png)

新建一个kv存储：hello_kv:

`./teracli create hello_kv`

![onebox_create_kv](https://github.com/BaiduPS/tera/blob/master/resources/images/onebox_create_kv.png)

新建一个表格：hello_table:

`./teracli create "hello_table{cf1, cf2, cf3}"`

![onebox_create_table](https://github.com/BaiduPS/tera/blob/master/resources/images/onebox_create_table.png)

获得表格schema:

`./teracli showschema hello_kv`

`./teracli showschema hello_table`

写入一条数据：

`./teracli put hello_kv row_first value`

`./teracli put hello_table row_first "cf1:qu" value`

读出一条数据：

`./teracli get hello_kv row_first`

`./teracli get hello_table row_first "cf1:qu"`

卸载表格：

`./teracli disable hello_kv`
`./teracli disable hello_table`

删除表格：

`./teracli drop hello_kv`
`./teracli drop hello_table`

## 写在最后
Tera onebox模式可以体验几乎所有的功能特性，希望通过上面的介绍可以让大家对Tera有一个初步的认识。
