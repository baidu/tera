
# Tera SDK及工具说明

## 目录
### 1. [主要数据结构](#main-data-structure)
 
* tera::[client](./sdk_reference/client.md)
* tera::[table](./sdk_reference/table.md)
* tera::[mutation](./sdk_reference/mutation.md)
* tera::[reader](./sdk_reference/reader.md)
* tera::[table_descriptor](./sdk_reference/table_descriptor.md)
* tera::[transaction](./sdk_reference/transaction.md)
* tera::[scan](./sdk_reference/scan.md)
* tera::[utils](./sdk_reference/utils.md)

### 2. [主要工具](#main-tools)
* [teracli](./tools/teracli.md)
* [terautil](./tools/terautil.md)
* [tera_bench & tera_mark](./tools/benchmark.md)
* [YCSB](./tools/ycsb.md)


<a name="main-data-structure"></a> 
### 1. 主要数据结构
#### (1) tera::client  访问tera服务主结构，所有对tera的访问或操作全部由此发起。
一个集群对应一个client即可，如需访问多个client，需要创建多个
##### 主要功能包括：
* 表格操作：建、删、加载、卸载、打开、关闭、更新表结构、获取表格信息、快照等
* 用户管理：建、删、修改密码、组管理等
* 集群信息获取：获取全部表格列表、状态等
 
#### (2) tera::table  表格主结构，对表格的所有增删查改操作由此发起。
由tera::Client::OpenTable产生，tera::Client::CloseTable关闭，不可析构。
 
#### (3) tera::error_code 错误码，很多操作会返回，注意检查。

#### (4) tera::mutation
 
#### (5) tera::scan 扫描操作，并获取返回数据。
 
#### (6) tera::reader 读取操作，并获取返回数据。
 
#### (7) tera::table_descriptor 表格描述符主体
 
#### (8) tera::transaction 单行事务
 
#### (9) tera::scan 扫描
 
#### (10) tera::utils 编码解码
 
<a name="main-tools"></a> 
### 2. 主要工具
#### (1) teracli  操作tera的工具
* 实际上封装了对数据的操作等，可用来进行表格创建、schema更新等管理、控制操作。
* 查看有哪些命令可用 ：./teracli help；
* 查看某个命令的help：./teracli help [cmd]，例如./teracli help tablet
 
#### (2) terautil  集群间数据迁移的dump工具

* 具体用法./terautil dump help
* 建表主要用法：./terautil --flagfile=../conf/terautil.flag dump prepare_safe
* 扫表run起来主要用法：./terautil --flagfile=../conf/terautil.flag dump run
* flag配置
<table>
<tr>
<th>flag名称</th>
<th>flag默认值或格式</th>
<th>flag介绍</th>
</tr>
<tr>
<td>dump_tera_src_conf </td>
<td>../conf/src_tera.flag（格式）</td>
<td>tera的源集群</td>
</tr>
<tr>
<td>dump_tera_dest_conf</td>
<td>../conf/dest_tera.flag（格式）</td>
<td>tera的目的集群</td>
</tr>
<tr>
<td>dump_tera_src_root_path</td>
<td>/xxx_（路径格式）</td>
<td>tera的源路径</td>
</tr>
<tr>
<td>dump_tera_dest_root_path</td>
<td>/xxx_（路径格式）</td>
<td>tera的目的路径</td>
</tr>
<tr>
<td>ins_cluster_addr</td>
<td>terautil_ins（格式）</td>
<td>锁服务器的地址</td>
</tr>
<tr>
<td>ins_cluster_root_path</td>
<td>/terautil/dump/xxxx（格式）</td>
<td>锁服务器路径</td>
</tr>
<tr>
<td>dump_tera_src_meta_addr</td>
<td>“”</td>
<td>源meta表的地址</td>
</tr>
<tr>
<td>dump_tera_dest_meta_addr</td>
<td>“”</td>
<td>目的meta表的地址</td>
</tr>
<tr>
<td>dump_manual_split_interval</td>
<td>1000</td>
<td>手动分裂时间间隔，单位为ms</td>
</tr>
<tr>
<td>dump_enable_manual_split</td>
<td>false</td>
<td>是否允许手动分裂</td>
</tr>
</table>

 
#### (3) tera_mark   读写数据
* 支持异步读写scan
```
＃示例：
./tera_mark --mode=w --tablename=test --type=async  --verify=false --entry_limit=1000
```
* 参数列表

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
 
#### (4) tera_bench 造数据的工具
```
./tera_bench --compression_ratio=1 --key_seed=1 --value_seed=20  --value_size=1000 --num=200000
--benchmarks=random  --key_size=24 --key_step=1
```
 
#### (5) YCSB 业界通用NoSQL测试的基准测试工具
 
* 全称Yahoo! Cloud Serving Benchmark，Yahoo公司开发的专门用于NoSQL测试的基准测试工具
* YCSB支持各种不同的数据分布方式，如Uniform（等概论随机选择记录）、Zipfian（随机选择记录，存在热记录）、Latest（近期写入的记录为热记录）

