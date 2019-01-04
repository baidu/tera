
 
# terautil 
 
集群间数据迁移的dump工具
### 1. 用法
```
./terautil dump help
```
#### (1)建表
```
./terautil --flagfile=../conf/terautil.flag dump prepare_safe
```
#### (2) 将扫表操作run起来
```
./terautil --flagfile=../conf/terautil.flag dump run
```
 
### 2. flag配置
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



集群间数据迁移后的diff工具
### 1. 用法
```
./terautil diff help
```
#### (1)准备工作
```
./terautil --flagfile=../conf/tera.flag diff prepare
```
#### (2) 分布式进行求diff
```
./terautil --flagfile=../conf/tera.flag diff run
```
#### (3) 查看diff运行的进度
```
./terautil --flagfile=../conf/tera.flag diff progress
```
#### (4) 统计diff结果并显示
```
./terautil --flagfile=../conf/tera.flag diff result
```
#### (5) 删除用过的数据
```
./terautil --flagfile=../conf/tera.flag diff clean
```

### 2. 说明
首先，需要通过配置文件指定原集群和目标集群比较的表名，通过diff_tables_map_file文件指定，
格式：tables_name1:lg1|lg2,tables_name2:lg1|lg2
前后表名用,分割，可以指定比较哪些lg，如果不指定可以连同:都不写，表示比较所有的lg，一行一对儿表名

terautil的主配置文件tera.flag中，主要需要指定上述map文件，原集群的flag文件和目的集群的flag文件，
其他配置如Ins相关配置，日志相关配置，如需指定求diff数据的时间段，可以指定dump_endtime。
其他配置使用默认即可。
有一个比较重要的配置是ins_cluster_diff_root_path，默认值是/terautil/diff，所有diff过程需要的元数据的key都以这个为前缀

diff prepare
从diff_tables_map_file，读取表信息数据，转换成cf, cf对应是否multiversion数据，各tablet的范围数据，存放到nexus中，供后面diff过程使用

diff run
真正开始运行diff，这个可以多个实例并行运行，每个实例每开始比较一个范围会加锁
注意：scan过程中会失败，这个失败的范围不会重试再次比较，因此当观察日志发现所有实例都运行结束了，但通过diff progress查看发现并不是所有范围都diff结束了，
这时候需要启动运行一次实例（运行几个都可以），再次观察diff progress的输出，重复这个过程知道所有范围都完成diff比较


