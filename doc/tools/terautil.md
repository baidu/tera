
 
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


