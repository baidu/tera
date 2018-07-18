
# YCSB工具使用说明
 
### 1. 属性
 
#### 1.1 核心YCSB属性
所有工作量文件可以指定以下属性：
<table>
<tr>
<th>参数名</th>
<th>意义</th>
<th>默认值</th>
</tr> 
 
<tr>
<td>workload</td>
<td>要使用的工作量类，如com.yahoo.ycsb.workloads.CoreWorkload</td>
<td></td>
</tr>
 
<tr>
<td>db</td>
<td>要使用的数据库类。可选地，这在命令行可以指定</td>
<td>com.yahoo.ycsb.BasicDB</td>
</tr>
 
<tr>
<td>exporter</td>
<td>要是用的测量结果的输出类</td>
<td>com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter</td>
</tr>
 
<tr>
<td>exportfile</td>
<td>用于替代stdout的输出文件路径</td>
<td>未定义/输出到stdout</td>
</tr>
 
<tr>
<td>threadcount</td>
<td>YCSB客户端的线程数。可选地，这可以在命令行指定</td>
<td>1</td>
</tr>
 
<tr>
<td>measurementtype</td>
<td>支持的测量结果类型有直方图和时间序列</td>
<td>直方图</td>
</tr>
</table>

 
 
 
 
 
#### 1.2 核心工作量包属性
和核心工作量构造器一起使用的属性文件可以指定以下属性及值 
#####1.2.1 重要参数 
<table>
<tr>
<th>参数名</th>
<th>意义</th>
<th>默认值</th>
<th>有效取值</th>
</tr> 
 
<tr>
<td>recordcount</td>
<td>数据行数，装载进数据库的初始记录数</td>
<td>0</td>
<td></td>
</tr>
 
<tr>
<td>operationcount</td>
<td>要进行的操作数数量</td>
<td>无</td>
<td></td>
</tr>
 
<tr>
<td>fieldcount</td>
<td>每行的qualifier个数</td>
<td>10</td>
<td></td>
</tr>
 
<tr>
<td>fieldlength</td>
<td每个字段的大小</td>
<td>100</td>
<td></td>
</tr>
 
<tr>
<td>requestdistribution</td>
<td>随机读的数据分布</td>
<td>uniform</td>
<td>uniform、zipfian、latest</td>
</tr>
 
<tr>
<td>insertorder</td>
<td>写入顺序，ordered是顺序写，hashed是随机写</td>
<td>hashed</td>
<td>ordered、hashed</td>
</tr>
 
<tr>
<td>readallfields</td>
<td>读取所有qualifier还是只读一个qualifier</td>
<td>true</td>
<td>true、false</td>
</tr>
 
<tr>
<td>readproportion</td>
<td>随机读占所有操作的比例</td>
<td>0.95</td>
<td></td>
</tr>
 
<tr>
<td>readproportion</td>
<td>更新（写入）占所有操作的比例</td>
<td>0.05</td>
<td></td>
</tr>
 
<tr>
<td>target</td>
<td>每秒总共操作的次数</td>
<td>unthrottled</td>
<td></td>
</tr>
 
<tr>
<td>thread</td>
<td>客户端线程数</td>
<td>1</td>
<td></td>
</tr>

</table>
 
##### 1.2.2 非必需参数（对tera测试意义不大，用默认值即可）
 <table>
<tr>
<th>参数名</th>
<th>意义</th>
<th>默认值</th>
<th>有效取值</th>
</tr>
 
 
<tr>
<td>insertproportion</td>
<td>插入（写入）占所有操作的比例</td>
<td>0</td>
<td></td>
</tr>
 
<tr>
<td>scanproportion</td>
<td>scan占所有操作的比例，tera_mark不支持</td>
<td>0</td>
<td></td>
</tr>
 
<tr>
<td>readmodifywriteproportion</td>
<td>readmodifywrite占所有操作的比例，tera不支持该操作</td>
<td>0</td>
<td></td>
</tr>
 
<tr>
<td>maxscanlength</td>
<td>每次scan需要读取的行数，tera不支持指定行数的scan</td>
<td>1000</td>
<td></td>
</tr>
 
<tr>
<td>scanlengthdistribution</td>
<td>scan的行数选择策略</td>
<td>uniform</td>
<td></td>
</tr>
 
<tr>
<td>maxexecutiontime</td>
<td>最大执行时间，超过此时间会强行结束测试（单位为秒）</td>
<td></td>
<td></td>
</tr>
 
<tr>
<td>table</td>
<td>表名，tera_mark不支持</td>
<td>usertable</td>
<td></td>
</tr>
</table>
 
#### 1.3 测量结果属性
每一个测量结果类型可以为如下属性形式：
 <table>
<tr>
<th>类型</th>
<th>参数名</th>
<th>意义</th>
<th>默认值</th>
<th>有效取值</th>
</tr> 
 
<tr>
<td>直方图</td>
<td>histogram.buckets</td>
<td>直方图输出的区间数</td>
<td>1000</td>
<td></td>
</tr>
 
<tr>
<td>时间序列</td>
<td>timeseries.granularity</td>
<td>时间序列输出的粒度</td>
<td>1000</td>
<td></td>
</tr>
 
</table>
 
### 2 运行时参数
即使工作负载类和参数文件定义了一个特定的工作负载，在运行基准测试时你还是想指定一些额外的设置。当你运行YCSB客户端时命令行提供了这些设置。这些设置包括：
* -threads :客户端的线程。默认地，YCSB客户端使用一个工作者线程，但是额外的线程可以被指定。当需要增加对数据库的装载数量时这是经常使用的。
* -target：每秒的目标操作数。默认地，YCSB客户端将试图尽可能地执行最多操作。例如，如果每个操作平均使用了100ms，客户端每个工作者线程每秒将执行10个操作。然而，你可以限制每秒的目标操作数。比如，为了生成一条延迟-吞吐量曲线，你可以指定不同的目标吞吐量，以测试每种吞吐量下的延迟。
* -s：状态。对于一个运行时间长的工作负载，让客户端报告状态是有用的，这可以让你知道它并没有挂掉，并且给你某些对它的执行过程的想法。通过在命令行指定“-s”，客户端将每10秒输出状态到stderr。



 
### 3 用法
 
#### 3.1 相关命令
* load: 执行加载命令
* run: 执行工作负载
* shell: 交互式模式
```
＃basic参数告诉客户端使用哑BasicDB层。你也可以在你的参数文件中使用“db”属性指定它（例如，“db=com.yahoo.ycsb.BasicDB”）
./bin/ycsb shell basic           
> help
Commands:
read key [field1 field2 ...]                  // Read a record
scan key recordcount [field1 field2 ...]     // Scan starting at key
insert key name1=value1 [name2=value2 ...]  // Insert a new record
update key name1=value1 [name2=value2 ...] // Update a record
delete key                                // Delete a record
table [tablename]                        // Get or [set] the name of the table
quit                                    // Quit
```
 
#### 3.2 使用方法
使用时，先建表，再加载数据，最后执行相关事务。

##### 3.2.1 建表
ycsb的生成的row都是“user”+19位数字的格式，如 user9105318085603802964。 因此，如果需要预分表，必须以“user”+N个数字作为分隔，建议选择2个数字。 例如要预分4个tablet，分隔字符串为：user25、user50、user75
```
create 'usertable','f1','f2','f3'
```

##### 3.2.2 向tera中加载测试数据
```
bin/ycsb load tera -p workload=com.yahoo.ycsb.workloads.CoreWorkload \          //load参数告诉客户端执行工作负载的装载阶段。
                   -p recordcount=$(ROW_NUM) \                                  //-p参数被用于设置参数，-P参数用于装载属性文件。
                   -p fieldlength=$(QUALIFIER_NUM) \
                   -p fieldcount=$(VALUE_SIZE)
```
 
##### 3.2.3 执行测试
```
bin/ycsb run tera -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
                  -p recordcount=$(ROW_NUM) \
                  -p operationcount=$(ROW_NUM) \
                  -p requestdistribution=$(DIST) \
                  -p fieldlength=$(QUALIFIER_NUM) \
                  -p fieldcount=$(VALUE_SIZE) \
                  -p updateproportion=$(WRITE_PROP) \
                  -p readproportion=$(READ_PROP)
```
 

