#基本介绍
## Tera是什么
Tera是一个分布式结构化数据库,Tera中的数据以“表”为单位存放数据,每个表包含若干条"记录",每条记录包含一个"行键"(RowKey)和若干"列"，记录间按行键有序,行键可以是任意字符串,每列都包含一个"列名"(Column)和若干个"列值"(Value),每个列值上都附有一个时间戳(Timestamp).

## Tera适用于怎样的场景  
Tera适用于需要数据持久存储,并需要高效顺序或随机访问的场景. Tera将数据存储在分布式文件系统上,保证了数据的持久化,将数据按行键\列名\时间戳有序组织,保证了高效的顺序访问,实现了稠密索引,将热数据放在多级缓存(内存\SSD)中,保证了高效随机访问.

## Tera的单机性能  
因为不同场景硬件环境\数据压缩比\数据格式不同,性能数据会有较大差别,这里只能给出一个大概的值,可以认为是在16核心CPU,128G内存,2块SSD,6块Sata磁盘的服务器下的性能:  
顺序写80MB/S,随机写10MB/S,顺序读性能100MB/S,随机读1万QPS(SSD),500QPS(磁盘)

## Tera的扩展性  
Tera可以将单机性能水平扩展至几千台机器,在当前生产环境中,Tera单机群2000台机器.

# 使用Tera
## 编译、搭建
编译Tera，参考[构建Tera](https://github.com/baidu/tera/blob/master/BUILD)  
搭建Tera，参考[集群搭建](https://github.com/baidu/tera/blob/master/doc/cluster_setup.md)  
Tera的配置说明，参考[tera_flag.md](https://github.com/baidu/tera/blob/master/doc/tera_flag.md)  
TeraClient使用，参考[teracli.md](https://github.com/baidu/tera/blob/master/doc/teracli.md)  
TeraSDK使用，参考[API使用方法](https://github.com/baidu/tera/wiki/%E4%B8%BB%E8%A6%81API%E4%BD%BF%E7%94%A8%E6%96%B9%E6%B3%95)  

## 使用注意事项  
1. 注意Rowkey的设计,避免全局的顺序写入.  
因为Tera表是全局序的,Tera将一个表格按rowkey划分成多个区间,如果用当前时间作为rowkey,会导致所有的写操作都落入最后一个区间,相当于只有一台机器在服务,完全无法发挥出集群的性能.Tera的按负载自动分片无法解决这个问题,因为无论怎么分,用户都只写入最后一个分片.  
2. 尽量选择IO性能高的机器搭建tera的TabletServer.  
Tera是个存储系统,Tera的持久存储DFS和Tera本身的cache系统都会直接访问磁盘,TabeltServer上的读写操作多数也要通过读写磁盘实现,所以磁盘的IO性能决定了Tera对外提供的读写吞吐. 所以搭建tera的机器最好能拥有SSD作为TabletServer的本地cache,用于多块磁盘作为DFS的数据存储介质.虽然Tera在只有单块磁盘的机器上也可以搭建运行,也可以发挥出这台机器的最大存储性能.  

## Tera使用FAQ  
1. LG，CF，都是什么概念，划分原则是什么样的？  
CF即Columnfamily、列族。在常规数据库中，需要在创建表时确定都包含哪些列，运行中不能随意更改。而Tera为了提供了动态Schema，让用户可以随意增删列，同时又保证让访问控制有迹可循，所以将常规数据库中的列拆成了Columnfamily、Qualifier两个维度，Columnfamily是访问控制的最小单位，必须在创建表时指定。Qualifier是tera中实际的列，用户可以在运行期随意创建和删除，但Qualifier必须从属于某个Columnfamliy，便于确定归属和进行访问控制。  
LG即Localitygroup、局部性群组，是对存储的一个优化。设想，我们可以将所有的列族存储在一起，但这样我们要单独访问一个列族时，效率就不够高，我们也可以将每个列族都单独存储，这样要单独访问一个列族效率很高，但要同时访问一行（或者同时访问多个列族）效率就会很低。所以在这里做了一个折衷，我们将一些经常要一起访问的列族存储在一起，成为一个Localitygroup，而经常单独访问的列族，可以单独作为一个Localitygroup存储。所以一个Localitygroup个存储维度的概念，而Columnfamily是个逻辑概念，一个Localitygroup可能包含1个或多个Columnfamily。  
2. mutation Reader 的读写接口中 的参数qualifier是什么？  
如1中所述，qualifier是Tera中实际的列，或者说列名的意思。  
3. scan接口描述中在设置区间返回后，做了下面两个add，desc->AddColumnFamily("family21");desc->AddColumn("family22", "qualifier22");是指定scan的列么？如果不指定，可以scan整行么？scan多列之后，如何从result_stream中读某列数据？  
AddColumnFamily是指定要scan的列族，如果指定就只scan出特定列族，如果不指定，会scan出整行，起一个过滤器的作用。  
scan多列后，可以使用ResultStream的ToMap接口读取，也可以用Next遍历，找到某列数据。
4. sdk中有tera.h，有tera_easy.h……这几个不同的头文件分别有什么用？  
Tera的标准SDK在tera.h中，推荐用户使用，tera_easy.h是一个简化后的sdk，功能有限，一般用户不需要关注。  
TeraSDK的使用，参见文档 [API使用方法](https://github.com/baidu/tera/wiki/%E4%B8%BB%E8%A6%81API%E4%BD%BF%E7%94%A8%E6%96%B9%E6%B3%95)  
