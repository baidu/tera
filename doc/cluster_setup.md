Tera集群搭建
============

## 搭建要求

### 系统要求
* Linux系统集群，至少3个节点

### 软件要求
* zookeeper，推荐使用3.3或3.4的stable版本
* HDFS，推荐使用1.2.1版本

## 准备工作

### zookeeper准备工作
1. 搭建zookeeper
  1. 请参考 http://zookeeper.apache.org/doc/r3.4.6/zookeeperStarted.html
2. 配置zookeeper
  1. 创建一个zk节点作为tera根节点，其下创建3个子节点：*master-lock，ts，kick*
  2. 修改根节点和子节点的属性，对tera集群所有节点开放读写权限

### hdfs准备工作
1. 搭建hdfs
  1. 请参考 https://hadoop.apache.org/docs/r1.2.1/index.html
2. 配置hdfs
  1. 创建一个hdfs目录作为tera的数据根目录
  2. 修改目录属性，对tera集群所有节点开放读写权限
3. 让Tera找到hdfs
  1. Tera使用libhdfs.so与hdfs通讯，所以要将hadoop客户端里libhdfs.so的路径配置在环境变量LD_LIBRARY_PATH中，让tera在启动时能找到它。

### 搭建步骤
1. 构建tera
  * 请参考 BUILD文档
2. 部署tera
  * 在集群各节点创建tera根目录：*${tera_install}*，其下创建log、data两个子目录
  * 将构建生成的build目录的bin、conf拷贝到${tera_install}下面
3. 配置tera
  * 用编辑器打开${tera_install}/conf/tera.flag，对下面四项配置进行修改：
  ```
  tera_tabletnode_path_prefix：hdfs上的tera数据目录，如*/user/tera/test/*
  tera_tabletnode_cache_paths：ts的本地数据缓存目录，建议指向Flash/SSD硬盘，如*/home/ssd1;/home/ssd2*
  tera_zk_addr_list：zookeeper节点ip:port列表，以逗号隔开，如*${host1}:2180,${host2}:2180*
  tera_zk_root_path：zookeeper上的tera根节点，如/tera/test
  ```
  其它配置项一般不需要修改，如有需要，请参考src/tera_flag.cc
4. 启动tera
  * 在master节点上，执行以下命令
  ```
  cd ${tera_prefix}/bin
  nohup ./tera_main --flagfile=../conf/tera.flag --tera_role=master &> ../log/master.stderr &
  ```
  * 在TabletServer节点上，执行以下命令
  ```
  cd ${tera_prefix}/bin
  nohup ./tera_main --flagfile=../conf/tera.flag --tera_role=tabletnode &> ../log/tabletserver.stderr &
  ```
5. 停止tera
  * 用kill命令杀掉tera_main对应的进程即可
