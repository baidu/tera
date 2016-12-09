ubuntu安装tera(单机和集群)
=========================

## 1. 单机安装

* 参考：https://github.com/baidu/tera/blob/master/BUILD-cn

* 一些必须的组件或者package链接中说的比较完备了。下面记录安装过程中遇到的问题：

1. ubuntu版本推荐14.04之后的版本，tera要求gcc版本在4.8.2以上，所以ubuntu 14.04之前的版本需要升级gcc版本。重点是，国内通过gcc的ppa源升级gcc会导致一些网络问题，因此一般gcc升级需要下载源码编译安装，过程缓慢。建议使用ubuntu 16.04。

2. tera提供了一个build.sh脚本，执行sh build.sh，就会检查列表中的包，如果没有会进行安装。其中存在一些没有检查的包，遇到的包括以下几种：

  1). 提示zlib.h：没有那个文件或目录：sudo apt-get install zlib1g-dev

  2). 编译和安装openssl时出错，POD document had syntax errors：
  ```
  smime.pod around line 285： Expected text after =item， not a number
  smime.pod around line 289： Expected text after =item， not a number
  POD document had syntax errors at /usr/bin/pod2man line 71.
  ```
  解决方法：删除 pod2man文件 sudo rm /usr/bin/pod2man

  3). ld时候提示没有libcrypto错误：链接过程中需要以来crypto包，这个包是在openssl中，理论
  上安装里openssl就会有这个包，但是有些时候还是会出现莫名的问题。如果此时需要重新安装
  openssl，可以按照一些步骤：
  ```
  // 如果有老版本，先卸载
  apt-get purge openssl
  rm -rf /etc/ssl #删除配置文件

  wget ftp://ftp.openssl.org/source/openssl-1.0.0c.tar.gz
  tar -zxf openssl-1.0.0c.tar.gz
  cd openssl-1.0.0c/
  ./config  --prefix=/usr/local --openssldir=/usr/local/ssl
  make && make install
  ./config shared --prefix=/usr/local --openssldir=/usr/local/ssl
  make clean
  make && make install
  ```
  4). 提示错误：fatal error: readline/history.h：
  sudo apt-get install libreadline-dev
  
  5). 提示缺少ncurses包：、
  sudo apt-get install libncurses5-dev
  
3. 等待编译结束 & 单机体验

  将编译生成的tera_main和teracli文件copy到example/onebox/bin目录下，进入目录执行：sh launch_tera.sh。然后执行./teracli进终端交互。Have fun！
  详见：https://github.com/baidu/tera/blob/master/doc/onebox-cn.md


## 2. 安装集群
集群安装会稍微麻烦一点，主要是要安装zk和hdfs这些依赖，加上有一些配置，比较容易弄错。参考：https://github.com/baidu/tera/blob/master/doc/cluster_setup.md

1. 安装3个节点集群，我使用的是3个ubuntu 16.04的虚拟机(virtualbox)，注意virtual的网络连接方式设置为“桥接网卡”，这样的设置使得三台虚拟机都有局域网内自己的IP。同时需要注意，在一些公司，内部网络是安全加密网络，virtualbox的“桥接”模式使用不了，那么，唔，回家装吧:)。

2. zookeeper使用3.4.6版本，hadoop使用了2.7.2版本。注意，在3台机子上都需要安装zk和hdfs，但是tera只需要一个机子编译了就OK，后面的使用只需要binary可执行文件，所以将build好的可执行文件copy到另外两个机器就可以了。

3. 安装zookeeper

  1). 首先下载一个3.4.6的安装包：http://www.apache.org/dyn/closer.cgi/zookeeper/

  2). 解压后放在home目录下(自己喜欢的目录都可以)，
     cd ~/zookeeper-3.4.6/conf/,
     cp zoo_sample.cfg zoo.cfg，
     修改zoo.cfg文件，参考配置如下：
     ```
	tickTime=2000

	initLimit=10

	syncLimit=5

	// 目录选择自己喜欢的目录，mkdir相应的文件夹
    	// 例如zookeeperdir/data是我自己创建的目录
	dataDir=/home/xx/zookeeperdir/data
	dataLogDir=/home/xx/zookeeperdir/log

	clientPort=2181

	// zookeeper集群中三台机器编号以及IP
  	// server后面的1,2,3是机器编号，下面会进行设置
	server.1=192.168.0.107:2888:3888
	server.2=192.168.0.113:2888:3888
	server.3=192.168.0.114:2888:3888
     ```
  3). 在上面的dataDir目录下增加myid文件，代表当前机器的id，zk需要使用。如果当前机器编号是1(这个自己决定就行), 那么echo "1" > /home/xx/zookeeperdir/data/myid 。（三个机器都需要配置）

  4). OK，至此zk的配置完成，下面需要创建tera节点。
    ```
    > 运行zk：./bin/zkServer.sh start 
 	注意：需要将三台机器全部启动，只要有一台没有启动，那么其他的都会等待。
	执行命令“./bin/zkServer.sh status”，临时报错：
	Error contacting service. It is probably not running.
	等待三个机器全部启动，那么会发现有一台leader，其他的是follower。

    > 进入任意一台机子终端执行：./bin/zkCli.sh 
    > 进入cli之后，配置节点：执行 “ls /”会发现一个默认目录zookeeper
	1). 现在创建tera根节点： create /zk tera 。目录就叫做zk，关联字符串是tera，这个可以随便。
	2). 创建子节点：
	
	create /zk/master-lock master_lock; 
	create /zk/kick kick; 
	create /zk/ts ts
	
	现在 ls /zk 会看到创建好的三个子节点。这一步只需要在一个zk机器上执行，然后自动会同步到其他机器。
    ```

4. 安装Hadoop

  下载地址：http://www.apache.org/dyn/closer.cgi/hadoop/common ，本机下载的是2.7.2版本。

  1). 解压源码包到home目录下(选择自己喜欢的目录)，包名太长了，我改成了：```mv hadoop-2.7.2 hadoop```.

  2). 配置：cd hadoop/etc/hadoop目录
  ```
  > 配置hadoop-env.sh
    主要修改export JAVA_HOME=你的jdk路径，例如我的是/usr/lib/jvm/java-1.8.0-openjdk-amd64

  > 配置core-site.xml，下面是参考配置
    <configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>file:/home/xx/hadoop/tmp</value>
    </property>
    <property>
        <name>io.file.buffer.size</name>
        <value>131702</value>
    </property>
    <property>
        <name>dfs.permissions</name>
        <value>false</value>
    </property>
    </configuration>
    注意：上面的/home/xx/hadoop/tmp是自己创建的tmp目录，并且在tmp目录下创建hdfs目录，然后进入hdfs创建name和data目录，下面需要使用。
    
  > 配置hdfs-site.xml
  <configuration>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:/home/xx/hadoop/tmp/hdfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/home/xx/hadoop/tmp/tmp/hdfs/data</value>
    </property>

    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>

    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>localhost:9001</value>
    </property>
    <property>
      <name>dfs.webhdfs.enabled</name>
      <value>true</value>
    </property>
  </configuration>
  注意：上面的/home/xx/hadoop/tmp/hdfs/name和/home/xx/hadoop/tmp/tmp/hdfs/data在之前已经创建了。
  ```
  3). 配置ssh
  ```
  > 首先执行sshd看下是否安装了ssh，如果没有按照提示安装。
  > ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
  > cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
  > 测试是否成功：ssh localhost
  ```
  
  4). 配置hadoop一些路径，这里配置的比较多，参考如下，写在/etc/profile文件中。
  ```
  # zk的一些配置
  export ZOOKEEPER_HOME=/home/xx/zookeeper-3.4.6    # 这是你的zk路径
  PATH=$ZOOKEEPER_HOME/bin:$PATH
  export PATH

  # java一些配置
  export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64  # 这是你的jdk路径
  export JRE_HOME=$JAVA_HOME/jre
  export CLASSPATH=.:$JAVA_HOME/lib:$JRE_HOME/lib
  export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH

  # hadoop，hdfs一些配置
  export HADOOP_HOME=/home/xx/hadoop     # 这是你的hadoop路径
  export HADOOP_PREFIX=$HADOOP_HOME
  export PATH=$PATH:$HADOOP_HOME/bin
  export PATH=$PATH:$HADOOP_HOME/sbin
  export HADOOP_MAPRED_HOME=$HADOOP_HOME
  export HADOOP_COMMON_HOME=$HADOOP_HOME
  export HADOOP_HDFS_HOME=$HADOOP_HOME
  export YARN_HOME=$HADOOP_HOME
  export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native

  # 下面路径配置的是tera调用的一些外库的路径，例如libhdfs.so
  export LD_LIBRARY_PATH=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server:$HADOOP_COMMON_LIB_NATIVE_DIR:$LD_LIBRARY_PATH

  export tera_install=/home/xx/tera_root
  ```

  5). 初始化hdfs：进入haddop目录，```bin/hdfs namenode -format```

  6). 启动```./sbin/start-dfs.sh```
  
  7). 检查是否成功：
  ```
  > 执行jps，是否有DataNode   SecondaryNameNode   NameNode
  > 或者进入网页：http://localhost:50070/，看能否打开
  ```

  8). 配置hdfs的tera目录：
  ```
  > 执行 bin/hdfs dfs -mkdir /tera
  > 查看 bin/hdfs dfs -ls / ， 如果看到tera，那么创建目录成功。
  ```
  至此，hdfs配置完成。

5. 启动tera集群

  1). 建立tera执行目录，创建在home目录就好，例如创建为：```mkdir tera_root```，进入目录，创建子目录：```mkdir log；mkdir data```，同时将之前一台机器上build文件copy到3台机器的tera_root目录下。copy之前生成的build目录下的bin和conf目录到tera_root。
  
  2). 修改conf目录下配置文件tera.flag，配置如下：
  ```
  # common
  --log_dir=../log
  --v=6
  --logbufsecs=0

  # master
  --tera_master_port=1100
  --tera_master_split_tablet_size=2048

  # tablet node
  --tera_tabletnode_port=2200
  # 下面是hdfs中创建的tera目录(创建的时候是什么此处就写什么)
  --tera_tabletnode_path_prefix=/tera
  # 这是一个cache目录，可以多个，如果没有请创建
  --tera_tabletnode_cache_paths=/home/xx/tera_cache

  # zk 
  # 3台机器的ip:port，默认zk的port就是2180
  --tera_zk_addr_list=192.168.0.107:2180,192.168.0.113:2180,192.168.0.114:2180
  # 之前zookeeper中创建的tera的根目录
  --tera_zk_root_path=/zk
  --tera_zk_timeout=10000

  ```

  至此，所有基本配置都完成了。

  3). 启动
  ```
  > 在zookeeper目录中执行：./bin/zkServer.sh status，找到leader那台机器，即master
  > 进入tera_root的bin目录，
    在master上执行：
    nohup ./tera_main --flagfile=../conf/tera.flag --tera_role=master &> ../log/master.stderr &
    
    在其他两台slave机器上执行：
    nohup ./tera_main --flagfile=../conf/tera.flag --tera_role=tabletnode &> ../log/tabletserver.stderr &

  > 在任意一台机器上的tera_root/bin目录中执行：./teracli
    Have Fun！
  ```

  不出意外，现在就能happy地使用tera了。




























