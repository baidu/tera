Docker部署Tera
===============
##第一章 准备
###Dokcer篇
* 安装Docker
  请参见https://docs.docker.com/

* 为Docker添加Non-root权限
  请参见https://docs.docker.com/installation/ubuntulinux/ 之“Create a Docker group”篇

###机器篇
* 配置集群间免密码ssh登陆

###Python和Docker镜像篇
* 安装Python包并下载Tera镜像

  执行exsample/docker/install.sh脚本。参数为Tera镜像名。脚本会自动安装Python所需包并将Tera镜像打包存储到本地tera.tar.gz。

###分发镜像
* 分发镜像有两种方法：

  1. 登陆每台机器，下载Tera镜像
  2. 将install.sh打包的Tera镜像拷贝到每台机器。在机器上执行docker load < ./tera.tar.gz

##第二章 奔跑
###配置篇
* 修改example/docker目录下conf中的配置，其中：

  ```
  ip    ：集群ip地址，用冒号分隔
  hdfs  ：hdfs集群中datanode的个数
  zk    ：Zookeeper集群中zk个数
  tera  ：Tera集群中tablenode个数
  log_prefix：log目录
  ```

###执行篇
* 执行cluster_setup.py
  cluster_setup.py脚本默认启动zk，hdfs和Tera，可通过参数选择启动某一或两项。

      --help：  显示帮助
      固定参数：配置文件路径
      --docker：Tera的docker镜像ID
      --zk：    启动zookeeper集群
      --hdfs：  启动hdfs集群
      --tera：  启动tera集群

      
  例如：

  ```
  python cluster_setup.py conf --docker abc   // 启动zk，hdfs和tera集群
  
  python cluster_setup.py conf --docker abc --zk --hdfs   // 启动zk和hdfs集群
  ```
