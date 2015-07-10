Docker部署Tera
===============
##第一章 准备
###Dokcer篇
* 安装Docker
  请参见https://docs.docker.com/ 。Ubuntu下Docker安装命令为：

  ```
    wget -qO- https://get.docker.com/ | sh
  ```

* 为Docker添加Non-root权限

  执行sudo usermod -aG docker $USER，登出再登进

###机器篇
* 配置集群间免密码ssh登陆

###Python和Docker镜像篇
* 安装Python包并下载Tera镜像

  执行exsample/docker/install.sh脚本。参数为集群中除本机外的ip地址。例如，集群中有三台主机：192.168.100.2，192.168.100.3，192.168.100.4，当前登录192.168.100.2，则安装命令为：
  
  ```
    ./install.sh 192.168.100.3 192.168.100.4
  ```


##第二章 奔跑

###执行篇

* 一键启动

  直接执行/example/docker/cluster_setup.py会自动搭建一个本地Tera集群。配置如下：
  
  ```
    hdfs：namenode*1，datanode*1
    zk：  standalone
    tera：master*1， tabletnode*1
    log： $HOME
  ```
  
* 启动自定义Tera集群

  cluster_setup.py脚本默认启动zk，hdfs和Tera，可通过参数选择启动某一或两项。


      --help：  显示帮助
      --conf：  配置文件路径（配置文件说明见配置篇）
      --docker：Tera的docker镜像ID
      --zk：    启动zookeeper集群
      --hdfs：  启动hdfs集群
      --tera：  启动tera集群

      
  例如：

  ```
  python cluster_setup.py --conf my_config --docker abc   // 使用镜像abc启动配置为my_config的Tera集群
  
  python cluster_setup.py --zk --hdfs   // 启动单机版zk和hdfs
  ```

###配置篇


* 修改example/docker目录下conf中的配置自定义Tera集群，其中：

  ```
  ip    ：集群ip地址，用冒号分隔
  hdfs  ：hdfs集群中datanode的个数
  zk    ：Zookeeper集群中zk个数
  tera  ：Tera集群中tablenode个数
  log_prefix：log目录（默认为$HOME）
  
  例如 {"ip":"192.168.100.2:192.168.100.3:192.168.100.4", "hdfs":3, "zk":3, "tera":3}
  ```
