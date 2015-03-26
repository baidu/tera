# 通信

## 通信协议

使用protobuf定义通信协议，参见`src/proto/`.

# meta表

## meta表的加载
master找到meta表(meta表持久化存储的位置是约定好的，大家都知道的)，
选择某个ts加载meta表，然后将载有meta表的ts位置[ip:port]记录在zk下约定好的文件中。

## client如何获取meta表？
参考[meta表的加载]，从zk读取某个特定文件就可以知道哪个ts加载了meta表，进而访问meta表。

# 系统启动(和三者交互有关的部分)

## ts的启动
1. 在zk上注册特定文件
1. 在该文件中写入自身[ip:port]

## master的启动
1. 通过zk获取master锁
1. 扫描特定目录发现活跃状态的ts
1. 与每个ts通信获取已经被载入的tablet列表
1. 如果meta表未载入，则master命令某个ts载入meta表
1. 读取meta表，获知所有tablet的信息以及未被载入的tablet集合

# 寻址

## client如何获知某个tablet在哪个ts上？
先访问meta表，然后根据meta表的内容知道某个tablet在哪个ts上。

## client如何获知master位置？
master启动时会在zk上的master锁中记下自己的位置[ip:port].

## master如何获知ts的信息？
参考master的启动，可以获知master初始化时各个ts的状态(例如位置[ip:port])，

通过zk获知ts的加入或退出。
