# 通信

## 通信协议

使用protobuf定义通信协议，参见`src/proto/`.

# meta表

## meta表的加载
1. meta表持久化存储在dfs中约定好的目录下；
1. master启动时先从zk获取master锁，再扫描zk上的文件获取ts状态，与众ts通信获知在服务状态的tablet.
1. 从meta表目录下找到meta表(meta表持久化存储的位置是约定好的，大家都知道的)，
选择某个ts加载meta表，然后将载有meta表的ts位置[ip:port]记录在zk下约定好的文件中。

## client如何获取meta表？
参考[meta表的加载]，从zk读取某个特定文件就可以知道哪个ts加载了meta表，进而访问meta表。

# 寻址

## client如何获知某个tablet在哪个ts上？
先访问meta表，然后根据meta表的内容知道某个tablet在哪个ts上。

## client如何获知master位置？
master启动时会在zk上的master锁中记下自己的位置[ip:port].

## master如何获知ts的加入或退出？
定期扫描特定目录（ts启动时会通过zk在该目录下创建文件锁）。
