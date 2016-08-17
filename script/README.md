# 通用操作脚本

## manual_rebalance.sh

手动负载均衡，将指定key范围的数据分片移至负载最低的服务器上。

### 用法

```
sh manual_rebalance.sh <key-range-list-file>
```

其中key-range-list-file 文件结构为：
  * 每一行分为两列，为对应key range的start/end
  * 支持多个range同时操作
  
### 配置

```
max_tablet_num=10    # 最大均衡并发数，若超过此值，放弃执行，控制风险
teracli="./teracli"  # 对应访问集群的客户端程序
table="hello"        # 目标表名
```


