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

### 其他

#### 低负载ts选择规则

 * 先按lread指标排序
  * 选出其中2 * max_tablet_num个最小的ts做为候选
  * 保证读和扫描负载相对低
 * 将侯选ts再按cpu消耗排序
  * 选出其中最小的max_tablet_num个做为最终目标ts

#### 输入文件格式

 * 不支持空、含有空格等字符串key，为了保证输入文件格式简单
