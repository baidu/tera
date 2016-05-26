# tablet可用性监控与统计

基于tablet维度，给出系统可用性指标。

tablet有多种状态，只有ready状态的tablet才可以正常提供服务（read、write、scan等），
tablet会在各种状态间迁移（例如负载均衡），迁移时，tablet会有短时间（正常情况是秒级）不可服务，
我们认为不可服务时间到达一定阈值（例如30s）的tablet处于不可用状态，这个阈值与使用场景紧密相关。

## 数据采集
```
          t1                            t2
tablet-a  |  <-not-ready->              |
tablet-b  |          <-not-ready--------|---->
tablet-c  |                             |
tablet..  |                             |
```


tablet状态的迁移由master控制，
master用`std::map<std::string, int64_t>`结构采集tablet状态数据，
std::map中std::string即tablet的path，是tablet全局唯一的id；int64_t即not-ready的开始时间。

当一个tablet状态迁移为not-ready时，master将`<tablet->GetPath(), current_time>`记录在std::map里，
在这个tablet恢复ready时再从map中删除掉。

master重启时可以恢复这个内存结构，所以不用持久化。

### 持久化的数据

master在定时任务（例如小时一次）里通过上述std::map计算过去一小时中，`tablet不可用总时间`（秒级）和`tablet应服务总时间`，并存入stat表。

`tablet不可用总时间`：例如tablet-a不可用5s，tablet-b不可用10s，那么过去1小时内，tablet总的不可用时间为15s；

在定时任务里，上述std::map里可以找到当前非ready状态的tablet（如图中tablet-b），采集它们的不可用时间；
还有部分tablet在定时任务之前已经恢复ready（如图中tablet-a），我们在将其从std::map中删除时，用counter累计它们在这一小时内不可用时间。

`tablet应服务总时间`：例如执行定时任务时总共有1000个tablet，那么过去1小时应服务总时间近似为 1000 * 60 * 60s.

## 分析计算

- 当前可用tablet比例（%）[master定时任务，数据采集时顺便完成]
    - 当前可用tablet比例 = 1 - 不可用tablet数/总tablet数
    - 不可用tablet数：master定时检查map中not-ready的tablet，如果not-ready达到一定阈值判定为不可用，计为1个不可用tablet
    - 总tablet数：通过tablet_manager获取

- 过去1h/1d/1m/1y的可用性 [teracli功能]
    - 每小时可用性数据已由master存入stat表；
    - teracli读stat表，获取原始数据，进行计算，给出过去1h/1d/1m/1y的可用性数据。
    - 过去1h可用性 = 1 - `tablet不可用总时间`/`tablet应服务总时间`


## FAQ

- master挂掉导致的数据缺失？
    - master控制tablet状态的迁移，除非集群接近崩溃否则tablet状态不会大规模变化。
    - 如果master有0.1%的时间不可用，那么损失0.1%的原始数据，对可用性计算影响不大。当master长时间不可用时，系统已不工作，可用性统计失去意义。
