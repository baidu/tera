表格schema在线更新
-----

**实现ing...**

## 作用：不停服更新schema

目前支持在线更新：

1. 表格级属性，如splitsize.

1. 增加、删除ColumnFamily，修改ColumnFamily属性，如max versions.

## 语意
当且仅当update请求返回成功时，新的schema已经同步到集群内的所有节点上。
在这之后，新schema一定生效。在返回成功前，对新schema的任何假设都是未定义的。
例如，新schema添加了一个新ColumnFamily名为cf3，update返回成功前，写入cf3的数据可能成功，也可能失败，也可能返回成功但在之后被gc掉，这属于未定义行为。

## 结果
update请求是一个同步操作，如果中途因为各种原因中断(例如超时)，
需要用户再调用IsUpdateSchemaDone(schema)来检查上次update的结果。

## 并发安全
并发执行update时，最终生效的是master收到的最后一个update请求。

## 实现

### master通知tabletnode表格schema发生变化
1. master收到client的update请求时，写成功meta表之后，
对当前所有处于ready状态的tablet发起schema的同步。

1. 对于此刻没有处于ready状态的tablet，
它要想提供服务，一定会经过master的ProcessReadyTablet()函数，
在它变为ready时通知对应的tabletnode更新schema.
如此，每个tablet都不会被遗漏。

1. master每300毫秒（可配）检查一次表格的所有tablet是否都已同步到最新的schema，
如果完成则向client返回成功。

1. 如果master中途挂掉，重启时通过Query拿到每个tablet的schema，以meta表中的schema为准，
如果需要则通知tabletnode更新。

# tabletnode应用新的schema
// TODO

# 实现计划
- [  ] master将新schema同步到整个集群，tabletnode收到新schema后无操作直接返回成功
    - 如果只更新表级属性，不需同步到tabletnode
    - 对于ready的tablet，立即同步到tabletnode; 否则等tablet变为ready状态时再同步
- [  ] tabletnode根据新的schema执行apply操作(ColumnFamily的更新)
- [  ] master中途挂掉重启后Query以及必要的同步操作
- [  ] IsUpdateSchemaDone(schema)接口的实现
