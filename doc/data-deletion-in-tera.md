# Tera中的数据删除实现

tera中数据删除分为两种：
  * kv存储中，采用leveldb标记删除方法。
  * 表格存储中，采用多种删除标记方法实现。
  
## 删除标记类型

```
src/leveldb/include/leveldb/option.h
enum TeraKeyType {
    ...
    TKT_DEL            = 1,  // 行删除
    TKT_DEL_COLUMN     = 2,  // 列族删除
    TKT_DEL_QUALIFIERS = 3,  // 单列删除（所有版本）
    TKT_DEL_QUALIFIER  = 4,  // 单列删除（最新版本）
    TKT_VALUE          = 5,  // 值类型
    ...
};
```

## 实现原理

Tera Rawkey拼装格式：
```
/**
 *  readable encoding format:
 *  [rowkey\0|column\0|qualifier\0|type|timestamp]
 *  [ rlen+1B| clen+1B| qlen+1B   | 1B | 7B      ]
 *
 *  binary encoding format:
 *  [rowkey|column\0|qualifier|type|timestamp|rlen|qlen]
 *  [ rlen | clen+1B| qlen    | 1B |   7B    | 2B | 2B ]
 **/
```

其中type字段为TeraKeyType类型，值为[1-4]时，此key为对应删除标记。行删除标记里，列族、列字段为空；列族删除标记里，列字段为空。

rawkey之间进行比较时，按字段从前向后比较，这样可以实现：
  * 行删除标记一定在行首。
  * 列族删除标记一定在列首。
  * 列删除标记一定在列首。
  
当我们发起读操作或compact时，可以尽早发现数据删除。

**注**

  * timestamp字段是补码，排序时会将时间更近的key排在前面。
