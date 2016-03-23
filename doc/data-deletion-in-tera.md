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

## 语义

### 行删除
**组成**：行key + 时间戳 + `TKT_DEL`

**作用**：删除所有 <= 此时间戳数据，包括所有列族、列、版本、删除标记等。

**注**：多Locality Group情况下，会被多次写入所有LG。

### 列族删除
**组成**：行key + 列族名 + 时间戳 + `TKT_DEL_COLUMN`

**作用**：删除对应列族中所有 <= 此时间戳数据，包括所有列、版本。

### 列删除（所有版本）
**组成**：行key + 列族名 + 列名 + 时间戳 + `TKT_DEL_QUALIFIERS`

**作用**：删除对应列所有 <= 此时间戳数据，包括所有版本。

### 列删除（最近版本）
**组成**：行key + 列族名 + 列名 + 时间戳 + `TKT_DEL_QUALIFIER`

**作用**：删除 <= 此时间戳的最新一个版本数据。

## 原理

删除某个元素时，在对应位置插入删除标记，数据读取时进行屏蔽。待后台compact执行时，进行物理删除。

删除标记与数据使用统一的拼装格式，Tera Rawkey拼装格式：
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

rawkey之间进行比较时，比较顺序为：行->列族->列->时间戳->标记类型。这样可以实现：
 * 行删除标记一定在行首。
 * 列族删除标记一定在列首。
 * 列删除标记一定在列首。
 * 有一种特殊情况，比如恰好有一个列名为空，时间戳又更近，此时列删除标记可能不在列首。但是时间戳更近代表此列是在删除之后重新写入的，所以排在删除标记之前并不会出现问题。行删除、列族删除情况类似。

当我们发起读操作或compact时，可以尽早发现数据删除。不必要读出更多无用数据。

**注**：timestamp字段是补码，排序时会将时间更近的key排在前面。

## 实现

数据删除被实现为一个可插入的策略，代码实现：

```
src/leveldb/include/leveldb/option.h
bool DefaultCompactStrategy::Drop(const Slice& tera_key, ...)
bool DefaultCompactStrategy::ScanDrop(const Slice& tera_key, ...)
```

这两个接口分别接受一个tera rawkey:
 * 返回true时，丢弃此key
 * 返回false时，保留此key
 
其中ScanDrop在数据读取时调用；Drop在后台Compact时调用。

其区别在于读取数据时，删除标记本身一定是被丢弃的；而comapct时，删除标记可能在未来还会被用到，并不一定被丢弃。
