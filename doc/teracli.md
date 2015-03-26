teracli命令行工具使用手册
---

<!--## 表格创建 (create)-->

## 表格schema更新-全量更新 (update)

`create   <tablename> <schema>`

1. schema的语法必须和create表格时一致。tera会用新的schema覆盖原有schema.

1. tablename不要超过256个字符，合法字符包括：数字、大小写字母、下划线`_`、连字符`-`，
首字符不能为数字。

1. schema字符串请用引号括起来，避免`#`等特殊符号被shell解释。

## 表格schema更新-增量更新 (update-part)

适用于只需更新少数几个属性的场景，
建表时未指定的属性值，也可以用`update-part`在建表完成后设定。

`update-part <tablename> <part-schema>`

part-schema 语法:

    "prefix1:property1=value1, prefix2:property2=value2, ..."

prefix: 如果更新的是表格的属性，则取值"table"；如果更新的是lg属性，则取值为lg名字，cf与lg同理。

property: 待更新属性名，例如： splitsize | compress | ttl | ...

value:    待更新属性的新值。

每次可以更新一个或者多个属性，它们之间用逗号分隔。

例1： "table:splitsize=9, lg0:compress=none, cf3:ttl=0"

例2： "lg0:use_memtable_on_leveldb=true"

