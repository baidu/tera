# RowMutation

tera sdk中通过RowMutation结构描述一次行更新操作，包含删除操作。
一个RowMutaion中可以同时对多列进行操作，保证：
 * 服务端生效时序与RowMutation的执行时序相同。比如对某列的删除+更新，服务端生效时不会乱序，导致先更新再删除的情况发生。
 * 同一个RowMutation中的操作保证同时成功或失败。
 * 操作不存在的列会返回成功，但无法读取。
 
## API

### Key-Value存储

```

```

### 表格存储
