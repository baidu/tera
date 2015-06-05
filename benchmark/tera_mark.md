# 测试方法

## tera_mark

#### 主要flag
1. 基本（最小）配置

  ```
  1.测试中使用的table名
    -tablename (table_name) type: string default: ""
  2.测试模式：w表示write（只写）、r表示random-read（只读）、s表示sequential-read（顺序读）、m表示mix（读写混合模式）
    -mode (mode [w|r|s|m]) type: string default: "w"
  3.同步/异步选项
    -type (type [sync|async]) type: string default: "async"
  ```
  
2. 限速和流控配置

  ```
  1.异步方式下，最大允许的未完成RPC个数/request大小
    在压力测试下，当TabletServer的处理能力达到极限后，可避免对其产生更大压力
    -pend_count (max_pending_count) type: int64 default: 100000
    -pend_size (max_pending_size) type: int64 default: 100
  2.最大输出带宽和RPC频率，用于控制测试压力
    -max_outflow (max_outflow) type: int64 default: -1
    -max_rate (max_rate) type: int64 default: -1
  ```
  
3. Scan配置
  
  ```
  1.Row范围和Column列表
    -start_key (start_key(scan)) type: string default: ""
    -end_key (end_key(scan)) type: string default: ""
    -cf_list (cf_list(scan)) type: string default: ""
  2.流式开关，打开后可避免RPC通信开销的影响，提升scan性能
    -scan_streaming (enable streaming scan) type: bool default: false
  ```
  
4. 其它配置
  
  ```
  1.是否打开数据正确性验证，打开后，写入时会在value后加入checksum，读取时用此checksum对value做校验
    -verify (md5 verify(writer&read)) type: bool default: true
  2.Scan时每个RPC的可携带的结果大小
    -buf_size (scan_buffer_size) type: int32 default: 65536
  3.Scan时是否打印读出的数据
    -print (print(scan)) type: bool default: false
  ```

#### 使用方法
1. write模式

  ```
  通过标准输入（stdin）传入row、column列表、timestamp、value，每行一个
  格式：ROW  \t  VALUE  [\t  FAMILY1:QUALIFIER11,QUALIFIER12,…;FAMILY2:QUALIFIER21,QUALIFIER22,…;…]  [\t  timestamp]
  实例：user8989436402715314678      #.?27:3?-)95)%#6!,(?2      cf1:field2,filed7      12345
        user8989436402715314678      #.?27:3?-)95)%#6!,(?2      cf1:      12345
        user8989436402715314678      #.?27:3?-)95)%#6!,(?2      cf1:field2,filed7
  ```
2. random-read模式

  ```
  通过标准输入（stdin）传入row、colume列表、timestamp范围（smallest_timestamp、largest_timestamp），每行一个
  格式：ROW  [\t  FAMILY1:QUALIFIER11,QUALIFIER12,…;FAMILY2:QUALIFIER21,QUALIFIER22,…;…]  [\t  largest_timestamp[,smallest_timestamp]]
  实例：user8989436402715314678      cf3:field6;cf4:filed7      12345,10000
        user8989436402715314678      ;      12345,10000
        user8989436402715314678      cf3:field6;cf4:filed7      12345
        user8989436402715314678      cf3:field6;cf4:filed7
        user8989436402715314678
  ```
3. scan(sequential-read)模式

  ```
  通过flag传入row的范围（start_key、end_key）和column列表（cf_list）
  ```
4. mix模式

  ```
  通过标准输入（stdin）传入要write/random-read的行信息，第一列用PUT/GET分别表示write/random-read，其它列的格式与上述1、2相同
  实例：PUT      user8989436402715314678      #.?27:3?-)95)%#6!,(?2      cf1:field2,filed7      12345
  实例：GET      user8989436402715314678      cf3:field6;cf4:filed7      12345,10000
  ```

#### 使用实例
1. write模式
  
  ```
  echo -e "user8989436402715314678\t#.?27:3?-)95)%#6!,(?2\tcf1:field2,filed7\t12345" | ./tera_mark --mode=w --tablename=usertable --type=sync --verify=true
  ```
2. random-read模式

  ```
  echo -e "user8989436402715314678\tcf3:field6;cf4:filed7\t12345,10000" | ./tera_mark --mode=r --tablename=usertable --type=async --verify=false
  ```
3. scan(sequential-read)模式

  ```
  ./tera_mark --mode=s --tablename=usertable --start_key=user8 --end_key=user9 --cf_list=cf1,cf2,cf3
  ```
4. mix模式

  ```
  echo -e "PUT\tuser8989436402715314678\t#.?27:3?-)95)%#6!,(?2\tcf1:field2,filed7\t12345" | ./tera_mark --mode=m --tablename=usertable --type=async --verify=false
  echo -e "GET\tuser8989436402715314678\tcf3:field6;cf4:filed7\t12345,10000" | ./tera_mark --mode=m --tablename=usertable --type=async --verify=false
  ```
