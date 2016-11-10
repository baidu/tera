#读取工具使用说明

##单行读取工具seekone

`./seekone —tera_conf=[conf_path] —table_name=[table_name]  —key=[row_key] —snapshot=[snapshot_id] —fields=[fields]`

其中，`tera_conf`为tera的配置文件所在路径，`table_name`为要读取的表名，`row_key`为需要读取的行，`snapshot`为指定某个快照下的数据，默认为0，即不指定快照，`fields`为需要读取的`cf`或`qualifier`，可以指定多个`cf`和`qualifier`，格式为`cf0:qualifier1+cf1:qualifier2+cf2` ，即：用`cf:qualifer`的形式指定需要读取的字段，如果`cf`后面不跟`qualifier`，则读出此`cf`下的所有字段，不同字段之间用`+`连接

##多行读取工具seeksign

`./seeksign —tera_conf=[conf_path] —table=[table_name] —keys=[keys_file_path] —snapshot=[snapshot_id] —fields=[fields]`

其中，`keys_file_path`为需要读取的keys列表所在的文件路径，每行一个key，其它所有参数与seekone中意义相同

##扫描读取工具multiseek

`./multiseek —tera_conf=[conf_path] —table_name=[table_name] —start_key=[start_key] —end_key=[end_key] —snapshot=[snapshot_id] —fields=[fields] --max_row_num=[row_num]`

其中，`start_key`指定了扫描的超始key，`end_key`热宝了扫描的结束key，前闭后开，如果均为空，则扫描全表。`row_num`指定了整个扫描过程中，最大扫描的行数，达到此行数之后，整个过程即结束。其余参数与seekone中意义相同
