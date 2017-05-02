通过OneBox体验Tera
=====

为了最快速度体验Tera，Tera提供一个OneBox方式布署，利用本地存储代替DFS，利用本地文件模拟ZK，单机多进程运行master、tabletnode。此模式下可正常使用Tera的全部功能，包括（不限于）：
* 表格新建(create)、schema修改(update)、删除(drop)、卸载(disable)、加载(enable)等
* 数据写入(put)、删除(delete)、随机读(get)、顺序读(scan)等
* 数据分片分裂(split)、合并(merge)、负载均衡(move)等

## 准备工作
1. 完成Tera的编译，请参考：https://github.com/baidu/tera/wiki/Build-Manual
2. 将编译生成的tera_master, tabletserver, teracli三个二进制文件放入example/onebox/bin
3. 如有需要，通过修改example/onebox/bin/config中的选项配置tabletnode个数
4. 进入example/onebox/bin/目录

## 启动、停止Tera
* 执行./launch_tera.sh即可启动Tera
```
[tera@localhost bin]$ sh launch_tera.sh 
launching tabletnode 1...
launching tabletnode 2...
launching tabletnode 3...
launching master...
```
* 执行./kill_tera.sh即可停止Tera

## 体验开始！
如果启动正常，尝试查看集群基本信息: `./teracli show`
```
[tera@localhost bin]$ ./teracli show
     tablename   status        size   lg_size  tablet  notready
-----------------------------------------------------------------
  0  meta_table  kTableEnable  4.01K  4.01K    1       0       
  1  stat_table  kTableEnable  4K     4K       1       0       
  -  total       -             8.01K  -        2       0    
```

查看更详细的表格信息：`./teracli showx`
```
[tera@localhost bin]$ ./teracli showx
     tablename   status        size    lg_size  tablet  notready  lread  read  rmax  rspeed  write  wmax  wspeed  scan  smax  sspeed
--------------------------------------------------------------------------------------------------------------------------------------
  0  meta_table  kTableEnable  4.01K   4.01K    1       0         0      0     0     0B/s    0      0     0B/s    0     0     0B/s  
  1  stat_table  kTableEnable  28.06K  28.06K   1       0         0      0     0     0B/s    0      0     0B/s    0     0     0B/s  
  -  total       -             32.07K  -        2       0         0      0     -     0B/s    0      -     0B/s    0     -     0B/s  
```

查看当前有哪些存活的tabletnode: `./teracli showts`
```
[tera@localhost bin]$ ./teracli showts
     address         status  workload  tablet  load  busy
-----------------------------------------------------------
  0  127.0.0.1:7701  kReady  4.01K     1       0     0   
  1  127.0.0.1:7702  kReady  32.06K    1       0     0   
  2  127.0.0.1:7703  kReady  0         0       0     0   
```

查看更详细的tabletnode信息：`./teracli showtsx`
```
[tera@localhost bin]$ ./teracli showtsx
     address         status  size    num  lread  r  rspd  w  wspd  s  sspd  rdly  rp  wp  sp  ld  bs  mem     cpu  net_tx  net_rx  dfs_r  dfs_w
-------------------------------------------------------------------------------------------------------------------------------------------------
  0  127.0.0.1:7701  kReady  4.01K   1    0      0  0B    0  0B    0  0B    0ms   0   0   0   0   0   12.10M  0    12.11K  5.57K   0      0    
  1  127.0.0.1:7702  kReady  32.06K  1    0      0  0B    0  0B    0  0B    0ms   0   0   0   0   0   11.83M  0    12.11K  5.57K   0      0    
  2  127.0.0.1:7703  kReady  0       0    0      0  0B    0  0B    0  0B    0ms   0   0   0   0   0   11.08M  0    12.11K  5.57K   0      0    
```

新建一个表格hello，包含两个列族cf0和cf1: `./teracli create 'hello{cf0,cf1}'`
```
[tera@localhost bin]$ ./teracli create 'hello{cf0,cf1}'
  hello <splitsize=512,mergesize=0> {
      lg0 <storage=disk> {
          cf0,
          cf1,
      },
  }
[tera@localhost bin]$ ./teracli show
     tablename   status        size    lg_size  tablet  notready
------------------------------------------------------------------
  0  hello       kTableEnable  4.01K   4.01K    1       0       
  1  meta_table  kTableEnable  4.01K   4.01K    1       0       
  2  stat_table  kTableEnable  44.12K  44.12K   1       0       
  -  total       -             52.14K  -        3       0   
```

写入一条数据(列族是cf0，列是column1): `./teracli put hello first_row_key cf0:column1 value`

随机读出刚写入的数据： `./teracli get hello first_row_key`
```
[tera@localhost bin]$ ./teracli get hello first_row_key                    
first_row_key:cf0:column1:1477367889042685:value
```

多写一些数据：
```
[tera@localhost bin]$ ./teracli put hello row2 cf1:column0 value2   
[tera@localhost bin]$ ./teracli put hello row2 cf1:column1 value3 
[tera@localhost bin]$ ./teracli put hello row0 cf1:column1 value0 
```

顺序读出整个表格的数据： `./teracli scan hello "" ""`
```
[tera@localhost bin]$ ./teracli scan hello "" ""
row0:cf1:column1:1477368174895134:value0
row1:cf0:column1:1477368150979077:value1
row2:cf1:column0:1477368164358077:value2
row2:cf1:column1:1477368169176632:value3
```

卸载表格： `./teracli disable hello`
```
[tera@localhost bin]$ ./teracli disable hello
>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 100%  1.000  1.000/s  00:00:01 
[tera@localhost bin]$ ./teracli show         
     tablename   status         size    lg_size  tablet  notready
-------------------------------------------------------------------
  0  hello       kTableDisable  4.01K   4.01K    1       0       
  1  meta_table  kTableEnable   4.01K   4.01K    1       0       
  2  stat_table  kTableEnable   44.12K  44.12K   1       0       
  -  total       -              52.14K  -        3       0   
```

删除表格： `./teracli drop hello`
```
[tera@localhost bin]$ ./teracli drop hello
[tera@localhost bin]$ ./teracli show      
     tablename   status        size    lg_size  tablet  notready
------------------------------------------------------------------
  0  meta_table  kTableEnable  4.01K   4.01K    1       0       
  1  stat_table  kTableEnable  44.12K  44.12K   1       0       
  -  total       -             48.13K  -        2       0  
```

## 写在最后
Tera onebox模式可以体验几乎所有的功能特性，希望通过上面的介绍可以让大家对Tera有一个初步的认识。
