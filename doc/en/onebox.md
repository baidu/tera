OneBox Tera(Pseudo Distributed Mode)
=====

Your can run Tera in pseudo-distributed mode which means that Tera runs on a single host, but each Tera daemon(Master, TabletServer) runs as a separate process. Under pseudo-distributed mode, we store your data in local filesystem.

## Quick Start


1. git clone https://github.com/baidu/tera.git
1. cd tera
1. ./build.sh
1. cp {tera_main,teracli} example/onebox/bin
1. cd example/onebox/bin


## Start & Stop Tera
* Start Tera

```
[tera@localhost bin]$ ./launch_tera.sh
launching tabletnode 1...
launching tabletnode 2...
launching tabletnode 3...
launching master...
```

* Stop Tera

```
[tera@localhost bin]$ ./kill_tera.sh
```

## Teracli Exercises

* List all tables

```
[tera@localhost bin]$ ./teracli show
     tablename   status        size   lg_size  tablet  notready
-----------------------------------------------------------------
  0  meta_table  kTableEnable  4.01K  4.01K    1       0
  1  stat_table  kTableEnable  4K     4K       1       0
  -  total       -             8.01K  -        2       0
```

* List all tables with more details

```
[tera@localhost bin]$ ./teracli showx
     tablename   status        size    lg_size  tablet  notready  lread  read  rmax  rspeed  write  wmax  wspeed  scan  smax  sspeed
--------------------------------------------------------------------------------------------------------------------------------------
  0  meta_table  kTableEnable  4.01K   4.01K    1       0         0      0     0     0B/s    0      0     0B/s    0     0     0B/s
  1  stat_table  kTableEnable  28.06K  28.06K   1       0         0      0     0     0B/s    0      0     0B/s    0     0     0B/s
  -  total       -             32.07K  -        2       0         0      0     -     0B/s    0      -     0B/s    0     -     0B/s
```

* List all tablet servers

```
[tera@localhost bin]$ ./teracli showts
     address         status  workload  tablet  load  busy
-----------------------------------------------------------
  0  127.0.0.1:7701  kReady  4.01K     1       0     0
  1  127.0.0.1:7702  kReady  32.06K    1       0     0
  2  127.0.0.1:7703  kReady  0         0       0     0
```

* List all tablet servers with more details

```
[tera@localhost bin]$ ./teracli showtsx
     address         status  size    num  lread  r  rspd  w  wspd  s  sspd  rdly  rp  wp  sp  ld  bs  mem     cpu  net_tx  net_rx  dfs_r  dfs_w
-------------------------------------------------------------------------------------------------------------------------------------------------
  0  127.0.0.1:7701  kReady  4.01K   1    0      0  0B    0  0B    0  0B    0ms   0   0   0   0   0   12.10M  0    12.11K  5.57K   0      0
  1  127.0.0.1:7702  kReady  32.06K  1    0      0  0B    0  0B    0  0B    0ms   0   0   0   0   0   11.83M  0    12.11K  5.57K   0      0
  2  127.0.0.1:7703  kReady  0       0    0      0  0B    0  0B    0  0B    0ms   0   0   0   0   0   11.08M  0    12.11K  5.57K   0      0
```

* Create a table with specified schema

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

* Put data into your table

```
[tera@localhost bin]$ ./teracli put hello first_row_key cf0:column1 value
```

* Read data

```
[tera@localhost bin]$ ./teracli get hello first_row_key
first_row_key:cf0:column1:1477367889042685:value
```

* Put more data

```
[tera@localhost bin]$ ./teracli put hello row2 cf1:column0 value2
[tera@localhost bin]$ ./teracli put hello row2 cf1:column1 value3
[tera@localhost bin]$ ./teracli put hello row0 cf1:column1 value0
```

* Scan the table for all data at once

```
[tera@localhost bin]$ ./teracli scan hello "" ""
row0:cf1:column1:1477368174895134:value0
row1:cf0:column1:1477368150979077:value1
row2:cf1:column0:1477368164358077:value2
row2:cf1:column1:1477368169176632:value3
```

* Disable a table

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


* Drop the table

```
[tera@localhost bin]$ ./teracli drop hello
[tera@localhost bin]$ ./teracli show
     tablename   status        size    lg_size  tablet  notready
------------------------------------------------------------------
  0  meta_table  kTableEnable  4.01K   4.01K    1       0
  1  stat_table  kTableEnable  44.12K  44.12K   1       0
  -  total       -             48.13K  -        2       0
```
