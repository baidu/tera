# 多租户用户手册

## 权限管理


## Quota控制
Quota控制目前实现了Table级别的流量控制，具体使用说明可用`./teracli help quota`查看。

### show
展示Table级别的Quota值。
```
quota show

     TABLENAME  WRITEREQS(w/s)  WRITEBYTES(B/s)  READREQS(r/s)  READBYTES(B/s)  SCANREQS(s/s)  SCANBYTES(B/s)
---------------------------------------------------------------------------------------------------------------
        test       1000/3            2000/1          3000/2          -1/1            5000/1        6000/1
```

### showx
所有的Ts级别所有Table的Quota值。
```
quota showx

例子：
quota showx
          TSADDR     TABLENAME     WRITEREQS(w/s)  WRITEBYTES(B/s)  READREQS(r/s)  READBYTES(B/s)  SCANREQS(s/s)  SCANBYTES(B/s)
--------------------------------------------------------------------------------------------------------------------------------
       ip_addr1:3100    test          500/3            1000/1          1500/2          -1/1            2500/1        3000/1
       ip_addr2:3100    test          500/3            1000/1          1500/2          -1/1            2500/1        3000/1
```

### set
设置Table的Quota。

```
quota set <table_name> <limit_args>
其中：
<limit_args> 设置选项为：WRITEREQS|WRITEBYTES|READREQS|READBYTES|SCANREQS|SCANBYTES=[limit]/[period]
如果不设置period，默认为1s。

例子：
quota set test WRITEREQS=1000/2 READBYTES=4000/3 SCANREQS=100 SCANBYTES=-1
表示，设置表名为test的表Quota，WRITEREQS为每2s 1000 reqs，READBYTES为每3s 4000B，SCANREQS为每1s 100 reqs，SCANBYTES为-1表示quota不限制，如果用户不设置默认为不限制quota。
```
