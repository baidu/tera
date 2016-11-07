基础的访问控制方案


目标
---

1. 实现基础的读写访问控制，例如
   - 用户A不能读取表格T1
   - 用户B不能写入表格T2
1. 主要用在内网环境，暂不考虑恶意攻击，考虑后续实现更安全的方案
1. 这里主要目标是实现基础的多用户功能，暂不考虑完整的多租户体系
1. 目前还是表格级访问控制


概述
---

三个步骤：认证（Authentication）、授权（Authorization）、计帐（Accounting）

1. 认证：根据读写请求识别用户身份
    - 用户发起读写请求时带上token
    - ts 根据token判定用户身份
1. 授权：根据已判定的用户身份决定权限（是否允许读写）
    - 表格schema里记录读、写权限组
    - ts 记录着用户信息（在哪些组）
    - ts 对用户信息进行检查，决定是否允许访问
1. 计帐：记录消耗的资源（例如1qps）~~并执行quota限制策略~~
    - 记录消耗的资源~~计算quota决定是否允许本次请求~~
    
实现
---

### 用户


```

message UserInfo {
    required string user_name = 1;
    repeated string group_name = 2;
    optional string token = 3;
}
```

例如：

```
{
    user_name: "zhangsan",
    group_name: {"group_0", "group_1", "group_2"},
    token: "zhangsan's token",
}
```

用户信息存储在meta表里。

创建、更新（所属group、quota）、删除

1. client向master发起请求，master写入meta表

更新的实现：

用户信息的变更均通过master实现，所以master能够知道用户信息的变更；

ts获知用户信息变更：

方案一：定期scan meta表，重建内存

优点：比较简单，外部依赖尽量少

缺点：用户信息更新会有延迟；增加对meta表的压力

针对缺点的优化：生产环境中，从用户提出变更权限 到 实际使用，一般能接受分钟级响应延迟。
单个ts在半小时到一小时（可配）内随机选个时间点执行scan meta，
用户变更是管理操作，管理员（管理系统）在更新用户信息以后直接触发ts更新用户信息，完成后告知用户。（整个操作可以自动化）

方案二：master通知ts［决定采用方案二］

1. master写meta表，ts在启动时从meta表读取userinfo
2. 写meta表成功则向client返回“更新进行中”，写失败返回错误；最终结果client自己check
3. master取当前ts列表存入一个set，通知它们userinfo变更（增、删、改）
4. ts收到变更请求时原子性的变更userinfo（内存操作）
5. ts多次失败kick掉（纯内存操作失败不正常）；ts返回ok或者ts挂掉时从set删掉;当set变空时本次userinfo变更完成
4. 如果中途master重启，初始化时读meta表拿到userinfo，在query ts时拿到ts汇报的userinfo，如果不一致则重启更新过程，通知ts更新

优点：及时，对meta表无额外压力

缺点：实现比较复杂（考虑ts重启、master重启、网络异常导致通知一部分ts失败等）

Q：为什么更新userinfo的最终结果要client自己check而不是等完成后返回ok？

A：因为整个过程没法原子完成，client需要获知结果时，必须考虑更新过程中master挂掉。对master重启这种情况，只能提供一个接口让用户去check上次更新是否完成。所以，只有check才能保证100%拿到准确的结果。


### 表格权限

默认不设置权限（兼容）

表格schema里有3个权限组：
read_group/write_group/scan_group

schema里某个组不存在或为空时，对应的操作不限制。
例如某个表的 read_group 未设置（默认），或者为空，则这个表的读不进行限制（兼容旧版）


举例
---

表格：read_table这个表允许spider和build这2个组读取；写和扫描不限制。

```
read_table<read_group=spider:build,splitsize=500>{lg0{cf0}}
```

用户：

```
dns {
	group_name: {
		spider, 
		www
	}
}


rts {
	group_name: {
		rank
	}
}
```

有权限读取：

1. 用户dns读取read_table时，ts从请求中取出token，识别出是dns这个用户；
1. dns用户想要读read_table表，这个表允许spider和build这2个group（组）读取，而dns属于spider组，所以应该允许dns的读请求

无权限读取：

1. 用户rts想要读取read_table时，ts发现rts用户所在的组**都**没有读取权限，拒绝请求