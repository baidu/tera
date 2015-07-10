### tera monitor ui
用于查看节点运行状态，主要用于以图表方式显示单个节点上面状态参数

### 依赖
依赖 teramo命令，请在src/bootstrap/settings.py里面配置 TERA_MO_BIN参数，指定teramo绝对路径

### 修改zk配置
请在 src/bootstrap/settings.py里面配置ZK_DICT参数

### 初始化安装环境
在机器上面安装 pip工具 请前往 [http://pip.baidu.com/](http://pip.baidu.com/),不需要外网访问

`sh build-runtime.sh`

### 启动服务

`sh start.sh`

### 停止服务

`sh shutdown.sh`


