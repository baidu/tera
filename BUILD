Tera构建手册
============

本文件指导你从Tera源代码构建出Tera的SDK头文件、SDK库文件、服务端
可执行程序、以及其它工具。

系统要求
========

操作系统
* Linux系统
第三方软件
* sofa-pbrpc (1.0.0 or newer)
  https://github.com/BaiduPS/sofa-pbrpc/
* Protocol Buffers (2.4.1 or newer)
  https://developers.google.com/protocol-buffers/
* snappy (1.1.1 or newer)
  https://code.google.com/p/snappy/
* zookeeper (3.3.3 or newer)
  https://zookeeper.apache.org/
* gflags (2.1.1 or newer)
  https://github.com/gflags/gflags/
* glog (0.3.3 or newer)
  https://github.com/google/glog/
* gperftools (2.2.1 only)
  https://code.google.com/p/gperftools/
* boost (1.53.0 or newer)
  http://www.boost.org/

此外，在64位系统中，gperftools可能依赖以下软件，请参见gperftools
的安装说明。
* libunwind (0.99 only)
  http://www.nongnu.org/libunwind/

下列必需的开发库在多数Linux发行版中会默认预装，但在某些发行版中
可能需要自行安装。
* readline
  https://cnswww.cns.cwru.edu/php/chet/readline/rltop.html
* ncurses
  https://www.gnu.org/software/ncurses/

编译步骤
========

1. 编辑depends.mk
1.1. 在以下变量的=右边填上对应软件的安装路径
  SOFA_PBRPC_PREFIX=
  PROTOBUF_PREFIX=
  SNAPPY_PREFIX=
  ZOOKEEPER_PREFIX=
  GFLAGS_PREFIX=
  GLOG_PREFIX=.
  GPERFTOOLS_PREFIX=
以"SNAPPY_PREFIX="为例，假设snappy安装在/usr/local，那么这一行
应该改为：
  SNAPPY_PREFIX=/usr/local

1.2. 在"BOOST_INCDIR="右边填上boost源代码的存放路径
例如，boost源代码放在/usr/src/boost_1_57_0，那么这一行应该改为：
  BOOST_INCDIR=/usr/src/boost_1_57_0

1.3. 其它变量仅在特殊情况下需要修改
一般情况下软件的头文件和库文件分别位于安装目录下的include和lib，
仅在不是这种情况时才需要单独修改相应的*_INCDIR或*_LIBDIR变量。

2. 执行以下命令
  make -j4

一键构建
========

为了方便快速试用，我们提供了一键构建功能，执行下面这个命令：
  sh build.sh
它会自动下载所有第三方软件，安装在thirdparty目录下，并以静态连接
方式生成Tera的可执行程序。

构建结果
========

如果构建成功，构建结果将位于build目录下，目录结构如下：
* include: SDK头文件
* lib: SDK库文件
* bin: 可执行程序，包括服务端程序和其它工具
