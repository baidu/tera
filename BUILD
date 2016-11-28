Tera Build Manual
=================

This document is written for people who intend to build the SDK
libraries, server binaries and other utilities of Tera from the
source code.

Pre-requisite
=============

Operating System
* Linux
Dependency Overview
* sofa-pbrpc (1.1.0 or newer)
  https://github.com/baidu/sofa-pbrpc/
* Protocol Buffers (2.6.1 or newer)
  https://developers.google.com/protocol-buffers/
* snappy (1.1.1 or newer)
  http://google.github.io/snappy/
* zookeeper (3.3.3 or newer)
  https://zookeeper.apache.org/
* gflags (2.1.1 or newer)
  https://github.com/gflags/gflags/
* glog (0.3.3 or newer)
  https://github.com/google/glog/
* googletest (1.7.0 or newer)
  https://github.com/google/googletest/
* gperftools (2.2.1 only)
  https://code.google.com/p/gperftools/
* ins (0.14 or newer)
  https://github.com/baidu/ins/
* boost (1.53.0 or newer)
  http://www.boost.org/

On a 64-bit system, the following library is strongly recommended
by gperftools. See INSTALL document of gperftools for more details.
* libunwind (0.99 only)
  http://www.nongnu.org/libunwind/

Most Linux distributions have the following libraries
pre-installed. But if they are not, install them by yourself.
* readline
  https://cnswww.cns.cwru.edu/php/chet/readline/rltop.html
* ncurses
  https://www.gnu.org/software/ncurses/

Some dependencies mentioned above may need cmake to build.
* cmake (3.2.x or newer)
  https://cmake.org/

Basic Build
===========

1. Edit depends.mk
1.1. Open depends.mk
If there is no file named 'depends.mk' under project's root directory,
type 'cp depends.mk.template depends.mk' to create one.
1.2. Fill the right side of '=' of the following lines with the
locations of corresponding dependencies.
  SOFA_PBRPC_PREFIX=
  PROTOBUF_PREFIX=
  SNAPPY_PREFIX=
  ZOOKEEPER_PREFIX=
  GFLAGS_PREFIX=
  GLOG_PREFIX=
  GTEST_PREFIX=
  GPERFTOOLS_PREFIX=
  INS_PREFIX=
Take 'SNAPPY_PREFIX=' as an example. Assume snappy is installed under
/usr/local, then modify this line to:
  SNAPPY_PREFIX=/usr/local

1.3. Fill 'BOOST_INCDIR=' with the location of boost source code.
For example, if boost source code is under /usr/src/boost_1_57_0,
then this line should be modified to:
  BOOST_INCDIR=/usr/src/boost_1_57_0

1.4. Normally, other variables do not need to be modified.
In general, the headers and libraries of a software should be found
under 'include' and 'lib' of its installation directory, respectively.
*_INCDIR or *_LIBDIR need to be modified only if exceptions occur.

2. Type 'make' to compile the package.
  make -j4

One-command Build
=================

For convenience, we provide an one-command build script. Run the script:
  sh build.sh
It will automatically download the tarballs of all the needed dependencies,
install them under 'thirdparty' directory and link their static libraries
into Tera.

Output of Build
===============

After success build, the output will be put under 'build' directory.
The structure of the directory is as below:
* include: SDK headers
* lib: SDK libraries
* bin: executable program, including servers and utilities
