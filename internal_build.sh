#!/bin/bash

########################################
# download & build depend software
########################################

DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared"
DEPS_BUILD="./configure ${DEPS_CONFIG} && make -j4 && make install"

export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}

cd ${DEPS_SOURCE}

git clone http://gitlab.baidu.com/baidups/third.git ${DEPS_SOURCE}
git clone http://gitlab.baidu.com/baidups/sofa-pbrpc.git ${DEPS_SOURCE}/sofa-pbrpc

# boost
tar zxf boost_1_57_0.tar.gz
mv boost_1_57_0 ${DEPS_PREFIX}

# protobuf
tar zxf protobuf-2.6.0.tar.gz
( cd protobuf-2.6.0 && eval ${DEPS_BUILD} )
if [ $? -ne 0 ]; then exit 1; fi

# snappy
tar zxf snappy-1.1.1.tar.gz
( cd snappy-1.1.1 && eval ${DEPS_BUILD} )
if [ $? -ne 0 ]; then exit 1; fi

# sofa-pbrpc
( cd sofa-pbrpc && sed -i '/BOOST_HEADER_DIR=/ d' depends.mk \
  && sed -i '/PROTOBUF_DIR=/ d' depends.mk \
  && sed -i '/SNAPPY_DIR=/ d' depends.mk \
  && echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/boost_1_57_0" >> depends.mk \
  && echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk \
  && echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk \
  && echo "PREFIX=${DEPS_PREFIX}" >> depends.mk \
  && make proto && make -j4 && make install )
if [ $? -ne 0 ]; then exit 1; fi

# zookeeper
tar zxf zookeeper-3.4.6.tar.gz
( cd zookeeper-3.4.6/src/c && eval ${DEPS_BUILD} )
if [ $? -ne 0 ]; then exit 1; fi

# cmake for gflags
tar zxf CMake-3.2.1.tar.gz
( cd CMake-3.2.1 && ./configure --prefix=${DEPS_PREFIX} && make -j4 && make install )
if [ $? -ne 0 ]; then exit 1; fi

# gflags
tar zxf gflags-2.1.1.tar.gz
( cd gflags-2.1.1 && cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google \
  && make -j4 && make install )
if [ $? -ne 0 ]; then exit 1; fi

# glog
tar zxf glog-0.3.3.tar.gz
( cd glog-0.3.3 && eval ${DEPS_BUILD} )
if [ $? -ne 0 ]; then exit 1; fi

# libunwind for gperftools
tar zxf libunwind-0.99-beta.tar.gz
( cd libunwind-0.99-beta && ./configure ${DEPS_CONFIG} CFLAGS=-U_FORTIFY_SOURCE && make -j4 && make install )
if [ $? -ne 0 ]; then exit 1; fi

# gperftools (tcmalloc)
tar zxf gperftools-2.2.1.tar.gz
( cd gperftools-2.2.1 && ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include \
  LDFLAGS=-L${DEPS_PREFIX}/lib && make -j4 && make install )
if [ $? -ne 0 ]; then exit 1; fi

cd -

########################################
# config depengs.mk
########################################

sed -i 's/SOFA_PBRPC_PREFIX=/SOFA_PBRPC_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/PROTOBUF_PREFIX=/PROTOBUF_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/SNAPPY_PREFIX=/SNAPPY_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/ZOOKEEPER_PREFIX=/ZOOKEEPER_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/GFLAGS_PREFIX=/GFLAGS_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/GLOG_PREFIX=/GLOG_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/GPERFTOOLS_PREFIX=/GPERFTOOLS_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/BOOST_INCDIR=/BOOST_INCDIR=.\/thirdparty\/boost_1_57_0/' depends.mk

########################################
# build tera
########################################

sh build_version.sh
make proto
make -j4

