#!/bin/bash

set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
########################################

WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"

export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}

cd ${DEPS_SOURCE}

# boost
wget http://softlayer-sng.dl.sourceforge.net/project/boost/boost/1.57.0/boost_1_57_0.tar.gz
tar zxf boost_1_57_0.tar.gz
rm -rf ${DEPS_PREFIX}/boost_1_57_0
mv boost_1_57_0 ${DEPS_PREFIX}

# protobuf
# wget --no-check-certificate https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz
git clone --depth=1 https://github.com/00k/protobuf
mv protobuf/protobuf-2.6.1.tar.gz .
tar zxf protobuf-2.6.1.tar.gz
cd protobuf-2.6.1
./configure ${DEPS_CONFIG}
make -j4
make install
cd -

# snappy
# wget --no-check-certificate https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz
git clone --depth=1 https://github.com/00k/snappy
mv snappy/snappy-1.1.1.tar.gz .
tar zxf snappy-1.1.1.tar.gz
cd snappy-1.1.1
./configure ${DEPS_CONFIG}
make -j4
make install
cd -

# sofa-pbrpc
wget --no-check-certificate -O sofa-pbrpc-1.0.0.tar.gz https://github.com/BaiduPS/sofa-pbrpc/archive/v1.0.0.tar.gz
tar zxf sofa-pbrpc-1.0.0.tar.gz
cd sofa-pbrpc-1.0.0
sed -i '/BOOST_HEADER_DIR=/ d' depends.mk
sed -i '/PROTOBUF_DIR=/ d' depends.mk
sed -i '/SNAPPY_DIR=/ d' depends.mk
echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/boost_1_57_0" >> depends.mk
echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk
echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk
echo "PREFIX=${DEPS_PREFIX}" >> depends.mk
cd src
PROTOBUF_DIR=${DEPS_PREFIX} sh compile_proto.sh
cd ..
make -j4
make install
cd ..

# zookeeper
wget http://www.us.apache.org/dist/zookeeper/stable/zookeeper-3.4.6.tar.gz
tar zxf zookeeper-3.4.6.tar.gz
cd zookeeper-3.4.6/src/c
./configure ${DEPS_CONFIG}
make -j4
make install
cd -

# cmake for gflags
wget --no-check-certificate -O CMake-3.2.1.tar.gz https://github.com/Kitware/CMake/archive/v3.2.1.tar.gz
tar zxf CMake-3.2.1.tar.gz
cd CMake-3.2.1
./configure --prefix=${DEPS_PREFIX}
make -j4
make install
cd -

# gflags
wget --no-check-certificate -O gflags-2.1.1.tar.gz https://github.com/schuhschuh/gflags/archive/v2.1.1.tar.gz
tar zxf gflags-2.1.1.tar.gz
cd gflags-2.1.1
cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC
make -j4
make install
cd -

# glog
wget --no-check-certificate -O glog-0.3.3.tar.gz https://github.com/google/glog/archive/v0.3.3.tar.gz
tar zxf glog-0.3.3.tar.gz
cd glog-0.3.3
./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
make -j4
make install
cd -

# libunwind for gperftools
wget http://download.savannah.gnu.org/releases/libunwind/libunwind-0.99-beta.tar.gz
tar zxf libunwind-0.99-beta.tar.gz
cd libunwind-0.99-beta
./configure ${DEPS_CONFIG}
make CFLAGS=-fPIC -j4
make CFLAGS=-fPIC install
cd -

# gperftools (tcmalloc)
# wget --no-check-certificate https://googledrive.com/host/0B6NtGsLhIcf7MWxMMF9JdTN3UVk/gperftools-2.2.1.tar.gz
git clone --depth=1 https://github.com/00k/gperftools
mv gperftools/gperftools-2.2.1.tar.gz .
tar zxf gperftools-2.2.1.tar.gz
cd gperftools-2.2.1
./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
make -j4
make install
cd -

git clone https://github.com/fxsjy/ins
cd ins
sed -i "s/^PREFIX=.*/PREFIX=${DEPS_PREFIX}/" Makefile
make -j3
make install
cd -

cd ${WORK_DIR}

########################################
# config depengs.mk
########################################

sed -i 's/^SOFA_PBRPC_PREFIX=.*/SOFA_PBRPC_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/^PROTOBUF_PREFIX=.*/PROTOBUF_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/^SNAPPY_PREFIX=.*/SNAPPY_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/^ZOOKEEPER_PREFIX=.*/ZOOKEEPER_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/^GFLAGS_PREFIX=.*/GFLAGS_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/^GLOG_PREFIX=.*/GLOG_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/^GPERFTOOLS_PREFIX=.*/GPERFTOOLS_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/^BOOST_INCDIR=.*/BOOST_INCDIR=.\/thirdparty\/boost_1_57_0/' depends.mk
sed -i 's/^INS_PREFIX=.*/INS_PREFIX=.\/thirdparty/' depends.mk

########################################
# build tera
########################################

make -j4

