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
rm -rf ${DEPS_SOURCE}/*
git clone --depth=1 http://gitlab.baidu.com/baidups/third.git ${DEPS_SOURCE}
git clone --depth=1 http://gitlab.baidu.com/baidups/sofa-pbrpc.git ${DEPS_SOURCE}/sofa-pbrpc
git clone --depth=1 http://gitlab.baidu.com/baidups/ins.git ${DEPS_SOURCE}/ins

cd ${DEPS_SOURCE}

# boost
if [ ! -d "${DEPS_PREFIX}/boost_1_57_0/boost" ]; then
    tar zxf boost_1_57_0.tar.gz
    rm -rf ${DEPS_PREFIX}/boost_1_57_0
    mv boost_1_57_0 ${DEPS_PREFIX}
fi

# protobuf
if [ ! -f "${DEPS_PREFIX}/lib/libprotobuf.a" ] || [ ! -d "${DEPS_PREFIX}/include/google/protobuf" ]; then
    tar zxf protobuf-2.6.1.tar.gz
    cd protobuf-2.6.1
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
fi

# snappy
if [ ! -f "${DEPS_PREFIX}/lib/libsnappy.a" ] || [ ! -f "${DEPS_PREFIX}/include/snappy.h" ]; then
    tar zxf snappy-1.1.1.tar.gz
    cd snappy-1.1.1
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
fi

# sofa-pbrpc
if [ ! -f "${DEPS_PREFIX}/lib/libsofa-pbrpc.a" ] || [ ! -d "${DEPS_PREFIX}/include/sofa/pbrpc" ]; then
    cd sofa-pbrpc
    sed -i '/BOOST_HEADER_DIR=/ d' depends.mk
    sed -i '/PROTOBUF_DIR=/ d' depends.mk
    sed -i '/SNAPPY_DIR=/ d' depends.mk
    echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/boost_1_57_0" >> depends.mk
    echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "PREFIX=${DEPS_PREFIX}" >> depends.mk
    make -j4
    make install
    cd -
fi

# zookeeper
if [ ! -f "${DEPS_PREFIX}/lib/libzookeeper_mt.a" ] || [ ! -d "${DEPS_PREFIX}/include/zookeeper" ]; then
    tar zxf zookeeper-3.4.6.tar.gz
    cd zookeeper-3.4.6/src/c
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
fi

# cmake for gflags
if [ ! -f "${DEPS_PREFIX}/bin/cmake" ]; then
    tar zxf CMake-3.2.1.tar.gz
    cd CMake-3.2.1
    ./configure --prefix=${DEPS_PREFIX}
    make -j4
    make install
    cd -
fi

# gflags
if [ ! -f "${DEPS_PREFIX}/lib/libgflags.a" ] || [ ! -d "${DEPS_PREFIX}/include/gflags" ]; then
    tar zxf gflags-2.1.1.tar.gz
    cd gflags-2.1.1
    cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC
    make -j4
    make install
    cd -
fi

# glog
if [ ! -f "${DEPS_PREFIX}/lib/libglog.a" ] || [ ! -d "${DEPS_PREFIX}/include/glog" ]; then
    tar zxf glog-0.3.3.tar.gz
    cd glog-0.3.3
    ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
    make -j4
    make install
    cd -
fi

# gtest
if [ ! -f "${DEPS_PREFIX}/lib/libgtest.a" ] || [ ! -d "${DEPS_PREFIX}/include/gtest" ]; then
    unzip gtest-1.7.0.zip
    cd gtest-1.7.0
    ./configure ${DEPS_CONFIG}
    make
    cp -a lib/.libs/* ${DEPS_PREFIX}/lib
    cp -a include/gtest ${DEPS_PREFIX}/include
    cd -
fi

# libunwind for gperftools
if [ ! -f "${DEPS_PREFIX}/lib/libunwind.a" ] || [ ! -f "${DEPS_PREFIX}/include/libunwind.h" ]; then
    tar zxf libunwind-0.99-beta.tar.gz
    cd libunwind-0.99-beta
    ./configure ${DEPS_CONFIG}
    make CFLAGS=-fPIC -j4
    make CFLAGS=-fPIC install
    cd -
fi

# gperftools (tcmalloc)
if [ ! -f "${DEPS_PREFIX}/lib/libtcmalloc_minimal.a" ]; then
    tar zxf gperftools-2.2.1.tar.gz
    cd gperftools-2.2.1
    ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
    make -j4
    make install
    cd -
fi

# ins
if [ ! -f "${DEPS_PREFIX}/lib/libins_sdk.a" ] || [ ! -f "${DEPS_PREFIX}/include/ins_sdk.h" ]; then
    cd ins
    sed -i "s|^PREFIX=.*|PREFIX=${DEPS_PREFIX}|" Makefile
    sed -i "s|^PROTOC=.*|PROTOC=${DEPS_PREFIX}/bin/protoc|" Makefile
    BOOST_PATH=${DEPS_PREFIX}/boost_1_57_0 make -j4 install_sdk
    cd -
fi

# functional test: nose
if [ ! -f "${DEPS_PREFIX}/bin/nosetests" ] || [ ! -d "${DEPS_PREFIX}/include/nose" ]; then
    tar zxf nose.tar.gz
    rm -rf ${DEPS_PREFIX}/include/nose
    mv nose ${DEPS_PREFIX}/include/
    mv ${DEPS_PREFIX}/include/nose/nosetests ${DEPS_PREFIX}/bin
fi

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
sed -i 's/^GTEST_PREFIX=.*/GTEST_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/^GPERFTOOLS_PREFIX=.*/GPERFTOOLS_PREFIX=.\/thirdparty/' depends.mk
sed -i 's/^BOOST_INCDIR=.*/BOOST_INCDIR=.\/thirdparty\/boost_1_57_0/' depends.mk
sed -i 's/^INS_PREFIX=.*/INS_PREFIX=.\/thirdparty/' depends.mk

########################################
# build tera
########################################

make -j4

