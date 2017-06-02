#!/bin/bash

set -e -u -E # this script will exit if any sub-command fails

MIRROR=china
if [ $# -ge 1 ]; then
    MIRROR=$1
fi

WORK_DIR=$(cd $(dirname $0); pwd)
if [ ! -f "${WORK_DIR}/build.conf" ]; then
    cp ${WORK_DIR}/build.conf.template ${WORK_DIR}/build.conf
fi
source ./build.conf ${MIRROR}

########################################
# download & build depend software
########################################

DEPS_SOURCE=$WORK_DIR/thirdsrc
DEPS_PREFIX=$WORK_DIR/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"
FLAG_DIR=$WORK_DIR/.build

export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX} ${FLAG_DIR}

if [ ! -f "$WORK_DIR/depends.mk" ]; then
    cp $WORK_DIR/depends.mk.template $WORK_DIR/depends.mk
fi

cd ${DEPS_SOURCE}

# boost
if [ ${BOOST_VERSION} == "DISABLE" ]; then
    echo "Disable boost."
elif [ ! -f "${FLAG_DIR}/boost_${BOOST_VERSION}" ] \
    || [ ! -d "${DEPS_PREFIX}/boost_${BOOST_VERSION}/boost" ]; then
    wget --no-check-certificate -O boost_${BOOST_VERSION}.tar.bz2 ${BOOST_URL}
    tar xjf boost_${BOOST_VERSION}.tar.bz2 --recursive-unlink
    rm -rf ${DEPS_PREFIX}/boost_${BOOST_VERSION}
    mv boost_${BOOST_VERSION} ${DEPS_PREFIX}
    touch "${FLAG_DIR}/boost_${BOOST_VERSION}"
fi

# protobuf
if [ ${PROTOBUF_VERSION} == "DISABLE" ]; then
    echo "Disable protobuf."
elif [ ! -f "${FLAG_DIR}/protobuf_${PROTOBUF_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libprotobuf.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/google/protobuf" ]; then
    wget --no-check-certificate -O protobuf-${PROTOBUF_VERSION}.tar.bz2 ${PROTOBUF_URL}
    tar jxf protobuf-${PROTOBUF_VERSION}.tar.bz2 --recursive-unlink
    cd protobuf-${PROTOBUF_VERSION}
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/protobuf_${PROTOBUF_VERSION}"
fi

# snappy
if [ ${SNAPPY_VERSION} == "DISABLE" ]; then
    echo "Disable snappy."
elif [ ! -f "${FLAG_DIR}/snappy_${SNAPPY_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libsnappy.a" ] \
    || [ ! -f "${DEPS_PREFIX}/include/snappy.h" ]; then
    wget --no-check-certificate -O snappy-${SNAPPY_VERSION}.tar.gz ${SNAPPY_URL}
    tar zxf snappy-${SNAPPY_VERSION}.tar.gz --recursive-unlink
    cd snappy-${SNAPPY_VERSION}
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/snappy_${SNAPPY_VERSION}"
fi

# sofa-pbrpc
if [ ${SOFA_PBRPC_VERSION} == "DISABLE" ]; then
    echo "Disable sofa-pbrpc."
elif [ ! -f "${FLAG_DIR}/sofa-pbrpc_${SOFA_PBRPC_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libsofa-pbrpc.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/sofa/pbrpc" ]; then
    wget --no-check-certificate -O sofa-pbrpc-${SOFA_PBRPC_VERSION}.tar.gz ${SOFA_PBRPC_URL}
    tar zxf sofa-pbrpc-${SOFA_PBRPC_VERSION}.tar.gz --recursive-unlink
    cd sofa-pbrpc-${SOFA_PBRPC_VERSION}
    sed -i '/BOOST_HEADER_DIR=/ d' depends.mk
    sed -i '/PROTOBUF_DIR=/ d' depends.mk
    sed -i '/SNAPPY_DIR=/ d' depends.mk
    echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/boost_${BOOST_VERSION}" >> depends.mk
    echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "PREFIX=${DEPS_PREFIX}" >> depends.mk
    make -j4
    make install
    cd ..
    touch "${FLAG_DIR}/sofa-pbrpc_${SOFA_PBRPC_VERSION}"
fi

# zookeeper
if [ ${ZOOKEEPER_VERSION} == "DISABLE" ]; then
    echo "Disable zookeeper."
elif [ ! -f "${FLAG_DIR}/zookeeper_${ZOOKEEPER_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libzookeeper_mt.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/zookeeper" ]; then
    wget --no-check-certificate -O zookeeper-${ZOOKEEPER_VERSION}.tar.gz ${ZOOKEEPER_URL}
    tar zxf zookeeper-${ZOOKEEPER_VERSION}.tar.gz --recursive-unlink
    cd zookeeper-${ZOOKEEPER_VERSION}/src/c
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/zookeeper_${ZOOKEEPER_VERSION}"
fi

# gflags
if [ ${GFLAGS_VERSION} == "DISABLE" ]; then
    echo "Disable gflags."
elif [ ! -f "${FLAG_DIR}/gflags_${GFLAGS_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libgflags.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/gflags" ]; then
    wget --no-check-certificate -O gflags-${GFLAGS_VERSION}.tar.gz ${GFLAGS_URL}
    tar zxf gflags-${GFLAGS_VERSION}.tar.gz --recursive-unlink
    cd gflags-${GFLAGS_VERSION}
    cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/gflags_${GFLAGS_VERSION}"
fi

# glog
if [ ${GLOG_VERSION} == "DISABLE" ]; then
    echo "Disable glog."
elif [ ! -f "${FLAG_DIR}/glog_${GLOG_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libglog.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/glog" ]; then
    wget --no-check-certificate -O glog-${GLOG_VERSION}.tar.gz ${GLOG_URL}
    tar zxf glog-${GLOG_VERSION}.tar.gz --recursive-unlink
    cd glog-${GLOG_VERSION}
    ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/glog_${GLOG_VERSION}"
fi

# gtest
if [ ${GTEST_VERSION} == "DISABLE" ]; then
    echo "Disable gtest."
elif [ ! -f "${FLAG_DIR}/gtest_${GTEST_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libgtest.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/gtest" ]; then
    wget --no-check-certificate -O googletest-release-${GTEST_VERSION}.tar.gz ${GTEST_URL}
    tar zxf googletest-release-${GTEST_VERSION}.tar.gz --recursive-unlink
    cd googletest-release-${GTEST_VERSION}/googletest

    # XXX make gcc3 happy; what's the cmake way?
    sed -i 's/ -Wno-missing-field-initializers/ /' cmake/internal_utils.cmake

    mkdir tmpbuild
    cd tmpbuild
    cmake -DCMAKE_C_FLAGS="-fPIC" -DCMAKE_CXX_FLAGS="-fPIC" ..
    make
    cd ..
    cp -af tmpbuild/libgtest.a ${DEPS_PREFIX}/lib
    cp -af tmpbuild/libgtest_main.a ${DEPS_PREFIX}/lib
    cp -af include/gtest ${DEPS_PREFIX}/include
    cd ../..
    touch "${FLAG_DIR}/gtest_${GTEST_VERSION}"
fi

# libunwind for gperftools
if [ ${LIBUNWIND_VERSION} == "DISABLE" ]; then
    echo "Disable libunwind."
elif [ ! -f "${FLAG_DIR}/libunwind_${LIBUNWIND_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libunwind.a" ] \
    || [ ! -f "${DEPS_PREFIX}/include/libunwind.h" ]; then
    wget --no-check-certificate -O libunwind-${LIBUNWIND_VERSION}.tar.gz ${LIBUNWIND_URL}
    tar xzf libunwind-${LIBUNWIND_VERSION}.tar.gz --recursive-unlink
    cd libunwind-${LIBUNWIND_VERSION}
    ./configure ${DEPS_CONFIG}
    make CFLAGS=-fPIC -j4
    make CFLAGS=-fPIC install
    cd -
    touch "${FLAG_DIR}/libunwind_${LIBUNWIND_VERSION}"
fi

# gperftools (tcmalloc)
if [ ${GPERFTOOLS_VERSION} == "DISABLE" ]; then
    echo "Disable gperftools."
elif [ ! -f "${FLAG_DIR}/gperftools_${GPERFTOOLS_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libtcmalloc_minimal.a" ]; then
    wget --no-check-certificate -O gperftools-${GPERFTOOLS_VERSION}.tar.gz ${GPERFTOOLS_URL}
    tar zxf gperftools-${GPERFTOOLS_VERSION}.tar.gz --recursive-unlink
    cd gperftools-${GPERFTOOLS_VERSION}
    ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/gperftools_${GPERFTOOLS_VERSION}"
fi

# ins
if [ ${INS_VERSION} == "DISABLE" ]; then
    echo "Disable ins."
elif [ ! -f "${FLAG_DIR}/ins_${INS_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libins_sdk.a" ] \
    || [ ! -f "${DEPS_PREFIX}/include/ins_sdk.h" ]; then
    wget --no-check-certificate -O ins-${INS_VERSION}.tar.gz ${INS_URL}
    tar zxf ins-${INS_VERSION}.tar.gz --recursive-unlink
    cd ins-${INS_VERSION}
    sed -i "s|^PREFIX=.*|PREFIX=${DEPS_PREFIX}|" Makefile
    sed -i "s|^PROTOC=.*|PROTOC=${DEPS_PREFIX}/bin/protoc|" Makefile
    BOOST_PATH=${DEPS_PREFIX}/boost_${BOOST_VERSION} make install_sdk
    make -j4 install_sdk
    cd -
    touch "${FLAG_DIR}/ins_${INS_VERSION}"
fi

# nose (for test)
if [ ${NOSE_VERSION} == "DISABLE" ]; then
    echo "Disable nose."
elif [ ! -f "${FLAG_DIR}/nose_${NOSE_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/bin/nosetests" ] \
    || [ ! -d "${DEPS_PREFIX}/lib/"nose* ]; then
    wget --no-check-certificate -O nose-${NOSE_VERSION}.tar.gz ${NOSE_URL}
    tar zxf nose-${NOSE_VERSION}.tar.gz --recursive-unlink
    cd nose-${NOSE_VERSION}
    export PYTHONPATH=${DEPS_PREFIX}/lib
    python setup.py install --prefix=. --install-scripts=${DEPS_PREFIX}/bin --install-lib=${DEPS_PREFIX}/lib
    cd -
    touch "${FLAG_DIR}/nose_${NOSE_VERSION}"
fi

# readline (teracli_main.cc use this and lead to compile failed)
if [ ${READLINE_VERSION} == "DISABLE" ]; then
    echo "Disable readline."
elif [ ! -f "${FLAG_DIR}/readline_${READLINE_VERSION}" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libreadline.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/readline" ]; then
    wget --no-check-certificate -O readline-${READLINE_VERSION}.tar.gz ${READLINE_URL}
    tar zxf readline-${READLINE_VERSION}.tar.gz --recursive-unlink
    cd readline-${READLINE_VERSION}
    ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
    make install
    cd -
    touch "${FLAG_DIR}/readline_${READLINE_VERSION}"
fi

cd ${WORK_DIR}

########################################
# config depengs.mk
########################################

sed -i "s:^SOFA_PBRPC_PREFIX=.*:SOFA_PBRPC_PREFIX=$DEPS_PREFIX:" depends.mk
sed -i "s:^PROTOBUF_PREFIX=.*:PROTOBUF_PREFIX=$DEPS_PREFIX:" depends.mk
sed -i "s:^SNAPPY_PREFIX=.*:SNAPPY_PREFIX=$DEPS_PREFIX:" depends.mk
sed -i "s:^ZOOKEEPER_PREFIX=.*:ZOOKEEPER_PREFIX=$DEPS_PREFIX:" depends.mk
sed -i "s:^GFLAGS_PREFIX=.*:GFLAGS_PREFIX=$DEPS_PREFIX:" depends.mk
sed -i "s:^GLOG_PREFIX=.*:GLOG_PREFIX=$DEPS_PREFIX:" depends.mk
sed -i "s:^GTEST_PREFIX=.*:GTEST_PREFIX=$DEPS_PREFIX:" depends.mk
sed -i "s:^GPERFTOOLS_PREFIX=.*:GPERFTOOLS_PREFIX=$DEPS_PREFIX:" depends.mk
sed -i "s:^BOOST_INCDIR=.*:BOOST_INCDIR=$DEPS_PREFIX/boost_${BOOST_VERSION}:" depends.mk
sed -i "s:^INS_PREFIX=.*:INS_PREFIX=$DEPS_PREFIX:" depends.mk

########################################
# build tera
########################################

make clean
make -j4
