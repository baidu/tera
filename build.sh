#!/bin/bash

#language: cpp
#compiler: gcc
#env:
    PROTOBUF_VERSION=2.6.0
    mkdir -p thirdparty

#install:
    cd thirdparty

    sudo apt-get install libboost-dev

    wget https://github.com/google/protobuf/releases/download/v2.6.0/protobuf-2.6.0.tar.gz
    tar xf protobuf-2.6.0.tar.gz
    ( cd protobuf-$PROTOBUF_VERSION && ./configure && make -j4 && sudo make install && sudo ldconfig )

    git clone https://github.com/google/snappy
    ( cd snappy && sh ./autogen.sh && ./configure && make -j4 && sudo make install )

    sudo apt-get install zlib1g-dev

    wget http://www.openssl.org/source/openssl-1.0.2.tar.gz
    tar zxvf openssl-1.0.2.tar.gz
    ( cd openssl-1.0.2 && ./config && make -j4 && sudo make install )

    git clone https://github.com/koalademo/sofa-pbrpc
    ( cd sofa-pbrpc && make proto && make -j4 && make install )

    wget http://www.us.apache.org/dist/zookeeper/stable/zookeeper-3.4.6.tar.gz
    tar zxvf zookeeper-3.4.6.tar.gz
    ( cd zookeeper-3.4.6/src/c && ./configure --disable-shared && make -j4 && sudo make install )

    git clone https://github.com/schuhschuh/gflags
    ( cd gflags && cmake && make -j4 && sudo make install )

    git clone https://github.com/google/glog
    ( cd glog && git checkout v0.3.3 && ./configure && make -j4 && sudo make install )

    get http://download.savannah.gnu.org/releases/libunwind/libunwind-0.99-beta.tar.gz
    tar zxvf libunwind-0.99-beta.tar.gz
    ( cd libunwind-0.99-beta && ./configure CFLAGS=-U_FORTIFY_SOURCE && make -j4 && sudo make install )

    wget https://googledrive.com/host/0B6NtGsLhIcf7MWxMMF9JdTN3UVk/gperftools-2.4.tar.gz
    tar zxvf gperftools-2.4.tar.gz
    ( cd gperftools-2.4 && ./configure && make -j4 && sudo make install )

    cd -

#script:
    make proto
    make -j4
