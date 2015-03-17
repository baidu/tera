# OPT ?= -O2 -DNDEBUG       # (A) Production use (optimized mode)
OPT ?= -g2 -Wall -Wno-sign-compare    # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG # (C) Profiling mode: opt, but w/debugging symbols

CC=cc
CXX=g++

SHARED_CFLAGS=-fPIC
SHARED_LDFLAGS=-shared -Wl,-soname -Wl,

INCPATH += -I./src -I./include -I./src/leveldb/include -I./src/leveldb \
           -I/usr/local/include -I./thirdparty/sofa-pbrpc/output/include
LDPATH += -L/usr/local/lib -L/usr/local/ssl/lib -L./thirdparty/sofa-pbrpc/output/lib \
	  -L./src/leveldb
CFLAGS += $(OPT) $(INCPATH)
CXXFLAGS += $(OPT) $(INCPATH)
LDFLAGS += $(LDPATH) -lleveldb -lsofa-pbrpc -lprotobuf -lsnappy -ltcmalloc \
	   -lzookeeper_mt -lgflags -lglog -lpthread -lrt -lz -ldl -lcrypto -lssl


MASTER_SRC = $(wildcard src/master/*.cc)
TABLETNODE_SRC = $(wildcard src/tabletnode/*.cc)
IO_SRC = $(wildcard src/io/*.cc)
SDK_SRC = $(wildcard src/sdk/*.cc)
PROTO_SRC = $(wildcard src/proto/*.cc)
OTHER_SRC = $(wildcard src/zk/*.cc) $(wildcard src/utils/*.cc) src/tera_flags.cc \
            src/version.cc
COMMON_SRC = $(wildcard src/common/base/*.cc) $(wildcard src/common/net/*.cc) \
             $(wildcard src/common/file/*.cc) $(wildcard src/common/file/recordio/*.cc)
SERVER_SRC = src/tera_main.cc src/tera_entry.cc
CLIENT_SRC = src/teracli_main.cc

MASTER_OBJ = $(MASTER_SRC:.cc=.o)
TABLETNODE_OBJ = $(TABLETNODE_SRC:.cc=.o)
IO_OBJ = $(IO_SRC:.cc=.o)
SDK_OBJ = $(SDK_SRC:.cc=.o)
PROTO_OBJ = $(PROTO_SRC:.cc=.o)
OTHER_OBJ = $(OTHER_SRC:.cc=.o)
COMMON_OBJ = $(COMMON_SRC:.cc=.o)
SERVER_OBJ = $(SERVER_SRC:.cc=.o)
CLIENT_OBJ = $(CLIENT_SRC:.cc=.o)
LEVELDB_LIB = src/leveldb/libleveldb.a

PROTO_FILES = $(wildcard src/proto/*.proto)
PROTOC=protoc

PROGRAM = tera_main teracli
LIBRARY = libtera.a

default: all

all: $(PROGRAM) $(LIBRARY)
	mkdir -p build/include build/lib build/bin
	cp $(PROGRAM) build/bin
	cp $(LIBRARY) build/lib
	cp src/sdk/tera.h build/include
	echo 'Done'

test:
	echo "No test now!"
	
clean:
	rm -rf $(MASTER_OBJ) $(TABLETNODE_OBJ) $(IO_OBJ) $(SDK_OBJ) $(PROTO_OBJ) \
	$(OTHER_OBJ) $(COMMON_OBJ) $(SERVER_OBJ) $(CLIENT_OBJ)
	$(MAKE) clean -C src/leveldb
	rm -rf $(PROGRAM) $(LIBRARY)

cleanall:
	$(MAKE) clean
	rm -rf build

tera_main: $(SERVER_OBJ) $(LEVELDB_LIB) $(MASTER_OBJ) $(TABLETNODE_OBJ) $(IO_OBJ) \
           $(SDK_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ)
	$(CXX) -o $@ $(SERVER_OBJ) $(MASTER_OBJ) $(TABLETNODE_OBJ) $(IO_OBJ) $(SDK_OBJ) \
	$(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LDFLAGS)

libtera.a: $(SDK_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ)
	$(AR) -rs $@ $(SDK_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ)

teracli: $(CLIENT_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(CLIENT_OBJ) $(LIBRARY) $(LDFLAGS)
 
proto: $(PROTO_FILES)
	$(PROTOC) --proto_path=./src/proto/ --proto_path=/usr/local/include \
		  --proto_path=./thirdparty/sofa-pbrpc/output/include/ \
		  --cpp_out=./src/proto/ $(PROTO_FILES)

src/leveldb/libleveldb.a:
	$(MAKE) -C src/leveldb

.cc.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

