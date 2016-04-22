include depends.mk

# OPT ?= -O2 -DNDEBUG       # (A) Production use (optimized mode)
OPT ?= -g2 -Wall -Werror        # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG   # (C) Profiling mode: opt, but w/debugging symbols

CC = cc
CXX = g++

INCPATH += -I./src -I./include -I./src/leveldb/include -I./src/leveldb \
           -I./src/sdk/java/native-src $(DEPS_INCPATH) 
CFLAGS += $(OPT) $(INCPATH) -fPIC -fvisibility=hidden # hide internal symbol of tera
CXXFLAGS += $(CFLAGS)
LDFLAGS += -rdynamic $(DEPS_LDPATH) $(DEPS_LDFLAGS) -lpthread -lrt -lz -ldl \
           -lreadline -lncurses
SO_LDFLAGS += -rdynamic $(DEPS_LDPATH) $(SO_DEPS_LDFLAGS) -lpthread -lrt -lz -ldl \
              -shared -Wl,--version-script,so-version-script # hide symbol of thirdparty libs

PROTO_FILES := $(wildcard src/proto/*.proto)
PROTO_OUT_CC := $(PROTO_FILES:.proto=.pb.cc)
PROTO_OUT_H := $(PROTO_FILES:.proto=.pb.h)

MASTER_SRC := $(wildcard src/master/*.cc)
TABLETNODE_SRC := $(wildcard src/tabletnode/*.cc)
IO_SRC := $(wildcard src/io/*.cc)
SDK_SRC := $(wildcard src/sdk/*.cc)
HTTP_SRC := $(wildcard src/sdk/http/*.cc)
PROTO_SRC := $(filter-out %.pb.cc, $(wildcard src/proto/*.cc)) $(PROTO_OUT_CC)
JNI_TERA_SRC := $(wildcard src/sdk/java/native-src/*.cc)
VERSION_SRC := src/version.cc
OTHER_SRC := $(wildcard src/zk/*.cc) $(wildcard src/utils/*.cc) $(VERSION_SRC) \
             src/tera_flags.cc
COMMON_SRC := $(wildcard src/common/base/*.cc) $(wildcard src/common/net/*.cc) \
              $(wildcard src/common/file/*.cc) $(wildcard src/common/file/recordio/*.cc) \
              $(wildcard src/common/console/*.cc) 
SERVER_SRC := src/tera_main.cc src/tera_entry.cc
CLIENT_SRC := src/teracli_main.cc
TEST_CLIENT_SRC := src/tera_test_main.cc
TERA_C_SRC := src/tera_c.cc
MONITOR_SRC := src/monitor/teramo_main.cc
MARK_SRC := src/benchmark/mark.cc src/benchmark/mark_main.cc
TEST_SRC := src/utils/test/prop_tree_test.cc src/utils/test/tprinter_test.cc \
            src/io/test/tablet_io_test.cc

TEST_OUTPUT := test_output
UNITTEST_OUTPUT := $(TEST_OUTPUT)/unittest

MASTER_OBJ := $(MASTER_SRC:.cc=.o)
TABLETNODE_OBJ := $(TABLETNODE_SRC:.cc=.o)
IO_OBJ := $(IO_SRC:.cc=.o)
SDK_OBJ := $(SDK_SRC:.cc=.o)
PROTO_OBJ := $(PROTO_SRC:.cc=.o)
JNI_TERA_OBJ := $(JNI_TERA_SRC:.cc=.o)
OTHER_OBJ := $(OTHER_SRC:.cc=.o)
COMMON_OBJ := $(COMMON_SRC:.cc=.o)
SERVER_OBJ := $(SERVER_SRC:.cc=.o)
CLIENT_OBJ := $(CLIENT_SRC:.cc=.o)
TEST_CLIENT_OBJ := $(TEST_CLIENT_SRC:.cc=.o)
TERA_C_OBJ := $(TERA_C_SRC:.cc=.o)
MONITOR_OBJ := $(MONITOR_SRC:.cc=.o)
MARK_OBJ := $(MARK_SRC:.cc=.o)
HTTP_OBJ := $(HTTP_SRC:.cc=.o)
TEST_OBJ := $(TEST_SRC:.cc=.o)
ALL_OBJ := $(MASTER_OBJ) $(TABLETNODE_OBJ) $(IO_OBJ) $(SDK_OBJ) $(PROTO_OBJ) \
           $(JNI_TERA_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(SERVER_OBJ) $(CLIENT_OBJ) \
           $(TEST_CLIENT_OBJ) $(TERA_C_OBJ) $(MONITOR_OBJ) $(MARK_OBJ) $(TEST_OBJ)
LEVELDB_LIB := src/leveldb/libleveldb.a

PROGRAM = tera_main teracli teramo tera_test
LIBRARY = libtera.a
SOLIBRARY = libtera.so
TERA_C_SO = libtera_c.so
JNILIBRARY = libjni_tera.so
BENCHMARK = tera_bench tera_mark
TESTS = prop_tree_test tprinter_test string_util_test tablet_io_test \
		fragment_test progress_bar_test

.PHONY: all clean cleanall test

all: $(PROGRAM) $(LIBRARY) $(SOLIBRARY) $(TERA_C_SO) $(JNILIBRARY) $(BENCHMARK) $(TESTS)
	mkdir -p build/include build/lib build/bin build/log build/benchmark
	mkdir -p $(UNITTEST_OUTPUT)
	mv $(TESTS) $(UNITTEST_OUTPUT)
	cp $(PROGRAM) build/bin
	cp $(LIBRARY) $(SOLIBRARY) $(TERA_C_SO) $(JNILIBRARY) build/lib
	cp src/leveldb/tera_bench .
	cp -r benchmark/*.sh $(BENCHMARK) build/benchmark
	cp src/sdk/tera.h build/include
	cp -r conf build
	echo 'Done'

check: $(TESTS)
	( cd $(UNITTEST_OUTPUT); \
	for t in $(TESTS); do echo "***** Running $$t"; ./$$t || exit 1; done )
	$(MAKE) check -C src/leveldb
	sh ./src/sdk/python/checker.sh

clean:
	rm -rf $(ALL_OBJ) $(PROTO_OUT_CC) $(PROTO_OUT_H) $(TEST_OUTPUT)
	$(MAKE) clean -C src/leveldb
	rm -rf $(PROGRAM) $(LIBRARY) $(SOLIBRARY) $(TERA_C_SO) $(JNILIBRARY) $(BENCHMARK) $(TESTS) terahttp

cleanall:
	$(MAKE) clean
	rm -rf build

tera_main: $(SERVER_OBJ) $(MASTER_OBJ) $(TABLETNODE_OBJ) $(IO_OBJ) $(SDK_OBJ) \
           $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_LIB)
	$(CXX) -o $@ $^ $(LDFLAGS)

libtera.a: $(SDK_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ)
	$(AR) -rs $@ $^

libtera.so: $(SDK_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ)
	$(CXX) -o $@ $^ $(SO_LDFLAGS)

libtera_c.so: $(TERA_C_OBJ) $(LIBRARY)
	$(CXX) -o $@ $^ $(SO_LDFLAGS)

teracli: $(CLIENT_OBJ) $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

teramo: $(MONITOR_OBJ) $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

tera_mark: $(MARK_OBJ) $(LIBRARY) $(LEVELDB_LIB)
	$(CXX) -o $@ $^ $(LDFLAGS)

tera_test: $(TEST_CLIENT_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(TEST_CLIENT_OBJ) $(LIBRARY) $(LDFLAGS)

terahttp: $(HTTP_OBJ) $(PROTO_OBJ) $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

libjni_tera.so: $(JNI_TERA_OBJ) $(LIBRARY) 
	$(CXX) -o $@ $^ $(SO_LDFLAGS)

src/leveldb/libleveldb.a: FORCE
	$(MAKE) -C src/leveldb

tera_bench:

# unit test
prop_tree_test: src/utils/test/prop_tree_test.o $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

tprinter_test: src/utils/test/tprinter_test.o $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

string_util_test: src/utils/test/string_util_test.o $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

tablet_io_test: src/io/test/tablet_io_test.o src/tabletnode/tabletnode_sysinfo.o \
                $(IO_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_LIB)
	$(CXX) -o $@ $^ $(LDFLAGS)

fragment_test: src/utils/test/fragment_test.o src/utils/fragment.o
	$(CXX) -o $@ $^ $(LDFLAGS)

progress_bar_test: src/common/console/progress_bar_test.o src/common/console/progress_bar.o
	$(CXX) -o $@ $^ $(LDFLAGS)

$(ALL_OBJ): %.o: %.cc $(PROTO_OUT_H)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(VERSION_SRC): FORCE
	sh build_version.sh

.PHONY: FORCE
FORCE:

.PHONY: proto
proto: $(PROTO_OUT_CC) $(PROTO_OUT_H)

%.pb.cc %.pb.h: %.proto
	$(PROTOC) --proto_path=./src/proto/ --proto_path=$(PROTOBUF_INCDIR) \
              --proto_path=$(SOFA_PBRPC_INCDIR) \
              --cpp_out=./src/proto/ $<
