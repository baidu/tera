include depends.mk

# OPT ?= -O2 -DNDEBUG       # (A) Production use (optimized mode)
OPT ?= -g2 -Wall -Werror        # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG   # (C) Profiling mode: opt, but w/debugging symbols

ifndef CXX
    CXX = g++
endif
ifndef CC
    CC = gcc
endif
TEST_OPT = -g2 -Wall -Dprivate=public

INCPATH += -I./src -I./include -I./src/leveldb/include -I./src/leveldb -I./src/sdk \
           -I./src/sdk/java/native-src $(DEPS_INCPATH) 
LEVELDB_INCPATH = "-I../ -I../../thirdparty/include/"
CFLAGS += $(OPT) $(INCPATH) -fPIC -fvisibility=hidden # hide internal symbol of tera
TEST_CFLAGS += $(TEST_OPT) $(INCPATH) -fPIC -fvisibility=hidden # hide internal symbol of tera
CXXFLAGS += -std=gnu++11 $(CFLAGS)
TEST_CXXFLAGS += -std=gnu++11 $(TEST_CFLAGS)
LDFLAGS += -rdynamic $(DEPS_LDPATH) $(DEPS_LDFLAGS) -lpthread -lrt -lz -ldl \
           -lreadline -lncurses -fPIC
SO_LDFLAGS += -rdynamic $(DEPS_LDPATH) $(SO_DEPS_LDFLAGS) -lpthread -lrt -lz -ldl \
              -shared -fPIC -Wl,--version-script,so-version-script # hide symbol of thirdparty libs

PROTO_FILES := $(wildcard src/proto/*.proto)
PROTO_OUT_CC := $(PROTO_FILES:.proto=.pb.cc)
PROTO_OUT_H := $(PROTO_FILES:.proto=.pb.h)

ACCESS_SRC := $(wildcard src/access/*.cc) $(wildcard src/access/authorization/*.cc) \
              $(wildcard src/access/helpers/*.cc) $(wildcard src/access/giano/*.cc) \
              $(wildcard src/access/identification/*.cc) $(wildcard src/access/verification/*.cc)
QUOTA_SRC := $(wildcard src/quota/*.cc) $(wildcard src/quota/helpers/*.cc) \
             $(wildcard src/quota/limiter/*.cc)
MASTER_SRC := $(wildcard src/master/*.cc)
TABLETNODE_SRC := $(wildcard src/tabletnode/*.cc)
IO_SRC := $(wildcard src/io/*.cc)
SDK_SRC := $(wildcard src/sdk/*.cc) $(wildcard src/sdk/test/global_txn_testutils.cc) \
		   src/observer/rowlocknode/rowlocknode_zk_adapter.cc src/observer/rowlocknode/ins_rowlocknode_zk_adapter.cc
HTTP_SRC := $(wildcard src/sdk/http/*.cc)
PROTO_SRC := $(filter-out %.pb.cc, $(wildcard src/proto/*.cc)) $(PROTO_OUT_CC)
JNI_TERA_SRC := $(wildcard src/sdk/java/native-src/*.cc)
VERSION_SRC := src/version.cc
OTHER_SRC := $(wildcard src/zk/*.cc) $(wildcard src/utils/*.cc) $(VERSION_SRC) \
             src/tera_flags.cc src/sdk/test/global_txn_testutils.cc
COMMON_SRC := $(wildcard src/common/base/*.cc) $(wildcard src/common/net/*.cc) \
              $(wildcard src/common/file/*.cc) $(wildcard src/common/file/recordio/*.cc) \
              $(wildcard src/common/console/*.cc) $(wildcard src/common/log/*.cc) \
			  $(wildcard src/common/metric/*.cc) $(wildcard src/common/*.cc)
SERVER_WRAPPER_SRC := src/tera_main_wrapper.cc
SERVER_SRC := src/tera_main.cc src/common/tera_entry.cc
CLIENT_SRC := src/teracli_main.cc src/io/io_flags.cc
TERAUTIL_SRC := src/terautil.cc src/io/io_flags.cc
GTXN_TEST_SRC := src/sdk/test/global_txn_test_tool.cc
TEST_CLIENT_SRC := src/tera_test_main.cc
TERA_C_SRC := src/tera_c.cc
#MONITOR_SRC := src/monitor/teramo_main.cc
MARK_SRC := src/benchmark/mark.cc src/benchmark/mark_main.cc
COMMON_TEST_SRC := $(wildcard src/common/test/*.cc)
TEST_SRC := src/utils/test/prop_tree_test.cc src/utils/test/tprinter_test.cc \
            src/io/test/tablet_io_test.cc src/io/test/tablet_scanner_test.cc \
            src/io/test/load_test.cc src/master/test/master_test.cc \
            src/master/test/trackable_gc_test.cc \
            src/observer/test/rowlock_test.cc src/observer/test/scanner_test.cc \
			src/observer/test/observer_test.cc \
			$(wildcard src/sdk/test/*_test.cc) $(COMMON_TEST_SRC)

TIMEORACLE_SRC := $(wildcard src/timeoracle/*.cc) src/common/tera_entry.cc
TIMEORACLE_BENCH_SRC := src/timeoracle/bench/timeoracle_bench.cc
ROWLOCK_SRC := $(wildcard src/observer/rowlocknode/*.cc) src/sdk/rowlock_client.cc
ROWLOCK_PROXY_SRC := $(wildcard src/observer/rowlockproxy/*.cc) 
OBSERVER_SRC := src/observer/executor/scanner_impl.cc src/observer/executor/random_key_selector.cc src/observer/executor/notification_impl.cc
OBSERVER_DEMO_SRC := $(wildcard src/observer/observer_demo.cc)

TEST_OUTPUT := test_output
UNITTEST_OUTPUT := $(TEST_OUTPUT)/unittest

ACCESS_OBJ := $(ACCESS_SRC:.cc=.o)
QUOTA_OBJ := $(QUOTA_SRC:.cc=.o)
MASTER_OBJ := $(MASTER_SRC:.cc=.o)
TABLETNODE_OBJ := $(TABLETNODE_SRC:.cc=.o)
IO_OBJ := $(IO_SRC:.cc=.o)
SDK_OBJ := $(SDK_SRC:.cc=.o)
PROTO_OBJ := $(PROTO_SRC:.cc=.o)
JNI_TERA_OBJ := $(JNI_TERA_SRC:.cc=.o)
OTHER_OBJ := $(OTHER_SRC:.cc=.o)
COMMON_OBJ := $(COMMON_SRC:.cc=.o)
SERVER_WRAPPER_OBJ := $(SERVER_WRAPPER_SRC:.cc=.o)
SERVER_OBJ := $(SERVER_SRC:.cc=.o)
CLIENT_OBJ := $(CLIENT_SRC:.cc=.o)
TERAUTIL_OBJ := $(TERAUTIL_SRC:.cc=.o)
GTXN_TEST_OBJ := $(GTXN_TEST_SRC:.cc=.o)
TEST_CLIENT_OBJ := $(TEST_CLIENT_SRC:.cc=.o)
TERA_C_OBJ := $(TERA_C_SRC:.cc=.o)
MONITOR_OBJ := $(MONITOR_SRC:.cc=.o)
MARK_OBJ := $(MARK_SRC:.cc=.o)
HTTP_OBJ := $(HTTP_SRC:.cc=.o)
COMMON_TEST_OBJ := $(COMMON_TEST_SRC:.cc=.o)
TEST_OBJ := $(TEST_SRC:.cc=.o)
TIMEORACLE_OBJ := $(TIMEORACLE_SRC:.cc=.o)
TIMEORACLE_BENCH_OBJ := $(TIMEORACLE_BENCH_SRC:.cc=.o)
ROWLOCK_OBJ := $(ROWLOCK_SRC:.cc=.o)
ROWLOCK_PROXY_OBJ := $(ROWLOCK_PROXY_SRC:.cc=.o)
OBSERVER_OBJ := $(OBSERVER_SRC:.cc=.o)
OBSERVER_DEMO_OBJ := $(OBSERVER_DEMO_SRC:.cc=.o)
ALL_OBJ := $(ACCESS_OBJ) $(QUOTA_OBJ) $(MASTER_OBJ) $(TABLETNODE_OBJ) $(IO_OBJ) $(SDK_OBJ) $(PROTO_OBJ) \
           $(JNI_TERA_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(SERVER_OBJ) $(CLIENT_OBJ) $(TERAUTIL_OBJ) \
           $(TEST_CLIENT_OBJ) $(TERA_C_OBJ) $(MONITOR_OBJ) $(MARK_OBJ) \
           $(SERVER_WRAPPER_OBJ) $(TIMEORACLE_OBJ) $(ROWLOCK_OBJ) $(ROWLOCK_PROXY_OBJ)  $(OBSERVER_OBJ) $(OBSERVER_DEMO_OBJ)
LEVELDB_LIB := src/leveldb/libleveldb.a
LEVELDB_UTIL := src/leveldb/util/histogram.o src/leveldb/port/port_posix.o

PROGRAM = tera_main tera_master tabletserver teracli terautil tera_test timeoracle timeoracle_bench rowlock observer_demo rowlock_proxy
TEST_PROGRAM=gtxn_test_tool

LIBRARY = libtera.a
SOLIBRARY = libtera.so
TERA_C_SO = libtera_c.so
JNILIBRARY = libjni_tera.so
OBSERVER_LIBRARY = libobserver.a
BENCHMARK = tera_bench tera_mark
TESTS = prop_tree_test tprinter_test string_util_test tablet_io_test \
        tablet_scanner_test fragment_test progress_bar_test master_test load_test \
        common_test sdk_test 

.PHONY: all clean cleanall test

all: $(PROGRAM) $(TEST_PROGRAM) $(LIBRARY) $(SOLIBRARY) $(TERA_C_SO) $(JNILIBRARY) $(BENCHMARK) $(OBSERVER_LIBRARY)
	mkdir -p build/include build/lib build/bin build/log build/benchmark
	cp $(PROGRAM) build/bin
	cp $(LIBRARY) $(SOLIBRARY) $(TERA_C_SO) $(JNILIBRARY) $(OBSERVER_LIBRARY) build/lib
	cp src/leveldb/tera_bench .
	cp -r benchmark/*.sh benchmark/ycsb4tera/ $(BENCHMARK) build/benchmark
	cp -r include build/
	cp -r conf build
	mkdir -p test/tools
	cp $(TEST_PROGRAM) test/tools
	echo 'Done'

test: $(TESTS)
	mkdir -p $(UNITTEST_OUTPUT)
	mv $(TESTS) $(UNITTEST_OUTPUT)
	$(MAKE) test -C src/leveldb
	cp src/leveldb/*_test $(UNITTEST_OUTPUT)

check: test
	( cd $(UNITTEST_OUTPUT); \
	for t in $(TESTS); do echo "***** Running $$t"; ./$$t || exit 1; done )
	$(MAKE) check -C src/leveldb
	sh ./src/sdk/python/checker.sh

clean:
	rm -rf $(ALL_OBJ) $(TEST_OBJ) $(PROTO_OUT_CC) $(PROTO_OUT_H) $(TEST_OUTPUT)
	$(MAKE) clean -C src/leveldb
	rm -rf $(PROGRAM) $(TEST_PROGRAM) $(LIBRARY) $(OBSERVER_LIBRARY) $(SOLIBRARY) $(TERA_C_SO) $(JNILIBRARY) $(BENCHMARK) $(TESTS) terahttp 

cleanall:
	$(MAKE) clean
	rm -rf build
	rm -rf test/tools

tera_main: src/tera_main_wrapper.o src/version.o src/tera_flags.o
	$(CXX) -o $@ $^ $(LDFLAGS)

tera_master: $(SERVER_OBJ) $(MASTER_OBJ) $(IO_OBJ) $(SDK_OBJ) \
             $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_LIB) $(ACCESS_OBJ) $(QUOTA_OBJ)
	$(CXX) -o $@ $^ $(LDFLAGS)

MASTER_ENTRY_OBJ=src/master/master_entry.o
tabletserver: $(SERVER_OBJ) $(TABLETNODE_OBJ) $(IO_OBJ) $(SDK_OBJ) $(filter-out $(MASTER_ENTRY_OBJ),$(MASTER_OBJ)) \
              $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_LIB) $(ACCESS_OBJ) $(QUOTA_OBJ)
	$(CXX) -o $@ $^ $(LDFLAGS)

libtera.a: $(SDK_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_UTIL)
	$(AR) -rs $@ $^

observer_demo : $(OBSERVER_DEMO_OBJ) $(OBSERVER_LIBRARY) $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

libobserver.a: $(OBSERVER_OBJ) $(SDK_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_UTIL) \
	           $(IO_OBJ)
	$(AR) -rs $@ $^
	
libtera.so: $(SDK_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_UTIL) $(IO_OBJ) 
	$(CXX) -o $@ $^ $(SO_LDFLAGS)

libtera_c.so: $(TERA_C_OBJ) $(LIBRARY)
	$(CXX) -o $@ $^ $(SO_LDFLAGS)

teracli: $(CLIENT_OBJ) $(LIBRARY) $(LEVELDB_LIB) $(ACCESS_OBJ)
	$(CXX) -o $@ $^ $(LDFLAGS)

terautil: $(TERAUTIL_OBJ) $(LEVELDB_LIB) $(LIBRARY) $(ACCESS_OBJ)
	$(CXX) -o $@ $^ $(LDFLAGS)

gtxn_test_tool: $(GTXN_TEST_OBJ) $(LIBRARY) $(ACCESS_OBJ)
	$(CXX) -o $@ $^ $(LDFLAGS)

#teramo: $(MONITOR_OBJ) $(LIBRARY)
#	$(CXX) -o $@ $^ $(LDFLAGS)

tera_mark: $(MARK_OBJ) $(LIBRARY) $(LEVELDB_LIB) $(ACCESS_OBJ)
	$(CXX) -o $@ $^ $(LDFLAGS)

tera_test: $(TEST_CLIENT_OBJ) $(LIBRARY) $(ACCESS_OBJ)
	$(CXX) -o $@ $(TEST_CLIENT_OBJ) $(ACCESS_OBJ) $(LIBRARY) $(LDFLAGS)

timeoracle: $(TIMEORACLE_OBJ) $(PROTO_OBJ) $(COMMON_OBJ) $(OTHER_OBJ) $(SDK_OBJ) $(LEVELDB_LIB) $(ACCESS_OBJ)
	$(CXX) -o $@ $^ $(LDFLAGS)

timeoracle_bench : $(TIMEORACLE_BENCH_OBJ) $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

rowlock : $(SERVER_OBJ) $(ROWLOCK_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(SDK_OBJ) $(LEVELDB_LIB) $(ACCESS_OBJ)
	$(CXX) -o $@ $^ $(LDFLAGS)

rowlock_proxy : $(SERVER_OBJ) $(ROWLOCK_PROXY_OBJ) $(PROTO_OBJ) $(COMMON_OBJ) $(OBSERVER_LIBRARY) $(LEVELDB_LIB)
	$(CXX) -o $@ $^ $(LDFLAGS)

terahttp: $(HTTP_OBJ) $(PROTO_OBJ) $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

libjni_tera.so: $(JNI_TERA_OBJ) $(LIBRARY) 
	$(CXX) -o $@ $^ $(SO_LDFLAGS)

src/leveldb/libleveldb.a: FORCE
	CXXFLAGS=$(LEVELDB_INCPATH) LDFLAGS="$(LDFLAGS)" CC=$(CC) CXX=$(CXX) $(MAKE) -C src/leveldb

tera_bench:

# unit test
common_test: $(COMMON_TEST_OBJ) $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

prop_tree_test: src/utils/test/prop_tree_test.o $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

tprinter_test: src/utils/test/tprinter_test.o $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

string_util_test: src/utils/test/string_util_test.o $(LIBRARY)
	$(CXX) -o $@ $^ $(LDFLAGS)

tablet_io_test: src/sdk/tera.o src/io/test/tablet_io_test.o src/tabletnode/tabletnode_sysinfo.o src/common/tera_entry.cc\
                $(IO_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_LIB) $(TABLETNODE_OBJ) $(SDK_OBJ)
	$(CXX) $(TEST_CXXFLAGS) -o $@ $^ $(LDFLAGS)

load_test: src/sdk/tera.o src/io/test/load_test.o src/tabletnode/tabletnode_sysinfo.o  src/common/tera_entry.cc\
			$(IO_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_LIB) $(TABLETNODE_OBJ) $(SDK_OBJ)
	$(CXX) $(TEST_CXXFLAGS) -o $@ $^ $(LDFLAGS)

fragment_test: src/utils/test/fragment_test.o src/utils/fragment.o
	$(CXX) -o $@ $^ $(LDFLAGS)

progress_bar_test: src/common/console/progress_bar_test.o src/common/console/progress_bar.o
	$(CXX) -o $@ $^ $(LDFLAGS)

tablet_scanner_test: src/sdk/tera.o src/io/test/tablet_scanner_test.o src/tabletnode/tabletnode_sysinfo.o src/common/tera_entry.cc\
					 $(IO_OBJ) $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_LIB) $(TABLETNODE_OBJ) $(SDK_OBJ)
	$(CXX) $(TEST_CXXFLAGS) -o $@ $^ $(LDFLAGS)

master_test: src/master/test/master_test.o src/master/test/trackable_gc_test.o src/common/tera_entry.cc $(MASTER_OBJ) $(IO_OBJ) $(SDK_OBJ) \
             $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_LIB)
	$(CXX) -o $@ $^ $(LDFLAGS) $(TEST_CXXFLAGS)

sdk_test: src/sdk/test/global_txn_internal_test.o src/sdk/test/global_txn_test.o \
          src/sdk/test/filter_utils_test.o src/sdk/test/scan_impl_test.o \
          src/sdk/test/sdk_timeout_manager_test.o src/sdk/test/sdk_test.o $(SDK_OBJ) \
          $(PROTO_OBJ) $(OTHER_OBJ) $(COMMON_OBJ) $(LEVELDB_LIB) 
	$(CXX) -o $@ $^ $(LDFLAGS)

#observer_test: src/observer/test/rowlock_test.o src/observer/test/scanner_test.o  src/observer/test/observer_test.o src/observer/observer_demo/demo_observer.o $(PROTO_OBJ) $(COMMON_OBJ) $(OTHER_OBJ) $(OBSERVER_OBJ) $(LIBRARY)
#	$(CXX) -o $@ $^ $(LDFLAGS)

$(ALL_OBJ): %.o: %.cc $(PROTO_OUT_H)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(TEST_OBJ): %.o: %.cc $(PROTO_OUT_H)
	$(CXX) $(TEST_CXXFLAGS) -c $< -o $@

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

# install output into system directories
.PHONY: install
install: $(PROGRAM) $(LIBRARY) $(SOLIBRARY) $(TERA_C_SO) $(JNILIBRARY) 
	mkdir -p $(INSTALL_PREFIX)/bin $(INSTALL_PREFIX)/include $(INSTALL_PREFIX)/lib
	cp -rf $(PROGRAM) $(INSTALL_PREFIX)/bin
	cp -rf include/* $(INSTALL_PREFIX)/include
	cp -rf $(LIBRARY) $(SOLIBRARY) $(TERA_C_SO) $(JNILIBRARY) $(INSTALL_PREFIX)/lib 
