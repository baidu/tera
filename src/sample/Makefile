include ../../depends.mk

OPT ?= -g2 -Wall -Werror        # (B) Debug mode, w/ full line-level debugging symbols

CC = cc
CXX = g++

SHARED_CFLAGS = -fPIC
SHARED_LDFLAGS = -shared -Wl,-soname -Wl,

INCPATH += -I../../include $(DEPS_INCPATH)
CFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH)
CXXFLAGS += -std=gnu++11 $(OPT) $(SHARED_CFLAGS) $(INCPATH)
LDFLAGS += ../../build/lib/libtera.a $(DEPS_LDPATH) $(DEPS_LDFLAGS) -lpthread -lz

SAMPLE_SRC := ./tera_sample.cc tera_row_txn_sample.cc atomic_sample.cc global_txn_async_sample.cc
SAMPLE_OBJ := $(SAMPLE_SRC:.cc=.o)

.PHONY: clean

all: sample_demo tera_row_txn_sample atomic_sample global_txn_async_sample global_txn_sync_sample

sample_demo: ./tera_sample.o
	$(CXX) -o $@ $^ $(LDFLAGS)

tera_row_txn_sample: tera_row_txn_sample.o
	$(CXX) -o $@ $^ $(LDFLAGS)

global_txn_async_sample: global_txn_async_sample.o
	$(CXX) -o $@ $^ $(LDFLAGS)

global_txn_sync_sample: global_txn_sync_sample.o
	$(CXX) -o $@ $^ $(LDFLAGS)

atomic_sample: atomic_sample.o
	$(CXX) -o $@ $^ $(LDFLAGS)

$(SAMPLE_OBJ): %.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f *.o
	rm -f ./sample_demo
	rm -f ./tera_row_txn_sample
	rm -f ./global_txn_async_sample
	rm -f ./global_txn_sync_sample
	rm -f ./atomic_sample

