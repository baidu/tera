include ../../depends.mk

OPT ?= -g2 -Wall -Werror        # (B) Debug mode, w/ full line-level debugging symbols

CC = cc
CXX = g++

SHARED_CFLAGS = -fPIC
SHARED_LDFLAGS = -shared -Wl,-soname -Wl,

INCPATH += -I../../include $(DEPS_INCPATH)
CFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH)
CXXFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH)
LDFLAGS += ../../build/lib/libtera.a $(DEPS_LDPATH) $(DEPS_LDFLAGS) -lpthread -lz

SAMPLE_SRC := ./tera_sample.cc tera_row_txn_sample.cc
SAMPLE_OBJ := $(SAMPLE_SRC:.cc=.o)

.PHONY: clean

all: sample_demo tera_row_txn_sample

sample_demo: ./tera_sample.o
	$(CXX) -o $@ $^ $(LDFLAGS)
	rm -f ./tera_sample.o

tera_row_txn_sample: tera_row_txn_sample.o
	$(CXX) -o $@ $^ $(LDFLAGS)
	rm -f ./tera_row_txn_sample.o

$(SAMPLE_OBJ): %.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f ./tera_sample.o
	rm -f ./sample_demo

