include ../../depends.mk

OPT ?= -g -Wall -Werror        # (B) Debug mode, w/ full line-level debugging symbols

CC = cc
CXX = g++

SHARED_CFLAGS = -fPIC
SHARED_LDFLAGS = -shared -Wl,-soname -Wl,

INCPATH += -I../ $(DEPS_INCPATH)
CFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH)
CXXFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH)
LDFLAGS += ../../build/lib/libtera.a $(DEPS_LDPATH) $(DEPS_LDFLAGS) -lpthread -lz -lrt

SAMPLE_SRC := ./tera_sample.cc ./tera_ha_sample.cc
SAMPLE_OBJ := $(SAMPLE_SRC:.cc=.o)

.PHONY: all clean

all: sample_demo ha_sample_demo
	rm ./tera_sample.o
	rm ./tera_ha_sample.o

sample_demo: ./tera_sample.o
	$(CXX) -o $@ $^ $(LDFLAGS)

ha_sample_demo: ./tera_ha_sample.o
	$(CXX) -o $@ $^ $(LDFLAGS)

$(SAMPLE_OBJ): %.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f ./tera_sample.o
	rm -f ./sample_demo
	rm -f ./tera_ha_sample.o
	rm -f ./ha_sample_demo
