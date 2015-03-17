CXX=g++
CXX_FLAGS=-g -fPIC -Wall -Wextra -pipe -Wno-unused
CXX_INC= -I../include
CXX_LINK=-Xlinker "-(" \
  ../lib/libleveldb.a \
  -lpthread \
  -lcrypto \
  -lrt \
  -Xlinker "-)"

OBJS=sample

all: $(OBJS)

sample: sample.cpp
	$(CXX) $^ $(CXX_INC) $(CXX_LINK) $(CXX_FLAGS) -o $@

clean:
	-rm -f *.o *.a $(OBJS)
