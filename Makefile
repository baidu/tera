

TS_SRC=$(wildcard tera/tabletnode/*.cc)
IO_SRC=$(wildcard tera/io/*.cc)
MASTER_SRC=$(wildcard tera/master/*.cc)
PROTO_SRC=$(wildcard tera/proto/*.cc)
SDK_SRC=$(wildcard tera/sdk/*.cc)
OTHER_SRC=$(wildcard tera/*.cc) $(wildcard tera/zk/*.cc) $(wildcard tera/utils/*.cc)

PROTO_FILES=$(wildcard tera/proto/*.proto)

INCPATH=-I./third_party/protobuf/include -I./third_party/sofa-pbrpc/include

LDFLAG+= -L./third_party/protobuf/lib -lprotobuf \
         -L./third_party/sofa-pbrpc/lib -lsofa_pbrpc

PROTOC=$(PROTOC_PATH)protoc

all: tera_main

proto: $(PROTO_FILES)
	$(PROTOC) --proto_path=./tera/proto/ --proto_path=/usr/local/include \
		  --proto_path=./third_party/sofa-pbrpc/output/include/ \
		  --cpp_out=./tera/proto/ $(PROTO_FILES)

tera_main: $(SRC)
	echo 'Done'

test:
	echo "No test now!"
