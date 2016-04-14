include ../depends.mk

OUTPUT = $(shell pwd)/build
MYSQL_PATH = mysql-cluster-gpl-7.4.10
TERA_PLUGIN_PATH = $(MYSQL_PATH)/storage/tera

.PHONY: all clean cleanall

all: $(MYSQL_PATH) FORCE
	rm -rf $(TERA_PLUGIN_PATH)
	cp -a src $(TERA_PLUGIN_PATH)
	cp -a ../build/lib/libtera.so $(TERA_PLUGIN_PATH)
	cp -a ../build/include/tera.h $(TERA_PLUGIN_PATH)
	cd $(MYSQL_PATH) && cmake -DCMAKE_INSTALL_PREFIX=$(OUTPUT) \
		-DCMAKE_BUILD_TYPE=Debug -DWITH_NDB_JAVA=OFF \
		-DWITH_NDB_DEBUG=ON -DWITH_TERA_STORAGE_ENGINE=ON
	$(MAKE) -C $(MYSQL_PATH)
	$(MAKE) install -C $(MYSQL_PATH)
	cp -af $(TERA_PLUGIN_PATH)/libtera.so $(OUTPUT)/lib/plugin
	cp -af script/* $(OUTPUT)/bin
	echo 'Done'

clean:
	$(MAKE) clean -C $(MYSQL_PATH)

cleanall:
	$(MAKE) clean
	rm -rf $(OUTPUT)

.PHONY: FORCE
FORCE:

