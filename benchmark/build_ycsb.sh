#!/bin/bash

if [[ ! -e ycsb-0.12.0.tar.gz ]]; then
    wget --no-check-certificate https://github.com/brianfrankcooper/YCSB/archive/0.12.0.tar.gz -O ycsb-0.12.0.tar.gz
fi

rm -rf YCSB-0.12.0
tar zxvf ycsb-0.12.0.tar.gz

cp ycsb4tera/tera/ YCSB-0.12.0/ -fr
sed -i '/tarantool/a"tera":"com.yahoo.ycsb.TeraDB",' YCSB-0.12.0/bin/ycsb
sed -i '/<module>tarantool<\/module>/a<module>tera</module>' YCSB-0.12.0/pom.xml
sed -i '/tarantool.version/a<tera.version>0.6.1</tera.version>' YCSB-0.12.0/pom.xml
cd YCSB-0.12.0 && mvn -pl com.yahoo.ycsb:tera-binding -am clean package
