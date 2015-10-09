set -x -e

mkdir tmp
cp -r example/onebox/* tmp
cp build/bin/teracli tmp/bin
cp build/bin/tera_main tmp/bin
mkdir -p tmp/log
mkdir -p tmp/data
cp -r test/testcase tmp/bin

cd tmp/bin/
sh kill_tera.sh
sh launch_tera.sh
sleep 3

export PYTHONPATH=$PYTHONPATH:../../thirdparty/include/; export PATH=$PATH:../../thirdparty/bin/ 
#nosetests -s -v -x > ../log/test.log

#sh kill_tera.sh

