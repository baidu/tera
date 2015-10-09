set -x

mkdir tmp
cp -r example/onebox/* tmp
cp build/bin/teracli tmp/bin
cp build/bin/tera_main tmp/bin
mkdir -p tmp/log
mkdir -p tmp/data
cp -r test/testcase tmp/bin

cd tmp/bin/
sh launch_tera.sh
export PYTHONPATH=$PYTHONPATH:../../thirdparty/include/; export PATH=$PATH:../../thirdparty/bin/ 
cd testcase
nosetests -s -v > ../../log/test.log
sh kill_tera.sh

