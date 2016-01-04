set -x -e

rm -rf tmp
mkdir tmp
cp -r example/onebox/* tmp
cp build/bin/teracli tmp/bin
cp build/bin/tera_main tmp/bin
cp build/benchmark/tera_bench tmp/bin
cp build/benchmark/tera_mark tmp/bin

mkdir -p tmp/log
mkdir -p tmp/data
cp -r test/testcase tmp/bin
cp -r test/testcase/shell_script/* tmp/bin
cp src/sdk/python/TeraSdk.py tmp/bin/testcase
cp build/lib/libtera_c.so tmp/bin

cd tmp/bin/
sh kill_tera.sh
sh launch_tera.sh
sleep 2

export PYTHONPATH=$PYTHONPATH:../../thirdparty/include/; export PATH=$PATH:../../thirdparty/bin/
nosetests -s -v -x > ../log/test.log

sh kill_tera.sh
