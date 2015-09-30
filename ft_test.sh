set -x

mkdir tmp
cp -r example/onebox/* tmp
cp build/bin/teracli tmp/bin
cp build/bin/tera_main tmp/bin
mkdir -p tmp/log
mkdir -p tmp/data
cp test/*test*.py tmp/bin

cd tmp/bin/
sh launch_tera.sh
export PYTHONPATH=$PYTHONPATH:../../thirdparty/include/; ../../thirdparty/bin/nosetests -s -v test_data.py > ../log/test.log
sh kill_tera.sh
cd -

