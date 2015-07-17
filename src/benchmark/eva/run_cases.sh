#!/bin/bash
set -x

rm ../tmp/mail_report

sh kill_all.sh
sleep 2
:<<BLOCK
sh start_all.sh
sleep 8
python run.py seq_write 4 1024
sh kill_all.sh
sleep 2

sh start_all.shi
sleep 8
python run.py ran_write 4 1024
sh kill_all.sh
sleep 2

sh start_all.sh
sleep 8
python run.py seq_write 4 512
sh kill_all.sh
sleep 2
BLOCK
sh start_all.sh
sleep 8
python run.py ran_write 2 512
rm teramo.INFO*
#sh kill_all.sh
sleep 2

#sh start_all.sh
sleep 
python run.py ran_read 2 512
rm teramo.INFO*
sh kill_all.sh
sleep2

cat ../tmp/mail_report | /usr/sbin/sendmail -oi leiliyuan@baidu.com\
    #,yanshiguang02@baidu.com

