#!/bin/bash

cd /home/work/dev/tera/bin && \
    nohup ./tera_main --flagfile=../conf/tera.flag --tera_role=tabletnode &> ../log/tabletserver.stderr &

sleep 3

ssh work@st01-spi-session0.st01.baidu.com "cd /home/work/tera/bin && source ~/.bash_profile && sh start.sh"
