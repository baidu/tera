#!/bin/bash

killall -9 tera_main
ssh work@st01-spi-session0.st01.baidu.com "killall -9 tera_main"
sleep 1
hadoop fs -rmr /user
