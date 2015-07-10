#!/usr/bin/env sh
echo "[INFO] close tera monitor ui"
pid_list=`ps -ef | grep tera_monitor_boot | grep -v grep | awk '{print $2}'`
if [ -n "$pid_list" ];then
	echo "[INFO] close the existing monitor "
	for pid in $pid_list;do
		kill -9 $pid
		echo "[INFO] monitor with pid $pid closed"
	done
	echo "[INFO] close monitor successfully"
else
	echo "[INFO] no monitor is existing"
fi
