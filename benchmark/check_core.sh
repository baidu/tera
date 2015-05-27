#!/bin/bash

source client_conf.sh

for((i=1; i<=FS_NUM; i++)); do
	#echo "#############   ${FS_HOST[$i]}    #################"
	ssh ${FS_HOST[$i]} "cd ~/writer; ls core.* &>/dev/null; if [ \$? -eq 0 ]; then echo \"${FS_HOST[$i]} find core\"; fi" &
done

wait
