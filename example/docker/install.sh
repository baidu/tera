#!/bin/bash

# python related
sudo apt-get install -y python python-dev python-pip
sudo pip install argparse
sudo pip install paramiko

# docker images preparation
echo 'pulling lylei/tera:latest'
docker pull lylei/tera:latest

echo 'saving lylei/tera:latest to tera.tar'
docker save lylei/tera:latest > ./tera.tar

while [ $# -gt 0 ]; do
	echo $1
	scp tera.tar $1:$HOME
	echo 'loading lylei/tera:latest'
	ssh $1 "cd $HOME && docker load < tera.tar && rm tera.tar"
	shift
done

