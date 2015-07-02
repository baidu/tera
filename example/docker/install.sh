#!/bin/bash

if [ $# != 1 ]; then
        echo "Error!! usage: ./install docker_images_id"
        exit 0
fi

# python related
sudo apt-get install -y python python-dev python-pip
sudo pip install argparse
sudo pip install paramiko

# docker images preparation
docker pull $1
docker save > ./tera.tar.gz
