#!/usr/bin/env sh
cp ../monitor.proto src/tera
protoc --proto_path=src/tera --python_out=src/tera src/tera/monitor.proto 
