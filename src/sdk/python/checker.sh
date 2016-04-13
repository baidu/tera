#!/bin/bash

# 通过Python调用libtera_c.so导出的函数时，需要明确指定每个函数的参数类型，
# 否则可能出现参数类型不兼容的问题，导致程序错误或core在libtera_c.so里，
# Python无法自动判断调用C函数时该传递什么类型参数，所以需要程序员自己指定或说明。
# 如果程序员忘记指定参数类型，Python也不会报错，所以只能通过一个外部脚本去检测，
# 保证每个导出的C函数都在Python代码里明确指定了参数类型。

# 例如：
# tera_c.h导出 
#   tera_client_t* tera_client_open(const char* conf_path, const char* log_prefix, char** err);
#
# Python代码里就应该对应
#   lib.tera_client_open.argtypes = [c_char_p, c_char_p, POINTER(c_char_p)]
#   lib.tera_client_open.restype = c_void_p

grep "tera_.*(" src/sdk/tera_c.h | awk '{print $2}' | awk -F'(' '{print $1}' | while read afunc
do 
    if grep -q "lib.$afunc.argtypes " src/sdk/python/TeraSdk.py && grep -q "lib.$afunc.restype " src/sdk/python/TeraSdk.py
    then
        continue
    fi
    echo "maybe forget to specify the required argument/return type(s) for $afunc"
    exit 1
done
