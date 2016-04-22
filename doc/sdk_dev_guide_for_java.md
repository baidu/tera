# Tera Java SDK

# 背景

  * Tera Java SDK通过JNI调用native（libjni_tera.so）接口实现。
  * 编译及使用采用maven。

# 体验方法

  1. 搭建tera环境，可以使用[onebox](https://github.com/baidu/tera/blob/master/doc/Onebox.md)简单搭建。
  1. 编译libjni_tera.so，编译teracli/tera_main时一起编译出来，并将其加入**LD_LIBRARY_PATH**环境变量。
  1. 将onebox中的tera.flag地址加入**TERA_CONF_PATH**环境变量。
  1. 进入src/sdk/java目录，运行mvn package即可运行单测（可以参考单测使用sdk接口），产出jar。
  1. 将jar传入mvn库，可被依赖使用。
  
# FAQ
