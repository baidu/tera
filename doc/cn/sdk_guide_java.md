# Tera Java SDK

# 背景

  * Tera Java SDK通过JNI（libjni_tera.so）调用native接口实现。
  * 使用Maven进行编译。

# 体验方法

  1. 编译Tera，参考Build[../../build.md]
  1. 部署Tera运行环境，可以参考[onebox](../onebox-cn.md)进行简单搭建。
  1. 将编译出的libjni_tera.so的路径加入环境变量**LD_LIBRARY_PATH**。
  1. 将环境变量**TERA_CONF_PATH**设为tera.flag的路径。
  1. 进入src/sdk/java目录，执行命令**mvn package**运行单测（可以参考单测使用sdk接口），并产出JAR文件。
  1. 将JAR文件安装到Maven库，以便将来依赖使用。
