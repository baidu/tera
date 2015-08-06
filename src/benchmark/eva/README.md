Tera指标体系
=====

本系统的目标在于给出Tera的性能指标，并且为开发者提供简单快速的测试框架。

## 配置及要求：
* 一个搭建好的Tera集群
* pyhton2.7 并在环境变量中添加PYTHONPATH=$HOME/{path}，其中path为指标系统的本目录

## 准备工作
请按以下方式将需要的文件拷贝到相应目录下：

  ```
  |--{path}
    |--bin
      |--teracli
      |--teramo
      |--tera_mark
      |--tera_bench
      |--tera.flag
      |--run.py
      |--eva_var.py
      |--eva_utils.py
      |--__init__.py
    |--conf
      |--tera.flag
    |--log
      |--eva
        |--(empty)
    |--tmp
  ```
